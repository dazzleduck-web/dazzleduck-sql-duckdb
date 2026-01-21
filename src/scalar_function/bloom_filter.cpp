#include "scalar_function/bloom_filter.hpp"

#include "duckdb/common/types/hash.hpp"
#include "duckdb/common/types/vector.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/main/extension/extension_loader.hpp"

namespace duckdb {
namespace ext_nanoarrow {

namespace {

//! Default bloom filter parameters
static constexpr int32_t DEFAULT_BITS_PER_ELEMENT = 10;  // ~1% false positive rate
static constexpr int32_t DEFAULT_NUM_HASH_FUNCTIONS = 3;
static constexpr int32_t MIN_BITS = 64;
static constexpr int32_t MAX_BITS = 1024 * 1024 * 8;  // 1MB max

//! Bloom filter header stored at the beginning of the BLOB
//! This allows dd_bloom_filter_contains to know the parameters
struct BloomFilterHeader {
	uint32_t magic;           // Magic number for validation
	uint32_t num_bits;        // Number of bits in the filter
	uint32_t num_hash_funcs;  // Number of hash functions
	uint32_t reserved;        // Reserved for future use
};

static constexpr uint32_t BLOOM_FILTER_MAGIC = 0x424C4F4D;  // "BLOM"

//! Compute hash for bloom filter insertion/lookup
static inline void ComputeHashes(const string_t& value, uint32_t num_hash_funcs, uint32_t num_bits,
                                  vector<uint32_t>& bit_positions) {
	hash_t base_hash = Hash(value.GetData(), value.GetSize());
	bit_positions.clear();
	for (uint32_t i = 0; i < num_hash_funcs; i++) {
		hash_t h = base_hash + i * (base_hash >> 16) + i * i;
		bit_positions.push_back(static_cast<uint32_t>(h % num_bits));
	}
}

//! Set a bit in the bloom filter data
static inline void SetBit(uint8_t* data, uint32_t bit_pos) {
	data[bit_pos / 8] |= (1 << (bit_pos % 8));
}

//! Check if a bit is set in the bloom filter data
static inline bool GetBit(const uint8_t* data, uint32_t bit_pos) {
	return (data[bit_pos / 8] & (1 << (bit_pos % 8))) != 0;
}

//! dd_bloom_filter_create(array VARCHAR[], [bits_per_element INTEGER], [num_hash_functions INTEGER]) -> BLOB
//! Creates a bloom filter from an array of strings
static void BloomFilterCreateFunction(DataChunk& args, ExpressionState& state, Vector& result) {
	auto count = args.size();
	auto& array_vec = args.data[0];

	// Get optional parameters
	int32_t bits_per_element = DEFAULT_BITS_PER_ELEMENT;
	int32_t num_hash_funcs = DEFAULT_NUM_HASH_FUNCTIONS;

	if (args.ColumnCount() >= 2) {
		auto& bits_vec = args.data[1];
		if (bits_vec.GetVectorType() == VectorType::CONSTANT_VECTOR && !ConstantVector::IsNull(bits_vec)) {
			bits_per_element = *ConstantVector::GetData<int32_t>(bits_vec);
			if (bits_per_element < 1) bits_per_element = 1;
			if (bits_per_element > 64) bits_per_element = 64;
		}
	}

	if (args.ColumnCount() >= 3) {
		auto& hash_vec = args.data[2];
		if (hash_vec.GetVectorType() == VectorType::CONSTANT_VECTOR && !ConstantVector::IsNull(hash_vec)) {
			num_hash_funcs = *ConstantVector::GetData<int32_t>(hash_vec);
			if (num_hash_funcs < 1) num_hash_funcs = 1;
			if (num_hash_funcs > 16) num_hash_funcs = 16;
		}
	}

	result.SetVectorType(VectorType::FLAT_VECTOR);
	auto& result_validity = FlatVector::Validity(result);

	UnifiedVectorFormat array_format;
	array_vec.ToUnifiedFormat(count, array_format);
	auto array_list_entries = UnifiedVectorFormat::GetData<list_entry_t>(array_format);

	auto& array_child = ListVector::GetEntry(array_vec);
	auto array_child_size = ListVector::GetListSize(array_vec);

	UnifiedVectorFormat array_child_format;
	array_child.ToUnifiedFormat(array_child_size, array_child_format);
	auto array_child_data = UnifiedVectorFormat::GetData<string_t>(array_child_format);

	vector<uint32_t> bit_positions;

	for (idx_t i = 0; i < count; i++) {
		auto array_idx = array_format.sel->get_index(i);

		if (!array_format.validity.RowIsValid(array_idx)) {
			result_validity.SetInvalid(i);
			continue;
		}

		auto& array_entry = array_list_entries[array_idx];

		// Calculate bloom filter size
		uint32_t num_elements = array_entry.length > 0 ? array_entry.length : 1;
		uint32_t num_bits = num_elements * bits_per_element;
		if (num_bits < MIN_BITS) num_bits = MIN_BITS;
		if (num_bits > MAX_BITS) num_bits = MAX_BITS;

		// Round up to nearest byte
		uint32_t num_bytes = (num_bits + 7) / 8;
		num_bits = num_bytes * 8;  // Actual number of bits

		// Total size: header + filter data
		uint32_t total_size = sizeof(BloomFilterHeader) + num_bytes;

		// Allocate the blob
		auto blob_data = StringVector::EmptyString(result, total_size);
		auto blob_ptr = blob_data.GetDataWriteable();

		// Write header
		auto* header = reinterpret_cast<BloomFilterHeader*>(blob_ptr);
		header->magic = BLOOM_FILTER_MAGIC;
		header->num_bits = num_bits;
		header->num_hash_funcs = num_hash_funcs;
		header->reserved = 0;

		// Get pointer to filter data (after header)
		uint8_t* filter_data = reinterpret_cast<uint8_t*>(blob_ptr + sizeof(BloomFilterHeader));

		// Initialize filter to zeros
		memset(filter_data, 0, num_bytes);

		// Insert all elements into the bloom filter
		for (idx_t j = 0; j < array_entry.length; j++) {
			idx_t child_idx = array_child_format.sel->get_index(array_entry.offset + j);
			if (!array_child_format.validity.RowIsValid(child_idx)) {
				continue;
			}

			const string_t& value = array_child_data[child_idx];
			ComputeHashes(value, num_hash_funcs, num_bits, bit_positions);

			for (uint32_t bit_pos : bit_positions) {
				SetBit(filter_data, bit_pos);
			}
		}

		blob_data.Finalize();
		FlatVector::GetData<string_t>(result)[i] = blob_data;
	}
}

//! dd_bloom_filter_contains(bloom_filter BLOB, value VARCHAR) -> BOOLEAN
//! Checks if a value may be contained in the bloom filter
static void BloomFilterContainsFunction(DataChunk& args, ExpressionState& state, Vector& result) {
	auto count = args.size();
	auto& filter_vec = args.data[0];
	auto& value_vec = args.data[1];

	result.SetVectorType(VectorType::FLAT_VECTOR);
	auto result_data = FlatVector::GetData<bool>(result);
	auto& result_validity = FlatVector::Validity(result);

	UnifiedVectorFormat filter_format;
	UnifiedVectorFormat value_format;
	filter_vec.ToUnifiedFormat(count, filter_format);
	value_vec.ToUnifiedFormat(count, value_format);

	auto filter_data = UnifiedVectorFormat::GetData<string_t>(filter_format);
	auto value_data = UnifiedVectorFormat::GetData<string_t>(value_format);

	vector<uint32_t> bit_positions;

	for (idx_t i = 0; i < count; i++) {
		auto filter_idx = filter_format.sel->get_index(i);
		auto value_idx = value_format.sel->get_index(i);

		if (!filter_format.validity.RowIsValid(filter_idx) ||
		    !value_format.validity.RowIsValid(value_idx)) {
			result_validity.SetInvalid(i);
			continue;
		}

		const string_t& filter_blob = filter_data[filter_idx];
		const string_t& value = value_data[value_idx];

		// Validate bloom filter
		if (filter_blob.GetSize() < sizeof(BloomFilterHeader)) {
			result_validity.SetInvalid(i);
			continue;
		}

		const auto* header = reinterpret_cast<const BloomFilterHeader*>(filter_blob.GetData());
		if (header->magic != BLOOM_FILTER_MAGIC) {
			result_validity.SetInvalid(i);
			continue;
		}

		uint32_t expected_size = sizeof(BloomFilterHeader) + (header->num_bits + 7) / 8;
		if (filter_blob.GetSize() < expected_size) {
			result_validity.SetInvalid(i);
			continue;
		}

		const uint8_t* bf_data = reinterpret_cast<const uint8_t*>(filter_blob.GetData() + sizeof(BloomFilterHeader));

		// Compute hashes and check all bits
		ComputeHashes(value, header->num_hash_funcs, header->num_bits, bit_positions);

		bool may_contain = true;
		for (uint32_t bit_pos : bit_positions) {
			if (!GetBit(bf_data, bit_pos)) {
				may_contain = false;
				break;
			}
		}

		result_data[i] = may_contain;
	}
}

//! dd_bloom_filter_contains_all(bloom_filter BLOB, values VARCHAR[]) -> BOOLEAN
//! Checks if all values in the array may be contained in the bloom filter
static void BloomFilterContainsAllFunction(DataChunk& args, ExpressionState& state, Vector& result) {
	auto count = args.size();
	auto& filter_vec = args.data[0];
	auto& values_vec = args.data[1];

	result.SetVectorType(VectorType::FLAT_VECTOR);
	auto result_data = FlatVector::GetData<bool>(result);
	auto& result_validity = FlatVector::Validity(result);

	UnifiedVectorFormat filter_format;
	UnifiedVectorFormat values_format;
	filter_vec.ToUnifiedFormat(count, filter_format);
	values_vec.ToUnifiedFormat(count, values_format);

	auto filter_data = UnifiedVectorFormat::GetData<string_t>(filter_format);
	auto values_list_entries = UnifiedVectorFormat::GetData<list_entry_t>(values_format);

	auto& values_child = ListVector::GetEntry(values_vec);
	auto values_child_size = ListVector::GetListSize(values_vec);

	UnifiedVectorFormat values_child_format;
	values_child.ToUnifiedFormat(values_child_size, values_child_format);
	auto values_child_data = UnifiedVectorFormat::GetData<string_t>(values_child_format);

	vector<uint32_t> bit_positions;

	for (idx_t i = 0; i < count; i++) {
		auto filter_idx = filter_format.sel->get_index(i);
		auto values_idx = values_format.sel->get_index(i);

		if (!filter_format.validity.RowIsValid(filter_idx) ||
		    !values_format.validity.RowIsValid(values_idx)) {
			result_validity.SetInvalid(i);
			continue;
		}

		const string_t& filter_blob = filter_data[filter_idx];
		auto& values_entry = values_list_entries[values_idx];

		// Empty array - return true
		if (values_entry.length == 0) {
			result_data[i] = true;
			continue;
		}

		// Validate bloom filter
		if (filter_blob.GetSize() < sizeof(BloomFilterHeader)) {
			result_validity.SetInvalid(i);
			continue;
		}

		const auto* header = reinterpret_cast<const BloomFilterHeader*>(filter_blob.GetData());
		if (header->magic != BLOOM_FILTER_MAGIC) {
			result_validity.SetInvalid(i);
			continue;
		}

		uint32_t expected_size = sizeof(BloomFilterHeader) + (header->num_bits + 7) / 8;
		if (filter_blob.GetSize() < expected_size) {
			result_validity.SetInvalid(i);
			continue;
		}

		const uint8_t* bf_data = reinterpret_cast<const uint8_t*>(filter_blob.GetData() + sizeof(BloomFilterHeader));

		// Check all values
		bool all_may_contain = true;
		for (idx_t j = 0; j < values_entry.length && all_may_contain; j++) {
			idx_t child_idx = values_child_format.sel->get_index(values_entry.offset + j);
			if (!values_child_format.validity.RowIsValid(child_idx)) {
				continue;  // Skip NULL values
			}

			const string_t& value = values_child_data[child_idx];
			ComputeHashes(value, header->num_hash_funcs, header->num_bits, bit_positions);

			for (uint32_t bit_pos : bit_positions) {
				if (!GetBit(bf_data, bit_pos)) {
					all_may_contain = false;
					break;
				}
			}
		}

		result_data[i] = all_may_contain;
	}
}

}  // namespace

void RegisterBloomFilterFunctions(ExtensionLoader& loader) {
	// dd_bloom_filter_create(array VARCHAR[]) -> BLOB
	// dd_bloom_filter_create(array VARCHAR[], bits_per_element INTEGER) -> BLOB
	// dd_bloom_filter_create(array VARCHAR[], bits_per_element INTEGER, num_hash_functions INTEGER) -> BLOB
	// Creates a bloom filter from an array of strings
	// Parameters:
	//   - array: Array of strings to add to the filter
	//   - bits_per_element: Bits per element (default 10, ~1% false positive rate)
	//   - num_hash_functions: Number of hash functions (default 3)

	auto create_func1 = ScalarFunction(
		"dd_bloom_filter_create",
		{LogicalType::LIST(LogicalType::VARCHAR)},
		LogicalType::BLOB,
		BloomFilterCreateFunction
	);
	create_func1.null_handling = FunctionNullHandling::SPECIAL_HANDLING;
	loader.RegisterFunction(create_func1);

	auto create_func2 = ScalarFunction(
		"dd_bloom_filter_create",
		{LogicalType::LIST(LogicalType::VARCHAR), LogicalType::INTEGER},
		LogicalType::BLOB,
		BloomFilterCreateFunction
	);
	create_func2.null_handling = FunctionNullHandling::SPECIAL_HANDLING;
	loader.RegisterFunction(create_func2);

	auto create_func3 = ScalarFunction(
		"dd_bloom_filter_create",
		{LogicalType::LIST(LogicalType::VARCHAR), LogicalType::INTEGER, LogicalType::INTEGER},
		LogicalType::BLOB,
		BloomFilterCreateFunction
	);
	create_func3.null_handling = FunctionNullHandling::SPECIAL_HANDLING;
	loader.RegisterFunction(create_func3);

	// dd_bloom_filter_contains(bloom_filter BLOB, value VARCHAR) -> BOOLEAN
	// Checks if a value may be contained in the bloom filter
	// Returns true if the value MAY be in the filter (possible false positive)
	// Returns false if the value is DEFINITELY NOT in the filter

	auto contains_func = ScalarFunction(
		"dd_bloom_filter_contains",
		{LogicalType::BLOB, LogicalType::VARCHAR},
		LogicalType::BOOLEAN,
		BloomFilterContainsFunction
	);
	contains_func.null_handling = FunctionNullHandling::SPECIAL_HANDLING;
	loader.RegisterFunction(contains_func);

	// dd_bloom_filter_contains_all(bloom_filter BLOB, values VARCHAR[]) -> BOOLEAN
	// Checks if all values in the array may be contained in the bloom filter
	// Returns true if ALL values MAY be in the filter
	// Returns false if ANY value is DEFINITELY NOT in the filter

	auto contains_all_func = ScalarFunction(
		"dd_bloom_filter_contains_all",
		{LogicalType::BLOB, LogicalType::LIST(LogicalType::VARCHAR)},
		LogicalType::BOOLEAN,
		BloomFilterContainsAllFunction
	);
	contains_all_func.null_handling = FunctionNullHandling::SPECIAL_HANDLING;
	loader.RegisterFunction(contains_all_func);
}

}  // namespace ext_nanoarrow
}  // namespace duckdb
