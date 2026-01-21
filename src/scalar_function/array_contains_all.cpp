#include "scalar_function/array_contains_all.hpp"

#include "duckdb/common/operator/comparison_operators.hpp"
#include "duckdb/common/types/vector.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/main/extension/extension_loader.hpp"

namespace duckdb {
namespace ext_nanoarrow {

namespace {

//! Row-based implementation: for each row, check all needle elements against haystack
static void ArrayContainsAllRowBased(DataChunk& args, ExpressionState& state, Vector& result) {
	auto count = args.size();
	auto& haystack_vec = args.data[0];
	auto& needle_vec = args.data[1];

	result.SetVectorType(VectorType::FLAT_VECTOR);
	auto result_data = FlatVector::GetData<bool>(result);
	auto& result_validity = FlatVector::Validity(result);

	UnifiedVectorFormat haystack_format;
	UnifiedVectorFormat needle_format;
	haystack_vec.ToUnifiedFormat(count, haystack_format);
	needle_vec.ToUnifiedFormat(count, needle_format);

	auto haystack_list_entries = UnifiedVectorFormat::GetData<list_entry_t>(haystack_format);
	auto needle_list_entries = UnifiedVectorFormat::GetData<list_entry_t>(needle_format);

	auto& haystack_child = ListVector::GetEntry(haystack_vec);
	auto& needle_child = ListVector::GetEntry(needle_vec);

	auto haystack_child_size = ListVector::GetListSize(haystack_vec);
	auto needle_child_size = ListVector::GetListSize(needle_vec);

	UnifiedVectorFormat haystack_child_format;
	UnifiedVectorFormat needle_child_format;
	haystack_child.ToUnifiedFormat(haystack_child_size, haystack_child_format);
	needle_child.ToUnifiedFormat(needle_child_size, needle_child_format);

	auto haystack_child_data = UnifiedVectorFormat::GetData<string_t>(haystack_child_format);
	auto needle_child_data = UnifiedVectorFormat::GetData<string_t>(needle_child_format);

	for (idx_t i = 0; i < count; i++) {
		auto haystack_idx = haystack_format.sel->get_index(i);
		auto needle_idx = needle_format.sel->get_index(i);

		if (!haystack_format.validity.RowIsValid(haystack_idx) ||
		    !needle_format.validity.RowIsValid(needle_idx)) {
			result_validity.SetInvalid(i);
			continue;
		}

		auto& haystack_entry = haystack_list_entries[haystack_idx];
		auto& needle_entry = needle_list_entries[needle_idx];

		if (needle_entry.length == 0) {
			result_data[i] = true;
			continue;
		}

		if (haystack_entry.length == 0) {
			result_data[i] = false;
			continue;
		}

		bool all_found = true;
		for (idx_t j = 0; j < needle_entry.length; j++) {
			idx_t needle_child_idx = needle_child_format.sel->get_index(needle_entry.offset + j);

			if (!needle_child_format.validity.RowIsValid(needle_child_idx)) {
				continue;
			}

			bool found = false;
			for (idx_t k = 0; k < haystack_entry.length; k++) {
				idx_t haystack_child_idx = haystack_child_format.sel->get_index(haystack_entry.offset + k);
				if (!haystack_child_format.validity.RowIsValid(haystack_child_idx)) {
					continue;
				}
				if (Equals::Operation(haystack_child_data[haystack_child_idx],
				                      needle_child_data[needle_child_idx])) {
					found = true;
					break;
				}
			}

			if (!found) {
				all_found = false;
				break;
			}
		}

		result_data[i] = all_found;
	}
}

//! Columnar implementation: process one needle element at a time across all rows
//! Optimized with selection vector to skip already-failed rows
//! Uses stack allocation for small batches to avoid heap overhead
static void ArrayContainsAllColumnar(DataChunk& args, ExpressionState& state, Vector& result) {
	auto count = args.size();
	auto& haystack_vec = args.data[0];
	auto& needle_vec = args.data[1];

	result.SetVectorType(VectorType::FLAT_VECTOR);
	auto result_data = FlatVector::GetData<bool>(result);
	auto& result_validity = FlatVector::Validity(result);

	UnifiedVectorFormat haystack_format;
	UnifiedVectorFormat needle_format;
	haystack_vec.ToUnifiedFormat(count, haystack_format);
	needle_vec.ToUnifiedFormat(count, needle_format);

	auto haystack_list_entries = UnifiedVectorFormat::GetData<list_entry_t>(haystack_format);
	auto needle_list_entries = UnifiedVectorFormat::GetData<list_entry_t>(needle_format);

	auto& haystack_child = ListVector::GetEntry(haystack_vec);
	auto& needle_child = ListVector::GetEntry(needle_vec);

	auto haystack_child_size = ListVector::GetListSize(haystack_vec);
	auto needle_child_size = ListVector::GetListSize(needle_vec);

	UnifiedVectorFormat haystack_child_format;
	UnifiedVectorFormat needle_child_format;
	haystack_child.ToUnifiedFormat(haystack_child_size, haystack_child_format);
	needle_child.ToUnifiedFormat(needle_child_size, needle_child_format);

	auto haystack_child_data = UnifiedVectorFormat::GetData<string_t>(haystack_child_format);
	auto needle_child_data = UnifiedVectorFormat::GetData<string_t>(needle_child_format);

	// Pre-compute and cache indices for each row
	struct RowInfo {
		idx_t haystack_offset;
		idx_t haystack_length;
		idx_t needle_offset;
		idx_t needle_length;
	};

	// Stack allocation for small batches (STANDARD_VECTOR_SIZE is typically 2048)
	static constexpr idx_t STACK_THRESHOLD = STANDARD_VECTOR_SIZE;

	// Stack buffers
	RowInfo stack_row_info[STACK_THRESHOLD];
	idx_t stack_active_rows[STACK_THRESHOLD];

	// Use stack or heap based on count
	RowInfo* row_info = count <= STACK_THRESHOLD ? stack_row_info : new RowInfo[count];
	idx_t* active_rows = count <= STACK_THRESHOLD ? stack_active_rows : new idx_t[count];
	idx_t active_count = 0;

	idx_t max_needle_length = 0;

	// Initialize results and build active row list
	for (idx_t i = 0; i < count; i++) {
		auto haystack_idx = haystack_format.sel->get_index(i);
		auto needle_idx = needle_format.sel->get_index(i);

		// Handle NULL arrays
		if (!haystack_format.validity.RowIsValid(haystack_idx) ||
		    !needle_format.validity.RowIsValid(needle_idx)) {
			result_validity.SetInvalid(i);
			result_data[i] = false;
			continue;
		}

		auto& needle_entry = needle_list_entries[needle_idx];
		auto& haystack_entry = haystack_list_entries[haystack_idx];

		if (needle_entry.length == 0) {
			result_data[i] = true;
		} else if (haystack_entry.length == 0) {
			result_data[i] = false;
		} else {
			row_info[i].haystack_offset = haystack_entry.offset;
			row_info[i].haystack_length = haystack_entry.length;
			row_info[i].needle_offset = needle_entry.offset;
			row_info[i].needle_length = needle_entry.length;

			result_data[i] = true;
			active_rows[active_count++] = i;

			if (needle_entry.length > max_needle_length) {
				max_needle_length = needle_entry.length;
			}
		}
	}

	// Process one needle position at a time, only for active rows
	for (idx_t needle_pos = 0; needle_pos < max_needle_length && active_count > 0; needle_pos++) {
		idx_t write_idx = 0;

		for (idx_t read_idx = 0; read_idx < active_count; read_idx++) {
			idx_t i = active_rows[read_idx];
			auto& info = row_info[i];

			if (needle_pos >= info.needle_length) {
				active_rows[write_idx++] = i;
				continue;
			}

			idx_t needle_child_idx = needle_child_format.sel->get_index(info.needle_offset + needle_pos);

			if (!needle_child_format.validity.RowIsValid(needle_child_idx)) {
				active_rows[write_idx++] = i;
				continue;
			}

			// Search for this needle element in haystack
			const string_t& needle_str = needle_child_data[needle_child_idx];
			bool found = false;

			for (idx_t k = 0; k < info.haystack_length; k++) {
				idx_t haystack_child_idx = haystack_child_format.sel->get_index(info.haystack_offset + k);
				if (!haystack_child_format.validity.RowIsValid(haystack_child_idx)) {
					continue;
				}
				if (Equals::Operation(haystack_child_data[haystack_child_idx], needle_str)) {
					found = true;
					break;
				}
			}

			if (found) {
				active_rows[write_idx++] = i;
			} else {
				result_data[i] = false;
			}
		}

		active_count = write_idx;
	}

	// Clean up heap allocations if used
	if (count > STACK_THRESHOLD) {
		delete[] row_info;
		delete[] active_rows;
	}
}

//! Main entry point - dispatches to appropriate implementation based on optional flag parameter
//! Default is columnar processing (true)
static void ArrayContainsAllFunction(DataChunk& args, ExpressionState& state, Vector& result) {
	bool use_columnar = true;  // Default to columnar processing

	// Check if third argument (flag) is provided
	if (args.ColumnCount() >= 3) {
		auto& flag_vec = args.data[2];
		if (flag_vec.GetVectorType() == VectorType::CONSTANT_VECTOR) {
			if (!ConstantVector::IsNull(flag_vec)) {
				use_columnar = *ConstantVector::GetData<bool>(flag_vec);
			}
		} else {
			// For non-constant, use first value
			UnifiedVectorFormat flag_format;
			flag_vec.ToUnifiedFormat(args.size(), flag_format);
			auto flag_data = UnifiedVectorFormat::GetData<bool>(flag_format);
			auto idx = flag_format.sel->get_index(0);
			if (flag_format.validity.RowIsValid(idx)) {
				use_columnar = flag_data[idx];
			}
		}
	}

	if (use_columnar) {
		ArrayContainsAllColumnar(args, state, result);
	} else {
		ArrayContainsAllRowBased(args, state, result);
	}
}

}  // namespace

void RegisterArrayContainsAll(ExtensionLoader& loader) {
	// array_contains_all(haystack_array, needle_array, [use_columnar]) -> boolean
	// Returns true if all elements in needle_array exist in haystack_array
	// Only supports VARCHAR arrays
	// Optional third parameter: use_columnar (default true)
	//   - true: process one needle at a time across all rows (columnar/vectorized style)
	//   - false: process all needles for one row at a time (row-based style)

	// 2-argument version (default columnar processing)
	auto func2 = ScalarFunction(
		"array_contains_all",
		{LogicalType::LIST(LogicalType::VARCHAR), LogicalType::LIST(LogicalType::VARCHAR)},
		LogicalType::BOOLEAN,
		ArrayContainsAllFunction
	);
	func2.null_handling = FunctionNullHandling::SPECIAL_HANDLING;
	loader.RegisterFunction(func2);

	// 3-argument version with explicit flag
	auto func3 = ScalarFunction(
		"array_contains_all",
		{LogicalType::LIST(LogicalType::VARCHAR), LogicalType::LIST(LogicalType::VARCHAR), LogicalType::BOOLEAN},
		LogicalType::BOOLEAN,
		ArrayContainsAllFunction
	);
	func3.null_handling = FunctionNullHandling::SPECIAL_HANDLING;
	loader.RegisterFunction(func3);
}

}  // namespace ext_nanoarrow
}  // namespace duckdb
