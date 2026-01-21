#include "scalar_function/dd_search.hpp"

#include "duckdb/function/scalar_function.hpp"
#include "duckdb/main/extension/extension_loader.hpp"

namespace duckdb {
namespace ext_nanoarrow {

namespace {

//! dd_search function - takes a column and a list of literals, always returns true
//! This function is intended for filter pushdown where the search can be sent to the server
static void DDSearchFunction(DataChunk& args, ExpressionState& state, Vector& result) {
	// Always return true for all rows
	auto count = args.size();
	result.SetVectorType(VectorType::FLAT_VECTOR);
	auto result_data = FlatVector::GetData<bool>(result);
	for (idx_t i = 0; i < count; i++) {
		result_data[i] = true;
	}
}

}  // namespace

void RegisterDDSearch(ExtensionLoader& loader) {
	// dd_search(column, list_of_values) -> always returns true
	// First argument: any type (the column to search in)
	// Second argument: list of any type (the values to search for)
	auto func = ScalarFunction(
		"dd_search",
		{LogicalType::ANY, LogicalType::LIST(LogicalType::ANY)},
		LogicalType::BOOLEAN,
		DDSearchFunction
	);

	// Handle NULLs specially - always return true even for NULL inputs
	func.null_handling = FunctionNullHandling::SPECIAL_HANDLING;

	loader.RegisterFunction(func);
}

}  // namespace ext_nanoarrow
}  // namespace duckdb
