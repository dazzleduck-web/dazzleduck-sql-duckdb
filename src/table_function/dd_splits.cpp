#include "table_function/dd_splits.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "http/split_info.hpp"

namespace duckdb {
namespace ext_nanoarrow {

namespace {

//! Bind data for dd_splits function
struct DDSplitsBindData : public TableFunctionData {
  string url;
  string query;
  string auth_token;
  int64_t split_size = -1;
};

//! Global state for dd_splits function
struct DDSplitsGlobalState : public GlobalTableFunctionState {
  vector<SplitInfo> splits;
  idx_t current_idx = 0;
  bool done = false;

  idx_t MaxThreads() const override {
    return 1;
  }
};

//! Bind function for dd_splits
static unique_ptr<FunctionData> DDSplitsBind(ClientContext& context, TableFunctionBindInput& input,
                                             vector<LogicalType>& return_types, vector<string>& names) {
  // Extract required parameter: url
  auto url = input.inputs[0].GetValue<string>();

  // Extract optional named parameters
  string source_table;
  string sql_query;
  string auth_token;
  int64_t split_size = -1;

  for (auto& kv : input.named_parameters) {
    if (kv.first == "source_table") {
      source_table = kv.second.GetValue<string>();
    } else if (kv.first == "sql") {
      sql_query = kv.second.GetValue<string>();
    } else if (kv.first == "auth_token") {
      auth_token = kv.second.GetValue<string>();
    } else if (kv.first == "split_size") {
      split_size = kv.second.GetValue<int64_t>();
    }
  }

  // Build query from source_table or sql parameter
  string query;
  if (!source_table.empty()) {
    query = "SELECT * FROM " + source_table;
  } else if (!sql_query.empty()) {
    query = sql_query;
  } else {
    throw InvalidInputException("dd_splits requires either 'source_table' or 'sql' parameter");
  }

  // Create bind data
  auto result = make_uniq<DDSplitsBindData>();
  result->url = url;
  result->query = query;
  result->auth_token = auth_token;
  result->split_size = split_size;

  // Define output columns
  return_types.emplace_back(LogicalType::VARCHAR);  // split_id
  names.emplace_back("split_id");

  return_types.emplace_back(LogicalType::BIGINT);  // query_id
  names.emplace_back("query_id");

  return_types.emplace_back(LogicalType::VARCHAR);  // query
  names.emplace_back("query");

  return_types.emplace_back(LogicalType::VARCHAR);  // producer_id
  names.emplace_back("producer_id");

  return_types.emplace_back(LogicalType::BIGINT);  // split_size
  names.emplace_back("split_size");

  return std::move(result);
}

//! Init global state for dd_splits
static unique_ptr<GlobalTableFunctionState> DDSplitsInitGlobal(ClientContext& context,
                                                               TableFunctionInitInput& input) {
  auto& bind_data = input.bind_data->Cast<DDSplitsBindData>();

  auto result = make_uniq<DDSplitsGlobalState>();

  // Fetch splits from plan API
  result->splits = FetchPlanSplits(context, bind_data.url, bind_data.query, bind_data.auth_token, bind_data.split_size);

  return std::move(result);
}

//! Scan function for dd_splits
static void DDSplitsScan(ClientContext& context, TableFunctionInput& data, DataChunk& output) {
  auto& global_state = data.global_state->Cast<DDSplitsGlobalState>();

  if (global_state.done) {
    return;
  }

  idx_t count = 0;
  idx_t max_count = STANDARD_VECTOR_SIZE;

  while (global_state.current_idx < global_state.splits.size() && count < max_count) {
    auto& split = global_state.splits[global_state.current_idx];

    // split_id
    output.SetValue(0, count, Value(split.id));

    // query_id
    output.SetValue(1, count, Value::BIGINT(split.query_id));

    // query
    output.SetValue(2, count, Value(split.query));

    // producer_id
    output.SetValue(3, count, Value(split.producer_id));

    // split_size
    output.SetValue(4, count, Value::BIGINT(split.split_size));

    global_state.current_idx++;
    count++;
  }

  output.SetCardinality(count);

  if (global_state.current_idx >= global_state.splits.size()) {
    global_state.done = true;
  }
}

}  // namespace

void RegisterDDSplits(ExtensionLoader& loader) {
  TableFunction func("dd_splits", {LogicalType::VARCHAR}, DDSplitsScan, DDSplitsBind, DDSplitsInitGlobal);

  func.named_parameters["source_table"] = LogicalType::VARCHAR;
  func.named_parameters["sql"] = LogicalType::VARCHAR;
  func.named_parameters["auth_token"] = LogicalType::VARCHAR;
  func.named_parameters["split_size"] = LogicalType::BIGINT;

  loader.RegisterFunction(func);
}

}  // namespace ext_nanoarrow
}  // namespace duckdb
