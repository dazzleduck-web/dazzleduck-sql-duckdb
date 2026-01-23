#include "table_function/read_arrow_dd.hpp"

#include "http/arrow_http_client.hpp"
#include "http/cancel_monitor.hpp"
#include "http/split_info.hpp"
#include "ipc/http_stream_factory.hpp"
#include "table_function/arrow_ipc_function_data.hpp"

#include "duckdb/function/table/arrow.hpp"
#include "duckdb/function/table/arrow/arrow_duck_schema.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/planner/filter/conjunction_filter.hpp"
#include "duckdb/planner/filter/constant_filter.hpp"
#include "duckdb/planner/filter/null_filter.hpp"

#include <random>

namespace duckdb {
namespace ext_nanoarrow {

//===----------------------------------------------------------------------===//
// Split Mode Data Structures
//===----------------------------------------------------------------------===//

//! Extended function data that stores parameters for pushdown query construction
struct ReadArrowDDFunctionData : public ArrowIPCFunctionData {
  ReadArrowDDFunctionData(std::unique_ptr<ArrowIPCStreamFactory> factory, string url,
                              string original_query, vector<string> column_names,
                              string auth_token = "")
      : ArrowIPCFunctionData(std::move(factory)),
        url(std::move(url)),
        original_query(std::move(original_query)),
        column_names(std::move(column_names)),
        auth_token(std::move(auth_token)) {}

  string url;
  string original_query;
  vector<string> column_names;
  string auth_token;  //! JWT auth token for Authorization header
  bool split_mode = false;  //! Whether to use split mode for parallel execution
  int64_t split_size = -1;  //! Split size hint for plan API (-1 means not specified)
  int64_t query_id = -1;  //! Generated query ID for non-split mode cancellation
};

//! Custom global state for parallel split execution
struct ReadArrowDDGlobalState : public GlobalTableFunctionState {
  //! Whether we're in split mode
  bool split_mode = false;

  //! URL for fetching splits
  string url;

  //! Auth token for HTTP requests
  string auth_token;

  //! Split size hint for plan API
  int64_t split_size = -1;

  //! Vector of splits to process
  vector<PlanResponse> splits;

  //! Generated query IDs for each split (for cancellation tracking)
  vector<int64_t> query_ids;

  //! Next split index to process (atomic for thread safety)
  atomic<idx_t> next_split_idx{0};

  //! Mutex for split access (used for initialization)
  mutex split_mutex;

  //! For non-split mode: the underlying ArrowScanGlobalState
  unique_ptr<GlobalTableFunctionState> arrow_global_state;

  //! Column IDs for projection
  vector<column_t> column_ids;

  //! Scanned types for conversion
  vector<LogicalType> scanned_types;

  //! Projection IDs for filter column removal
  vector<idx_t> projection_ids;

  //! Client context reference for HTTP calls
  ClientContext* context = nullptr;

  //! Cancel guard for automatic query cancellation on interrupt
  QueryCancelGuard cancel_guard;

  idx_t MaxThreads() const override {
    if (split_mode) {
      return splits.size() > 0 ? splits.size() : 1;
    }
    if (arrow_global_state) {
      return arrow_global_state->MaxThreads();
    }
    return 1;
  }

  bool CanRemoveFilterColumns() const {
    return !projection_ids.empty();
  }
};

//! Per-thread local state for split mode
struct ReadArrowDDLocalState : public LocalTableFunctionState {
  //! Current split index being processed
  idx_t current_split_idx = DConstants::INVALID_INDEX;

  //! Stream factory for current split
  unique_ptr<HttpIPCStreamFactory> factory;

  //! Response data buffer (must outlive factory)
  string response_data;

  //! Current Arrow chunk being processed
  shared_ptr<ArrowArrayWrapper> current_chunk;

  //! Offset within current chunk
  idx_t chunk_offset = 0;

  //! Column IDs for this scan
  vector<column_t> column_ids;

  //! Array states for Arrow conversion
  unordered_map<idx_t, unique_ptr<ArrowArrayScanState>> array_states;

  //! The DataChunk containing all read columns
  DataChunk all_columns;

  //! Client context reference
  ClientContext* context = nullptr;

  //! Arrow stream wrapper
  unique_ptr<ArrowArrayStreamWrapper> stream;

  //! Whether current split is exhausted
  bool current_split_done = true;

  ArrowArrayScanState& GetState(idx_t child_idx, ClientContext& ctx) {
    auto it = array_states.find(child_idx);
    if (it == array_states.end()) {
      auto child_p = make_uniq<ArrowArrayScanState>(ctx);
      auto& child = *child_p;
      array_states.emplace(child_idx, std::move(child_p));
      return child;
    }
    return *it->second;
  }

  void Reset() {
    chunk_offset = 0;
    for (auto& col : array_states) {
      col.second->Reset();
    }
  }
};

//===----------------------------------------------------------------------===//
// Utility Functions
//===----------------------------------------------------------------------===//

//! Get next available split for processing (thread-safe)
static bool GetNextSplit(ReadArrowDDGlobalState& global_state,
                         ReadArrowDDLocalState& local_state) {
  idx_t split_idx = global_state.next_split_idx.fetch_add(1);

  if (split_idx >= global_state.splits.size()) {
    return false;
  }

  local_state.current_split_idx = split_idx;
  local_state.current_split_done = false;
  local_state.chunk_offset = 0;
  local_state.current_chunk.reset();
  local_state.stream.reset();

  // Create factory for this split's query with the generated query ID
  auto& plan_response = global_state.splits[split_idx];
  auto query_id = global_state.query_ids[split_idx];
  local_state.factory = make_uniq<HttpIPCStreamFactory>(
      *global_state.context, global_state.url, plan_response.descriptor.statement_handle.query,
      global_state.auth_token, query_id);
  local_state.factory->InitReader();

  // Create stream from factory
  ArrowStreamParameters params;
  local_state.stream = ArrowIPCStreamFactory::Produce(
      reinterpret_cast<uintptr_t>(local_state.factory.get()), params);

  // Consume the schema from the stream (each split stream starts with a schema)
  ArrowSchemaWrapper schema;
  local_state.stream->GetSchema(schema);

  return true;
}

//===----------------------------------------------------------------------===//
// ReadArrowDDFunction
//===----------------------------------------------------------------------===//

struct ReadArrowDDFunction : ArrowTableFunction {
  //! Convert a TableFilter to SQL WHERE clause fragment
  static string FilterToSQL(const TableFilter& filter, const string& column_name) {
    switch (filter.filter_type) {
      case TableFilterType::CONSTANT_COMPARISON: {
        auto& const_filter = filter.Cast<ConstantFilter>();
        return const_filter.ToString(column_name);
      }
      case TableFilterType::IS_NULL: {
        return column_name + " IS NULL";
      }
      case TableFilterType::IS_NOT_NULL: {
        return column_name + " IS NOT NULL";
      }
      case TableFilterType::CONJUNCTION_AND: {
        auto& conj = filter.Cast<ConjunctionAndFilter>();
        string result = "(";
        for (idx_t i = 0; i < conj.child_filters.size(); i++) {
          if (i > 0) {
            result += " AND ";
          }
          result += FilterToSQL(*conj.child_filters[i], column_name);
        }
        result += ")";
        return result;
      }
      case TableFilterType::CONJUNCTION_OR: {
        auto& conj = filter.Cast<ConjunctionOrFilter>();
        string result = "(";
        for (idx_t i = 0; i < conj.child_filters.size(); i++) {
          if (i > 0) {
            result += " OR ";
          }
          result += FilterToSQL(*conj.child_filters[i], column_name);
        }
        result += ")";
        return result;
      }
      default:
        // For unsupported filter types, return empty (filter won't be pushed)
        return "";
    }
  }

  //! Build the pushdown query with projections and filters
  static string BuildPushdownQuery(const string& original_query,
                                   const vector<string>& all_column_names,
                                   const vector<column_t>& column_ids,
                                   optional_ptr<TableFilterSet> filters) {
    // Build SELECT clause from projected columns
    // The column_ids are in the order DuckDB wants them, so we build the SELECT list in that order
    string select_clause;
    for (idx_t i = 0; i < column_ids.size(); i++) {
      auto col_idx = column_ids[i];
      if (col_idx == COLUMN_IDENTIFIER_ROW_ID) {
        continue;  // Skip row ID column
      }
      if (!select_clause.empty()) {
        select_clause += ", ";
      }
      select_clause += "\"" + all_column_names[col_idx] + "\"";
    }

    // If no columns selected (shouldn't happen), fall back to SELECT *
    if (select_clause.empty()) {
      select_clause = "*";
    }

    // Build WHERE clause from filters
    // Note: filter indices are based on the projected column order, not original table order
    // We need to map through column_ids to get the actual column name
    string where_clause;
    if (filters) {
      vector<string> filter_conditions;
      for (auto& filter_entry : filters->filters) {
        auto filter_col_idx = filter_entry.first;
        // The filter index corresponds to position in column_ids
        if (filter_col_idx < column_ids.size()) {
          auto actual_col_idx = column_ids[filter_col_idx];
          if (actual_col_idx < all_column_names.size() && actual_col_idx != COLUMN_IDENTIFIER_ROW_ID) {
            string column_name = "\"" + all_column_names[actual_col_idx] + "\"";
            string condition = FilterToSQL(*filter_entry.second, column_name);
            if (!condition.empty()) {
              filter_conditions.push_back(condition);
            }
          }
        }
      }
      if (!filter_conditions.empty()) {
        where_clause = " WHERE ";
        for (idx_t i = 0; i < filter_conditions.size(); i++) {
          if (i > 0) {
            where_clause += " AND ";
          }
          where_clause += filter_conditions[i];
        }
      }
    }

    // Construct final query: SELECT <columns> FROM (<original>) AS _subq WHERE <filters>
    return "SELECT " + select_clause + " FROM (" + original_query + ") AS _subq" + where_clause;
  }

  static unique_ptr<FunctionData> Bind(ClientContext& context,
                                       TableFunctionBindInput& input,
                                       vector<LogicalType>& return_types,
                                       vector<string>& names) {
    // Extract required parameter: url
    auto url = input.inputs[0].GetValue<string>();

    // Extract optional named parameters
    string source_table;
    string sql_query;
    string auth_token;
    bool split_mode = false;
    int64_t split_size = -1;

    for (auto& kv : input.named_parameters) {
      if (kv.first == "source_table") {
        source_table = kv.second.GetValue<string>();
      } else if (kv.first == "sql") {
        sql_query = kv.second.GetValue<string>();
      } else if (kv.first == "split") {
        split_mode = kv.second.GetValue<bool>();
      } else if (kv.first == "auth_token") {
        auth_token = kv.second.GetValue<string>();
      } else if (kv.first == "split_size") {
        split_size = kv.second.GetValue<int64_t>();
      }
    }

    // Validate that exactly one of source_table or sql is provided
    if (source_table.empty() && sql_query.empty()) {
      throw InvalidInputException(
          "dd_read_arrow requires either 'source_table' or 'sql' parameter");
    }
    if (!source_table.empty() && !sql_query.empty()) {
      throw InvalidInputException(
          "dd_read_arrow accepts either 'source_table' or 'sql' parameter, not both");
    }

    // Construct the query
    string query;
    if (!source_table.empty()) {
      // If source_table is provided, wrap it in a SELECT statement
      query = "SELECT * FROM " + source_table;
    } else {
      // Use the provided SQL query
      query = sql_query;
    }

    // Generate a random query ID for tracking/cancellation
    std::random_device rd;
    std::mt19937_64 gen(rd());
    std::uniform_int_distribution<int64_t> dis(1, std::numeric_limits<int64_t>::max());
    int64_t query_id = dis(gen);

    // Create the stream factory and fetch initial data to get schema
    auto stream_factory = make_uniq<HttpIPCStreamFactory>(context, url, query, auth_token, query_id);
    stream_factory->InitReader();

    // Store column names for pushdown query construction (will get names after schema)
    vector<string> column_names;

    // Create extended function data first, then populate schema directly into it
    // (ArrowSchemaWrapper doesn't have proper move semantics)
    auto res = make_uniq<ReadArrowDDFunctionData>(
        std::move(stream_factory), url, query, column_names, auth_token);

    // Set split mode, size, and query ID
    res->split_mode = split_mode;
    res->split_size = split_size;
    res->query_id = query_id;

    // Get the schema directly into res
    res->factory->GetFileSchema(res->schema_root);

    // Populate the Arrow table schema
    DBConfig& db_config = DatabaseInstance::GetDatabase(context).config;
    PopulateArrowTableSchema(db_config, res->arrow_table, res->schema_root.arrow_schema);

    names = res->arrow_table.GetNames();
    return_types = res->arrow_table.GetTypes();
    res->all_types = return_types;

    if (return_types.empty()) {
      throw InvalidInputException("Remote query returned no columns");
    }

    // Store column names for pushdown query construction
    res->column_names = names;
    res->url = std::move(url);
    res->original_query = std::move(query);

    return std::move(res);
  }

  static unique_ptr<GlobalTableFunctionState> InitGlobal(ClientContext& context,
                                                         TableFunctionInitInput& input) {
    auto& data = input.bind_data->Cast<ReadArrowDDFunctionData>();

    // Check if we have projections or filters to push down to the server
    bool has_projection = input.column_ids.size() < data.column_names.size();
    bool has_filters = input.filters && !input.filters->filters.empty();

    // Build pushdown query if needed
    string pushdown_query = data.original_query;
    if (has_projection || has_filters) {
      pushdown_query =
          BuildPushdownQuery(data.original_query, data.column_names, input.column_ids, input.filters);
    }

    if (data.split_mode) {
      // Split mode: create our custom global state
      auto result = make_uniq<ReadArrowDDGlobalState>();
      result->split_mode = true;
      result->url = data.url;
      result->auth_token = data.auth_token;
      result->split_size = data.split_size;
      result->context = &context;
      result->column_ids = input.column_ids;

      // Build scanned types based on column IDs
      for (auto col_id : input.column_ids) {
        if (col_id == COLUMN_IDENTIFIER_ROW_ID) {
          continue;
        }
        result->scanned_types.push_back(data.all_types[col_id]);
      }

      // Store projection IDs if we need to remove filter columns
      result->projection_ids = input.projection_ids;

      // Fetch splits from plan API
      try {
        result->splits = FetchPlanSplits(context, data.url, pushdown_query, data.auth_token, data.split_size);
      } catch (const std::exception& e) {
        // If plan API fails, fall back to single query mode
        throw IOException("Failed to fetch splits from plan API: %s", e.what());
      }

      // If no splits returned, treat as error (server should return at least one)
      if (result->splits.empty()) {
        throw IOException("Plan API returned no splits");
      }

      // Generate random query IDs for each split (for tracking/cancellation)
      // Use random_device and mt19937 for good randomness
      std::random_device rd;
      std::mt19937_64 gen(rd());
      std::uniform_int_distribution<int64_t> dis(1, std::numeric_limits<int64_t>::max());

      result->query_ids.reserve(result->splits.size());
      for (size_t i = 0; i < result->splits.size(); i++) {
        result->query_ids.push_back(dis(gen));
      }

      // Register all generated query IDs with the cancel guard for automatic cancellation on interrupt
      for (auto query_id : result->query_ids) {
        result->cancel_guard.AddQuery(&context, data.url, data.auth_token, query_id);
      }

      return std::move(result);
    }

    // Non-split mode: use standard Arrow scan
    // Generate a new query ID if we have pushdown (query changes), otherwise use the one from Bind
    int64_t query_id = data.query_id;

    if (has_projection || has_filters) {
      // Generate new query ID for the modified query
      std::random_device rd;
      std::mt19937_64 gen(rd());
      std::uniform_int_distribution<int64_t> dis(1, std::numeric_limits<int64_t>::max());
      query_id = dis(gen);

      // Re-create the stream factory with the pushdown query and new ID
      auto new_factory =
          make_uniq<HttpIPCStreamFactory>(context, data.url, pushdown_query, data.auth_token, query_id);
      new_factory->InitReader();

      // Update the factory in the function data (const_cast needed due to DuckDB API)
      auto& mutable_data = const_cast<ReadArrowDDFunctionData&>(data);
      mutable_data.factory = std::move(new_factory);
      mutable_data.query_id = query_id;

      // IMPORTANT: Also update stream_factory_ptr since ArrowScanInitGlobal uses it
      mutable_data.stream_factory_ptr = reinterpret_cast<uintptr_t>(mutable_data.factory.get());
    }

    // Create our wrapper global state that holds the arrow global state
    auto result = make_uniq<ReadArrowDDGlobalState>();
    result->split_mode = false;
    result->url = data.url;
    result->auth_token = data.auth_token;
    result->context = &context;

    // Register query ID with cancel guard for non-split mode
    result->cancel_guard.AddQuery(&context, data.url, data.auth_token, query_id);

    result->arrow_global_state = ArrowScanInitGlobal(context, input);
    return std::move(result);
  }

  static unique_ptr<LocalTableFunctionState> InitLocal(ExecutionContext& context,
                                                       TableFunctionInitInput& input,
                                                       GlobalTableFunctionState* global_state_p) {
    auto& global_state = global_state_p->Cast<ReadArrowDDGlobalState>();

    if (global_state.split_mode) {
      // Split mode: create our custom local state
      auto local_state = make_uniq<ReadArrowDDLocalState>();
      local_state->context = &context.client;
      local_state->column_ids = global_state.column_ids;

      // Initialize all_columns DataChunk for conversion
      local_state->all_columns.Initialize(context.client, global_state.scanned_types);

      // Get first split
      if (!GetNextSplit(global_state, *local_state)) {
        // No splits available - this thread has nothing to do
        local_state->current_split_done = true;
      }

      return std::move(local_state);
    }

    // Non-split mode: use standard Arrow local state
    return ArrowScanInitLocal(context, input, global_state.arrow_global_state.get());
  }

  //! Scan function for split mode - processes splits in parallel
  static void SplitScanFunction(ClientContext& context, TableFunctionInput& data,
                                DataChunk& output) {
    auto& global_state = data.global_state->Cast<ReadArrowDDGlobalState>();
    auto& local_state = data.local_state->Cast<ReadArrowDDLocalState>();
    auto& bind_data = data.bind_data->Cast<ReadArrowDDFunctionData>();

    while (true) {
      // If current split is done, get next split
      if (local_state.current_split_done) {
        if (!GetNextSplit(global_state, local_state)) {
          // No more splits - we're done
          output.SetCardinality(0);
          return;
        }
      }

      // Process current split
      // Get next chunk from stream if needed
      if (!local_state.current_chunk || local_state.chunk_offset >= local_state.current_chunk->arrow_array.length) {
        // Need to get next batch from stream
        local_state.Reset();

        auto& stream = local_state.stream;

        if (!stream) {
          local_state.current_split_done = true;
          continue;
        }

        auto chunk = stream->GetNextChunk();
        auto error = stream->GetError();
        if (error && strlen(error) > 0) {
          throw IOException("Failed to read Arrow chunk: %s", error);
        }

        if (!chunk || chunk->arrow_array.length == 0) {
          // Current split exhausted
          local_state.current_split_done = true;
          continue;
        }

        local_state.current_chunk = std::move(chunk);
        local_state.chunk_offset = 0;
      }

      // Convert Arrow data to DuckDB output
      auto& chunk = local_state.current_chunk;
      idx_t remaining = chunk->arrow_array.length - local_state.chunk_offset;
      idx_t output_size = MinValue<idx_t>(remaining, STANDARD_VECTOR_SIZE);

      local_state.all_columns.Reset();
      local_state.all_columns.SetCardinality(output_size);

      // Convert each column
      auto& arrow_schema = chunk->arrow_array;
      auto& columns = bind_data.arrow_table.GetColumns();
      for (idx_t col_idx = 0; col_idx < local_state.column_ids.size(); col_idx++) {
        auto arrow_col_idx = local_state.column_ids[col_idx];
        if (arrow_col_idx == COLUMN_IDENTIFIER_ROW_ID) {
          continue;
        }

        // In split mode, columns are already projected by the server query
        // so we use sequential indexing into the returned data
        auto& arrow_array = *arrow_schema.children[col_idx];
        auto& out_vec = local_state.all_columns.data[col_idx];
        auto& array_state = local_state.GetState(col_idx, context);
        array_state.owned_data = local_state.current_chunk;

        auto col_it = columns.find(arrow_col_idx);
        if (col_it == columns.end()) {
          throw InternalException("Column %llu not found in arrow table schema", arrow_col_idx);
        }
        auto& arrow_type = *col_it->second;
        ArrowToDuckDBConversion::ColumnArrowToDuckDB(
            out_vec, arrow_array, local_state.chunk_offset, array_state,
            output_size, arrow_type);
      }

      local_state.chunk_offset += output_size;

      // Copy to output (handles projection ID mapping if needed)
      if (global_state.CanRemoveFilterColumns()) {
        output.ReferenceColumns(local_state.all_columns, global_state.projection_ids);
      } else {
        output.Reference(local_state.all_columns);
      }
      output.SetCardinality(output_size);
      output.Verify();
      return;
    }
  }

  //! Main scan function - dispatches to appropriate implementation
  static void ScanFunction(ClientContext& context, TableFunctionInput& data,
                           DataChunk& output) {
    auto& global_state = data.global_state->Cast<ReadArrowDDGlobalState>();

    if (global_state.split_mode) {
      SplitScanFunction(context, data, output);
    } else {
      // Non-split mode: use arrow global state for scanning
      TableFunctionInput arrow_input(data.bind_data, data.local_state,
                                     global_state.arrow_global_state.get());
      ArrowScanFunction(context, arrow_input, output);
    }
  }

  static TableFunction Function() {
    // Only url is required as positional parameter
    TableFunction func("dd_read_arrow", {LogicalType::VARCHAR}, ScanFunction,
                       Bind, InitGlobal, InitLocal);

    // Add named parameters
    func.named_parameters["source_table"] = LogicalType::VARCHAR;
    func.named_parameters["sql"] = LogicalType::VARCHAR;
    func.named_parameters["split"] = LogicalType::BOOLEAN;
    func.named_parameters["split_size"] = LogicalType::BIGINT;
    func.named_parameters["auth_token"] = LogicalType::VARCHAR;

    func.cardinality = ArrowScanCardinality;
    func.projection_pushdown = true;
    func.filter_pushdown = true;
    func.filter_prune = true;

    return func;
  }
};

void RegisterReadArrowDD(ExtensionLoader& loader) {
  auto function = ReadArrowDDFunction::Function();
  loader.RegisterFunction(function);
}

}  // namespace ext_nanoarrow
}  // namespace duckdb
