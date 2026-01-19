#include "table_function/read_arrow_dd.hpp"

#include "http/arrow_http_client.hpp"
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

namespace duckdb {
namespace ext_nanoarrow {

//! Extended function data that stores parameters for pushdown query construction
struct ReadArrowDDFunctionData : public ArrowIPCFunctionData {
  ReadArrowDDFunctionData(std::unique_ptr<ArrowIPCStreamFactory> factory, string url,
                              string original_query, vector<string> column_names)
      : ArrowIPCFunctionData(std::move(factory)),
        url(std::move(url)),
        original_query(std::move(original_query)),
        column_names(std::move(column_names)) {}

  string url;
  string original_query;
  vector<string> column_names;
};

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
    string where_clause;
    if (filters) {
      vector<string> filter_conditions;
      for (auto& filter_entry : filters->filters) {
        auto col_idx = filter_entry.first;
        if (col_idx < all_column_names.size()) {
          string column_name = "\"" + all_column_names[col_idx] + "\"";
          string condition = FilterToSQL(*filter_entry.second, column_name);
          if (!condition.empty()) {
            filter_conditions.push_back(condition);
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

    for (auto& kv : input.named_parameters) {
      if (kv.first == "source_table") {
        source_table = kv.second.GetValue<string>();
      } else if (kv.first == "sql") {
        sql_query = kv.second.GetValue<string>();
      }
    }

    // Validate that exactly one of source_table or sql is provided
    if (source_table.empty() && sql_query.empty()) {
      throw InvalidInputException(
          "read_arrow_dd requires either 'source_table' or 'sql' parameter");
    }
    if (!source_table.empty() && !sql_query.empty()) {
      throw InvalidInputException(
          "read_arrow_dd accepts either 'source_table' or 'sql' parameter, not both");
    }

    // Construct the query
    string query;
    if (!source_table.empty()) {
      // If source_table is provided, use it directly as the query
      query = source_table;
    } else {
      // Use the provided SQL query
      query = sql_query;
    }

    // Create the stream factory and fetch initial data to get schema
    auto stream_factory = make_uniq<HttpIPCStreamFactory>(context, url, query);
    stream_factory->InitReader();

    // Store column names for pushdown query construction (will get names after schema)
    vector<string> column_names;

    // Create extended function data first, then populate schema directly into it
    // (ArrowSchemaWrapper doesn't have proper move semantics)
    auto res = make_uniq<ReadArrowDDFunctionData>(
        std::move(stream_factory), url, query, column_names);

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

    if (has_projection || has_filters) {
      // Build the pushdown query with projections and/or filters
      string pushdown_query =
          BuildPushdownQuery(data.original_query, data.column_names, input.column_ids, input.filters);

      // Re-create the stream factory with the pushdown query
      auto new_factory =
          make_uniq<HttpIPCStreamFactory>(context, data.url, pushdown_query);
      new_factory->InitReader();

      // Update the factory in the function data (const_cast needed due to DuckDB API)
      auto& mutable_data = const_cast<ReadArrowDDFunctionData&>(data);
      mutable_data.factory = std::move(new_factory);

      // IMPORTANT: Also update stream_factory_ptr since ArrowScanInitGlobal uses it
      mutable_data.stream_factory_ptr = reinterpret_cast<uintptr_t>(mutable_data.factory.get());
    }

    // Now call the standard ArrowScanInitGlobal
    return ArrowScanInitGlobal(context, input);
  }

  static TableFunction Function() {
    // Only url is required as positional parameter
    TableFunction func("read_arrow_dd", {LogicalType::VARCHAR}, ArrowScanFunction,
                       Bind, InitGlobal, ArrowScanInitLocal);

    // Add named parameters
    func.named_parameters["source_table"] = LogicalType::VARCHAR;
    func.named_parameters["sql"] = LogicalType::VARCHAR;

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
