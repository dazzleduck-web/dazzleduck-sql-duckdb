//===----------------------------------------------------------------------===//
//                         DuckDB - dazzleduck
//
// table_function/read_arrow_dd.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/main/extension/extension_loader.hpp"
#include "table_function/arrow_ipc_function_data.hpp"

namespace duckdb {
namespace ext_nanoarrow {

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
  string aggregation_pushdown_query;  //! If set by optimizer, use this query instead of building pushdown
};

//! Register the read_arrow_dd table function
void RegisterReadArrowDD(ExtensionLoader& loader);

}  // namespace ext_nanoarrow
}  // namespace duckdb
