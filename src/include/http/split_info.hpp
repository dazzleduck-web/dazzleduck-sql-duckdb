//===----------------------------------------------------------------------===//
//                         DuckDB - nanoarrow
//
// http/split_info.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/string.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/main/client_context.hpp"

namespace duckdb {
namespace ext_nanoarrow {

//! Represents one split from the plan API
struct SplitInfo {
  string id;           //! Split identifier
  string query;        //! Query for this split
  string producer_id;  //! Producer ID
  int64_t query_id = -1;    //! Query ID
  int64_t split_size = -1;  //! Split size in bytes
};

//! Fetch and parse splits from the plan API
//! @param context The client context for HTTP utilities
//! @param url The base URL of the server
//! @param query The SQL query to get splits for
//! @param auth_token Optional JWT auth token
//! @param split_size Optional split size hint (-1 means not specified)
//! @return Vector of SplitInfo containing the parsed splits
vector<SplitInfo> FetchPlanSplits(ClientContext& context, const string& url,
                                  const string& query, const string& auth_token = "",
                                  int64_t split_size = -1);

}  // namespace ext_nanoarrow
}  // namespace duckdb
