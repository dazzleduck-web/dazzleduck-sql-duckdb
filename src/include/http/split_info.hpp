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

//! Statement handle from the plan API
struct StatementHandle {
  string query;              //! The SQL query (potentially modified for this split)
  int64_t query_id = -1;     //! Unique identifier for the query
  string producer_id;        //! UUID of the producer that created this plan
  int64_t split_size = -1;   //! Size of this split in bytes (-1 if not split)
  string query_checksum;     //! Base64-encoded checksum for query validation
};

//! Descriptor containing the statement handle
struct Descriptor {
  StatementHandle statement_handle;
};

//! Represents one plan response (split) from the plan API
struct PlanResponse {
  vector<string> endpoints;  //! HTTP endpoint URLs where this split can be executed
  Descriptor descriptor;
};

//! Fetch and parse splits from the plan API
//! @param context The client context for HTTP utilities
//! @param url The base URL of the server
//! @param query The SQL query to get splits for
//! @param auth_token Optional JWT auth token
//! @param split_size Optional split size hint (-1 means not specified)
//! @return Vector of PlanResponse containing the parsed splits
vector<PlanResponse> FetchPlanSplits(ClientContext& context, const string& url,
                                     const string& query, const string& auth_token = "",
                                     int64_t split_size = -1);

}  // namespace ext_nanoarrow
}  // namespace duckdb
