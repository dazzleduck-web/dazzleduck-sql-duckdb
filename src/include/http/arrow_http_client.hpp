//===----------------------------------------------------------------------===//
//                         DuckDB - nanoarrow
//
// http/arrow_http_client.hpp
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

//! HTTP client for fetching Arrow IPC streams from remote endpoints
class ArrowHttpClient {
 public:
  //! Fetches an Arrow IPC stream from a remote server by executing a query.
  //! Makes a GET request to {url}/v1/query?q={query} with Arrow stream Accept header.
  //! @param context The client context for HTTP utilities
  //! @param url The base URL of the server (e.g., "http://localhost:8080")
  //! @param query The SQL query to execute
  //! @param auth_token Optional JWT auth token for Authorization header
  //! @return The raw Arrow IPC stream as binary data (string is used as a byte buffer
  //!         since DuckDB's HTTPUtil returns response body as string; std::string can
  //!         safely hold binary data including null bytes)
  static string FetchArrowStream(ClientContext& context, const string& url,
                                 const string& query, const string& auth_token = "");

  //! Fetches execution plan splits from the server.
  //! Makes a GET request to {url}/v1/plan?q={query} and returns JSON response.
  //! @param context The client context for HTTP utilities
  //! @param url The base URL of the server (e.g., "http://localhost:8080")
  //! @param query The SQL query to get plan for
  //! @param auth_token Optional JWT auth token for Authorization header
  //! @param split_size Optional split size hint (-1 means not specified)
  //! @return JSON response as string containing array of split information
  static string FetchPlanJson(ClientContext& context, const string& url,
                              const string& query, const string& auth_token = "",
                              int64_t split_size = -1);
};

}  // namespace ext_nanoarrow
}  // namespace duckdb
