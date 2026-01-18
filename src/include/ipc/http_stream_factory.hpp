//===----------------------------------------------------------------------===//
//                         DuckDB - nanoarrow
//
// ipc/http_stream_factory.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "ipc/stream_factory.hpp"
#include "duckdb/main/client_context.hpp"

namespace duckdb {
namespace ext_nanoarrow {

//! Stream factory for fetching Arrow IPC streams over HTTP
class HttpIPCStreamFactory final : public ArrowIPCStreamFactory {
 public:
  explicit HttpIPCStreamFactory(ClientContext& context, string url, string query);
  void InitReader() override;

  ClientContext& context;
  string url;
  string query;

  //! Buffer holding the response data - must outlive the reader
  string response_data;
};

}  // namespace ext_nanoarrow
}  // namespace duckdb
