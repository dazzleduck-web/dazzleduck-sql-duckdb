#include "ipc/http_stream_factory.hpp"

#include "http/arrow_http_client.hpp"
#include "ipc/stream_reader/ipc_buffer_stream_reader.hpp"

namespace duckdb {
namespace ext_nanoarrow {

HttpIPCStreamFactory::HttpIPCStreamFactory(ClientContext& context, string url, string query)
    : ArrowIPCStreamFactory(Allocator::Get(context)),
      context(context),
      url(std::move(url)),
      query(std::move(query)) {}

void HttpIPCStreamFactory::InitReader() {
  // Fetch the Arrow IPC stream from the remote server using DuckDB's HTTP utilities
  response_data = ArrowHttpClient::FetchArrowStream(context, url, query);

  // Create buffer pointing to the response data
  vector<ArrowIPCBuffer> buffers;
  buffers.emplace_back(reinterpret_cast<uint64_t>(response_data.data()),
                       static_cast<uint64_t>(response_data.size()));

  // Create the buffer stream reader
  reader = make_uniq<IPCBufferStreamReader>(std::move(buffers), allocator);
}

}  // namespace ext_nanoarrow
}  // namespace duckdb
