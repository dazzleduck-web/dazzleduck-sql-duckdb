#include "ipc/stream_factory.hpp"

#include <utility>

#include "ipc/stream_reader/ipc_buffer_stream_reader.hpp"

namespace duckdb {
namespace ext_nanoarrow {
ArrowIPCStreamFactory::ArrowIPCStreamFactory(Allocator& allocator_p)
    : allocator(allocator_p) {}

unique_ptr<ArrowArrayStreamWrapper> ArrowIPCStreamFactory::Produce(
    uintptr_t factory_ptr, ArrowStreamParameters& parameters) {
  auto factory =
      static_cast<ArrowIPCStreamFactory*>(reinterpret_cast<void*>(factory_ptr));

  if (!factory->reader) {
    throw InternalException("IpcStreamReader was not initialized or was already moved");
  }

  if (!parameters.projected_columns.columns.empty()) {
    factory->reader->SetColumnProjection(parameters.projected_columns.columns);
  }

  auto out = make_uniq<ArrowArrayStreamWrapper>();
  IpcArrayStream(std::move(factory->reader)).ToArrayStream(&out->arrow_array_stream);
  return out;
}

void ArrowIPCStreamFactory::GetFileSchema(ArrowSchemaWrapper& schema) const {
  if (!reader) {
    throw InternalException("IpcStreamReader is no longer valid");
  }

  NANOARROW_THROW_NOT_OK(
      ArrowSchemaDeepCopy(reader->GetBaseSchema(), &schema.arrow_schema));
}

BufferIPCStreamFactory::BufferIPCStreamFactory(ClientContext& context,
                                               const vector<ArrowIPCBuffer>& buffers_p)
    : ArrowIPCStreamFactory(BufferAllocator::Get(context)), buffers(buffers_p) {}

void BufferIPCStreamFactory::InitReader() {
  if (reader) {
    throw InternalException("ArrowArrayStream or IpcStreamReader already initialized");
  }
  reader = make_uniq<IPCBufferStreamReader>(buffers, allocator);
}

}  // namespace ext_nanoarrow
}  // namespace duckdb
