//===----------------------------------------------------------------------===//
//                         DuckDB - nanoarrow
//
// ipc/ipc_stream_factory.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "ipc/array_stream.hpp"

#include "duckdb/common/arrow/arrow_wrapper.hpp"
#include "duckdb/function/table/arrow.hpp"

namespace duckdb {
namespace ext_nanoarrow {

//! Arrow IPC Buffer, basically a pointer to the buffer and its size
struct ArrowIPCBuffer {
  ArrowIPCBuffer(const uint64_t ptr, const uint64_t size) : ptr(ptr), size(size) {};
  uint64_t ptr;
  uint64_t size;
};

class ArrowStreamFactory {
  ArrowStreamFactory() {};
};
//! This Factory is a type invented by DuckDB. Notably, the Produce()
//! function pointer is passed to the constructor of the ArrowScanFunctionData
//! constructor (which we wrap).
class ArrowIPCStreamFactory {
 public:
  virtual ~ArrowIPCStreamFactory() = default;
  explicit ArrowIPCStreamFactory(Allocator& allocator);

  //! Called once when initializing Scan States
  static unique_ptr<ArrowArrayStreamWrapper> Produce(uintptr_t factory_ptr,
                                                     ArrowStreamParameters& parameters);

  //! Get the schema of the arrow object
  void GetFileSchema(ArrowSchemaWrapper& schema) const;

  //! Opens the file, wraps it in the ArrowIpcInputStream, and wraps it in
  //! the ArrowArrayStream reader.
  virtual void InitReader() {
    throw NotImplementedException("ArrowIPCStreamFactory::InitReader not implemented");
  }

  Allocator& allocator;
  unique_ptr<IPCStreamReader> reader;
  ArrowError error{};
};

class BufferIPCStreamFactory final : public ArrowIPCStreamFactory {
 public:
  explicit BufferIPCStreamFactory(ClientContext& context,
                                  const vector<ArrowIPCBuffer>& buffers);
  void InitReader() override;

  vector<ArrowIPCBuffer> buffers;
};

}  // namespace ext_nanoarrow
}  // namespace duckdb
