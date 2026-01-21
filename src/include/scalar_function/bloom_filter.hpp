#pragma once

namespace duckdb {
class ExtensionLoader;

namespace ext_nanoarrow {

void RegisterBloomFilterFunctions(ExtensionLoader& loader);

}  // namespace ext_nanoarrow
}  // namespace duckdb
