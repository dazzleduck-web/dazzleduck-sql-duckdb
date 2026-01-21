//===----------------------------------------------------------------------===//
//                         DuckDB - DazzleDuck
//
// table_function/dd_splits.hpp
//
//===----------------------------------------------------------------------===//

#pragma once

namespace duckdb {

class ExtensionLoader;

namespace ext_nanoarrow {

//! Register the dd_splits table function
void RegisterDDSplits(ExtensionLoader& loader);

}  // namespace ext_nanoarrow
}  // namespace duckdb
