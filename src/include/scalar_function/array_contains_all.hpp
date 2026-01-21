//===----------------------------------------------------------------------===//
//                         DuckDB - DazzleDuck
//
// scalar_function/array_contains_all.hpp
//
//===----------------------------------------------------------------------===//

#pragma once

namespace duckdb {

class ExtensionLoader;

namespace ext_nanoarrow {

//! Register the array_contains_all scalar function
void RegisterArrayContainsAll(ExtensionLoader& loader);

}  // namespace ext_nanoarrow
}  // namespace duckdb
