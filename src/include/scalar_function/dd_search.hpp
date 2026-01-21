//===----------------------------------------------------------------------===//
//                         DuckDB - DazzleDuck
//
// scalar_function/dd_search.hpp
//
//===----------------------------------------------------------------------===//

#pragma once

namespace duckdb {

class ExtensionLoader;

namespace ext_nanoarrow {

//! Register the dd_search scalar function
void RegisterDDSearch(ExtensionLoader& loader);

}  // namespace ext_nanoarrow
}  // namespace duckdb
