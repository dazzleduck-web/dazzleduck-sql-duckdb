//===----------------------------------------------------------------------===//
//                         DuckDB - DazzleDuck
//
// scalar_function/dd_login.hpp
//
//===----------------------------------------------------------------------===//

#pragma once

namespace duckdb {

class ExtensionLoader;

namespace ext_nanoarrow {

//! Register the dd_login scalar function
void RegisterDDLogin(ExtensionLoader& loader);

}  // namespace ext_nanoarrow
}  // namespace duckdb
