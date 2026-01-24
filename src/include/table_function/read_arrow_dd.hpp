//===----------------------------------------------------------------------===//
//                         DuckDB - dazzleduck
//
// table_function/read_arrow_dd.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/main/extension/extension_loader.hpp"

namespace duckdb {
namespace ext_nanoarrow {

//! Register the read_arrow_dd table function
void RegisterReadArrowDD(ExtensionLoader& loader);

}  // namespace ext_nanoarrow
}  // namespace duckdb
