//===----------------------------------------------------------------------===//
//                         DuckDB - dazzleduck
//
// optimizer/aggregation_pushdown.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/main/extension/extension_loader.hpp"

namespace duckdb {
namespace ext_nanoarrow {

//! Register the aggregation pushdown optimizer extension
void RegisterAggregationPushdown(ExtensionLoader& loader);

}  // namespace ext_nanoarrow
}  // namespace duckdb
