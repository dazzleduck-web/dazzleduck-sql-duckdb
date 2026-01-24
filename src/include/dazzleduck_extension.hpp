//===----------------------------------------------------------------------===//
//                         DuckDB - dazzleduck
//
// dazzleduck_extension.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/main/database.hpp"
#include "duckdb/main/extension/extension_loader.hpp"

namespace duckdb {

class DazzleduckExtension : public Extension {
 public:
  void Load(ExtensionLoader& db) override;
  std::string Name() override;
  std::string Version() const override;
};

}  // namespace duckdb
