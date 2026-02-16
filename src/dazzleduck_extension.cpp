#include "dazzleduck_extension.hpp"

#include <string>

#include "duckdb/function/scalar_function.hpp"
#include "duckdb/main/extension/extension_loader.hpp"

#include "nanoarrow/nanoarrow.hpp"

#include "optimizer/aggregation_pushdown.hpp"
#include "scalar_function/array_contains_all.hpp"
#include "scalar_function/bloom_filter.hpp"
#include "scalar_function/dd_login.hpp"
#include "scalar_function/dd_search.hpp"
#include "table_function/dd_splits.hpp"
#include "table_function/read_arrow_dd.hpp"

namespace duckdb {

namespace {

struct DazzleDuckVersion {
  static void Register(ExtensionLoader& loader) {
    auto fn = ScalarFunction("dd_version", {}, LogicalType::VARCHAR, ExecuteFn);
    loader.RegisterFunction(fn);
  }

  static void ExecuteFn(DataChunk& args, ExpressionState& state, Vector& result) {
#ifdef EXT_VERSION_DAZZLEDUCK
    result.SetValue(0, StringVector::AddString(result, EXT_VERSION_DAZZLEDUCK));
#else
    result.SetValue(0, StringVector::AddString(result, "dev"));
#endif
    result.SetVectorType(VectorType::CONSTANT_VECTOR);
  }
};

void LoadInternal(ExtensionLoader& loader) {
  DazzleDuckVersion::Register(loader);
  ext_nanoarrow::RegisterArrayContainsAll(loader);
  ext_nanoarrow::RegisterBloomFilterFunctions(loader);
  ext_nanoarrow::RegisterDDLogin(loader);
  ext_nanoarrow::RegisterDDSearch(loader);
  ext_nanoarrow::RegisterDDSplits(loader);
  ext_nanoarrow::RegisterReadArrowDD(loader);
  ext_nanoarrow::RegisterAggregationPushdown(loader);
}

}  // namespace

void DazzleduckExtension::Load(ExtensionLoader& loader) { LoadInternal(loader); }

std::string DazzleduckExtension::Name() { return "dazzleduck"; }

std::string DazzleduckExtension::Version() const {
#ifdef EXT_VERSION_DAZZLEDUCK
  return EXT_VERSION_DAZZLEDUCK;
#else
  return "";
#endif
}

}  // namespace duckdb

extern "C" {
DUCKDB_CPP_EXTENSION_ENTRY(dazzleduck, loader) { duckdb::LoadInternal(loader); }
}
