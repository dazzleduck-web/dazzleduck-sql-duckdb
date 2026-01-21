#include "dazzle_duck_extension.hpp"

#include <string>

#include "duckdb/function/scalar_function.hpp"
#include "duckdb/main/extension/extension_loader.hpp"

#include "nanoarrow/nanoarrow.hpp"

#include "scalar_function/array_contains_all.hpp"
#include "scalar_function/dd_login.hpp"
#include "scalar_function/dd_search.hpp"
#include "table_function/dd_splits.hpp"
#include "table_function/read_arrow_dd.hpp"

namespace duckdb {

namespace {

struct NanoarrowVersion {
  static void Register(ExtensionLoader& loader) {
    auto fn = ScalarFunction("nanoarrow_version", {}, LogicalType::VARCHAR, ExecuteFn);
    loader.RegisterFunction(fn);
  }

  static void ExecuteFn(DataChunk& args, ExpressionState& state, Vector& result) {
    result.SetValue(0, StringVector::AddString(result, ArrowNanoarrowVersion()));
    result.SetVectorType(VectorType::CONSTANT_VECTOR);
  }
};

void LoadInternal(ExtensionLoader& loader) {
  NanoarrowVersion::Register(loader);
  ext_nanoarrow::RegisterArrayContainsAll(loader);
  ext_nanoarrow::RegisterDDLogin(loader);
  ext_nanoarrow::RegisterDDSearch(loader);
  ext_nanoarrow::RegisterDDSplits(loader);
  ext_nanoarrow::RegisterReadArrowDD(loader);
}

}  // namespace

void DazzleDuckExtension::Load(ExtensionLoader& loader) { LoadInternal(loader); }

std::string DazzleDuckExtension::Name() { return "dazzle_duck"; }

std::string DazzleDuckExtension::Version() const {
#ifdef EXT_VERSION_DAZZLE_DUCK
  return EXT_VERSION_DAZZLE_DUCK;
#else
  return "";
#endif
}

}  // namespace duckdb

extern "C" {
DUCKDB_CPP_EXTENSION_ENTRY(dazzle_duck, loader) { duckdb::LoadInternal(loader); }
}
