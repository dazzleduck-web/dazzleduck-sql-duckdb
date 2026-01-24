# This file is included by DuckDB's build system. It specifies which extension to load

# Extension from this repo
duckdb_extension_load(dazzleduck
                      SOURCE_DIR
                      ${CMAKE_CURRENT_LIST_DIR}
                      LOAD_TESTS
                      EXTENSION_VERSION "0.0.2"
                      LINKED_LIBS
                      "../../_deps/nanoarrow-build/lib*.a")

# Any extra extensions that should be built
# e.g.: duckdb_extension_load(json)
