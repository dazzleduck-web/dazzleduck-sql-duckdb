cmake_minimum_required(VERSION 3.5)
include(FetchContent)

# Set extension name here
set(TARGET_NAME dazzleduck)
set(EXTENSION_VERSION "0.0.5")

set(NANOARROW_IPC ON)
set(NANOARROW_NAMESPACE "DuckDBExt${TARGET_NAME}")
fetchcontent_declare(nanoarrow
                     URL "https://github.com/apache/arrow-nanoarrow/archive/4bf5a9322626e95e3717e43de7616c0a256179eb.zip"
                     URL_HASH SHA256=49d588ee758a2a1d099ed4525c583a04adf71ce40405011e0190aa1e75e61b59
)
fetchcontent_makeavailable(nanoarrow)

set(EXTENSION_NAME ${TARGET_NAME}_extension)
set(LOADABLE_EXTENSION_NAME ${TARGET_NAME}_loadable_extension)

project(${TARGET_NAME})
include_directories(src/include)

set(EXTENSION_SOURCES
    src/http/arrow_http_client.cpp
    src/http/cancel_monitor.cpp
    src/http/split_info.cpp
    src/ipc/array_stream.cpp
    src/ipc/http_stream_factory.cpp
    src/ipc/stream_factory.cpp
    src/ipc/stream_reader/base_stream_reader.cpp
    src/ipc/stream_reader/ipc_buffer_stream_reader.cpp
    src/optimizer/aggregation_pushdown.cpp
    src/scalar_function/array_contains_all.cpp
    src/scalar_function/bloom_filter.cpp
    src/scalar_function/dd_login.cpp
    src/scalar_function/dd_search.cpp
    src/scanner/read_arrow_dd.cpp
    src/table_function/dd_splits.cpp
    src/dazzleduck_extension.cpp)

build_static_extension(${TARGET_NAME} ${EXTENSION_SOURCES})
build_loadable_extension(${TARGET_NAME} " " ${EXTENSION_SOURCES})

# Link nanoarrow in both the static library as the loadable extension
target_link_libraries(${EXTENSION_NAME} nanoarrow::nanoarrow nanoarrow::nanoarrow_ipc)
target_link_libraries(${LOADABLE_EXTENSION_NAME} nanoarrow::nanoarrow
                      nanoarrow::nanoarrow_ipc)

# Set extension version macro
target_compile_definitions(${EXTENSION_NAME} PRIVATE EXT_VERSION_DAZZLEDUCK="${EXTENSION_VERSION}")
target_compile_definitions(${LOADABLE_EXTENSION_NAME} PRIVATE EXT_VERSION_DAZZLEDUCK="${EXTENSION_VERSION}")

install(TARGETS ${EXTENSION_NAME}
        EXPORT "${DUCKDB_EXPORT_SET}"
        LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
        ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")
