PROJ_DIR := $(dir $(abspath $(lastword $(MAKEFILE_LIST))))

# Configuration of extension
EXT_NAME=nanoarrow
EXT_CONFIG=${PROJ_DIR}extension_config.cmake

# Define test target BEFORE include to take precedence
# Default test runs unit tests only (no Docker dependencies)
.PHONY: test
test: test_unit

# Include the Makefile from extension-ci-tools
include extension-ci-tools/makefiles/duckdb_extension.Makefile

# Override test_release_internal to run unit tests only (no Docker dependencies)
test_release_internal: test_unit
	@true

# Client tests
DEBUG_EXT_PATH='$(PROJ_DIR)build/debug/extension/nanoarrow/nanoarrow.duckdb_extension'
RELDEBUG_EXT_PATH='$(PROJ_DIR)build/release/extension/nanoarrow/nanoarrow.duckdb_extension'
RELEASE_EXT_PATH='$(PROJ_DIR)build/release/extension/nanoarrow/nanoarrow.duckdb_extension'

test_js:
test_debug_js:
	ARROW_EXTENSION_BINARY_PATH=$(DEBUG_EXT_PATH) mocha -R spec --timeout 480000 -n expose-gc --exclude 'test/*.ts' -- "test/nodejs/**/*.js"
test_release_js:
	ARROW_EXTENSION_BINARY_PATH=$(RELEASE_EXT_PATH) mocha -R spec --timeout 480000 -n expose-gc --exclude 'test/*.ts' -- "test/nodejs/**/*.js"

run_benchmark:
	python3 benchmark/lineitem.py $(RELEASE_EXT_PATH)

# Unit tests only (excludes slow tests requiring servers)
test_unit:
	./build/release/test/unittest "test/*" ~"*_slow*"

# Integration tests (require Docker)
test_integration:
	./scripts/run_integration_tests.sh

test_split:
	./scripts/run_split_tests.sh

test_cancel:
	./scripts/run_cancel_tests.sh

# Run all tests: unit tests + integration tests + split tests + cancel tests
test_all_internal: test_unit test_integration test_split test_cancel
	@echo "All tests completed!"

# Alias for convenience
test_all: test_all_internal

