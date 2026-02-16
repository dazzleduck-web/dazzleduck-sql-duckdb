#!/bin/bash
#
# Master script to run all dd_read_arrow tests
#

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
TOTAL_TESTS=0
PASSED_TESTS=0
FAILED_TESTS=0

echo -e "${BLUE}========================================${NC}"
echo -e "${BLUE}Running All DazzleDuck Tests${NC}"
echo -e "${BLUE}========================================${NC}"
echo ""

# Function to run a test script
run_test_suite() {
    local test_name=$1
    local script_path=$2

    echo -e "${YELLOW}Running $test_name...${NC}"
    if bash "$script_path"; then
        echo -e "${GREEN}✓ $test_name PASSED${NC}"
        PASSED_TESTS=$((PASSED_TESTS + 1))
    else
        echo -e "${RED}✗ $test_name FAILED${NC}"
        FAILED_TESTS=$((FAILED_TESTS + 1))
    fi
    TOTAL_TESTS=$((TOTAL_TESTS + 1))
    echo ""
}

# Run unit tests (no Docker required)
echo -e "${YELLOW}Running unit tests (no Docker required)...${NC}"
if ./build/release/test/unittest test/sql/dd_read_arrow.test test/sql/dd_read_arrow_aggregation_pushdown.test; then
    echo -e "${GREEN}✓ Unit tests PASSED${NC}"
    PASSED_TESTS=$((PASSED_TESTS + 1))
else
    echo -e "${RED}✗ Unit tests FAILED${NC}"
    FAILED_TESTS=$((FAILED_TESTS + 1))
fi
TOTAL_TESTS=$((TOTAL_TESTS + 1))
echo ""

# Run integration tests (requires Docker)
run_test_suite "Integration Tests" "$SCRIPT_DIR/run_integration_tests.sh"

# Run split mode tests (requires Docker)
run_test_suite "Split Mode Tests" "$SCRIPT_DIR/run_split_tests.sh"

# Summary
echo -e "${BLUE}========================================${NC}"
echo -e "${BLUE}Test Summary${NC}"
echo -e "${BLUE}========================================${NC}"
echo -e "Total test suites: $TOTAL_TESTS"
echo -e "${GREEN}Passed: $PASSED_TESTS${NC}"
if [ $FAILED_TESTS -gt 0 ]; then
    echo -e "${RED}Failed: $FAILED_TESTS${NC}"
    echo ""
    echo -e "${RED}Some tests failed!${NC}"
    exit 1
else
    echo -e "${GREEN}All tests passed!${NC}"
    exit 0
fi
