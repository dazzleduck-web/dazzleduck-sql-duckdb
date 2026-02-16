#!/bin/bash
#
# Script to run dd_read_arrow split mode tests with DuckLake server
#

set -e

CONTAINER_NAME="ducklake-test"
IMAGE="dazzleduck/dazzleduck:latest"
HTTP_PORT=8082
FLIGHT_PORT=59308
MAX_WAIT_SECONDS=60

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

cleanup() {
    echo -e "${YELLOW}Cleaning up split test container...${NC}"
    docker stop "$CONTAINER_NAME" 2>/dev/null || true
    docker rm "$CONTAINER_NAME" 2>/dev/null || true
}

# Set trap to cleanup on exit
trap cleanup EXIT

# Check if Docker is available
if ! command -v docker &> /dev/null; then
    echo -e "${RED}Error: Docker is not installed or not in PATH${NC}"
    exit 1
fi

# Check if the test binary exists
if [ ! -f "./build/release/test/unittest" ]; then
    echo -e "${RED}Error: Test binary not found. Run 'make' first.${NC}"
    exit 1
fi

# Stop any existing container with the same name
echo -e "${YELLOW}Stopping any existing split test container...${NC}"
docker stop "$CONTAINER_NAME" 2>/dev/null || true
docker rm "$CONTAINER_NAME" 2>/dev/null || true

# Start the DuckLake container with startup script
echo -e "${YELLOW}Starting DuckLake container on port $HTTP_PORT with startup script...${NC}"
docker run -d \
    --name "$CONTAINER_NAME" \
    -p "$HTTP_PORT:8081" \
    -p "$FLIGHT_PORT:59307" \
    "$IMAGE" \
    --conf dazzleduck_server.warehouse=/data \
    --conf dazzleduck_server.startup_script_provider.script_location=/startup/ducklake.sql \
    --conf dazzleduck_server.access_mode=RESTRICTED

# Wait for the server to be ready (give extra time for startup script execution)
echo -e "${YELLOW}Waiting for DuckLake server to be ready and startup script to execute...${NC}"
WAIT_COUNT=0
MAX_STARTUP_WAIT=$((MAX_WAIT_SECONDS + 30))  # Extra time for startup script
while [ $WAIT_COUNT -lt $MAX_STARTUP_WAIT ]; do
    # Try to query the demo table to confirm both server is ready AND startup script executed
    if curl -s "http://localhost:$HTTP_PORT/health" 2>/dev/null | strings | grep -q "UP"; then
        echo -e "${GREEN}DuckLake server is ready and database initialized!${NC}"
        break
    fi

    # Check if container is still running
    if ! docker ps -q -f name="$CONTAINER_NAME" | grep -q .; then
        echo -e "${RED}Error: Container stopped unexpectedly${NC}"
        docker logs "$CONTAINER_NAME"
        exit 1
    fi

    sleep 2
    WAIT_COUNT=$((WAIT_COUNT + 2))
    echo -n "."
done

if [ $WAIT_COUNT -ge $MAX_STARTUP_WAIT ]; then
    echo -e "${RED}Error: DuckLake server did not initialize within $MAX_STARTUP_WAIT seconds${NC}"
    echo -e "${YELLOW}Server logs:${NC}"
    docker logs "$CONTAINER_NAME"
    exit 1
fi

# Run the split mode tests
echo -e "${YELLOW}Running split mode tests...${NC}"

TOTAL_FAILED=0

# Run each test file separately
for TEST_FILE in test/sql/dd_read_arrow_split.test_slow \
                 test/sql/dd_read_arrow_aggregation_pushdown_split.test_slow \
                 test/sql/dd_read_arrow_all_types_demo.test_slow \
                 test/sql/dd_read_arrow_all_types_split_comprehensive.test_slow; do
    echo -e "${YELLOW}Running $(basename $TEST_FILE)...${NC}"
    ./build/release/test/unittest "$TEST_FILE"
    if [ $? -ne 0 ]; then
        TOTAL_FAILED=$((TOTAL_FAILED + 1))
    fi
done

if [ $TOTAL_FAILED -eq 0 ]; then
    echo -e "${GREEN}All split mode tests passed!${NC}"
    exit 0
else
    echo -e "${RED}$TOTAL_FAILED test file(s) failed${NC}"
    exit 1
fi
