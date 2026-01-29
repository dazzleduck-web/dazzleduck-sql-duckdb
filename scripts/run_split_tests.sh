#!/bin/bash
#
# Script to run dd_read_arrow split mode tests with DuckLake server
#

set -e

CONTAINER_NAME="ducklake-test"
IMAGE="dazzleduck/dazzleduck:0.0.17"
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

# Start the DuckLake container with restricted access mode
echo -e "${YELLOW}Starting DuckLake container on port $HTTP_PORT...${NC}"
docker run -d \
    --name "$CONTAINER_NAME" \
    -p "$HTTP_PORT:8081" \
    -p "$FLIGHT_PORT:59307" \
    "$IMAGE" \
    --conf 'dazzleduck_server.access_mode=RESTRICTED' \
    --conf 'dazzleduck_server.startup_script_provider.script_location="/startup/ducklake.sql"'

# Wait for the server to be ready
echo -e "${YELLOW}Waiting for DuckLake server to be ready...${NC}"
WAIT_COUNT=0
while [ $WAIT_COUNT -lt $MAX_WAIT_SECONDS ]; do
    # Try to connect to the HTTP endpoint
    if curl -s -o /dev/null -w "%{http_code}" "http://localhost:$HTTP_PORT/health" 2>/dev/null | grep -q "200"; then
        echo -e "${GREEN}DuckLake server is ready!${NC}"
        break
    fi

    # Check if container is still running
    if ! docker ps -q -f name="$CONTAINER_NAME" | grep -q .; then
        echo -e "${RED}Error: Container stopped unexpectedly${NC}"
        docker logs "$CONTAINER_NAME"
        exit 1
    fi

    sleep 1
    WAIT_COUNT=$((WAIT_COUNT + 1))
    echo -n "."
done

if [ $WAIT_COUNT -ge $MAX_WAIT_SECONDS ]; then
    echo -e "${RED}Error: DuckLake server did not become ready within $MAX_WAIT_SECONDS seconds${NC}"
    docker logs "$CONTAINER_NAME"
    exit 1
fi

# Run the split mode tests
echo -e "${YELLOW}Running split mode tests...${NC}"
./build/release/test/unittest --test-dir . "*dd_read_arrow_split*"
TEST_RESULT=$?

if [ $TEST_RESULT -eq 0 ]; then
    echo -e "${GREEN}All split mode tests passed!${NC}"
else
    echo -e "${RED}Some split tests failed${NC}"
fi

exit $TEST_RESULT
