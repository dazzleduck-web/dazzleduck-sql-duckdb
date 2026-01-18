#!/bin/bash
#
# Script to run read_arrow_dd integration tests with dazzleduck server
#

set -e

CONTAINER_NAME="dazzleduck-test"
IMAGE="dazzleduck/dazzleduck:latest"
HTTP_PORT=8081
FLIGHT_PORT=59307
MAX_WAIT_SECONDS=60

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

cleanup() {
    echo -e "${YELLOW}Cleaning up...${NC}"
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
echo -e "${YELLOW}Stopping any existing test container...${NC}"
docker stop "$CONTAINER_NAME" 2>/dev/null || true
docker rm "$CONTAINER_NAME" 2>/dev/null || true

# Start the dazzleduck container
echo -e "${YELLOW}Starting dazzleduck container...${NC}"
docker run -d \
    --name "$CONTAINER_NAME" \
    -p "$FLIGHT_PORT:$FLIGHT_PORT" \
    -p "$HTTP_PORT:$HTTP_PORT" \
    "$IMAGE" \
    --conf warehouse=/data

# Wait for the server to be ready
echo -e "${YELLOW}Waiting for server to be ready...${NC}"
WAIT_COUNT=0
while [ $WAIT_COUNT -lt $MAX_WAIT_SECONDS ]; do
    # Try to connect to the HTTP endpoint
    if curl -s -o /dev/null -w "%{http_code}" "http://localhost:$HTTP_PORT/v1/query?q=SELECT%201" 2>/dev/null | grep -q "200"; then
        echo -e "${GREEN}Server is ready!${NC}"
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
    echo -e "${RED}Error: Server did not become ready within $MAX_WAIT_SECONDS seconds${NC}"
    docker logs "$CONTAINER_NAME"
    exit 1
fi

# Run the integration tests
echo -e "${YELLOW}Running integration tests...${NC}"
./build/release/test/unittest --test-dir . "*read_arrow_dd_integration*"
TEST_RESULT=$?

if [ $TEST_RESULT -eq 0 ]; then
    echo -e "${GREEN}All integration tests passed!${NC}"
else
    echo -e "${RED}Some tests failed${NC}"
fi

exit $TEST_RESULT
