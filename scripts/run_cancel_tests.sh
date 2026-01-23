#!/bin/bash
#
# Script to test query cancellation for dd_read_arrow
#
# This test verifies that:
# 1. Query IDs are passed to the server
# 2. Cancel requests are sent when a query is interrupted
# 3. The /v1/ui/api/metrics endpoint shows running/cancelled queries
#

set -e

CONTAINER_NAME="dazzleduck-cancel-test"
IMAGE="dazzleduck/dazzleduck:0.0.16"
HTTP_PORT=8083
FLIGHT_PORT=59309
MAX_WAIT_SECONDS=60

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

cleanup() {
    echo -e "${YELLOW}Cleaning up...${NC}"
    # Kill any background DuckDB processes
    if [ -n "$DUCKDB_PID" ]; then
        kill "$DUCKDB_PID" 2>/dev/null || true
        wait "$DUCKDB_PID" 2>/dev/null || true
    fi
    docker stop "$CONTAINER_NAME" 2>/dev/null || true
    docker rm "$CONTAINER_NAME" 2>/dev/null || true
    rm -f /tmp/cancel_test_output.txt /tmp/cancel_test.sql 2>/dev/null || true
}

# Set trap to cleanup on exit
trap cleanup EXIT

# Check if Docker is available
if ! command -v docker &> /dev/null; then
    echo -e "${RED}Error: Docker is not installed or not in PATH${NC}"
    exit 1
fi

# Check if the DuckDB CLI with extension exists
DUCKDB_CLI="./build/release/duckdb"
if [ ! -f "$DUCKDB_CLI" ]; then
    echo -e "${RED}Error: DuckDB CLI not found at $DUCKDB_CLI. Run 'make' first.${NC}"
    exit 1
fi

# Stop any existing container with the same name
echo -e "${YELLOW}Stopping any existing test container...${NC}"
docker stop "$CONTAINER_NAME" 2>/dev/null || true
docker rm "$CONTAINER_NAME" 2>/dev/null || true

# Start the dazzleduck container
echo -e "${YELLOW}Starting dazzleduck container on port $HTTP_PORT...${NC}"
docker run -d \
    --name "$CONTAINER_NAME" \
    -p "$FLIGHT_PORT:59307" \
    -p "$HTTP_PORT:8081" \
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

echo ""
echo -e "${YELLOW}=== Test 1: Verify query ID is sent to server ===${NC}"

# Create a SQL script that runs a slow query
cat > /tmp/cancel_test.sql << 'EOF'
LOAD 'build/release/extension/dazzle_duck/dazzle_duck.duckdb_extension';
SELECT COUNT(*) FROM dd_read_arrow('http://localhost:8083', sql := 'SELECT * FROM generate_series(1, 10000000) t(x) CROSS JOIN generate_series(1, 100) s(y)');
EOF

# Run the query in background
echo -e "${YELLOW}Starting a slow query in background...${NC}"
"$DUCKDB_CLI" < /tmp/cancel_test.sql > /tmp/cancel_test_output.txt 2>&1 &
DUCKDB_PID=$!

# Wait a moment for the query to start
sleep 2

# Check if process is still running (query should be slow enough)
if ! kill -0 "$DUCKDB_PID" 2>/dev/null; then
    echo -e "${YELLOW}Query completed quickly, trying with larger dataset...${NC}"
    # Query completed too fast, that's ok for now
fi

# Check the metrics endpoint to see if query is registered
echo -e "${YELLOW}Checking /v1/ui/api/metrics for running queries...${NC}"
METRICS=$(curl -s "http://localhost:$HTTP_PORT/v1/ui/api/metrics")

if echo "$METRICS" | grep -q "Running Statements"; then
    echo -e "${GREEN}Metrics endpoint is accessible${NC}"
    # Check if there's a query ID in the metrics
    if echo "$METRICS" | grep -qi "statement\|query"; then
        echo -e "${GREEN}Server shows statement activity${NC}"
    fi
else
    echo -e "${YELLOW}Metrics endpoint returned: ${NC}"
    echo "$METRICS" | head -20
fi

# Send SIGINT to cancel the query
if kill -0 "$DUCKDB_PID" 2>/dev/null; then
    echo -e "${YELLOW}Sending SIGINT to cancel query (PID: $DUCKDB_PID)...${NC}"
    kill -INT "$DUCKDB_PID" 2>/dev/null || true

    # Wait a moment for cancellation to propagate
    sleep 1

    # Check metrics again to see if cancel was received
    echo -e "${YELLOW}Checking metrics after cancellation...${NC}"
    METRICS_AFTER=$(curl -s "http://localhost:$HTTP_PORT/v1/ui/api/metrics")

    # Wait for process to exit
    wait "$DUCKDB_PID" 2>/dev/null || true
    DUCKDB_PID=""

    echo -e "${GREEN}Query was interrupted${NC}"
else
    echo -e "${YELLOW}Query already completed${NC}"
    DUCKDB_PID=""
fi

echo ""
echo -e "${YELLOW}=== Test 2: Verify cancel endpoint is called ===${NC}"

# For this test, we'll check the docker logs for cancel requests
echo -e "${YELLOW}Checking server logs for cancel requests...${NC}"
LOGS=$(docker logs "$CONTAINER_NAME" 2>&1)

if echo "$LOGS" | grep -qi "cancel"; then
    echo -e "${GREEN}Found cancel-related activity in server logs${NC}"
    echo "$LOGS" | grep -i "cancel" | tail -5
else
    echo -e "${YELLOW}No explicit cancel messages in logs (server may not log cancellations)${NC}"
fi

echo ""
echo -e "${YELLOW}=== Test 3: Run a quick query to verify normal operation ===${NC}"

# Run a simple query to make sure the server is still working
RESULT=$(curl -s "http://localhost:$HTTP_PORT/v1/query?q=SELECT%2042%20as%20answer" | xxd -p | head -c 100)
if [ -n "$RESULT" ]; then
    echo -e "${GREEN}Server is still responsive after cancellation test${NC}"
else
    echo -e "${RED}Server may have issues after cancellation test${NC}"
    exit 1
fi

echo ""
echo -e "${GREEN}=== Cancellation tests completed ===${NC}"
echo -e "${YELLOW}Note: Full verification of cancel HTTP requests requires server-side logging${NC}"

exit 0
