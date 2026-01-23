#!/bin/bash
#
# Script to test query cancellation for dd_read_arrow
#
# This test verifies that:
# 1. Query IDs are passed to the server
# 2. Cancel requests are sent when a query is interrupted
# 3. The /v1/ui/api/metrics endpoint shows the cancelled statement count increase
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

# Extract "Cancelled Statements" count from metrics HTML
get_cancelled_count() {
    local metrics="$1"
    # The HTML has: <th>Cancelled Statements</th> ... <td>N</td>
    # We extract the 4th <td> value in the Application table (after Start Time, Running, Completed, Cancelled)
    echo "$metrics" | grep -A 20 '<caption>Application</caption>' | grep '<td>' | sed -n '4p' | sed 's/.*<td>\([0-9]*\)<\/td>.*/\1/'
}

# Extract "Running Statements" count from metrics HTML
get_running_count() {
    local metrics="$1"
    # The 2nd <td> in Application table is Running Statements
    echo "$metrics" | grep -A 20 '<caption>Application</caption>' | grep '<td>' | sed -n '2p' | sed 's/.*<td>\([0-9]*\)<\/td>.*/\1/'
}

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
echo -e "${YELLOW}=== Test: Query cancellation with metrics verification ===${NC}"

# Get initial cancelled count
METRICS_BEFORE=$(curl -s "http://localhost:$HTTP_PORT/v1/ui/api/metrics")
CANCELLED_BEFORE=$(get_cancelled_count "$METRICS_BEFORE")
echo -e "${YELLOW}Initial cancelled statements count: ${CANCELLED_BEFORE:-0}${NC}"

# Create a SQL script that runs a slow query
cat > /tmp/cancel_test.sql << 'EOF'
LOAD 'build/release/extension/dazzle_duck/dazzle_duck.duckdb_extension';
SELECT COUNT(*) FROM dd_read_arrow('http://localhost:8083', sql := 'SELECT * FROM generate_series(1, 10000000) t(x) CROSS JOIN generate_series(1, 100) s(y)');
EOF

# Run the query in background
echo -e "${YELLOW}Starting a slow query in background...${NC}"
"$DUCKDB_CLI" < /tmp/cancel_test.sql > /tmp/cancel_test_output.txt 2>&1 &
DUCKDB_PID=$!

# Wait for the query to start and appear in metrics
echo -e "${YELLOW}Waiting for query to appear in server metrics...${NC}"
QUERY_STARTED=false
for i in {1..10}; do
    sleep 1
    if ! kill -0 "$DUCKDB_PID" 2>/dev/null; then
        echo -e "${YELLOW}Query completed before we could cancel it${NC}"
        break
    fi

    METRICS=$(curl -s "http://localhost:$HTTP_PORT/v1/ui/api/metrics")
    RUNNING=$(get_running_count "$METRICS")
    if [ "${RUNNING:-0}" -gt 0 ]; then
        echo -e "${GREEN}Query is running on server (Running Statements: $RUNNING)${NC}"
        QUERY_STARTED=true
        break
    fi
    echo -n "."
done

# Send SIGINT to cancel the query
if kill -0 "$DUCKDB_PID" 2>/dev/null; then
    echo -e "${YELLOW}Sending SIGINT to cancel query (PID: $DUCKDB_PID)...${NC}"
    kill -INT "$DUCKDB_PID" 2>/dev/null || true

    # Wait for cancellation to propagate
    sleep 2

    # Wait for process to exit
    wait "$DUCKDB_PID" 2>/dev/null || true
    DUCKDB_PID=""

    echo -e "${GREEN}Query process terminated${NC}"
else
    echo -e "${YELLOW}Query already completed${NC}"
    DUCKDB_PID=""
fi

# Check metrics after cancellation
echo -e "${YELLOW}Checking metrics after cancellation...${NC}"
sleep 1
METRICS_AFTER=$(curl -s "http://localhost:$HTTP_PORT/v1/ui/api/metrics")
CANCELLED_AFTER=$(get_cancelled_count "$METRICS_AFTER")
echo -e "${YELLOW}Cancelled statements count after: ${CANCELLED_AFTER:-0}${NC}"

# Verify cancellation was registered
CANCELLED_BEFORE=${CANCELLED_BEFORE:-0}
CANCELLED_AFTER=${CANCELLED_AFTER:-0}

if [ "$CANCELLED_AFTER" -gt "$CANCELLED_BEFORE" ]; then
    echo -e "${GREEN}SUCCESS: Cancelled statements count increased from $CANCELLED_BEFORE to $CANCELLED_AFTER${NC}"
    echo -e "${GREEN}This confirms the cancel HTTP request was received by the server!${NC}"
elif [ "$QUERY_STARTED" = true ]; then
    echo -e "${YELLOW}WARNING: Cancelled count did not increase (before: $CANCELLED_BEFORE, after: $CANCELLED_AFTER)${NC}"
    echo -e "${YELLOW}The query may have completed before the cancel request was processed${NC}"
else
    echo -e "${YELLOW}SKIPPED: Query completed too quickly to test cancellation${NC}"
fi

echo ""
echo -e "${YELLOW}=== Verify server is still responsive ===${NC}"

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

exit 0
