# DazzleDuck Extension for DuckDB

This extension provides functionality to query remote Arrow IPC endpoints.

## Features

- **dd_read_arrow**: Query remote servers that return Arrow IPC streams with parallel execution support
- **dd_login**: Authenticate with remote servers and obtain JWT tokens
- **dd_splits**: Inspect execution plan splits for a query
- **dd_array_contains_all**: Check if all elements in needle array exist in haystack array
- **dd_bloom_filter_create**: Create bloom filter from string array
- **dd_bloom_filter_contains**: Check if value may exist in bloom filter
- **dd_bloom_filter_contains_all**: Check if all values may exist in bloom filter
- **dd_version**: Returns the extension version

## Installation

### From Community Extensions

```sql
INSTALL dazzleduck FROM community;
LOAD dazzleduck;
```

### From GitHub Releases

Download the extension for your platform from [GitHub Releases](https://github.com/dazzleduck-web/dazzleduck-sql-duckdb/releases):

| Platform | File |
|----------|------|
| Mac ARM64 (Apple Silicon) | `dazzleduck.darwin_arm64.duckdb_extension` |
| Linux x86_64 | `dazzleduck.linux_amd64.duckdb_extension` |

Then load it in DuckDB:

```sql
-- Load from downloaded file
LOAD '/path/to/dazzleduck.duckdb_extension';

-- Verify installation
SELECT * FROM duckdb_extensions() WHERE extension_name = 'dazzleduck';
```

**Example (Mac):**
```bash
# Download
curl -L -o dazzleduck.duckdb_extension \
  "https://github.com/dazzleduck-web/dazzleduck-sql-duckdb/releases/latest/download/dazzleduck.darwin_arm64.duckdb_extension"

# Use in DuckDB
duckdb -c "LOAD 'dazzleduck.duckdb_extension'; SELECT dd_version();"
```

**Example (Linux):**
```bash
# Download
curl -L -o dazzleduck.duckdb_extension \
  "https://github.com/dazzleduck-web/dazzleduck-sql-duckdb/releases/latest/download/dazzleduck.linux_amd64.duckdb_extension"

# Use in DuckDB
duckdb -c "LOAD 'dazzleduck.duckdb_extension'; SELECT dd_version();"
```

## Usage

### Authentication

Use `dd_login` to authenticate with a remote server and obtain a JWT token:

```sql
-- Login with username, password, and claims (returns JWT token)
SELECT dd_login(
    'http://localhost:8082',
    'admin',
    'admin',
    '{"database":"demo_db","schema":"main","table":"demo"}'
) AS token;
```

Parameters:
- `url` (required): The base URL of the server
- `username` (required): Username for authentication
- `password` (required): Password for authentication
- `claims` (required): JSON string with access claims (database, schema, table)

### Querying Remote Arrow Endpoints

The `dd_read_arrow` function queries remote servers that return Arrow IPC streams via HTTP.

```sql
-- Using SQL query parameter
SELECT * FROM dd_read_arrow('http://localhost:8081', sql := 'SELECT * FROM users');

-- Using source_table parameter (passed directly to server)
SELECT * FROM dd_read_arrow('http://localhost:8081', source_table := 'mydb.schema.users');

-- With authentication
SELECT * FROM dd_read_arrow(
    'http://localhost:8082',
    source_table := 'demo_db.main.demo',
    auth_token := dd_login('http://localhost:8082', 'admin', 'admin', '{"database":"demo_db","schema":"main","table":"demo"}')
);

-- With parallel execution (split mode)
SELECT * FROM dd_read_arrow(
    'http://localhost:8082',
    source_table := 'demo_db.main.demo',
    split := true,
    auth_token := dd_login('http://localhost:8082', 'admin', 'admin', '{"database":"demo_db","schema":"main","table":"demo"}')
);

-- With split size hint (controls how splits are generated)
SELECT * FROM dd_read_arrow(
    'http://localhost:8082',
    source_table := 'demo_db.main.demo',
    split := true,
    split_size := 1000000,
    auth_token := dd_login('http://localhost:8082', 'admin', 'admin', '{"database":"demo_db","schema":"main","table":"demo"}')
);

-- With filter pushdown (filters are sent to the server)
SELECT * FROM dd_read_arrow('http://localhost:8081', sql := 'SELECT * FROM orders')
WHERE status = 'pending' AND amount > 100;

-- With projection pushdown (only selected columns are fetched from server)
SELECT name, email FROM dd_read_arrow('http://localhost:8081', source_table := 'users');

-- Aggregation pushdown (aggregations are executed on the server)
SELECT region, count(*), sum(amount)
FROM dd_read_arrow('http://localhost:8081', source_table := 'orders')
WHERE status = 'pending'
GROUP BY region;
```

#### Aggregation Pushdown

The extension includes an optimizer that automatically detects aggregation queries above
`dd_read_arrow` and rewrites them to push the aggregation to the remote server. Instead of
fetching all rows and aggregating locally, only the aggregated result is transferred.

Supported aggregations:

| Aggregate | Non-split mode | Split mode |
|-----------|:-:|:-:|
| `COUNT(*)` | Yes | Yes |
| `COUNT(col)` | Yes | Yes |
| `SUM(col)` | Yes | Yes |
| `MIN(col)` | Yes | Yes |
| `MAX(col)` | Yes | Yes |
| `AVG(col)` | Yes | No (local fallback) |
| `COUNT(DISTINCT col)` | Yes | No (local fallback) |

In split mode, only split-safe aggregations (those whose partial results can be merged across
splits) are pushed down. Non-split-safe aggregations like `AVG` and `COUNT(DISTINCT ...)` fall
back to local execution automatically.

If any aggregate in a query is not supported for pushdown, the entire query falls back to local
execution -- there is no partial pushdown.

Parameters:
- `url` (required): The base URL of the server
- `source_table`: Table identifier passed directly to the server
- `sql`: SQL query to execute on the server
- `auth_token`: JWT token for authentication (from `dd_login`)
- `split`: Enable parallel execution mode (default: false)
- `split_size`: Hint for split size in bytes (passed to server via `x-dd-split-size` header)

Note: You must provide either `source_table` or `sql`, but not both.

### Inspecting Query Splits

Use `dd_splits` to see how a query would be split for parallel execution:

```sql
-- View splits for a table
SELECT * FROM dd_splits(
    'http://localhost:8082',
    source_table := 'demo_db.main.demo',
    auth_token := dd_login('http://localhost:8082', 'admin', 'admin', '{"database":"demo_db","schema":"main","table":"demo"}')
);

-- View splits with custom split size
SELECT * FROM dd_splits(
    'http://localhost:8082',
    source_table := 'demo_db.main.demo',
    split_size := 1000000,
    auth_token := dd_login('http://localhost:8082', 'admin', 'admin', '{"database":"demo_db","schema":"main","table":"demo"}')
);

-- View splits for a custom SQL query
SELECT * FROM dd_splits(
    'http://localhost:8082',
    sql := 'SELECT * FROM demo_db.main.demo WHERE partition >= 0',
    auth_token := dd_login('http://localhost:8082', 'admin', 'admin', '{"database":"demo_db","schema":"main","table":"demo"}')
);
```

Parameters:
- `url` (required): The base URL of the server
- `source_table`: Table identifier
- `sql`: SQL query to get splits for
- `auth_token`: JWT token for authentication
- `split_size`: Hint for split size in bytes (passed via `x-dd-split-size` header)

Returns columns:
- `split_id`: Unique identifier for the split
- `query_id`: Query identifier
- `query`: The SQL query for this split
- `producer_id`: Producer identifier
- `split_size`: Size of the split in bytes

### Array Contains All

Check if all elements in a needle array exist in a haystack array:

```sql
-- Basic usage
SELECT dd_array_contains_all(['a', 'b', 'c', 'd'], ['a', 'c']);
-- Returns: true

SELECT dd_array_contains_all(['a', 'b', 'c'], ['a', 'x']);
-- Returns: false

-- With pre-computed bloom filter for optimization
SELECT dd_array_contains_all(
    haystack,
    ['item_1', 'item_2'],
    dd_bloom_filter_create(haystack)
) FROM my_table;
```

Parameters:
- `haystack` (VARCHAR[]): Array to search in
- `needle` (VARCHAR[]): Array of elements to find
- `bloom_filter` (BLOB, optional): Pre-computed bloom filter for optimization

### Bloom Filter Functions

Bloom filters are probabilistic data structures for efficient set membership testing.

```sql
-- Create bloom filter from array
SELECT dd_bloom_filter_create(['apple', 'banana', 'cherry']);

-- Create with custom parameters (bits_per_element, num_hash_functions)
SELECT dd_bloom_filter_create(['apple', 'banana', 'cherry'], 20, 5);

-- Check if single value may exist
SELECT dd_bloom_filter_contains(
    dd_bloom_filter_create(['apple', 'banana', 'cherry']),
    'banana'
);
-- Returns: true

-- Check if all values may exist
SELECT dd_bloom_filter_contains_all(
    dd_bloom_filter_create(['a', 'b', 'c', 'd', 'e']),
    ['a', 'c', 'e']
);
-- Returns: true
```

**Note:** Bloom filters may have false positives but never false negatives. Only VARCHAR arrays are supported.

## Building

Clone with submodules:
```bash
git clone --recurse-submodules <repo-url>
cd duckdb-nanoarrow
```

Build:
```bash
make
```

Build outputs:
- `./build/release/duckdb` - DuckDB shell with extension loaded
- `./build/release/test/unittest` - Test runner
- `./build/release/extension/dazzleduck/dazzleduck.duckdb_extension` - Loadable extension

## Running Tests

### Unit Tests

Run all unit tests (no external dependencies):
```bash
./build/release/test/unittest
```

Run specific test files:
```bash
# Test the main extension
./build/release/test/unittest --test-dir . "*dazzleduck*"

# Test dd_read_arrow function
./build/release/test/unittest --test-dir . "*dd_read_arrow*"
```

### Integration Tests

Integration tests require a running DazzleDuck server.

**Option 1: Automated script**
```bash
./scripts/run_integration_tests.sh
```
This script:
- Starts a Docker container with dazzleduck server
- Waits for server readiness
- Runs integration tests
- Cleans up the container

**Option 2: Manual**
```bash
# Start the server
docker run -d -p 8081:8081 -p 59307:59307 dazzleduck/dazzleduck:latest --conf warehouse=/data

# Run integration tests
./build/release/test/unittest --test-dir . "*dd_read_arrow_integration*"

# Stop container when done
docker stop <container-id>
```

### Split Mode Integration Tests

Split mode tests require a DuckDB server with DuckLake configured:

```bash
# Start DuckLake test server (different port)
docker run -d --name ducklake-test -p 8082:8081 -p 59308:59307 \
    dazzleduck/dazzleduck:latest \
    --conf 'dazzleduck_server.access_mode=RESTRICTED' \
    --conf 'dazzleduck_server.startup_script_provider.script_location="/startup/ducklake.sql"'

# Wait for server to be ready
curl -s --retry 30 --retry-delay 2 --retry-connrefused http://localhost:8082/health

# Run split tests
./build/release/test/unittest --test-dir . "*dd_read_arrow_split*"

# Cleanup
docker stop ducklake-test && docker rm ducklake-test
```

### Aggregation Pushdown Tests

```bash
# Non-split mode (requires basic server on port 8081)
docker run -d -p 8081:8081 dazzleduck/dazzleduck:latest --conf warehouse=/data
./build/release/test/unittest --test-dir . "*dd_read_arrow_aggregation_pushdown.test_slow*"

# Split mode (requires DuckLake server on port 8082, see split mode setup above)
./build/release/test/unittest --test-dir . "*aggregation_pushdown_split*"
```

## Debugging

Launch an interactive debug session:
```bash
lldb build/release/duckdb
```

Or use VSCode with the CodeLLDB extension (Command Palette: *LLDB: Attach to process*).

## VSCode Integration

For CMake/clangd integration:
```bash
cp CMakeUserPresets.json duckdb/
```

Add to `.vscode/settings.json`:
```json
{
    "cmake.sourceDirectory": "${workspaceFolder}/duckdb"
}
```

Then reload the window and select the *Extension (Debug build)* preset.
