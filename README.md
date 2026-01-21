# DazzleDuck Extension for DuckDB

This extension provides functionality to query remote Arrow IPC endpoints.

## Features

- **read_arrow_dd**: Query remote servers that return Arrow IPC streams with parallel execution support
- **dd_login**: Authenticate with remote servers and obtain JWT tokens
- **dd_splits**: Inspect execution plan splits for a query
- **dd_search**: Search for tables on remote servers

## Installation

```sql
INSTALL dazzle_duck FROM community;
LOAD dazzle_duck;
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

The `read_arrow_dd` function queries remote servers that return Arrow IPC streams via HTTP.

```sql
-- Using SQL query parameter
SELECT * FROM read_arrow_dd('http://localhost:8081', sql := 'SELECT * FROM users');

-- Using source_table parameter (passed directly to server)
SELECT * FROM read_arrow_dd('http://localhost:8081', source_table := 'mydb.schema.users');

-- With authentication
SELECT * FROM read_arrow_dd(
    'http://localhost:8082',
    source_table := 'demo_db.main.demo',
    auth_token := dd_login('http://localhost:8082', 'admin', 'admin', '{"database":"demo_db","schema":"main","table":"demo"}')
);

-- With parallel execution (split mode)
SELECT * FROM read_arrow_dd(
    'http://localhost:8082',
    source_table := 'demo_db.main.demo',
    split := true,
    auth_token := dd_login('http://localhost:8082', 'admin', 'admin', '{"database":"demo_db","schema":"main","table":"demo"}')
);

-- With split size hint (controls how splits are generated)
SELECT * FROM read_arrow_dd(
    'http://localhost:8082',
    source_table := 'demo_db.main.demo',
    split := true,
    split_size := 1000000,
    auth_token := dd_login('http://localhost:8082', 'admin', 'admin', '{"database":"demo_db","schema":"main","table":"demo"}')
);

-- With filter pushdown (filters are sent to the server)
SELECT * FROM read_arrow_dd('http://localhost:8081', sql := 'SELECT * FROM orders')
WHERE status = 'pending' AND amount > 100;

-- With projection pushdown (only selected columns are fetched from server)
SELECT name, email FROM read_arrow_dd('http://localhost:8081', source_table := 'users');
```

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
- `./build/release/extension/dazzle_duck/dazzle_duck.duckdb_extension` - Loadable extension

## Running Tests

### Unit Tests

Run all unit tests (no external dependencies):
```bash
./build/release/test/unittest
```

Run specific test files:
```bash
# Test the main extension
./build/release/test/unittest --test-dir . "*dazzle_duck*"

# Test read_arrow_dd function
./build/release/test/unittest --test-dir . "*read_arrow_dd*"
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
./build/release/test/unittest --test-dir . "*read_arrow_dd_integration*"

# Stop container when done
docker stop <container-id>
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
