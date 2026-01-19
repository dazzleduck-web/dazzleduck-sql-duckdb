# DazzleDuck Extension for DuckDB

This extension provides functionality to query remote Arrow IPC endpoints.

## Features

- **read_arrow_dd**: Query remote servers that return Arrow IPC streams

## Installation

```sql
INSTALL dazzle_duck FROM community;
LOAD dazzle_duck;
```

## Usage

### Querying Remote Arrow Endpoints

The `read_arrow_dd` function queries remote servers that return Arrow IPC streams via HTTP.

```sql
-- Using SQL query parameter
SELECT * FROM read_arrow_dd('http://localhost:8081', sql := 'SELECT * FROM users');

-- Using source_table parameter (passed directly to server)
SELECT * FROM read_arrow_dd('http://localhost:8081', source_table := 'mydb.schema.users');

-- With filter pushdown (filters are sent to the server)
SELECT * FROM read_arrow_dd('http://localhost:8081', sql := 'SELECT * FROM orders')
WHERE status = 'pending' AND amount > 100;

-- With limit pushdown (include LIMIT in the SQL query)
SELECT * FROM read_arrow_dd('http://localhost:8081', sql := 'SELECT * FROM orders LIMIT 100');

-- With projection pushdown (only selected columns are fetched from server)
SELECT name, email FROM read_arrow_dd('http://localhost:8081', source_table := 'users');

-- Projection with expressions (column 'a' is fetched, 'a+1' computed locally)
SELECT a, a+1 FROM read_arrow_dd('http://localhost:8081', sql := 'SELECT * FROM numbers');
```

Parameters:
- `url` (required): The base URL of the server
- `source_table`: Table identifier passed directly to the server
- `sql`: SQL query to execute on the server (can include LIMIT for limit pushdown)

Note: You must provide either `source_table` or `sql`, but not both.

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
