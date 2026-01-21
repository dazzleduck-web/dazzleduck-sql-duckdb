# Building the Extension

This document describes how to build the dazzle_duck extension for different platforms.

## Quick Start

```bash
# Clone with submodules
git clone --recurse-submodules https://github.com/your-repo/duckdb-nanoarrow.git
cd duckdb-nanoarrow

# Build
make release

# Test
make test
```

## Platform-Specific Instructions

### Linux (Ubuntu/Debian)

```bash
# Install dependencies
sudo apt-get update
sudo apt-get install -y build-essential cmake ninja-build git

# Build
make release

# Test
make test
```

### Linux (RHEL/CentOS/Fedora)

```bash
# Install dependencies
sudo dnf install -y gcc gcc-c++ cmake ninja-build git

# Build
make release
```

### macOS

```bash
# Install Xcode command line tools
xcode-select --install

# Install dependencies (using Homebrew)
brew install cmake ninja

# Build
make release
```

### Windows (Visual Studio)

**Prerequisites:**
- Visual Studio 2019 or 2022 with "Desktop development with C++" workload
- CMake (https://cmake.org/download/)
- Git (https://git-scm.com/)

```powershell
# Open "Developer Command Prompt for VS 2022"
mkdir build\release
cd build\release

cmake -G "Visual Studio 17 2022" -A x64 ^
    -DEXTENSION_STATIC_BUILD=1 ^
    -DDUCKDB_EXTENSION_CONFIGS="..\..\extension_config.cmake" ^
    -DCMAKE_BUILD_TYPE=Release ^
    ..\..\duckdb

cmake --build . --config Release
```

### Windows (MSYS2/MinGW)

```bash
# Install MSYS2 from https://www.msys2.org/

# In MSYS2 MINGW64 terminal:
pacman -Syu
pacman -S mingw-w64-x86_64-toolchain mingw-w64-x86_64-cmake ninja git make

# Build
make release
```

## Build Output

After building, find the extension at:

```
build/release/extension/dazzle_duck/dazzle_duck.duckdb_extension
```

The DuckDB CLI with the extension built-in:

```
build/release/duckdb          # Linux/macOS
build/release/duckdb.exe      # Windows
```

## Running Tests

```bash
# Run all extension tests
make test

# Or run specific tests
./build/release/test/unittest "*dazzle_duck*"
./build/release/test/unittest "*array_contains_all*"
./build/release/test/unittest "*bloom_filter*"
```

## Loading the Extension

### Statically Linked (Built-in)

When using the DuckDB CLI built from this repo, the extension is automatically loaded:

```sql
SELECT nanoarrow_version();
```

### Dynamically Loaded

To load the extension into a standard DuckDB installation:

```sql
-- Load from file
LOAD '/path/to/dazzle_duck.duckdb_extension';

-- Verify
SELECT nanoarrow_version();
```

## Troubleshooting

### CMake Version

Requires CMake 3.5 or higher:

```bash
cmake --version
```

### Submodules Not Initialized

If DuckDB source is missing:

```bash
git submodule update --init --recursive
```

### Windows: "cmake not found"

Ensure CMake is in your PATH, or use the full path:

```powershell
& "C:\Program Files\CMake\bin\cmake.exe" --version
```

### Linux: Ninja Not Found

Fall back to Make:

```bash
# Instead of make release, use:
mkdir -p build/release
cmake -DEXTENSION_STATIC_BUILD=1 \
      -DDUCKDB_EXTENSION_CONFIGS="$(pwd)/extension_config.cmake" \
      -DCMAKE_BUILD_TYPE=Release \
      -S ./duckdb/ -B build/release
cmake --build build/release
```
