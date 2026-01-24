# Building the Extension

This document describes how to build the dazzleduck extension for different platforms.

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

### Windows (Native MSVC via WSL)

The recommended way to build for Windows is using native MSVC from WSL. This produces extensions compatible with official DuckDB Windows releases.

**Prerequisites (one-time setup):**

```powershell
# Install Visual Studio Build Tools (from PowerShell as admin)
winget install Microsoft.VisualStudio.2022.BuildTools --override "--add Microsoft.VisualStudio.Workload.VCTools --includeRecommended --passive"

# Install Git for Windows
winget install Git.Git
```

**Build:**

```bash
# From WSL, run the build script
./scripts/build-windows.sh

# For a clean rebuild
./scripts/build-windows.sh --clean
```

Output: `dist/v1.4.3/dazzleduck.windows_amd64.duckdb_extension`

The build uses `C:\duckdb-build\` as the working directory on Windows.

### Windows (Visual Studio - Manual)

If you prefer building directly on Windows without WSL:

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

## Docker Builds

Docker builds are available for Linux cross-compilation.

### Linux Build (Docker)

```bash
# First time: build the base image (slow, ~10 minutes)
./docker/build-linux.sh --base

# Build extension
./docker/build-linux.sh

# Or build both in one step
./docker/build-linux.sh --all
```

Output: `dist/v1.4.3/dazzleduck.linux_amd64.duckdb_extension`

### Build All Platforms

```bash
# Linux (Docker)
./docker/build-linux.sh --all

# Windows (native MSVC via WSL)
./scripts/build-windows.sh

# Check outputs
ls -la dist/v1.4.3/
```

## Build Output

After building, find the extension at:

```
build/release/extension/dazzleduck/dazzleduck.duckdb_extension
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
./build/release/test/unittest "*dazzleduck*"
./build/release/test/unittest "*array_contains_all*"
./build/release/test/unittest "*bloom_filter*"
```

## Loading the Extension

### Statically Linked (Built-in)

When using the DuckDB CLI built from this repo, the extension is automatically loaded:

```sql
SELECT dd_version();
```

### Dynamically Loaded

To load the extension into a standard DuckDB installation:

```sql
-- Load from file
LOAD '/path/to/dazzleduck.duckdb_extension';

-- Verify
SELECT dd_version();
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

## Versioning

The extension version is managed in `version.txt` (single source of truth).

### Updating the Version

```bash
# 1. Edit version.txt with new version
echo "0.0.2" > version.txt

# 2. Run the update script to sync CMakeLists.txt and description.yml
./scripts/update_version.sh

# 3. Commit and push
git add -A
git commit -m "Bump version to 0.0.2"
git push
```

### Version Files

| File | Purpose |
|------|---------|
| `version.txt` | Single source of truth |
| `CMakeLists.txt` | Build-time version (synced by script) |
| `extension_config.cmake` | Extension version for dd_version() (synced by script) |
| `description.yml` | Community extension version (synced by script) |

### Check Current Version

```sql
SELECT dd_version();
```
