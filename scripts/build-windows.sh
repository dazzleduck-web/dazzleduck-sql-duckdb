#!/bin/bash
#
# Build Windows extension natively using MSVC (run from WSL)
#
# Usage:
#   ./scripts/build-windows.sh          # Build extension
#   ./scripts/build-windows.sh --clean  # Clean and rebuild
#
set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ROOT_DIR="$(dirname "$SCRIPT_DIR")"

# Convert WSL path to Windows path
WIN_ROOT=$(wslpath -w "$ROOT_DIR")
WIN_SCRIPT="$WIN_ROOT\\scripts\\build-windows-native.ps1"

echo "Building Windows extension using MSVC..."
echo "Project: $WIN_ROOT"

# Run PowerShell script
if [[ "$1" == "--clean" ]]; then
    powershell.exe -ExecutionPolicy Bypass -File "$WIN_SCRIPT" -Clean
else
    powershell.exe -ExecutionPolicy Bypass -File "$WIN_SCRIPT"
fi

echo ""
echo "Output in: dist/v1.4.3/"
ls -lh "$ROOT_DIR/dist/v1.4.3/"*.duckdb_extension 2>/dev/null || true
