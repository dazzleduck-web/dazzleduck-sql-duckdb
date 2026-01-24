#!/bin/bash
#
# Build Linux extension using Docker
#
# Usage:
#   ./docker/build-linux.sh          # Build extension (requires base image)
#   ./docker/build-linux.sh --base   # Build base image first (slow, ~10 min)
#   ./docker/build-linux.sh --all    # Build both base and extension
#
set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ROOT_DIR="$(dirname "$SCRIPT_DIR")"
PLATFORM="linux/amd64"
DUCKDB_VERSION="v1.4.3"
BASE_IMAGE="duckdb-builder:${DUCKDB_VERSION}"
EXT_IMAGE="dazzle-duck-extension"

cd "$ROOT_DIR"

build_base() {
    echo "Building DuckDB base image (this takes ~10 minutes)..."
    docker build \
        --platform "$PLATFORM" \
        --build-arg DUCKDB_VERSION="$DUCKDB_VERSION" \
        -t "$BASE_IMAGE" \
        -f docker/Dockerfile.duckdb-base \
        .
    echo "Base image built: $BASE_IMAGE"
}

build_extension() {
    echo "Building extension..."
    docker build \
        --platform "$PLATFORM" \
        -t "$EXT_IMAGE" \
        -f docker/Dockerfile.extension \
        .
    echo "Extension image built: $EXT_IMAGE"

    # Extract the extension
    mkdir -p dist/v1.4.3
    docker run --rm --platform "$PLATFORM" "$EXT_IMAGE" > dist/v1.4.3/dazzleduck.linux_amd64.duckdb_extension
    echo "Extension extracted to: dist/v1.4.3/dazzleduck.linux_amd64.duckdb_extension"
    ls -lh dist/v1.4.3/dazzleduck.linux_amd64.duckdb_extension
}

case "${1:-}" in
    --base)
        build_base
        ;;
    --all)
        build_base
        build_extension
        ;;
    --help|-h)
        echo "Usage: $0 [--base|--all|--help]"
        echo ""
        echo "Options:"
        echo "  (none)    Build extension only (requires base image)"
        echo "  --base    Build base image only (slow, ~10 min)"
        echo "  --all     Build both base and extension"
        echo "  --help    Show this help"
        ;;
    *)
        build_extension
        ;;
esac
