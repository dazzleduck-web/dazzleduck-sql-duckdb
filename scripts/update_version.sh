#!/bin/bash
# Update version in CMakeLists.txt and description.yml from version.txt

set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ROOT_DIR="$(dirname "$SCRIPT_DIR")"

VERSION=$(cat "$ROOT_DIR/version.txt" | tr -d '[:space:]')

if [ -z "$VERSION" ]; then
    echo "Error: version.txt is empty"
    exit 1
fi

echo "Updating version to: $VERSION"

# Update CMakeLists.txt
sed -i '' "s/set(EXTENSION_VERSION.*/set(EXTENSION_VERSION \"$VERSION\")/" "$ROOT_DIR/CMakeLists.txt"
echo "  Updated CMakeLists.txt"

# Update extension_config.cmake
sed -i '' "s/EXTENSION_VERSION.*/EXTENSION_VERSION \"$VERSION\"/" "$ROOT_DIR/extension_config.cmake"
echo "  Updated extension_config.cmake"

# Update description.yml
sed -i '' "s/^  version:.*/  version: $VERSION/" "$ROOT_DIR/description.yml"
echo "  Updated description.yml"

echo "Done!"
