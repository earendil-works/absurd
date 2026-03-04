#!/bin/bash
set -euo pipefail

# Preparing release assets
# Usage: ./scripts/prepare-release.sh <version>

VERSION=$1
ARTIFACTS_DIR="artifacts"
RELEASE_DIR="release"

echo "Preparing release assets for version: $VERSION"

mkdir -p "$RELEASE_DIR"

# Copy habitat binaries and rename them
cp "$ARTIFACTS_DIR/habitat-linux-x86_64/habitat" "$RELEASE_DIR/habitat-linux-x86_64"
cp "$ARTIFACTS_DIR/habitat-linux-arm64/habitat" "$RELEASE_DIR/habitat-linux-arm64"
cp "$ARTIFACTS_DIR/habitat-macos-arm64/habitat" "$RELEASE_DIR/habitat-macos-arm64"

# Make binaries executable
chmod +x "$RELEASE_DIR/habitat-"*

# Copy absurdctl
cp absurdctl "$RELEASE_DIR/absurdctl"
chmod +x "$RELEASE_DIR/absurdctl"

# Copy SQL file
cp sql/absurd.sql "$RELEASE_DIR/absurd.sql"

echo "Release version: $VERSION"

# Copy migration files for this release
# This includes migrations like 0.0.3-0.0.4.sql and potentially 0.0.1-0.0.4.sql
if ls sql/migrations/*-"${VERSION}".sql 1> /dev/null 2>&1; then
  cp sql/migrations/*-"${VERSION}".sql "$RELEASE_DIR/"
  echo "Copied migration files:"
  ls -la sql/migrations/*-"${VERSION}".sql
else
  echo "No migration files found for version $VERSION"
fi

echo "Release preparation complete. Assets in $RELEASE_DIR:"
