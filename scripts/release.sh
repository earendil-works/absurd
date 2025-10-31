#!/bin/bash

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Get the directory of this script
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
SDK_DIR="$PROJECT_ROOT/sdks/typescript"

# Function to print colored messages
error() {
    echo -e "${RED}Error: $1${NC}" >&2
}

success() {
    echo -e "${GREEN}$1${NC}"
}

info() {
    echo -e "${YELLOW}$1${NC}"
}

# Check if we're in a git repository
if ! git rev-parse --git-dir > /dev/null 2>&1; then
    error "Not in a git repository"
    exit 1
fi

# Check if working directory is clean
if [[ -n $(git status -s) ]]; then
    error "Working directory is not clean. Please commit or stash your changes."
    exit 1
fi

# Function to display usage
usage() {
    cat << EOF
Usage: $0 [VERSION_TYPE|VERSION]

Bump the npm package version and create a git tag for release.

Arguments:
    VERSION_TYPE    One of: major, minor, patch (uses npm version)
    VERSION         Specific version number (e.g., 1.2.3)

Examples:
    $0 patch        # Bump patch version (0.1.0 -> 0.1.1)
    $0 minor        # Bump minor version (0.1.0 -> 0.2.0)
    $0 major        # Bump major version (0.1.0 -> 1.0.0)
    $0 1.2.3        # Set version to 1.2.3

The script will:
1. Update the version in sdks/typescript/package.json
2. Create a git commit with the version change
3. Create a git tag (without 'v' prefix, e.g., '1.0.0')
4. Ask if you want to push the changes and tag
EOF
}

# Check arguments
if [[ $# -ne 1 ]]; then
    usage
    exit 1
fi

VERSION_ARG="$1"

# Validate version argument
if [[ "$VERSION_ARG" =~ ^(major|minor|patch)$ ]]; then
    VERSION_TYPE="$VERSION_ARG"
elif [[ "$VERSION_ARG" =~ ^[0-9]+\.[0-9]+\.[0-9]+(-.*)?$ ]]; then
    VERSION_TYPE="$VERSION_ARG"
else
    error "Invalid version argument: $VERSION_ARG"
    echo ""
    usage
    exit 1
fi

# Navigate to SDK directory
cd "$SDK_DIR"

# Get current version
CURRENT_VERSION=$(node -p "require('./package.json').version")
info "Current version: $CURRENT_VERSION"

# Update version using npm version
# --no-git-tag-version prevents npm from creating a git tag (we'll do it manually)
info "Bumping version to $VERSION_TYPE..."
NEW_VERSION=$(npm version "$VERSION_TYPE" --no-git-tag-version)

# Remove 'v' prefix if npm added it
NEW_VERSION="${NEW_VERSION#v}"

success "Version updated to: $NEW_VERSION"

# Go back to project root for git operations
cd "$PROJECT_ROOT"

# Check for pending migration (migration with -main.sql suffix)
info "Checking for pending migrations..."
if ls "$PROJECT_ROOT"/sql/migrations/*-main.sql 1> /dev/null 2>&1; then
    error "Found pending migration(s) with -main.sql suffix:"
    ls "$PROJECT_ROOT"/sql/migrations/*-main.sql
    echo ""
    error "Please rename the migration to associate it with a release version before releasing."
    echo "  Example: mv sql/migrations/X.X.X-main.sql sql/migrations/$NEW_VERSION.sql"
    exit 1
fi

# Check if CHANGELOG.md has a section for the new version
info "Checking CHANGELOG.md..."
if ! grep -q "^# $NEW_VERSION" "$PROJECT_ROOT/CHANGELOG.md"; then
    error "CHANGELOG.md does not have a section for version $NEW_VERSION"
    echo ""
    echo "Please add a changelog section for this release:"
    echo "  1. Move the 'Unreleased' changes to a new '# $NEW_VERSION' section"
    echo "  2. Create a new empty 'Unreleased' section at the top"
    exit 1
fi

# Check if there's a migration for this version
info "Checking for migration..."
MIGRATION_FILE="$PROJECT_ROOT/sql/migrations/$NEW_VERSION.sql"
if [[ ! -f "$MIGRATION_FILE" ]]; then
    echo ""
    info "Warning: No migration file found at sql/migrations/$NEW_VERSION.sql"
    read -p "Is this expected (no schema changes in this release)? (y/N) " -n 1 -r
    echo
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        error "Release cancelled. Please create the migration or fix the version."
        exit 1
    fi
fi

# Update package-lock.json to reflect the new version
info "Updating lockfile..."
cd "$SDK_DIR"
npm install --package-lock-only
cd "$PROJECT_ROOT"

# Commit the version change
info "Creating git commit..."
git add sdks/typescript/package.json sdks/typescript/package-lock.json
git commit -m "Release $NEW_VERSION"

# Create git tag without 'v' prefix
info "Creating git tag: $NEW_VERSION"
git tag "$NEW_VERSION"

success "Successfully created release $NEW_VERSION"
echo ""
info "Next steps:"
echo "  To push the changes and trigger the release:"
echo "    git push origin main && git push origin $NEW_VERSION"
echo ""

# Ask if user wants to push
read -p "Do you want to push the changes and tag now? (y/N) " -n 1 -r
echo
if [[ $REPLY =~ ^[Yy]$ ]]; then
    info "Pushing changes and tag..."
    git push origin main
    git push origin "$NEW_VERSION"
    success "Release pushed successfully!"
    echo ""
    info "The CI will now:"
    echo "  - Build habitat binaries for multiple platforms"
    echo "  - Create a GitHub release with the binaries"
    echo "  - Publish the npm package to the registry"
else
    info "Skipping push. You can push manually later with:"
    echo "  git push origin main && git push origin $NEW_VERSION"
fi
