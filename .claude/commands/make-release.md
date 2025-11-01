Make a release of Absurd.

Release type: "$ARGUMENTS"

## Step-by-Step Process:

### 1. Determine release type

If no release type is provided via `$ARGUMENTS`, ask the user which type of release to make:
- `patch` - Bug fixes and minor updates (0.0.3 -> 0.0.4)
- `minor` - New features, backward compatible (0.0.3 -> 0.1.0)
- `major` - Breaking changes (0.0.3 -> 1.0.0)

### 2. Update the changelog

Run the `/update-changelog` command to ensure the changelog is up to date with recent changes.

### 3. Get the new version number

Before making changes, determine what the new version will be by running:

```bash
cd sdks/typescript
NEW_VERSION=$(npm version $RELEASE_TYPE --no-git-tag-version | sed 's/^v//')
git checkout package.json package-lock.json  # Revert the changes
cd ../..
echo $NEW_VERSION
```

This shows what the new version will be without actually making changes yet.

### 4. Update CHANGELOG.md

Edit the `CHANGELOG.md` file:
- Change the `# Unreleased` heading to `# $NEW_VERSION`
- Add a new `# Unreleased` section at the top (empty for now)

### 5. Rename pending migrations

If there are any migration files with `-main.sql` suffix in `sql/migrations/`, rename them to use the new version number. Note that the format is `OLD-NEW.sql`:

```bash
# Check for pending migrations
ls sql/migrations/*-main.sql 2>/dev/null || echo "No pending migrations"

# If found, rename them (example)
# mv sql/migrations/0.0.3-main.sql sql/migrations/0.0.3-0.0.4.sql
```

### 6. Run the release script

Execute the release script with the determined release type:

```bash
./scripts/release.sh $RELEASE_TYPE
```

This script will:
- Update the version in `sdks/typescript/package.json`
- Update `package-lock.json`
- Create a commit with message "Release $NEW_VERSION"
- Create a git tag with the version number

### 7. Show push instructions

After the release script completes, show the user the commands to push:

```bash
git push origin main && git push origin $NEW_VERSION
```

**Important:** Do NOT automatically push. Let the user review the commit and tag first, then they can manually run the push commands.

## Notes

- The release script will check for a clean working directory
- It will verify that CHANGELOG.md has a section for the new version
- It will check that no `-main.sql` migrations remain (they should have been renamed in step 5)
- The user should review the commit and tag before pushing
