Make a release of Absurd.

Version or release type: "$ARGUMENTS"

## Step-by-Step Process:

### 1. Determine the target version

The `$ARGUMENTS` can be either:
- An explicit version number (e.g., `0.0.4`) - recommended for making a specific release
- A release type: `patch`, `minor`, or `major` - which will bump from the current version

If `$ARGUMENTS` is an explicit version (e.g., `0.0.4`):
- Use that version directly as `$NEW_VERSION`
- This is the recommended approach as it allows retrying failed releases

If `$ARGUMENTS` is a release type (`patch`, `minor`, or `major`):
- Determine what the new version will be by running:
  ```bash
  cd sdks/typescript
  CURRENT_VERSION=$(node -p "require('./package.json').version")
  NEW_VERSION=$(npm version $ARGUMENTS --no-git-tag-version | sed 's/^v//')
  git checkout package.json package-lock.json  # Revert the changes
  cd ../..
  echo "Will release version: $NEW_VERSION"
  ```
- Then use this `$NEW_VERSION` for the rest of the process

If no argument is provided, ask the user which version or type to use.

### 2. Update the changelog

Run the `/update-changelog` command to ensure the changelog is up to date with recent changes.

### 3. Verify the version number

Double-check that `$NEW_VERSION` is correct before proceeding.

### 4. Update CHANGELOG.md

Edit the `CHANGELOG.md` file:
- Change the `# Unreleased` heading to `# $NEW_VERSION`
- Add a new `# Unreleased` section at the top (empty for now)

### 5. Commit changelog and run the release script

The changelog update must be committed before running the release script, because
the script requires a clean working directory:

```bash
git add CHANGELOG.md
git commit -m "meta: update changelog for $NEW_VERSION"
```

Then execute the release script with the explicit version number (NOT the release type):

```bash
./scripts/release.sh $NEW_VERSION
```

**Important:** Always pass the explicit version number (e.g., `0.0.4`) to the release script, not the release type (e.g., `patch`). This ensures that aborted releases can be retried without incrementing the version.

The script will automatically:
- Update the version in `sdks/typescript/package.json`
- Update `package-lock.json`
- Update `sql/absurd.sql` schema version to `$NEW_VERSION`
- Rename any pending `-main.sql` migrations to `OLD-$NEW_VERSION.sql`
- Create a commit with message "Release $NEW_VERSION" and a git tag
- Reset `sql/absurd.sql` schema version back to `main` for development
- Create a follow-up commit to keep the main branch in development mode

**Do NOT** manually rename migrations or edit `sql/absurd.sql` — the script handles both.

### 6. Show push instructions

After the release script completes, show the user the commands to push:

```bash
git push origin main && git push origin $NEW_VERSION
```

**Important:** Do NOT automatically push. Let the user review the commits and tag first, then they can manually run the push commands.

## Notes

- The release script will check for a clean working directory
- It will verify that CHANGELOG.md has a section for the new version
- It validates that schema version functions are correct in both `absurd.sql` and migrations
- The user should review the commits and tag before pushing
