Please update the `CHANGELOG.md` for absurd with changes between the last release and the current version (`main`) which were not incorporated yet.

Base line version: "$ARGUMENTS"

## Step-by-Step Process:

### 1. Determine baseline version
If no baseline version is provided, use the most recent git tag. You can find it with `git describe --tags --abbrev=0`.

### 2. Find the commits from git

Use the following commands to gather commit information:

```bash
# Get the baseline version (if not provided)
git describe --tags --abbrev=0

# Get all commits since the baseline version
git log <baseline-version>..HEAD --oneline

# Get more detailed commit messages if needed
git log <baseline-version>..HEAD --pretty=format:"%h %s" --reverse
```

Also check for merged pull requests:
```bash
# Find merged PRs since the baseline
git log <baseline-version>..HEAD --merges --oneline --grep="Merge pull request"
```

### 3. Update the changelog
Read the already existing `CHANGELOG.md` and check if there are changes not yet incorporated, then add them. Always add them to the "Unreleased" section only. If there is none yet, add it on the top.

## Ground Rules When Writing Changelogs

### Content Guidelines
* Focus on **notable changes** that affect users (features, fixes, breaking changes)
* Mention pull requests (`#NUMBER`) when available, but not raw commit hashes
* Ignore insignificant changes (typo fixes, internal refactoring, minor documentation updates)
* Group related changes together when appropriate
* Order entries by importance: breaking changes first, then features, then fixes

### Style Guidelines
* Use valid markdown syntax
* Start each entry with a past-tense verb or descriptive phrase
* Keep entries concise but descriptive enough to understand the change
* Use bullet points (`*` or `-`) for individual changes
* Format code references with backticks (e.g., `` `absurd.cleanup_tasks` ``)

### Example Format

Here's an example of well-formatted changelog entries (from minijinja):

```markdown
## 2.13.0

* Added multi-key support to the `|sort` filter.  #827
* Fix `not undefined` with strict undefined behavior.  #838
* Added support for free threading Python.  #841

## 2.12.0

* Item or attribute lookup will no longer swallow all errors in Python.  #814
* Added `|zip` filter.  #818
* Fix `break_on_hyphens` for the `|wordwrap` filter.  #823
* Prefer error message from `unknown_method_callback`.  #824
* Ignore `.jinja` and `.jinja2` as extensions in auto escape.  #832
```

### Good vs. Bad Examples

**Good:**
* `Fixed an issue with the TypeScript SDK which caused an incorrect config for CJS.`
* `Added support for claim timeout extension on checkpoint writes.`
* `Improved error reporting when task claim expires.`

**Bad:**
* `Fixed bug` (too vague)
* `Updated dependencies` (insignificant unless it fixes a security issue)
* `Refactored internal code structure` (internal change, not user-facing)
* `Fixed typo in comment` (insignificant)

## Notes

* If the current CHANGELOG.md already has an "Unreleased" section with content, append to it rather than replacing it
* Preserve the existing changelog style and formatting
* When in doubt about whether a change is significant, err on the side of including it
