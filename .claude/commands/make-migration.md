Please create a migration for absurd from the previous version to the current version (`main`).

Base line version: "$ARGUMENTS"

## Step-by-Step Process:

### 1. Determine baseline version
If no baseline version is provided, use the most recent git tag. You can find it with `git describe --tags --abbrev=0`.

### 2. Extract the base SQL file
Use `git show` to retrieve the `sql/absurd.sql` file from the baseline version and save it to a temporary file:

```bash
git show {OLD_TAG}:sql/absurd.sql > /tmp/base-absurd.sql
```

Replace `{OLD_TAG}` with the actual tag name (e.g., `0.0.3`).

### 2.a Find new Tag

the `NEW_TAG` for the moment is always set to `main` unless run from a different branch or tag.

### 3. Compare and create migration
Compare the base version (`/tmp/base-absurd.sql`) with the current version (`./sql/absurd.sql`) to understand what changed. Then carefully create a migration file and name it `./sql/migrations/{OLD_TAG}-{NEW_TAG}.sql`.

For example: `./sql/migrations/0.0.3-main.sql`

**Important:** if that migration already exists, STOP and ask the user if you should override it.  If the user declines, abort, otherwise continue and replace the file after you are done.

### 4. Validate the migration
To validate that the migration actually works, use `./scripts/validate-psql.py`:

```bash
./scripts/validate-psql.py \
    --base=/tmp/base-absurd.sql \
    --migration=./sql/migrations/{OLD_TAG}-{NEW_TAG}.sql \
    --current=./sql/absurd.sql \
    --migration-dump=/tmp/migration-schema.out \
    --current-dump=/tmp/current-schema.out
```

**Parameter explanations:**
- `--base`: Path to the base schema (the baseline version's absurd.sql extracted from git)
- `--migration`: Path to the migration you just created (`./sql/migrations/{OLD_TAG}-{NEW_TAG}.sql`)
- `--current`: Path to the current version of absurd.sql (always `./sql/absurd.sql`)
- `--migration-dump`: Output file where the base + migration schema will be dumped
- `--current-dump`: Output file where the current `absurd.sql` schema will be dumped

### 5. Compare the schema dumps
After running the validation script, compare the two schema dumps to ensure they match:

```bash
diff /tmp/migration-schema.out /tmp/current-schema.out
```

**Success criteria:** The diff should produce NO output (empty diff), which means the schemas are identical. If there are differences, the migration is incomplete or incorrect and needs to be fixed.

### 6. Clean up
After successful validation, delete the temporary files:

```bash
rm /tmp/base-absurd.sql /tmp/migration-schema.out /tmp/current-schema.out
```

## Ground rules when writing migrations:

* Migrations are **up only** (no down migrations)
* You must **NOT delete data** - this would be catastrophic for production systems
* Be careful and consider locking when necessary (migrations run online against live databases)
* Write idempotent migrations when possible (safe to run multiple times)
* Test thoroughly - the validation script catches schema mismatches but not all logic errors
