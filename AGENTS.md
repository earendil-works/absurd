# Agents

Absurd is a Postgres-native durable workflow system.  It moves the complexity of
durable execution into the database layer via stored procedures, keeping SDKs
lightweight and language-agnostic.  The core principle is to handle tasks that
may run for minutes, days, or years without losing state.

**Key concept**: A *task* is subdivided into *steps* that act as checkpoints.
Tasks can suspend (for sleep or events) and resume without data loss.  All state
lives in Postgres tables prefixed by queue name (`t_`, `r_`, `c_`, `e_`, `w_`).

## Repository Structure

- **sql/absurd.sql** - Core Postgres schema and stored procedures. This is the heart of the system.
- **sdks/typescript/** - TypeScript SDK (main SDK, currently unpublished, used from source)
- **habitat/** - Go-based web UI for monitoring tasks, runs, and events
- **absurdctl** - CLI tool (Go binary) for queue/task management and debugging
- **tests/** - Python test suite using psycopg and testcontainers

## Building and Testing

### TypeScript SDK
```bash
cd sdks/typescript
npm install
npm run build  # Compiles to dist/ (both ESM and CommonJS)
```

### Habitat (Web UI)
```bash
cd habitat
make build      # Build UI + Go binary -> bin/habitat
make dev        # Run dev server with hot reload
./bin/habitat run -db-name your-database-name
```

### Python Tests
```bash
cd tests
uv run pytest              # Run all tests
uv run pytest test_foo.py  # Run single test file
```

### Formatting
```bash
make format     # Format TypeScript SDK, habitat UI, and Python tests
```

## Habitat Web UI

Habitat connects directly to Postgres and serves a SolidJS dashboard.  It shows
task state, run history, checkpoint data, and events. Useful for debugging and
monitoring.

Configuration via flags or `HABITAT_*` env vars (see habitat/README.md).

## Additional Information

More information can be found in @README.md and @CONTRIBUTING.md
