# Habitat

Habitat is a web dashboard for monitoring Absurd tasks, runs, checkpoints, and
events.  It's a Go application with an embedded SolidJS frontend that connects
directly to Postgres.

<div style="text-align: center" align="center">
  <img src="../images/habitat-screenshot.png" width="550" alt="Screenshot of the Habitat dashboard">
</div>

## Building

Habitat requires Go and Node.js to build:

```bash
cd habitat
make build
```

This builds the SolidJS frontend, embeds it into the Go binary, and outputs
`./bin/habitat`.

For development with hot reload:

```bash
cd habitat
make dev
```

## Running

```bash
./bin/habitat run -db-name mydb
```

The dashboard is served at `http://localhost:7890` by default.

## Configuration

Habitat accepts configuration via command-line flags or environment variables
(prefixed with `HABITAT_`).

### Database Options

| Flag | Env Var | Default | Description |
|------|---------|---------|-------------|
| `-db-url` | `HABITAT_DB_URL` | — | Full Postgres connection URL |
| `-db-host` | `HABITAT_DB_HOST` | `localhost` | Postgres host |
| `-db-port` | `HABITAT_DB_PORT` | `5432` | Postgres port |
| `-db-name` | `HABITAT_DB_NAME` | `absurd` | Database name |
| `-db-user` | `HABITAT_DB_USER` | — | Database user |
| `-db-password` | `HABITAT_DB_PASSWORD` | — | Database password |
| `-db-sslmode` | `HABITAT_DB_SSLMODE` | `disable` | SSL mode |

### Server Options

| Flag | Env Var | Default | Description |
|------|---------|---------|-------------|
| `-listen` | `HABITAT_LISTEN` | `:7890` | Address to listen on |
| `-base-path` | `HABITAT_BASE_PATH` | — | URL prefix (e.g., `/habitat`) |

### Examples

```bash
# Connect with a full URL
./bin/habitat run -db-url "postgresql://user:pass@localhost:5432/mydb"

# Connect with individual flags
./bin/habitat run -db-host localhost -db-port 5432 -db-name mydb -db-user admin

# Environment variables
export HABITAT_DB_URL="postgresql://user:pass@localhost:5432/mydb"
export HABITAT_LISTEN=":8080"
./bin/habitat run

# Serve under a URL prefix (for reverse proxies)
./bin/habitat run -db-name mydb -base-path /habitat
```

## Reverse Proxy

When running behind a reverse proxy, Habitat honors these headers for
generating correct UI and API URLs:

- `X-Forwarded-Prefix`
- `X-Forwarded-Path`
- `X-Script-Name`

Example nginx configuration:

```nginx
location /habitat/ {
    proxy_pass http://localhost:7890/;
    proxy_set_header X-Forwarded-Prefix /habitat;
    proxy_set_header Host $host;
}
```

## What It Shows

Habitat provides a read-only view of your Absurd system:

- **Queues** — list of all queues with task counts
- **Tasks** — searchable/filterable list of tasks per queue with status, attempt
  count, and timing
- **Runs** — per-task run history showing each attempt, its status, error
  details, and duration
- **Checkpoints** — step results stored for each task, showing the persisted
  JSON state
- **Events** — emitted events with payloads and timestamps

This makes Habitat useful for debugging failed tasks (inspect error payloads and
checkpoint state), understanding retry behavior, and monitoring overall system
health.
