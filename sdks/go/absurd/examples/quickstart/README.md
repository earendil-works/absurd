# Go Quickstart

This mirrors the TypeScript and Python quickstart examples using the initial Go
SDK shape.

Files:

- `worker/main.go` — durable worker with `Step`, `BeginStep` / `CompleteStep`, and `AwaitEvent`
- `client/main.go` — spawn a task and inspect or await its result

## Run

These examples use the pgx `database/sql` driver (`DriverName: "pgx"`).

```bash
cd sdks/go/absurd
go run ./examples/quickstart/worker
go run ./examples/quickstart/client --await alice alice@example.com
```
