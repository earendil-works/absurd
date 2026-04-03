# Go Quickstart

This mirrors the TypeScript and Python quickstart examples using the initial Go
SDK shape.

Files:

- `worker/main.go` — durable worker with `Step`, `BeginStep` / `CompleteStep`, and `AwaitEvent`
- `client/main.go` — spawn a task and inspect or await its result

## Run

```bash
go run ./sdks/go/examples/quickstart/worker
go run ./sdks/go/examples/quickstart/client --await alice alice@example.com
```
