# Go SDK — Feature Parity TODO

Implemented in the Go SDK:

- [x] `ListQueues()`
- [x] `RetryTask()`
- [x] `CancelTask()`
- [x] `RetryStrategy`
- [x] `CancellationPolicy`
- [x] `IdempotencyKey`
- [x] `Headers` on spawn
- [x] `TaskContext.AwaitTaskResult()`
- [x] `before_spawn` hook
- [x] `wrap_task_execution` hook
- [x] `default_cancellation` on registration
- [x] stack traces in failure payloads

## Test Coverage Added

Ported parity coverage from the Python SDK into `sdks/go/absurdtest/` for:

- queue listing
- spawn option serialization
- idempotency
- default cancellation
- retrying failed tasks
- manual cancellation
- hooks
- waiting for child task results from task context
- traceback serialization
