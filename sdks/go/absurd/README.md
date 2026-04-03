# Absurd SDK for Go

Experimental Go SDK for [Absurd](https://github.com/earendil-works/absurd).

This bootstrap version focuses on the core durable execution model with a
Go-native API surface:

- queue-targeting control APIs take the queue name explicitly

- `context.Context` everywhere inside task code
- typed task definitions via `absurd.Task(...)`
- durable task operations as package-level functions such as `Step` and `AwaitEvent`
- `TaskFromContext` / `MustTaskContext` for task metadata when needed

## Status

Early work in progress. The initial implementation covers:

- queue creation
- task registration and spawning
- single-batch and continuous workers
- durable steps via `Step` and `BeginStep`
- event waits via `AwaitEvent`
- event emission
- task result polling

See `examples/quickstart` for the current shape.

For the full repository documentation, see the
[Go SDK guide](../../../docs/sdk-go.md).
