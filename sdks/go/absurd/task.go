package absurd

type TaskHandler[P, R any] interface {
	HandleAbsurdTask(ctx Context, params P) (R, error)
}

type TaskHandlerFunc[P, R any] func(ctx Context, params P) (R, error)

func (f TaskHandlerFunc[P, R]) HandleAbsurdTask(ctx Context, params P) (R, error) {
	return f(ctx, params)
}

type taskHandler interface {
	handleAbsurdTask(ctx Context, params any) (any, error)
}

type taskHandlerWrapper[P, R any] struct {
	inner TaskHandler[P, R]
}

// handle implements the type erased internal [taskHandler] interface and bridges non-generic with
// generic task handle code.
func (w *taskHandlerWrapper[P, R]) handleAbsurdTask(ctx Context, untypedParams any) (any, error) {
	params, ok := untypedParams.(P)
	if !ok {
		// Panic on invariant.
		panic("absurd: expected type erased params to have the same type as the generic handler")
	}

	return w.inner.HandleAbsurdTask(ctx, params)
}

// typeEraseTaskHandler type erases a [TaskHandler] so that generic [TaskHandler]s can be used with
// the non-generic [Absurd] struct.
func typeEraseTaskHandler[P, R any](handler TaskHandler[P, R]) taskHandler {
	return &taskHandlerWrapper[P, R]{
		inner: handler,
	}
}
