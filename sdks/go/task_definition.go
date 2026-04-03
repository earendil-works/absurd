package absurd

import (
	"context"
	"encoding/json"
	"fmt"
)

type taskRegistration interface {
	buildRegistered(defaultQueue string) (registeredTask, error)
}

// TaskDefinition is a typed task registration.
type TaskDefinition[P, R any] struct {
	name    string
	handler TaskHandler[P, R]
	options TaskOptions
}

// Task creates a typed task definition.
func Task[P, R any](name string, handler TaskHandler[P, R], options ...TaskOptions) TaskDefinition[P, R] {
	var opts TaskOptions
	if len(options) > 0 {
		opts = options[0]
	}
	return TaskDefinition[P, R]{
		name:    name,
		handler: handler,
		options: opts,
	}
}

func (t TaskDefinition[P, R]) Name() string {
	return t.name
}

// Spawn uses the typed task name and parameter type.
func (t TaskDefinition[P, R]) Spawn(ctx context.Context, c *Client, params P, options ...SpawnOptions) (SpawnResult, error) {
	return c.Spawn(ctx, t.name, params, options...)
}

func (t TaskDefinition[P, R]) buildRegistered(defaultQueue string) (registeredTask, error) {
	if t.name == "" {
		return registeredTask{}, fmt.Errorf("task registration requires a name")
	}
	if t.options.DefaultMaxAttempts < 0 {
		return registeredTask{}, fmt.Errorf("default max attempts must be at least 1")
	}
	queue := defaultQueue
	if t.options.QueueName != "" {
		queue = t.options.QueueName
	}
	queue, err := validateQueueName(queue)
	if err != nil {
		return registeredTask{}, err
	}
	return registeredTask{
		name:                  t.name,
		queueName:             queue,
		hasDefaultMaxAttempts: t.options.DefaultMaxAttempts > 0,
		defaultMaxAttempts:    t.options.DefaultMaxAttempts,
		handler: func(ctx context.Context, paramsRaw json.RawMessage) (any, error) {
			var params P
			if err := unmarshalJSON(paramsRaw, &params); err != nil {
				return nil, fmt.Errorf("unmarshal params for task %q: %w", t.name, err)
			}
			return t.handler(ctx, params)
		},
	}, nil
}

type registeredTask struct {
	name                  string
	queueName             string
	hasDefaultMaxAttempts bool
	defaultMaxAttempts    int
	handler               func(ctx context.Context, paramsRaw json.RawMessage) (any, error)
}
