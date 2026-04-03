package absurd

import (
	"context"
	"fmt"
	"os"
	"sync"
	"time"
)

type workerConfig struct {
	batch        WorkBatchOptions
	concurrency  int
	pollInterval time.Duration
	onError      func(error)
}

func normalizeWorkBatchOptions(options WorkBatchOptions) WorkBatchOptions {
	options.WorkerID = orString(options.WorkerID, defaultBatchWorkerID)
	options.ClaimTimeout = durationOr(options.ClaimTimeout, defaultClaimTimeout)
	options.BatchSize = positiveOr(options.BatchSize, 1)
	return options
}

func (c *Client) normalizeWorkerOptions(options WorkerOptions) workerConfig {
	workerID := options.WorkerID
	if workerID == "" {
		hostname, _ := os.Hostname()
		workerID = fmt.Sprintf("%s:%d", hostname, os.Getpid())
	}
	concurrency := positiveOr(options.Concurrency, 1)
	onError := options.OnError
	if onError == nil {
		onError = func(err error) {
			c.logger.Printf("worker error: %v", err)
		}
	}
	return workerConfig{
		batch: normalizeWorkBatchOptions(WorkBatchOptions{
			WorkerID:     workerID,
			ClaimTimeout: options.ClaimTimeout,
			BatchSize:    positiveOr(options.BatchSize, concurrency),
		}),
		concurrency:  concurrency,
		pollInterval: durationOr(options.PollInterval, defaultWorkerPollInterval),
		onError:      onError,
	}
}

func sleepContext(ctx context.Context, d time.Duration) error {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func (c *Client) claimTasks(ctx context.Context, options WorkBatchOptions) ([]claimedTask, error) {
	opts := normalizeWorkBatchOptions(options)

	rows, err := c.db.QueryContext(ctx, `SELECT run_id, task_id, attempt, task_name, params, headers, wake_event, event_payload
	       FROM absurd.claim_task($1, $2, $3, $4)`, c.queueName, opts.WorkerID, durationSeconds(opts.ClaimTimeout), opts.BatchSize)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var tasks []claimedTask
	for rows.Next() {
		var task claimedTask
		var headers []byte
		var wakeEvent *string
		var eventPayload []byte
		if err := rows.Scan(
			&task.RunID,
			&task.TaskID,
			&task.Attempt,
			&task.TaskName,
			&task.ParamsRaw,
			&headers,
			&wakeEvent,
			&eventPayload,
		); err != nil {
			return nil, err
		}
		task.HeadersRaw = cloneRawJSON(headers)
		if wakeEvent != nil {
			task.WakeEvent = *wakeEvent
		}
		task.EventRaw = cloneRawJSON(eventPayload)
		tasks = append(tasks, task)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return tasks, nil
}

func (c *Client) WorkBatch(ctx context.Context, options ...WorkBatchOptions) error {
	opts := normalizeWorkBatchOptions(first(options))
	tasks, err := c.claimTasks(ctx, opts)
	if err != nil {
		return err
	}
	for _, task := range tasks {
		if err := c.executeTask(ctx, task, opts.ClaimTimeout); err != nil {
			return err
		}
	}
	return nil
}

func (c *Client) RunWorker(ctx context.Context, options ...WorkerOptions) error {
	cfg := c.normalizeWorkerOptions(first(options))
	sem := make(chan struct{}, cfg.concurrency)
	var wg sync.WaitGroup
	defer wg.Wait()

	for {
		if err := ctx.Err(); err != nil {
			return err
		}

		available := cfg.concurrency - len(sem)
		if available == 0 {
			if err := sleepContext(ctx, cfg.pollInterval); err != nil {
				return err
			}
			continue
		}

		batch := cfg.batch
		if batch.BatchSize > available {
			batch.BatchSize = available
		}
		tasks, err := c.claimTasks(ctx, batch)
		if err != nil {
			cfg.onError(err)
			if err := sleepContext(ctx, cfg.pollInterval); err != nil {
				return err
			}
			continue
		}
		if len(tasks) == 0 {
			if err := sleepContext(ctx, cfg.pollInterval); err != nil {
				return err
			}
			continue
		}

		for _, task := range tasks {
			sem <- struct{}{}
			wg.Add(1)
			go func(task claimedTask, claimTimeout time.Duration) {
				defer wg.Done()
				defer func() { <-sem }()
				if err := c.executeTask(ctx, task, claimTimeout); err != nil {
					cfg.onError(err)
				}
			}(task, batch.ClaimTimeout)
		}
	}
}

func (c *Client) executeTask(ctx context.Context, task claimedTask, claimTimeout time.Duration) error {
	registration, ok := c.registry[task.TaskName]
	if !ok {
		return fmt.Errorf("unknown task %q", task.TaskName)
	}
	if registration.queueName != c.queueName {
		return fmt.Errorf("misconfigured task %q (queue mismatch)", task.TaskName)
	}
	taskCtx, err := newTaskContext(ctx, c, registration.queueName, task, claimTimeout)
	if err != nil {
		return err
	}
	execute := func() (any, error) {
		return registration.handler(taskCtx, task.ParamsRaw)
	}
	var result any
	if c.hooks.WrapTaskExecution != nil {
		result, err = c.hooks.WrapTaskExecution(taskCtx, execute)
	} else {
		result, err = execute()
	}
	if err != nil {
		switch err {
		case errSuspend, errCancelled, errFailedRun:
			return nil
		default:
			c.logger.Printf("[absurd] task execution failed: %v", err)
			return failTaskRun(context.WithoutCancel(ctx), c.db, c.queueName, task.RunID, err)
		}
	}
	return completeTaskRun(context.WithoutCancel(ctx), c.db, c.queueName, task.RunID, result)
}
