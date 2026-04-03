package absurd

import (
	"context"
	"fmt"
	"os"
	"sync"
	"time"
)

func (c *Client) claimTasks(ctx context.Context, options WorkBatchOptions) ([]claimedTask, time.Duration, error) {
	workerID := options.WorkerID
	if workerID == "" {
		workerID = "worker"
	}
	claimTimeout := options.ClaimTimeout
	if claimTimeout <= 0 {
		claimTimeout = 120 * time.Second
	}
	batchSize := options.BatchSize
	if batchSize <= 0 {
		batchSize = 1
	}

	rows, err := c.db.QueryContext(ctx, `SELECT run_id, task_id, attempt, task_name, params, retry_strategy, max_attempts,
              headers, wake_event, event_payload
       FROM absurd.claim_task($1, $2, $3, $4)`, c.queueName, workerID, durationSeconds(claimTimeout), batchSize)
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()

	var tasks []claimedTask
	for rows.Next() {
		var task claimedTask
		var retryStrategy []byte
		var maxAttempts any
		var headers []byte
		var wakeEvent *string
		var eventPayload []byte
		if err := rows.Scan(
			&task.RunID,
			&task.TaskID,
			&task.Attempt,
			&task.TaskName,
			&task.ParamsRaw,
			&retryStrategy,
			&maxAttempts,
			&headers,
			&wakeEvent,
			&eventPayload,
		); err != nil {
			return nil, 0, err
		}
		task.HeadersRaw = cloneRawJSON(headers)
		if wakeEvent != nil {
			task.WakeEvent = *wakeEvent
		}
		task.EventRaw = cloneRawJSON(eventPayload)
		tasks = append(tasks, task)
	}
	if err := rows.Err(); err != nil {
		return nil, 0, err
	}
	return tasks, claimTimeout, nil
}

func (c *Client) WorkBatch(ctx context.Context, options ...WorkBatchOptions) error {
	var opts WorkBatchOptions
	if len(options) > 0 {
		opts = options[0]
	}
	tasks, claimTimeout, err := c.claimTasks(ctx, opts)
	if err != nil {
		return err
	}
	for _, task := range tasks {
		if err := c.executeTask(ctx, task, claimTimeout); err != nil {
			return err
		}
	}
	return nil
}

func (c *Client) RunWorker(ctx context.Context, options ...WorkerOptions) error {
	var opts WorkerOptions
	if len(options) > 0 {
		opts = options[0]
	}
	workerID := opts.WorkerID
	if workerID == "" {
		hostname, _ := os.Hostname()
		workerID = fmt.Sprintf("%s:%d", hostname, os.Getpid())
	}
	claimTimeout := opts.ClaimTimeout
	if claimTimeout <= 0 {
		claimTimeout = 120 * time.Second
	}
	concurrency := opts.Concurrency
	if concurrency <= 0 {
		concurrency = 1
	}
	batchSize := opts.BatchSize
	if batchSize <= 0 {
		batchSize = concurrency
	}
	pollInterval := opts.PollInterval
	if pollInterval <= 0 {
		pollInterval = 250 * time.Millisecond
	}
	onError := opts.OnError
	if onError == nil {
		onError = func(err error) {
			c.logger.Printf("worker error: %v", err)
		}
	}

	sem := make(chan struct{}, concurrency)
	var wg sync.WaitGroup

	for {
		select {
		case <-ctx.Done():
			wg.Wait()
			return ctx.Err()
		default:
		}

		available := cap(sem) - len(sem)
		if available <= 0 {
			select {
			case <-ctx.Done():
				wg.Wait()
				return ctx.Err()
			case <-time.After(pollInterval):
			}
			continue
		}

		toClaim := batchSize
		if toClaim > available {
			toClaim = available
		}
		tasks, actualClaimTimeout, err := c.claimTasks(ctx, WorkBatchOptions{
			WorkerID:     workerID,
			ClaimTimeout: claimTimeout,
			BatchSize:    toClaim,
		})
		if err != nil {
			onError(err)
			select {
			case <-ctx.Done():
				wg.Wait()
				return ctx.Err()
			case <-time.After(pollInterval):
			}
			continue
		}
		if len(tasks) == 0 {
			select {
			case <-ctx.Done():
				wg.Wait()
				return ctx.Err()
			case <-time.After(pollInterval):
			}
			continue
		}

		for _, task := range tasks {
			sem <- struct{}{}
			wg.Add(1)
			go func(task claimedTask) {
				defer wg.Done()
				defer func() { <-sem }()
				if err := c.executeTask(ctx, task, actualClaimTimeout); err != nil {
					onError(err)
				}
			}(task)
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
	result, err := registration.handler(taskCtx, task.ParamsRaw)
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
