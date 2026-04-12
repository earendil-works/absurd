package absurd

import (
	"context"
	"errors"
	"fmt"
	"hash/fnv"
	"os"
	"runtime/debug"
	"sync"
	"time"
)

const (
	unknownTaskDeferBaseDelay    = 15 * time.Second
	unknownTaskDeferJitterWindow = 15 * time.Second
)

type workerConfig struct {
	batch               WorkBatchOptions
	concurrency         int
	pollInterval        time.Duration
	fatalOnLeaseTimeout bool
	onError             func(error)
}

func normalizeWorkBatchOptions(options WorkBatchOptions) WorkBatchOptions {
	options.WorkerID = orString(options.WorkerID, defaultBatchWorkerID)
	options.ClaimTimeout = normalizeLeaseDuration(options.ClaimTimeout, defaultClaimTimeout)
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
	fatalOnLeaseTimeout := true
	if options.FatalOnLeaseTimeout != nil {
		fatalOnLeaseTimeout = *options.FatalOnLeaseTimeout
	}
	return workerConfig{
		batch: normalizeWorkBatchOptions(WorkBatchOptions{
			WorkerID:     workerID,
			ClaimTimeout: options.ClaimTimeout,
			BatchSize:    positiveOr(options.BatchSize, concurrency),
		}),
		concurrency:         concurrency,
		pollInterval:        durationOr(options.PollInterval, defaultWorkerPollInterval),
		fatalOnLeaseTimeout: fatalOnLeaseTimeout,
		onError:             onError,
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

func waitCapacityOrPoll(ctx context.Context, capacityReleased <-chan struct{}, d time.Duration) error {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-capacityReleased:
		return nil
	case <-timer.C:
		return nil
	}
}

func unknownTaskDeferDelay(runID string) time.Duration {
	if unknownTaskDeferJitterWindow <= 0 {
		return unknownTaskDeferBaseDelay
	}
	hasher := fnv.New32a()
	_, _ = hasher.Write([]byte(runID))
	maxJitterSeconds := int(unknownTaskDeferJitterWindow / time.Second)
	jitter := time.Duration(hasher.Sum32()%uint32(maxJitterSeconds+1)) * time.Second
	return unknownTaskDeferBaseDelay + jitter
}

func (c *Client) deferClaimedRun(ctx context.Context, runID string, delay time.Duration) error {
	_, err := c.db.ExecContext(
		ctx,
		`SELECT absurd.schedule_run($1, $2, absurd.current_time() + make_interval(secs => $3))`,
		c.queueName,
		runID,
		durationSeconds(delay),
	)
	return err
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
		if err := c.executeTask(context.WithoutCancel(ctx), task, opts.ClaimTimeout, false); err != nil {
			return err
		}
	}
	return nil
}

func (c *Client) RunWorker(ctx context.Context, options ...WorkerOptions) error {
	cfg := c.normalizeWorkerOptions(first(options))
	sem := make(chan struct{}, cfg.concurrency)
	capacityReleased := make(chan struct{}, 1)
	var wg sync.WaitGroup
	defer wg.Wait()

	for {
		if err := ctx.Err(); err != nil {
			return err
		}

		available := cfg.concurrency - len(sem)
		if available == 0 {
			if err := waitCapacityOrPoll(ctx, capacityReleased, cfg.pollInterval); err != nil {
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
			if ctx.Err() != nil {
				return ctx.Err()
			}
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
				defer func() {
					<-sem
					select {
					case capacityReleased <- struct{}{}:
					default:
					}
				}()
				if err := c.executeTask(context.WithoutCancel(ctx), task, claimTimeout, cfg.fatalOnLeaseTimeout); err != nil {
					cfg.onError(err)
				}
			}(task, batch.ClaimTimeout)
		}
	}
}

func panicDetails(v any) (string, string) {
	if err, ok := v.(error); ok {
		return fmt.Sprintf("%T", err), fmt.Sprintf("panic: %v", err)
	}
	return fmt.Sprintf("%T", v), fmt.Sprintf("panic: %v", v)
}

func swallowTerminalTaskStateError(err error) error {
	if errors.Is(err, errCancelled) || errors.Is(err, errFailedRun) {
		return nil
	}
	return err
}

type leaseWatchdog struct {
	mu                  sync.Mutex
	warn                *time.Timer
	fatal               *time.Timer
	stopped             bool
	epoch               uint64
	logger              Logger
	taskLabel           string
	fatalOnLeaseTimeout bool
}

func newLeaseWatchdog(logger Logger, taskName string, taskID string, fatalOnLeaseTimeout bool) *leaseWatchdog {
	return &leaseWatchdog{
		logger:              logger,
		taskLabel:           fmt.Sprintf("%s (%s)", taskName, taskID),
		fatalOnLeaseTimeout: fatalOnLeaseTimeout,
	}
}

func (w *leaseWatchdog) schedule(lease time.Duration) {
	if lease <= 0 {
		return
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.stopped {
		return
	}
	w.epoch++
	epoch := w.epoch
	if w.warn != nil {
		w.warn.Stop()
	}
	if w.fatal != nil {
		w.fatal.Stop()
	}
	w.warn = time.AfterFunc(lease, func() {
		w.mu.Lock()
		if w.stopped || w.epoch != epoch {
			w.mu.Unlock()
			return
		}
		w.mu.Unlock()
		w.logger.Printf("[absurd] task %s exceeded claim timeout of %s", w.taskLabel, lease)
	})
	if !w.fatalOnLeaseTimeout {
		return
	}
	w.fatal = time.AfterFunc(2*lease, func() {
		w.mu.Lock()
		if w.stopped || w.epoch != epoch {
			w.mu.Unlock()
			return
		}
		w.mu.Unlock()
		w.logger.Printf("[absurd] task %s exceeded claim timeout of %s by more than 100%%; terminating process", w.taskLabel, lease)
		os.Exit(1)
	})
}

func (w *leaseWatchdog) stop() {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.stopped = true
	w.epoch++
	if w.warn != nil {
		w.warn.Stop()
		w.warn = nil
	}
	if w.fatal != nil {
		w.fatal.Stop()
		w.fatal = nil
	}
}

func (c *Client) executeTask(ctx context.Context, task claimedTask, claimTimeout time.Duration, fatalOnLeaseTimeout bool) (err error) {
	completionCtx := context.WithoutCancel(ctx)
	effectiveLease := normalizeLeaseDuration(claimTimeout, defaultClaimTimeout)
	watchdog := newLeaseWatchdog(c.logger, task.TaskName, task.TaskID, fatalOnLeaseTimeout)
	defer watchdog.stop()
	watchdog.schedule(effectiveLease)

	defer func() {
		if v := recover(); v != nil {
			name, message := panicDetails(v)
			c.logger.Printf("[absurd] task execution panicked: %s", message)
			err = swallowTerminalTaskStateError(
				failTaskRunWithTraceback(completionCtx, c.db, c.queueName, task.RunID, name, message, debug.Stack()),
			)
		}
	}()

	registration, ok := c.getRegistration(task.TaskName)
	if !ok {
		delay := unknownTaskDeferDelay(task.RunID)
		if err := c.deferClaimedRun(completionCtx, task.RunID, delay); err != nil {
			c.logger.Printf("[absurd] failed to defer unknown task %q (%s): %v", task.TaskName, task.TaskID, err)
			deferErr := fmt.Errorf("failed to defer unknown task %q (%s): %w", task.TaskName, task.TaskID, err)
			return swallowTerminalTaskStateError(
				failTaskRun(completionCtx, c.db, c.queueName, task.RunID, deferErr),
			)
		}
		c.logger.Printf("[absurd] claimed unknown task %q (%s); deferred run %s by %s", task.TaskName, task.TaskID, task.RunID, delay)
		return nil
	}
	if registration.queueName != c.queueName {
		err := fmt.Errorf("misconfigured task %q (queue mismatch)", task.TaskName)
		c.logger.Printf("[absurd] %v", err)
		return swallowTerminalTaskStateError(
			failTaskRun(completionCtx, c.db, c.queueName, task.RunID, err),
		)
	}
	taskCtx, err := newTaskContext(ctx, c, registration.queueName, task, effectiveLease, func(d time.Duration) {
		watchdog.schedule(d)
	})
	if err != nil {
		if errors.Is(err, errInvalidTaskHeaders) {
			c.logger.Printf("[absurd] %v", err)
			return swallowTerminalTaskStateError(
				failTaskRun(completionCtx, c.db, c.queueName, task.RunID, err),
			)
		}
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
		if errors.Is(err, errSuspend) || errors.Is(err, errCancelled) || errors.Is(err, errFailedRun) {
			return nil
		}
		c.logger.Printf("[absurd] task execution failed: %v", err)
		return swallowTerminalTaskStateError(
			failTaskRun(completionCtx, c.db, c.queueName, task.RunID, err),
		)
	}
	return swallowTerminalTaskStateError(
		completeTaskRun(completionCtx, c.db, c.queueName, task.RunID, result),
	)
}
