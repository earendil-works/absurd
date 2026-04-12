package absurdtest

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/earendil-works/absurd/sdks/go/absurd"
)

func setFakeNow(t *testing.T, db *sql.DB, ts *time.Time) {
	t.Helper()
	if ts == nil {
		if _, err := db.Exec(`set absurd.fake_now = default`); err != nil {
			t.Fatalf("reset fake_now: %v", err)
		}
		return
	}
	if _, err := db.Exec(fmt.Sprintf("set absurd.fake_now = '%s'", ts.UTC().Format(time.RFC3339Nano))); err != nil {
		t.Fatalf("set fake_now: %v", err)
	}
}

func fetchTaskRow(t *testing.T, db *sql.DB, queue, taskID string) (state string, attempts int, headers, retryStrategy, cancellation []byte, cancelledAt sql.NullTime) {
	t.Helper()
	query := fmt.Sprintf(`select state, attempts, headers, retry_strategy, cancellation, cancelled_at from absurd.t_%s where task_id = $1`, queue)
	if err := db.QueryRow(query, taskID).Scan(&state, &attempts, &headers, &retryStrategy, &cancellation, &cancelledAt); err != nil {
		t.Fatalf("fetch task row: %v", err)
	}
	return
}

func fetchFailure(t *testing.T, db *sql.DB, queue, runID string) map[string]any {
	t.Helper()
	query := fmt.Sprintf(`select failure_reason from absurd.r_%s where run_id = $1`, queue)
	var raw []byte
	if err := db.QueryRow(query, runID).Scan(&raw); err != nil {
		t.Fatalf("fetch failure: %v", err)
	}
	var rv map[string]any
	if err := json.Unmarshal(raw, &rv); err != nil {
		t.Fatalf("decode failure: %v", err)
	}
	return rv
}

func countTasks(t *testing.T, db *sql.DB, queue string) int {
	t.Helper()
	query := fmt.Sprintf(`select count(*) from absurd.t_%s`, queue)
	var count int
	if err := db.QueryRow(query).Scan(&count); err != nil {
		t.Fatalf("count tasks: %v", err)
	}
	return count
}

func countCheckpoints(t *testing.T, db *sql.DB, queue, taskID string) int {
	t.Helper()
	query := fmt.Sprintf(`select count(*) from absurd.c_%s where task_id = $1`, queue)
	var count int
	if err := db.QueryRow(query, taskID).Scan(&count); err != nil {
		t.Fatalf("count checkpoints: %v", err)
	}
	return count
}

func TestListQueues(t *testing.T) {
	queueA := randomQueueName("go_list_a")
	queueB := randomQueueName("go_list_b")
	client := newTestClient(t, queueA)
	if err := client.CreateQueue(context.Background(), queueB); err != nil {
		t.Fatalf("CreateQueue: %v", err)
	}

	queues, err := client.ListQueues(context.Background())
	if err != nil {
		t.Fatalf("ListQueues: %v", err)
	}
	all := strings.Join(queues, ",")
	if !strings.Contains(all, queueA) || !strings.Contains(all, queueB) {
		t.Fatalf("expected %q and %q in %v", queueA, queueB, queues)
	}
}

func TestQueuePolicyMethods(t *testing.T) {
	queue := randomQueueName("go_queue_policy")
	db := setupTestDatabase(t)
	client, err := absurd.New(absurd.Options{DB: db, QueueName: queue})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	ttl := "12345 seconds"
	limit := 77
	if err := client.CreateQueue(context.Background(), queue, absurd.CreateQueueOptions{
		StorageMode: absurd.QueueStoragePartitioned,
		QueuePolicyOptions: absurd.QueuePolicyOptions{
			PartitionLookahead: "35 days",
			PartitionLookback:  "2 days",
			CleanupTTL:         ttl,
			CleanupLimit:       &limit,
			DetachMode:         absurd.QueueDetachEmpty,
			DetachMinAge:       "45 days",
		},
	}); err != nil {
		t.Fatalf("CreateQueue with options: %v", err)
	}

	policy, err := client.GetQueuePolicy(context.Background(), queue)
	if err != nil {
		t.Fatalf("GetQueuePolicy: %v", err)
	}
	if policy == nil {
		t.Fatal("expected queue policy")
	}
	if policy.StorageMode != absurd.QueueStoragePartitioned {
		t.Fatalf("unexpected storage mode: %s", policy.StorageMode)
	}
	if policy.PartitionLookahead != "35 days" || policy.PartitionLookback != "2 days" {
		t.Fatalf("unexpected partition window: %#v", policy)
	}
	if policy.CleanupTTL != "3:25:45" && policy.CleanupTTL != "03:25:45" {
		t.Fatalf("unexpected cleanup ttl: %#v", policy)
	}
	if policy.CleanupLimit != 77 {
		t.Fatalf("unexpected cleanup policy: %#v", policy)
	}
	if policy.DetachMode != absurd.QueueDetachEmpty || policy.DetachMinAge != "45 days" {
		t.Fatalf("unexpected detach policy: %#v", policy)
	}

	updatedLimit := 12
	if err := client.SetQueuePolicy(context.Background(), queue, absurd.QueuePolicyOptions{
		CleanupTTL:   "4321 seconds",
		CleanupLimit: &updatedLimit,
	}); err != nil {
		t.Fatalf("SetQueuePolicy: %v", err)
	}

	updated, err := client.GetQueuePolicy(context.Background(), queue)
	if err != nil {
		t.Fatalf("GetQueuePolicy updated: %v", err)
	}
	if updated == nil {
		t.Fatal("expected updated queue policy")
	}
	if updated.CleanupTTL != "1:12:01" && updated.CleanupTTL != "01:12:01" {
		t.Fatalf("unexpected updated cleanup ttl: %#v", updated)
	}
	if updated.CleanupLimit != updatedLimit {
		t.Fatalf("unexpected updated cleanup policy: %#v", updated)
	}
}

func TestSpawnOptionsParity(t *testing.T) {
	queue := randomQueueName("go_spawn_opts")
	client := newTestClient(t, queue)
	client.MustRegister(absurd.Task("opts", func(ctx context.Context, params map[string]any) (map[string]any, error) {
		return map[string]any{"ok": true}, nil
	}))

	spawned, err := client.Spawn(context.Background(), "opts", map[string]any{"value": 42}, absurd.SpawnOptions{
		MaxAttempts: 3,
		Headers:     map[string]any{"trace_id": "abc"},
		RetryStrategy: &absurd.RetryStrategy{
			Kind:        "fixed",
			BaseSeconds: 30,
		},
		Cancellation:   &absurd.CancellationPolicy{MaxDelay: 60, MaxDuration: 120},
		IdempotencyKey: "idem-1",
	})
	if err != nil {
		t.Fatalf("Spawn: %v", err)
	}
	if !spawned.Created {
		t.Fatal("expected created task")
	}

	_, attempts, headersRaw, retryRaw, cancellationRaw, _ := fetchTaskRow(t, setupTestDatabase(t), queue, spawned.TaskID)
	if attempts != 1 {
		t.Fatalf("unexpected attempts: %d", attempts)
	}
	var headers map[string]any
	if err := json.Unmarshal(headersRaw, &headers); err != nil {
		t.Fatalf("headers decode: %v", err)
	}
	if headers["trace_id"] != "abc" {
		t.Fatalf("unexpected headers: %#v", headers)
	}
	var retry map[string]any
	if err := json.Unmarshal(retryRaw, &retry); err != nil {
		t.Fatalf("retry decode: %v", err)
	}
	if retry["kind"] != "fixed" {
		t.Fatalf("unexpected retry strategy: %#v", retry)
	}
	var cancellation map[string]any
	if err := json.Unmarshal(cancellationRaw, &cancellation); err != nil {
		t.Fatalf("cancellation decode: %v", err)
	}
	if cancellation["max_delay"].(float64) != 60 || cancellation["max_duration"].(float64) != 120 {
		t.Fatalf("unexpected cancellation: %#v", cancellation)
	}

	dupe, err := client.Spawn(context.Background(), "opts", map[string]any{"value": 99}, absurd.SpawnOptions{IdempotencyKey: "idem-1"})
	if err != nil {
		t.Fatalf("duplicate spawn: %v", err)
	}
	if dupe.TaskID != spawned.TaskID || dupe.RunID != spawned.RunID || dupe.Attempt != spawned.Attempt || dupe.Created {
		t.Fatalf("unexpected duplicate result: %#v vs %#v", dupe, spawned)
	}
	if got := countTasks(t, setupTestDatabase(t), queue); got != 1 {
		t.Fatalf("expected 1 task, got %d", got)
	}
}

func TestDefaultCancellationApplied(t *testing.T) {
	queue := randomQueueName("go_default_cancel")
	db := setupTestDatabase(t)
	client := newTestClient(t, queue)
	base := time.Date(2024, 5, 1, 8, 0, 0, 0, time.UTC)
	setFakeNow(t, db, &base)
	defer setFakeNow(t, db, nil)

	client.MustRegister(absurd.Task(
		"slow",
		func(ctx context.Context, params map[string]any) (map[string]any, error) {
			return map[string]any{"ok": true}, nil
		},
		absurd.TaskOptions{DefaultCancellation: &absurd.CancellationPolicy{MaxDelay: 60}},
	))

	spawned, err := client.Spawn(context.Background(), "slow", nil)
	if err != nil {
		t.Fatalf("Spawn: %v", err)
	}

	now := base.Add(61 * time.Second)
	setFakeNow(t, db, &now)
	if err := client.WorkBatch(context.Background(), absurd.WorkBatchOptions{WorkerID: "worker"}); err != nil {
		t.Fatalf("WorkBatch: %v", err)
	}

	state, _, _, _, _, cancelledAt := fetchTaskRow(t, db, queue, spawned.TaskID)
	if state != string(absurd.TaskCancelled) || !cancelledAt.Valid {
		t.Fatalf("expected cancelled task, got state=%s cancelledAt=%v", state, cancelledAt)
	}
}

func TestRetryTaskParity(t *testing.T) {
	queue := randomQueueName("go_retry_task")
	db := setupTestDatabase(t)
	client := newTestClient(t, queue)
	client.MustRegister(absurd.Task(
		"checkpoint-then-fail",
		func(ctx context.Context, params map[string]any) (map[string]any, error) {
			_, err := absurd.Step(ctx, "step-1", func(ctx context.Context) (map[string]any, error) {
				return map[string]any{"ok": true}, nil
			})
			if err != nil {
				return nil, err
			}
			return nil, errors.New("boom")
		},
		absurd.TaskOptions{DefaultMaxAttempts: 1},
	))

	spawned, err := client.Spawn(context.Background(), "checkpoint-then-fail", map[string]any{"payload": 1})
	if err != nil {
		t.Fatalf("Spawn: %v", err)
	}
	if err := client.WorkBatch(context.Background(), absurd.WorkBatchOptions{WorkerID: "worker"}); err != nil {
		t.Fatalf("WorkBatch: %v", err)
	}
	state, attempts, _, _, _, _ := fetchTaskRow(t, db, queue, spawned.TaskID)
	if state != string(absurd.TaskFailed) || attempts != 1 {
		t.Fatalf("unexpected failed task state=%s attempts=%d", state, attempts)
	}
	if got := countCheckpoints(t, db, queue, spawned.TaskID); got != 1 {
		t.Fatalf("expected 1 checkpoint, got %d", got)
	}

	retry, err := client.RetryTask(context.Background(), queue, spawned.TaskID)
	if err != nil {
		t.Fatalf("RetryTask: %v", err)
	}
	if retry.TaskID != spawned.TaskID || retry.Attempt != 2 || retry.Created {
		t.Fatalf("unexpected retry result: %#v", retry)
	}

	spawned2, err := client.Spawn(context.Background(), "checkpoint-then-fail", map[string]any{"payload": 2})
	if err != nil {
		t.Fatalf("Spawn second failing task: %v", err)
	}
	if err := client.WorkBatch(context.Background(), absurd.WorkBatchOptions{WorkerID: "worker", BatchSize: 10}); err != nil {
		t.Fatalf("WorkBatch second task: %v", err)
	}

	respawn, err := client.RetryTask(context.Background(), queue, spawned2.TaskID, absurd.RetryTaskOptions{SpawnNew: true})
	if err != nil {
		t.Fatalf("RetryTask spawn_new: %v", err)
	}
	if !respawn.Created || respawn.Attempt != 1 || respawn.TaskID == spawned2.TaskID {
		t.Fatalf("unexpected respawn result: %#v", respawn)
	}
	if got := countCheckpoints(t, db, queue, respawn.TaskID); got != 0 {
		t.Fatalf("expected 0 checkpoints on respawned task, got %d", got)
	}
}

func TestUnknownTaskRequiresExplicitQueueAtSpawn(t *testing.T) {
	queue := randomQueueName("go_unknown")
	client := newTestClient(t, queue)

	_, err := client.Spawn(context.Background(), "ghost-task", map[string]any{"value": 1})
	if err == nil {
		t.Fatal("expected spawn to fail for unregistered task without explicit queue")
	}
	if !strings.Contains(err.Error(), `task "ghost-task" is not registered`) {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestUnknownTaskWithExplicitQueueDefersClaimedRun(t *testing.T) {
	queue := randomQueueName("go_unknown_explicit")
	db := setupTestDatabase(t)
	client := newTestClient(t, queue)

	base := time.Date(2024, time.April, 1, 10, 0, 0, 0, time.UTC)
	setFakeNow(t, db, &base)
	defer setFakeNow(t, db, nil)

	spawned, err := client.Spawn(context.Background(), "ghost-task", map[string]any{"value": 1}, absurd.SpawnOptions{QueueName: queue, MaxAttempts: 1})
	if err != nil {
		t.Fatalf("Spawn: %v", err)
	}
	if err := client.WorkBatch(context.Background(), absurd.WorkBatchOptions{WorkerID: "worker"}); err != nil {
		t.Fatalf("WorkBatch: %v", err)
	}

	state, attempts, _, _, _, _ := fetchTaskRow(t, db, queue, spawned.TaskID)
	if state != string(absurd.TaskSleeping) || attempts != 1 {
		t.Fatalf("unexpected task state=%s attempts=%d", state, attempts)
	}

	query := fmt.Sprintf(`select state, available_at, failure_reason from absurd.r_%s where run_id = $1`, queue)
	var runState string
	var availableAt time.Time
	var failureReason []byte
	if err := db.QueryRow(query, spawned.RunID).Scan(&runState, &availableAt, &failureReason); err != nil {
		t.Fatalf("fetch run row: %v", err)
	}
	if runState != "sleeping" {
		t.Fatalf("unexpected run state=%s", runState)
	}
	if !availableAt.After(base) {
		t.Fatalf("expected available_at after base time: base=%s available_at=%s", base, availableAt)
	}
	if len(failureReason) != 0 {
		t.Fatalf("expected no failure reason, got: %s", string(failureReason))
	}
}

func TestQueueMismatchFailsClaimedRunImmediately(t *testing.T) {
	queue := randomQueueName("go_queue_mismatch")
	otherQueue := randomQueueName("go_queue_mismatch_other")
	db := setupTestDatabase(t)

	workerClient, err := absurd.New(absurd.Options{DB: db, QueueName: queue})
	if err != nil {
		t.Fatalf("New worker client: %v", err)
	}
	if err := workerClient.CreateQueue(context.Background(), queue); err != nil {
		t.Fatalf("CreateQueue queue: %v", err)
	}
	if err := workerClient.CreateQueue(context.Background(), otherQueue); err != nil {
		t.Fatalf("CreateQueue otherQueue: %v", err)
	}
	workerClient.MustRegister(absurd.Task("misqueued-task", func(ctx context.Context, params map[string]any) (map[string]any, error) {
		return map[string]any{"ok": true}, nil
	}, absurd.TaskOptions{QueueName: otherQueue, DefaultMaxAttempts: 1}))

	producer, err := absurd.New(absurd.Options{DB: db, QueueName: queue})
	if err != nil {
		t.Fatalf("New producer client: %v", err)
	}

	spawned, err := producer.Spawn(context.Background(), "misqueued-task", nil, absurd.SpawnOptions{QueueName: queue, MaxAttempts: 1})
	if err != nil {
		t.Fatalf("Spawn: %v", err)
	}
	if err := workerClient.WorkBatch(context.Background(), absurd.WorkBatchOptions{WorkerID: "worker"}); err != nil {
		t.Fatalf("WorkBatch: %v", err)
	}

	state, attempts, _, _, _, _ := fetchTaskRow(t, db, queue, spawned.TaskID)
	if state != string(absurd.TaskFailed) || attempts != 1 {
		t.Fatalf("unexpected task state=%s attempts=%d", state, attempts)
	}
	failure := fetchFailure(t, db, queue, spawned.RunID)
	message, _ := failure["message"].(string)
	if !strings.Contains(message, `queue mismatch`) {
		t.Fatalf("unexpected failure payload: %#v", failure)
	}
}

func TestCancelTaskParity(t *testing.T) {
	queue := randomQueueName("go_cancel")
	db := setupTestDatabase(t)
	client := newTestClient(t, queue)
	client.MustRegister(absurd.Task("pending-cancel", func(ctx context.Context, params map[string]any) (map[string]any, error) {
		return map[string]any{"ok": true}, nil
	}))

	spawned, err := client.Spawn(context.Background(), "pending-cancel", map[string]any{"data": 1})
	if err != nil {
		t.Fatalf("Spawn: %v", err)
	}
	if err := client.CancelTask(context.Background(), queue, spawned.TaskID); err != nil {
		t.Fatalf("CancelTask: %v", err)
	}
	state, _, _, _, _, cancelledAt := fetchTaskRow(t, db, queue, spawned.TaskID)
	if state != string(absurd.TaskCancelled) || !cancelledAt.Valid {
		t.Fatalf("unexpected cancelled task: state=%s cancelledAt=%v", state, cancelledAt)
	}
}

func TestHooksParity(t *testing.T) {
	queue := randomQueueName("go_hooks")
	var order []string
	var capturedHeaders []map[string]any
	client, err := absurd.New(absurd.Options{
		DB:        setupTestDatabase(t),
		QueueName: queue,
		Hooks: absurd.Hooks{
			BeforeSpawn: func(taskName string, params any, options absurd.SpawnOptions) (absurd.SpawnOptions, error) {
				options.Headers = map[string]any{"trace_id": "trace-123", "correlation_id": "corr-456"}
				return options, nil
			},
			WrapTaskExecution: func(ctx *absurd.TaskContext, execute func() (any, error)) (any, error) {
				order = append(order, "before")
				rv, err := execute()
				order = append(order, "after")
				return rv, err
			},
		},
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := client.CreateQueue(context.Background(), queue); err != nil {
		t.Fatalf("CreateQueue: %v", err)
	}
	client.MustRegister(absurd.Task("capture", func(ctx context.Context, params map[string]any) (string, error) {
		order = append(order, "handler")
		capturedHeaders = append(capturedHeaders, absurd.MustTaskContext(ctx).Headers())
		return "done", nil
	}))
	if _, err := client.Spawn(context.Background(), "capture", nil); err != nil {
		t.Fatalf("Spawn: %v", err)
	}
	if err := client.WorkBatch(context.Background(), absurd.WorkBatchOptions{WorkerID: "worker"}); err != nil {
		t.Fatalf("WorkBatch: %v", err)
	}
	if strings.Join(order, ",") != "before,handler,after" {
		t.Fatalf("unexpected order: %v", order)
	}
	if len(capturedHeaders) != 1 || capturedHeaders[0]["trace_id"] != "trace-123" || capturedHeaders[0]["correlation_id"] != "corr-456" {
		t.Fatalf("unexpected headers: %#v", capturedHeaders)
	}
}

func TestTaskContextAwaitTaskResultParity(t *testing.T) {
	parentQueue := randomQueueName("go_parent")
	childQueue := randomQueueName("go_child")
	db := setupTestDatabase(t)
	parentClient := newTestClient(t, parentQueue)
	childClient, err := absurd.New(absurd.Options{DB: db, QueueName: childQueue})
	if err != nil {
		t.Fatalf("New child client: %v", err)
	}
	if err := childClient.CreateQueue(context.Background(), childQueue); err != nil {
		t.Fatalf("CreateQueue child: %v", err)
	}

	childClient.MustRegister(absurd.Task("child", func(ctx context.Context, params map[string]any) (map[string]any, error) {
		return map[string]any{"child": "ok"}, nil
	}))
	parentClient.MustRegister(absurd.Task("parent", func(ctx context.Context, params map[string]any) (map[string]any, error) {
		taskCtx := absurd.MustTaskContext(ctx)
		snapshot, err := taskCtx.AwaitTaskResult(ctx, childQueue, params["child_id"].(string), absurd.AwaitTaskResultOptions{
			Timeout: 5 * time.Second,
		})
		if err != nil {
			return nil, err
		}
		var result map[string]any
		if err := snapshot.DecodeResult(&result); err != nil {
			return nil, err
		}
		return result, nil
	}))

	childSpawned, err := childClient.Spawn(context.Background(), "child", nil)
	if err != nil {
		t.Fatalf("spawn child: %v", err)
	}
	parentSpawned, err := parentClient.Spawn(context.Background(), "parent", map[string]any{"child_id": childSpawned.TaskID})
	if err != nil {
		t.Fatalf("spawn parent: %v", err)
	}

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		time.Sleep(150 * time.Millisecond)
		_ = childClient.WorkBatch(context.Background(), absurd.WorkBatchOptions{WorkerID: "child-worker"})
	}()
	defer wg.Wait()

	if err := parentClient.WorkBatch(context.Background(), absurd.WorkBatchOptions{WorkerID: "parent-worker", ClaimTimeout: time.Second}); err != nil {
		t.Fatalf("parent WorkBatch: %v", err)
	}
	snapshot, err := parentClient.AwaitTaskResult(context.Background(), parentQueue, parentSpawned.TaskID, absurd.AwaitTaskResultOptions{Timeout: 12 * time.Second})
	if err != nil {
		t.Fatalf("AwaitTaskResult: %v", err)
	}
	var result map[string]any
	if err := snapshot.DecodeResult(&result); err != nil {
		t.Fatalf("DecodeResult: %v", err)
	}
	if result["child"] != "ok" {
		t.Fatalf("unexpected result: %#v", result)
	}
}

func TestTaskContextAwaitTaskResultCheckpointSurvivesChildCleanupOnRetry(t *testing.T) {
	parentQueue := randomQueueName("go_parent_cleanup")
	childQueue := randomQueueName("go_child_cleanup")
	db := setupTestDatabase(t)
	parentClient := newTestClient(t, parentQueue)
	childClient, err := absurd.New(absurd.Options{DB: db, QueueName: childQueue})
	if err != nil {
		t.Fatalf("New child client: %v", err)
	}
	if err := childClient.CreateQueue(context.Background(), childQueue); err != nil {
		t.Fatalf("CreateQueue child: %v", err)
	}

	childClient.MustRegister(absurd.Task("child", func(ctx context.Context, params map[string]any) (map[string]any, error) {
		return map[string]any{"child": "ok"}, nil
	}))
	parentClient.MustRegister(absurd.Task(
		"parent",
		func(ctx context.Context, params map[string]any) (map[string]any, error) {
			taskCtx := absurd.MustTaskContext(ctx)
			snapshot, err := taskCtx.AwaitTaskResult(ctx, childQueue, params["child_id"].(string), absurd.AwaitTaskResultOptions{
				Timeout: 5 * time.Second,
			})
			if err != nil {
				return nil, err
			}
			var result map[string]any
			if err := snapshot.DecodeResult(&result); err != nil {
				return nil, err
			}
			if taskCtx.Attempt() == 1 {
				return nil, errors.New("retry after child result observed")
			}
			return result, nil
		},
		absurd.TaskOptions{DefaultMaxAttempts: 2},
	))

	childSpawned, err := childClient.Spawn(context.Background(), "child", nil)
	if err != nil {
		t.Fatalf("spawn child: %v", err)
	}
	if err := childClient.WorkBatch(context.Background(), absurd.WorkBatchOptions{WorkerID: "child-worker"}); err != nil {
		t.Fatalf("child WorkBatch: %v", err)
	}

	parentSpawned, err := parentClient.Spawn(context.Background(), "parent", map[string]any{"child_id": childSpawned.TaskID})
	if err != nil {
		t.Fatalf("spawn parent: %v", err)
	}
	if err := parentClient.WorkBatch(context.Background(), absurd.WorkBatchOptions{WorkerID: "parent-worker"}); err != nil {
		t.Fatalf("parent first WorkBatch: %v", err)
	}
	if got := countCheckpoints(t, db, parentQueue, parentSpawned.TaskID); got != 1 {
		t.Fatalf("expected durable child-result checkpoint, got %d", got)
	}

	var deleted int
	if err := db.QueryRowContext(context.Background(), `SELECT absurd.cleanup_tasks($1, $2, $3)`, childQueue, 0, 10).Scan(&deleted); err != nil {
		t.Fatalf("cleanup child tasks: %v", err)
	}
	if deleted != 1 {
		t.Fatalf("expected 1 deleted child task, got %d", deleted)
	}
	childSnapshot, err := childClient.FetchTaskResult(context.Background(), childQueue, childSpawned.TaskID)
	if err != nil {
		t.Fatalf("FetchTaskResult child: %v", err)
	}
	if childSnapshot != nil {
		t.Fatalf("expected cleaned up child snapshot to be gone, got %#v", childSnapshot)
	}

	if err := parentClient.WorkBatch(context.Background(), absurd.WorkBatchOptions{WorkerID: "parent-worker"}); err != nil {
		t.Fatalf("parent retry WorkBatch: %v", err)
	}
	snapshot, err := parentClient.AwaitTaskResult(context.Background(), parentQueue, parentSpawned.TaskID, absurd.AwaitTaskResultOptions{Timeout: 5 * time.Second})
	if err != nil {
		t.Fatalf("AwaitTaskResult parent: %v", err)
	}
	var result map[string]any
	if err := snapshot.DecodeResult(&result); err != nil {
		t.Fatalf("DecodeResult parent: %v", err)
	}
	if result["child"] != "ok" {
		t.Fatalf("unexpected result after retry: %#v", result)
	}
}

func TestTaskContextAwaitTaskResultHeartbeatUsesReceiverState(t *testing.T) {
	parentQueue := randomQueueName("go_parent_heartbeat")
	childQueue := randomQueueName("go_child_heartbeat")
	db := setupTestDatabase(t)
	parentClient := newTestClient(t, parentQueue)
	childClient, err := absurd.New(absurd.Options{DB: db, QueueName: childQueue})
	if err != nil {
		t.Fatalf("New child client: %v", err)
	}
	if err := childClient.CreateQueue(context.Background(), childQueue); err != nil {
		t.Fatalf("CreateQueue child: %v", err)
	}

	childClient.MustRegister(absurd.Task("slow-child", func(ctx context.Context, params map[string]any) (map[string]any, error) {
		time.Sleep(3 * time.Second)
		return map[string]any{"child": "ok"}, nil
	}))
	parentClient.MustRegister(absurd.Task("parent", func(ctx context.Context, params map[string]any) (map[string]any, error) {
		waitCtx, cancel := context.WithTimeout(context.Background(), 12*time.Second)
		defer cancel()

		taskCtx := absurd.MustTaskContext(ctx)
		snapshot, err := taskCtx.AwaitTaskResult(waitCtx, childQueue, params["child_id"].(string), absurd.AwaitTaskResultOptions{
			Timeout: 10 * time.Second,
		})
		if err != nil {
			return nil, err
		}
		var result map[string]any
		if err := snapshot.DecodeResult(&result); err != nil {
			return nil, err
		}
		return result, nil
	}))

	childSpawned, err := childClient.Spawn(context.Background(), "slow-child", nil)
	if err != nil {
		t.Fatalf("spawn child: %v", err)
	}
	parentSpawned, err := parentClient.Spawn(context.Background(), "parent", map[string]any{"child_id": childSpawned.TaskID})
	if err != nil {
		t.Fatalf("spawn parent: %v", err)
	}

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		_ = childClient.WorkBatch(context.Background(), absurd.WorkBatchOptions{WorkerID: "child-worker"})
	}()
	defer wg.Wait()

	if err := parentClient.WorkBatch(context.Background(), absurd.WorkBatchOptions{WorkerID: "parent-worker", ClaimTimeout: time.Second}); err != nil {
		t.Fatalf("parent WorkBatch: %v", err)
	}
	snapshot, err := parentClient.AwaitTaskResult(context.Background(), parentQueue, parentSpawned.TaskID, absurd.AwaitTaskResultOptions{Timeout: 5 * time.Second})
	if err != nil {
		t.Fatalf("AwaitTaskResult parent: %v", err)
	}
	var result map[string]any
	if err := snapshot.DecodeResult(&result); err != nil {
		t.Fatalf("DecodeResult parent: %v", err)
	}
	if result["child"] != "ok" {
		t.Fatalf("unexpected result: %#v", result)
	}
}

func TestFailureIncludesTraceback(t *testing.T) {
	queue := randomQueueName("go_traceback")
	db := setupTestDatabase(t)
	client := newTestClient(t, queue)
	client.MustRegister(absurd.Task("boom", func(ctx context.Context, params map[string]any) (map[string]any, error) {
		return nil, errors.New("boom")
	}, absurd.TaskOptions{DefaultMaxAttempts: 1}))

	spawned, err := client.Spawn(context.Background(), "boom", nil)
	if err != nil {
		t.Fatalf("Spawn: %v", err)
	}
	if err := client.WorkBatch(context.Background(), absurd.WorkBatchOptions{WorkerID: "worker"}); err != nil {
		t.Fatalf("WorkBatch: %v", err)
	}
	failure := fetchFailure(t, db, queue, spawned.RunID)
	if failure["message"] != "boom" {
		t.Fatalf("unexpected failure payload: %#v", failure)
	}
	traceback, _ := failure["traceback"].(string)
	if traceback == "" {
		t.Fatalf("expected traceback in failure payload, got %#v", failure)
	}
	if _, ok := failure["name"].(string); !ok {
		t.Fatalf("expected failure name in payload, got %#v", failure)
	}
}

func TestInvalidHeadersFailRun(t *testing.T) {
	queue := randomQueueName("go_invalid_headers")
	db := setupTestDatabase(t)
	client := newTestClient(t, queue)
	client.MustRegister(absurd.Task("headered", func(ctx context.Context, params map[string]any) (map[string]any, error) {
		_ = absurd.MustTaskContext(ctx).Headers()
		return map[string]any{"ok": true}, nil
	}))

	spawned, err := client.Spawn(context.Background(), "headered", nil)
	if err != nil {
		t.Fatalf("Spawn: %v", err)
	}

	query := fmt.Sprintf(`update absurd.t_%s set headers = '"not-an-object"'::jsonb where task_id = $1`, queue)
	if _, err := db.ExecContext(context.Background(), query, spawned.TaskID); err != nil {
		t.Fatalf("inject invalid headers: %v", err)
	}

	if err := client.WorkBatch(context.Background(), absurd.WorkBatchOptions{WorkerID: "worker"}); err != nil {
		t.Fatalf("WorkBatch: %v", err)
	}

	failure := fetchFailure(t, db, queue, spawned.RunID)
	message, _ := failure["message"].(string)
	if !strings.Contains(message, "invalid task headers") {
		t.Fatalf("unexpected failure payload: %#v", failure)
	}
}
