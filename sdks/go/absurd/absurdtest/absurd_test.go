package absurdtest

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/earendil-works/absurd/sdks/go/absurd"
	_ "github.com/lib/pq"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

var (
	testDB        *sql.DB
	testContainer testcontainers.Container
	testSetupOnce sync.Once
	testSetupErr  error
)

func TestMain(m *testing.M) {
	code := m.Run()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if testDB != nil {
		_ = testDB.Close()
	}
	if testContainer != nil {
		_ = testContainer.Terminate(ctx)
	}
	os.Exit(code)
}

func setupTestDatabase(t *testing.T) *sql.DB {
	t.Helper()
	testSetupOnce.Do(func() {
		if os.Getenv("DOCKER_HOST") == "" {
			dockerSock := filepath.Join(os.Getenv("HOME"), ".docker", "run", "docker.sock")
			if _, err := os.Stat(dockerSock); err == nil {
				_ = os.Setenv("DOCKER_HOST", "unix://"+dockerSock)
			}
		}

		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
		defer cancel()

		testContainer, testSetupErr = testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
			ContainerRequest: testcontainers.ContainerRequest{
				Image:        "postgres:16-alpine",
				ExposedPorts: []string{"5432/tcp"},
				Env: map[string]string{
					"POSTGRES_DB":       "absurd_test",
					"POSTGRES_USER":     "postgres",
					"POSTGRES_PASSWORD": "postgres",
				},
				WaitingFor: wait.ForLog("database system is ready to accept connections").WithOccurrence(2).WithStartupTimeout(90 * time.Second),
			},
			Started: true,
		})
		if testSetupErr != nil {
			return
		}

		host, err := testContainer.Host(ctx)
		if err != nil {
			testSetupErr = err
			return
		}
		port, err := testContainer.MappedPort(ctx, "5432/tcp")
		if err != nil {
			testSetupErr = err
			return
		}
		host = strings.ReplaceAll(host, "localhost", "127.0.0.1")
		dsn := fmt.Sprintf("postgres://postgres:postgres@%s:%s/absurd_test?sslmode=disable", host, port.Port())

		testDB, testSetupErr = sql.Open("postgres", dsn)
		if testSetupErr != nil {
			return
		}
		testDB.SetMaxOpenConns(4)
		testDB.SetMaxIdleConns(4)

		if testSetupErr = testDB.PingContext(ctx); testSetupErr != nil {
			return
		}

		_, filename, _, _ := runtime.Caller(0)
		schemaPath := filepath.Join(filepath.Dir(filename), "..", "..", "..", "..", "sql", "absurd.sql")
		var schema []byte
		schema, testSetupErr = os.ReadFile(schemaPath)
		if testSetupErr != nil {
			return
		}
		_, testSetupErr = testDB.ExecContext(ctx, string(schema))
	})
	if testSetupErr != nil {
		t.Fatalf("failed to set up test database: %v", testSetupErr)
	}
	return testDB
}

func newTestClient(t *testing.T, queue string) *absurd.Client {
	t.Helper()
	db := setupTestDatabase(t)
	client, err := absurd.New(absurd.Options{
		DB:        db,
		QueueName: queue,
	})
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}
	if err := client.CreateQueue(context.Background(), queue); err != nil {
		t.Fatalf("CreateQueue failed: %v", err)
	}
	return client
}

func randomQueueName(prefix string) string {
	return fmt.Sprintf("%s_%d", prefix, time.Now().UnixNano())
}

type welcomeParams struct {
	UserID int `json:"user_id"`
}

type welcomeResult struct {
	Status string `json:"status"`
	UserID int    `json:"user_id"`
}

func TestWorkBatchProcessesTaskAndPreservesTaskContext(t *testing.T) {
	queue := randomQueueName("go_sdk")
	client := newTestClient(t, queue)

	var (
		stepCalls    int
		derivedFound bool
	)

	welcomeTask := absurd.Task(
		"send-welcome",
		func(ctx context.Context, params welcomeParams) (welcomeResult, error) {
			task := absurd.MustTaskContext(ctx)
			if task.QueueName() != queue {
				t.Fatalf("unexpected queue: %s", task.QueueName())
			}

			derivedCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
			defer cancel()

			profile, err := absurd.Step(derivedCtx, "load-profile", func(stepCtx context.Context) (map[string]int, error) {
				stepCalls++
				derivedTask, ok := absurd.TaskFromContext(stepCtx)
				if !ok {
					t.Fatal("task context not found in derived context")
				}
				if derivedTask.TaskID() != task.TaskID() {
					t.Fatalf("unexpected task id: %s != %s", derivedTask.TaskID(), task.TaskID())
				}
				derivedFound = true
				return map[string]int{"user_id": params.UserID}, nil
			})
			if err != nil {
				return welcomeResult{}, err
			}

			handle, err := absurd.BeginStep[map[string]any](ctx, "audit")
			if err != nil {
				return welcomeResult{}, err
			}
			if !handle.Done {
				if _, err := handle.CompleteStep(ctx, map[string]any{"ok": true}); err != nil {
					return welcomeResult{}, err
				}
			}

			return welcomeResult{Status: "sent", UserID: profile["user_id"]}, nil
		},
	)
	client.MustRegister(welcomeTask)

	spawned, err := welcomeTask.Spawn(context.Background(), client, welcomeParams{UserID: 42})
	if err != nil {
		t.Fatalf("Spawn failed: %v", err)
	}
	if err := client.WorkBatch(context.Background(), absurd.WorkBatchOptions{WorkerID: "sync-worker"}); err != nil {
		t.Fatalf("WorkBatch failed: %v", err)
	}

	snapshot, err := client.FetchTaskResult(context.Background(), queue, spawned.TaskID)
	if err != nil {
		t.Fatalf("FetchTaskResult failed: %v", err)
	}
	if snapshot == nil {
		t.Fatal("expected task snapshot")
	}
	if snapshot.State != absurd.TaskCompleted {
		t.Fatalf("unexpected task state: %s", snapshot.State)
	}
	var result welcomeResult
	if err := snapshot.DecodeResult(&result); err != nil {
		t.Fatalf("DecodeResult failed: %v", err)
	}
	if result != (welcomeResult{Status: "sent", UserID: 42}) {
		t.Fatalf("unexpected result: %#v", result)
	}
	if stepCalls != 1 {
		t.Fatalf("expected 1 step call, got %d", stepCalls)
	}
	if !derivedFound {
		t.Fatal("expected task context to survive derived context")
	}
}

type provisionParams struct {
	UserID string `json:"user_id"`
	Email  string `json:"email"`
}

type activationEvent struct {
	ActivatedAt time.Time `json:"activated_at"`
}

type provisionResult struct {
	UserID      string    `json:"user_id"`
	Email       string    `json:"email"`
	Status      string    `json:"status"`
	ActivatedAt time.Time `json:"activated_at"`
}

func TestQuickstartStyleAwaitEventFlow(t *testing.T) {
	queue := randomQueueName("go_quickstart")
	client := newTestClient(t, queue)

	provisionTask := absurd.Task(
		"provision-user",
		func(ctx context.Context, params provisionParams) (provisionResult, error) {
			task := absurd.MustTaskContext(ctx)
			_, err := absurd.Step(ctx, "create-user-record", func(ctx context.Context) (map[string]string, error) {
				return map[string]string{
					"user_id": params.UserID,
					"email":   params.Email,
				}, nil
			})
			if err != nil {
				return provisionResult{}, err
			}

			outage, err := absurd.BeginStep[map[string]any](ctx, "demo-transient-outage")
			if err != nil {
				return provisionResult{}, err
			}
			if !outage.Done {
				if _, err := outage.CompleteStep(ctx, map[string]any{"simulated": false}); err != nil {
					return provisionResult{}, err
				}
			}

			eventName := fmt.Sprintf("user-activated:%s", params.UserID)
			activation, err := absurd.AwaitEvent[activationEvent](ctx, eventName, absurd.AwaitEventOptions{Timeout: time.Hour})
			if err != nil {
				return provisionResult{}, err
			}
			return provisionResult{
				UserID:      task.TaskID(),
				Email:       params.Email,
				Status:      "active",
				ActivatedAt: activation.ActivatedAt,
			}, nil
		},
		absurd.TaskOptions{DefaultMaxAttempts: 2},
	)
	client.MustRegister(provisionTask)

	spawned, err := provisionTask.Spawn(context.Background(), client, provisionParams{
		UserID: "alice",
		Email:  "alice@example.com",
	})
	if err != nil {
		t.Fatalf("Spawn failed: %v", err)
	}

	if err := client.WorkBatch(context.Background(), absurd.WorkBatchOptions{WorkerID: "waiter"}); err != nil {
		t.Fatalf("WorkBatch failed: %v", err)
	}

	snapshot, err := client.FetchTaskResult(context.Background(), queue, spawned.TaskID)
	if err != nil {
		t.Fatalf("FetchTaskResult failed: %v", err)
	}
	if snapshot == nil || snapshot.State != absurd.TaskSleeping {
		t.Fatalf("expected sleeping snapshot, got %#v", snapshot)
	}

	activatedAt := time.Now().UTC().Round(time.Second)
	if err := client.EmitEvent(context.Background(), queue, "user-activated:alice", activationEvent{ActivatedAt: activatedAt}); err != nil {
		t.Fatalf("EmitEvent failed: %v", err)
	}
	if err := client.WorkBatch(context.Background(), absurd.WorkBatchOptions{WorkerID: "waiter"}); err != nil {
		t.Fatalf("WorkBatch after event failed: %v", err)
	}

	finalSnapshot, err := client.AwaitTaskResult(context.Background(), queue, spawned.TaskID, absurd.AwaitTaskResultOptions{Timeout: 5 * time.Second})
	if err != nil {
		t.Fatalf("AwaitTaskResult failed: %v", err)
	}
	if finalSnapshot.State != absurd.TaskCompleted {
		t.Fatalf("unexpected final state: %s", finalSnapshot.State)
	}
	var result provisionResult
	if err := finalSnapshot.DecodeResult(&result); err != nil {
		t.Fatalf("DecodeResult failed: %v", err)
	}
	if result.Email != "alice@example.com" || result.Status != "active" {
		t.Fatalf("unexpected result: %#v", result)
	}
	if !result.ActivatedAt.Equal(activatedAt) {
		t.Fatalf("unexpected activated_at: %s != %s", result.ActivatedAt, activatedAt)
	}
}

func TestRunWorkerAndAwaitTaskResult(t *testing.T) {
	queue := randomQueueName("go_worker")
	client := newTestClient(t, queue)

	workerTask := absurd.Task(
		"double",
		func(ctx context.Context, params map[string]int) (map[string]int, error) {
			value, err := absurd.Step(ctx, "double", func(ctx context.Context) (int, error) {
				return params["value"] * 2, nil
			})
			if err != nil {
				return nil, err
			}
			return map[string]int{"processed": value}, nil
		},
	)
	client.MustRegister(workerTask)

	workerCtx, cancel := context.WithCancel(context.Background())
	defer cancel()

	workerDone := make(chan error, 1)
	go func() {
		workerDone <- client.RunWorker(workerCtx, absurd.WorkerOptions{
			WorkerID:     "bg-worker",
			Concurrency:  2,
			PollInterval: 50 * time.Millisecond,
		})
	}()

	spawned, err := workerTask.Spawn(context.Background(), client, map[string]int{"value": 21})
	if err != nil {
		t.Fatalf("Spawn failed: %v", err)
	}

	finalSnapshot, err := client.AwaitTaskResult(context.Background(), queue, spawned.TaskID, absurd.AwaitTaskResultOptions{Timeout: 10 * time.Second})
	if err != nil {
		t.Fatalf("AwaitTaskResult failed: %v", err)
	}
	if finalSnapshot.State != absurd.TaskCompleted {
		t.Fatalf("unexpected final state: %s", finalSnapshot.State)
	}
	var result map[string]int
	if err := finalSnapshot.DecodeResult(&result); err != nil {
		t.Fatalf("DecodeResult failed: %v", err)
	}
	if result["processed"] != 42 {
		t.Fatalf("unexpected processed result: %#v", result)
	}

	cancel()
	if err := <-workerDone; err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("RunWorker failed: %v", err)
	}
}

func TestAwaitTaskResultNegativeTimeoutTimesOutImmediately(t *testing.T) {
	queue := randomQueueName("go_negative_task_timeout")
	client := newTestClient(t, queue)
	client.MustRegister(absurd.Task("pending", func(ctx context.Context, params map[string]any) (map[string]any, error) {
		return map[string]any{"ok": true}, nil
	}))

	spawned, err := client.Spawn(context.Background(), "pending", nil)
	if err != nil {
		t.Fatalf("Spawn: %v", err)
	}

	startedAt := time.Now()
	_, err = client.AwaitTaskResult(context.Background(), queue, spawned.TaskID, absurd.AwaitTaskResultOptions{Timeout: -time.Second})
	var timeoutErr *absurd.TimeoutError
	if !errors.As(err, &timeoutErr) {
		t.Fatalf("expected TimeoutError, got %v", err)
	}
	if elapsed := time.Since(startedAt); elapsed > 250*time.Millisecond {
		t.Fatalf("negative timeout should fail fast, took %s", elapsed)
	}
}

func TestAwaitEventNegativeTimeoutDoesNotSleepForever(t *testing.T) {
	queue := randomQueueName("go_negative_event_timeout")
	client := newTestClient(t, queue)
	client.MustRegister(absurd.Task("wait-negative-timeout", func(ctx context.Context, params map[string]any) (map[string]any, error) {
		_, err := absurd.AwaitEvent[map[string]any](ctx, "never-emitted", absurd.AwaitEventOptions{Timeout: -time.Second})
		if err != nil {
			var timeoutErr *absurd.TimeoutError
			if errors.As(err, &timeoutErr) {
				return map[string]any{"timed_out": true}, nil
			}
			return nil, err
		}
		return map[string]any{"timed_out": false}, nil
	}))

	spawned, err := client.Spawn(context.Background(), "wait-negative-timeout", nil)
	if err != nil {
		t.Fatalf("Spawn: %v", err)
	}
	if err := client.WorkBatch(context.Background(), absurd.WorkBatchOptions{WorkerID: "worker"}); err != nil {
		t.Fatalf("WorkBatch first pass: %v", err)
	}
	if err := client.WorkBatch(context.Background(), absurd.WorkBatchOptions{WorkerID: "worker"}); err != nil {
		t.Fatalf("WorkBatch second pass: %v", err)
	}

	snapshot, err := client.AwaitTaskResult(context.Background(), queue, spawned.TaskID, absurd.AwaitTaskResultOptions{Timeout: time.Second})
	if err != nil {
		t.Fatalf("AwaitTaskResult: %v", err)
	}
	var result map[string]bool
	if err := snapshot.DecodeResult(&result); err != nil {
		t.Fatalf("DecodeResult: %v", err)
	}
	if !result["timed_out"] {
		t.Fatalf("expected timeout result, got %#v", result)
	}
}

func TestRunWorkerRecoversTaskPanics(t *testing.T) {
	queue := randomQueueName("go_worker_panic")
	db := setupTestDatabase(t)
	client := newTestClient(t, queue)

	client.MustRegister(absurd.Task("panic-task", func(ctx context.Context, params map[string]any) (map[string]any, error) {
		panic("boom")
	}, absurd.TaskOptions{DefaultMaxAttempts: 1}))
	client.MustRegister(absurd.Task("ok-task", func(ctx context.Context, params map[string]any) (map[string]bool, error) {
		return map[string]bool{"ok": true}, nil
	}, absurd.TaskOptions{DefaultMaxAttempts: 1}))

	workerCtx, cancel := context.WithCancel(context.Background())
	defer cancel()

	workerDone := make(chan error, 1)
	go func() {
		workerDone <- client.RunWorker(workerCtx, absurd.WorkerOptions{
			WorkerID:     "panic-worker",
			Concurrency:  1,
			PollInterval: 25 * time.Millisecond,
		})
	}()

	panicked, err := client.Spawn(context.Background(), "panic-task", nil)
	if err != nil {
		t.Fatalf("Spawn panic-task failed: %v", err)
	}

	panicSnapshot, err := client.AwaitTaskResult(context.Background(), queue, panicked.TaskID, absurd.AwaitTaskResultOptions{Timeout: 10 * time.Second})
	if err != nil {
		t.Fatalf("AwaitTaskResult panic-task failed: %v", err)
	}
	if panicSnapshot.State != absurd.TaskFailed {
		t.Fatalf("unexpected panic-task state: %s", panicSnapshot.State)
	}
	failure := fetchFailure(t, db, queue, panicked.RunID)
	message, _ := failure["message"].(string)
	if !strings.Contains(message, "panic: boom") {
		t.Fatalf("unexpected panic failure payload: %#v", failure)
	}

	select {
	case err := <-workerDone:
		t.Fatalf("worker exited after panic: %v", err)
	default:
	}

	okTask, err := client.Spawn(context.Background(), "ok-task", nil)
	if err != nil {
		t.Fatalf("Spawn ok-task failed: %v", err)
	}
	okSnapshot, err := client.AwaitTaskResult(context.Background(), queue, okTask.TaskID, absurd.AwaitTaskResultOptions{Timeout: 10 * time.Second})
	if err != nil {
		t.Fatalf("AwaitTaskResult ok-task failed: %v", err)
	}
	if okSnapshot.State != absurd.TaskCompleted {
		t.Fatalf("unexpected ok-task state: %s", okSnapshot.State)
	}

	cancel()
	select {
	case err := <-workerDone:
		if err != nil && !errors.Is(err, context.Canceled) {
			t.Fatalf("RunWorker failed: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for worker shutdown")
	}
}

func TestRunWorkerShutdownDrainsClaimedTasks(t *testing.T) {
	queue := randomQueueName("go_worker_shutdown")
	client := newTestClient(t, queue)

	started := make(chan struct{})
	release := make(chan struct{})

	client.MustRegister(absurd.Task("slow-task", func(ctx context.Context, params map[string]any) (string, error) {
		close(started)
		select {
		case <-release:
			return "done", nil
		case <-ctx.Done():
			return "", ctx.Err()
		}
	}, absurd.TaskOptions{DefaultMaxAttempts: 1}))

	workerCtx, cancel := context.WithCancel(context.Background())
	defer cancel()

	workerDone := make(chan error, 1)
	go func() {
		workerDone <- client.RunWorker(workerCtx, absurd.WorkerOptions{
			WorkerID:     "shutdown-worker",
			Concurrency:  1,
			PollInterval: 25 * time.Millisecond,
		})
	}()

	spawned, err := client.Spawn(context.Background(), "slow-task", nil)
	if err != nil {
		t.Fatalf("Spawn slow-task failed: %v", err)
	}

	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for slow-task to start")
	}

	cancel()
	select {
	case err := <-workerDone:
		t.Fatalf("worker exited before draining active task: %v", err)
	case <-time.After(200 * time.Millisecond):
	}

	close(release)

	select {
	case err := <-workerDone:
		if err != nil && !errors.Is(err, context.Canceled) {
			t.Fatalf("RunWorker failed: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for drained worker shutdown")
	}

	finalSnapshot, err := client.AwaitTaskResult(context.Background(), queue, spawned.TaskID, absurd.AwaitTaskResultOptions{Timeout: 10 * time.Second})
	if err != nil {
		t.Fatalf("AwaitTaskResult slow-task failed: %v", err)
	}
	if finalSnapshot.State != absurd.TaskCompleted {
		t.Fatalf("unexpected slow-task state: %s", finalSnapshot.State)
	}
}
