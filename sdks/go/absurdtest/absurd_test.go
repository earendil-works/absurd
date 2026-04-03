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

	absurd "github.com/earendil-works/absurd/sdks/go"
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
		schemaPath := filepath.Join(filepath.Dir(filename), "..", "..", "..", "sql", "absurd.sql")
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
				if _, err := handle.Complete(ctx, map[string]any{"ok": true}); err != nil {
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
				if _, err := outage.Complete(ctx, map[string]any{"simulated": false}); err != nil {
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
