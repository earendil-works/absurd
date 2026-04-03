package absurdtest

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/earendil-works/absurd/sdks/go/absurd"
)

func TestPopularPostgresDriversWithSharedDBHandle(t *testing.T) {
	for _, driverName := range popularPostgresDrivers {
		driverName := driverName
		t.Run(driverName, func(t *testing.T) {
			queue := randomQueueName(fmt.Sprintf("go_driver_db_%s", driverName))
			client := newTestClientWithDriver(t, queue, driverName)

			client.MustRegister(absurd.Task("echo", func(ctx context.Context, params map[string]int) (map[string]int, error) {
				value, err := absurd.Step(ctx, "double", func(ctx context.Context) (int, error) {
					return params["value"] * 2, nil
				})
				if err != nil {
					return nil, err
				}
				return map[string]int{"value": value}, nil
			}))

			spawned, err := client.Spawn(context.Background(), "echo", map[string]int{"value": 21})
			if err != nil {
				t.Fatalf("Spawn failed: %v", err)
			}
			if err := client.WorkBatch(context.Background(), absurd.WorkBatchOptions{WorkerID: "driver-test-worker"}); err != nil {
				t.Fatalf("WorkBatch failed: %v", err)
			}
			snapshot, err := client.AwaitTaskResult(context.Background(), queue, spawned.TaskID, absurd.AwaitTaskResultOptions{Timeout: 5 * time.Second})
			if err != nil {
				t.Fatalf("AwaitTaskResult failed: %v", err)
			}
			if snapshot.State != absurd.TaskCompleted {
				t.Fatalf("unexpected task state: %s", snapshot.State)
			}
			var result map[string]int
			if err := snapshot.DecodeResult(&result); err != nil {
				t.Fatalf("DecodeResult failed: %v", err)
			}
			if result["value"] != 42 {
				t.Fatalf("unexpected task result: %#v", result)
			}
		})
	}
}

func TestPopularPostgresDriversWithDriverNameOption(t *testing.T) {
	dsn := testDatabaseDSN(t)
	for _, driverName := range popularPostgresDrivers {
		driverName := driverName
		t.Run(driverName, func(t *testing.T) {
			queue := randomQueueName(fmt.Sprintf("go_driver_opt_%s", driverName))
			client, err := absurd.New(absurd.Options{
				DriverName:  driverName,
				DatabaseURL: dsn,
				QueueName:   queue,
			})
			if err != nil {
				t.Fatalf("New failed: %v", err)
			}
			defer func() {
				_ = client.Close()
			}()
			if err := client.CreateQueue(context.Background(), queue); err != nil {
				t.Fatalf("CreateQueue failed: %v", err)
			}

			client.MustRegister(absurd.Task("plus-one", func(ctx context.Context, params map[string]int) (map[string]int, error) {
				return map[string]int{"value": params["value"] + 1}, nil
			}))

			spawned, err := client.Spawn(context.Background(), "plus-one", map[string]int{"value": 41})
			if err != nil {
				t.Fatalf("Spawn failed: %v", err)
			}
			if err := client.WorkBatch(context.Background(), absurd.WorkBatchOptions{WorkerID: "driver-option-test-worker"}); err != nil {
				t.Fatalf("WorkBatch failed: %v", err)
			}
			snapshot, err := client.AwaitTaskResult(context.Background(), queue, spawned.TaskID, absurd.AwaitTaskResultOptions{Timeout: 5 * time.Second})
			if err != nil {
				t.Fatalf("AwaitTaskResult failed: %v", err)
			}
			if snapshot.State != absurd.TaskCompleted {
				t.Fatalf("unexpected task state: %s", snapshot.State)
			}
			var result map[string]int
			if err := snapshot.DecodeResult(&result); err != nil {
				t.Fatalf("DecodeResult failed: %v", err)
			}
			if result["value"] != 42 {
				t.Fatalf("unexpected task result: %#v", result)
			}
		})
	}
}
