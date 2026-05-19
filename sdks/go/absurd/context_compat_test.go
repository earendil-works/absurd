package absurd

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
)

func TestAwaitEventLegacyTimeoutFallbackWithoutTimedOutColumn(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	eventName := "legacy-timeout-event"
	checkpointName := "$awaitEvent:" + eventName
	client := &Client{db: db, queueName: "default"}
	taskCtx := &TaskContext{
		client:          client,
		queueName:       "default",
		taskID:          "task-1",
		runID:           "run-1",
		claimTimeout:    60 * time.Second,
		wakeEvent:       eventName,
		eventRaw:        nil,
		checkpointCache: make(map[string]json.RawMessage),
		stepNameCounter: make(map[string]int),
	}
	taskCtx.Context = context.WithValue(context.Background(), taskContextKey{}, taskCtx)

	mock.ExpectQuery(`SELECT state\s+FROM absurd\.get_task_checkpoint_state\(\$1, \$2, \$3\)`).
		WithArgs("default", "task-1", checkpointName).
		WillReturnRows(sqlmock.NewRows([]string{"state"}))

	mock.ExpectQuery(`SELECT \*\s+FROM absurd\.await_event\(\$1, \$2, \$3, \$4, \$5, \$6\)`).
		WithArgs("default", "task-1", "run-1", checkpointName, eventName, nil).
		WillReturnRows(sqlmock.NewRows([]string{"should_suspend", "payload"}).AddRow(false, nil))

	_, err = AwaitEvent[map[string]any](taskCtx, eventName)
	var timeoutErr *TimeoutError
	if !errors.As(err, &timeoutErr) {
		t.Fatalf("expected TimeoutError, got %v", err)
	}

	if taskCtx.wakeEvent != "" {
		t.Fatalf("expected wakeEvent to be cleared, got %q", taskCtx.wakeEvent)
	}
	if taskCtx.eventRaw != nil {
		t.Fatalf("expected eventRaw to be cleared, got %#v", taskCtx.eventRaw)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet sql expectations: %v", err)
	}
}
