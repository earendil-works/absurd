package server

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
)

var scriptedDriverCounter uint64

type scriptedQuery struct {
	match   func(query string, args []driver.NamedValue) error
	columns []string
	rows    [][]driver.Value
	err     error
}

type scriptedState struct {
	mu      sync.Mutex
	queries []scriptedQuery
	index   int
}

func (s *scriptedState) next(query string, args []driver.NamedValue) (driver.Rows, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.index >= len(s.queries) {
		return nil, fmt.Errorf("unexpected query %q with args %#v", query, args)
	}

	spec := s.queries[s.index]
	s.index++

	if spec.match != nil {
		if err := spec.match(query, args); err != nil {
			return nil, err
		}
	}

	if spec.err != nil {
		return nil, spec.err
	}

	return &scriptedRows{columns: spec.columns, rows: spec.rows}, nil
}

func (s *scriptedState) verifyConsumed() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.index != len(s.queries) {
		return fmt.Errorf("consumed %d/%d scripted queries", s.index, len(s.queries))
	}

	return nil
}

type scriptedDriver struct {
	state *scriptedState
}

func (d *scriptedDriver) Open(_ string) (driver.Conn, error) {
	return &scriptedConn{state: d.state}, nil
}

type scriptedConn struct {
	state *scriptedState
}

func (c *scriptedConn) Prepare(_ string) (driver.Stmt, error) {
	return nil, fmt.Errorf("prepare is not supported")
}

func (c *scriptedConn) Close() error {
	return nil
}

func (c *scriptedConn) Begin() (driver.Tx, error) {
	return nil, fmt.Errorf("transactions are not supported")
}

func (c *scriptedConn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	return c.state.next(query, args)
}

var _ driver.QueryerContext = (*scriptedConn)(nil)

type scriptedRows struct {
	columns []string
	rows    [][]driver.Value
	index   int
}

func (r *scriptedRows) Columns() []string {
	return r.columns
}

func (r *scriptedRows) Close() error {
	return nil
}

func (r *scriptedRows) Next(dest []driver.Value) error {
	if r.index >= len(r.rows) {
		return io.EOF
	}

	row := r.rows[r.index]
	r.index++
	copy(dest, row)
	return nil
}

func newScriptedDB(t *testing.T, queries []scriptedQuery) *sql.DB {
	t.Helper()

	state := &scriptedState{queries: queries}
	driverName := fmt.Sprintf("habitat_tasks_scripted_%d", atomic.AddUint64(&scriptedDriverCounter, 1))
	sql.Register(driverName, &scriptedDriver{state: state})

	db, err := sql.Open(driverName, "")
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}

	t.Cleanup(func() {
		_ = db.Close()
		if err := state.verifyConsumed(); err != nil {
			t.Errorf("scripted db expectations: %v", err)
		}
	})

	return db
}

func expectContains(sub string) func(string, []driver.NamedValue) error {
	return func(query string, _ []driver.NamedValue) error {
		if !strings.Contains(query, sub) {
			return fmt.Errorf("query %q does not contain %q", query, sub)
		}
		return nil
	}
}

func expectQueueTasksQuery(queueName string, limit int64) func(string, []driver.NamedValue) error {
	rTable := fmt.Sprintf(`FROM absurd.%q r`, "r_"+queueName)
	tTable := fmt.Sprintf(`JOIN absurd.%q t ON t.task_id = r.task_id`, "t_"+queueName)
	return func(query string, args []driver.NamedValue) error {
		if !strings.Contains(query, rTable) {
			return fmt.Errorf("query %q missing %q", query, rTable)
		}
		if !strings.Contains(query, tTable) {
			return fmt.Errorf("query %q missing %q", query, tTable)
		}
		if !strings.Contains(query, "ORDER BY r.run_id DESC") {
			return fmt.Errorf("query %q missing run_id ordering", query)
		}
		if len(args) != 1 {
			return fmt.Errorf("expected 1 arg, got %d", len(args))
		}
		if args[0].Value != limit {
			return fmt.Errorf("limit arg = %#v, want %#v", args[0].Value, limit)
		}
		return nil
	}
}

func expectRecentTaskNamesQuery(queueName string, limit int64) func(string, []driver.NamedValue) error {
	rTable := fmt.Sprintf(`FROM absurd.%q`, "r_"+queueName)
	tTable := fmt.Sprintf(`JOIN absurd.%q t ON t.task_id = r.task_id`, "t_"+queueName)
	return func(query string, args []driver.NamedValue) error {
		if !strings.Contains(query, rTable) {
			return fmt.Errorf("query %q missing %q", query, rTable)
		}
		if !strings.Contains(query, tTable) {
			return fmt.Errorf("query %q missing %q", query, tTable)
		}
		if !strings.Contains(query, "ORDER BY run_id DESC") {
			return fmt.Errorf("query %q missing run_id ordering", query)
		}
		if len(args) != 1 {
			return fmt.Errorf("expected 1 arg, got %d", len(args))
		}
		if args[0].Value != limit {
			return fmt.Errorf("limit arg = %#v, want %#v", args[0].Value, limit)
		}
		return nil
	}
}

func expectRetryTaskQuery(queueName string, taskID string, options map[string]any) func(string, []driver.NamedValue) error {
	return func(query string, args []driver.NamedValue) error {
		if !strings.Contains(query, "FROM absurd.retry_task($1, $2, $3::jsonb)") {
			return fmt.Errorf("query %q missing retry_task call", query)
		}
		if len(args) != 3 {
			return fmt.Errorf("expected 3 args, got %d", len(args))
		}
		if args[0].Value != queueName {
			return fmt.Errorf("queue arg = %#v, want %#v", args[0].Value, queueName)
		}
		if args[1].Value != taskID {
			return fmt.Errorf("task_id arg = %#v, want %#v", args[1].Value, taskID)
		}

		payload, ok := args[2].Value.(string)
		if !ok {
			return fmt.Errorf("options arg has type %T, want string", args[2].Value)
		}

		var got map[string]any
		if err := json.Unmarshal([]byte(payload), &got); err != nil {
			return fmt.Errorf("options arg is invalid JSON %q: %w", payload, err)
		}

		expectedJSON, err := json.Marshal(options)
		if err != nil {
			return err
		}
		var expected map[string]any
		if err := json.Unmarshal(expectedJSON, &expected); err != nil {
			return err
		}

		if !reflect.DeepEqual(got, expected) {
			return fmt.Errorf("options arg = %#v, want %#v", got, expected)
		}

		return nil
	}
}

func expectTaskAttemptsQuery(queueName string, taskID string) func(string, []driver.NamedValue) error {
	table := fmt.Sprintf(`FROM absurd.%q`, "t_"+queueName)
	return func(query string, args []driver.NamedValue) error {
		if !strings.Contains(query, "SELECT attempts") {
			return fmt.Errorf("query %q missing attempts select", query)
		}
		if !strings.Contains(query, table) {
			return fmt.Errorf("query %q missing %q", query, table)
		}
		if len(args) != 1 {
			return fmt.Errorf("expected 1 arg, got %d", len(args))
		}
		if args[0].Value != taskID {
			return fmt.Errorf("task_id arg = %#v, want %#v", args[0].Value, taskID)
		}
		return nil
	}
}

func TestHandleTasksFailsFastOnQueueQueryDeadline(t *testing.T) {
	now := time.Now().UTC()
	db := newScriptedDB(t, []scriptedQuery{
		{
			match:   expectContains(`SELECT queue_name FROM absurd.queues ORDER BY queue_name`),
			columns: []string{"queue_name"},
			rows: [][]driver.Value{
				{"alpha"},
				{"beta"},
			},
		},
		{
			match:   expectRecentTaskNamesQuery("alpha", 5000),
			columns: []string{"task_name"},
			rows: [][]driver.Value{
				{"process-webhook"},
			},
		},
		{
			match:   expectRecentTaskNamesQuery("beta", 5000),
			columns: []string{"task_name"},
			rows: [][]driver.Value{
				{"sync-account"},
			},
		},
		{
			match: expectQueueTasksQuery("alpha", 27),
			columns: []string{
				"task_id",
				"run_id",
				"queue_name",
				"task_name",
				"state",
				"attempt",
				"max_attempts",
				"created_at",
				"updated_at",
				"completed_at",
				"claimed_by",
				"params",
			},
			rows: [][]driver.Value{
				{
					uuid.NewString(),
					uuid.NewString(),
					"alpha",
					"process-webhook",
					"pending",
					int64(1),
					int64(5),
					now,
					now,
					nil,
					nil,
					nil,
				},
			},
		},
		{
			match: expectQueueTasksQuery("beta", 27),
			err:   context.DeadlineExceeded,
		},
	})

	srv := &Server{db: db}

	req := httptest.NewRequest(http.MethodGet, "/api/tasks?page=1&perPage=25", nil)
	resp := httptest.NewRecorder()

	srv.handleTasks(resp, req)

	if resp.Code != http.StatusGatewayTimeout {
		t.Fatalf("status = %d, want %d", resp.Code, http.StatusGatewayTimeout)
	}

	body := resp.Body.String()
	if !strings.Contains(body, "task query timed out") {
		t.Fatalf("expected timeout message in body, got %q", body)
	}
	if strings.Contains(body, `"items"`) {
		t.Fatalf("expected non-JSON error response for timeout, got %q", body)
	}
}

func TestHandleRetryTaskSuccess(t *testing.T) {
	taskID := uuid.NewString()
	runID := uuid.NewString()
	queueName := "default"

	db := newScriptedDB(t, []scriptedQuery{
		{
			match:   expectContains(`SELECT queue_name FROM absurd.queues WHERE queue_name = $1`),
			columns: []string{"queue_name"},
			rows:    [][]driver.Value{{queueName}},
		},
		{
			match:   expectRetryTaskQuery(queueName, taskID, map[string]any{"spawn_new": true}),
			columns: []string{"task_id", "run_id", "attempt", "created"},
			rows:    [][]driver.Value{{taskID, runID, int64(1), true}},
		},
	})

	srv := &Server{db: db}

	body := strings.NewReader(fmt.Sprintf(`{"taskId":%q,"queueName":%q,"spawnNewTask":true}`, taskID, queueName))
	req := httptest.NewRequest(http.MethodPost, "/api/tasks/retry", body)
	req.Header.Set("Content-Type", "application/json")
	resp := httptest.NewRecorder()

	srv.handleRetryTask(resp, req)

	if resp.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d (body=%q)", resp.Code, http.StatusOK, resp.Body.String())
	}

	if !strings.Contains(resp.Body.String(), `"created":true`) {
		t.Fatalf("expected created=true in response body, got %q", resp.Body.String())
	}
}

func TestHandleRetryTaskRejectsWrongMethod(t *testing.T) {
	srv := &Server{}
	req := httptest.NewRequest(http.MethodGet, "/api/tasks/retry", nil)
	resp := httptest.NewRecorder()

	srv.handleRetryTask(resp, req)

	if resp.Code != http.StatusMethodNotAllowed {
		t.Fatalf("status = %d, want %d", resp.Code, http.StatusMethodNotAllowed)
	}
}

func TestHandleRetryTaskInPlaceUsesBackendDefaultWhenNoOverrideProvided(t *testing.T) {
	taskID := uuid.NewString()
	runID := uuid.NewString()
	queueName := "default"

	db := newScriptedDB(t, []scriptedQuery{
		{
			match:   expectContains(`SELECT queue_name FROM absurd.queues WHERE queue_name = $1`),
			columns: []string{"queue_name"},
			rows:    [][]driver.Value{{queueName}},
		},
		{
			match:   expectRetryTaskQuery(queueName, taskID, map[string]any{}),
			columns: []string{"task_id", "run_id", "attempt", "created"},
			rows:    [][]driver.Value{{taskID, runID, int64(5), false}},
		},
	})

	srv := &Server{db: db}

	body := strings.NewReader(fmt.Sprintf(`{"taskId":%q,"queueName":%q}`, taskID, queueName))
	req := httptest.NewRequest(http.MethodPost, "/api/tasks/retry", body)
	req.Header.Set("Content-Type", "application/json")
	resp := httptest.NewRecorder()

	srv.handleRetryTask(resp, req)

	if resp.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d (body=%q)", resp.Code, http.StatusOK, resp.Body.String())
	}
}

func TestHandleRetryTaskInPlaceUsesExtraAttemptsOverride(t *testing.T) {
	taskID := uuid.NewString()
	runID := uuid.NewString()
	queueName := "default"

	db := newScriptedDB(t, []scriptedQuery{
		{
			match:   expectContains(`SELECT queue_name FROM absurd.queues WHERE queue_name = $1`),
			columns: []string{"queue_name"},
			rows:    [][]driver.Value{{queueName}},
		},
		{
			match:   expectTaskAttemptsQuery(queueName, taskID),
			columns: []string{"attempts"},
			rows:    [][]driver.Value{{int64(4)}},
		},
		{
			match:   expectRetryTaskQuery(queueName, taskID, map[string]any{"max_attempts": 6}),
			columns: []string{"task_id", "run_id", "attempt", "created"},
			rows:    [][]driver.Value{{taskID, runID, int64(5), false}},
		},
	})

	srv := &Server{db: db}

	body := strings.NewReader(fmt.Sprintf(`{"taskId":%q,"queueName":%q,"extraAttempts":2}`, taskID, queueName))
	req := httptest.NewRequest(http.MethodPost, "/api/tasks/retry", body)
	req.Header.Set("Content-Type", "application/json")
	resp := httptest.NewRecorder()

	srv.handleRetryTask(resp, req)

	if resp.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d (body=%q)", resp.Code, http.StatusOK, resp.Body.String())
	}
}

func TestHandleRetryTaskSpawnNewUsesExplicitMaxAttemptsOverride(t *testing.T) {
	taskID := uuid.NewString()
	runID := uuid.NewString()
	queueName := "default"

	db := newScriptedDB(t, []scriptedQuery{
		{
			match:   expectContains(`SELECT queue_name FROM absurd.queues WHERE queue_name = $1`),
			columns: []string{"queue_name"},
			rows:    [][]driver.Value{{queueName}},
		},
		{
			match:   expectRetryTaskQuery(queueName, taskID, map[string]any{"spawn_new": true, "max_attempts": 9}),
			columns: []string{"task_id", "run_id", "attempt", "created"},
			rows:    [][]driver.Value{{taskID, runID, int64(1), true}},
		},
	})

	srv := &Server{db: db}

	body := strings.NewReader(fmt.Sprintf(`{"taskId":%q,"queueName":%q,"spawnNewTask":true,"maxAttempts":9}`, taskID, queueName))
	req := httptest.NewRequest(http.MethodPost, "/api/tasks/retry", body)
	req.Header.Set("Content-Type", "application/json")
	resp := httptest.NewRecorder()

	srv.handleRetryTask(resp, req)

	if resp.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d (body=%q)", resp.Code, http.StatusOK, resp.Body.String())
	}
}
