package server

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/lib/pq"
)

func (s *Server) handleHealthz(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/plain")
	if err := s.db.PingContext(r.Context()); err != nil {
		http.Error(w, "database unavailable", http.StatusServiceUnavailable)
		return
	}
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok"))
}

func (s *Server) handleConfig(w http.ResponseWriter, _ *http.Request) {
	response := map[string]any{
		"authRequired": s.authRequired,
	}
	writeJSON(w, http.StatusOK, response)
}

func (s *Server) handleLogin(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	if !s.authRequired {
		writeJSON(w, http.StatusOK, map[string]any{"authRequired": false})
		return
	}

	var req loginRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid json payload", http.StatusBadRequest)
		return
	}

	req.Username = strings.TrimSpace(req.Username)
	if req.Username == "" || req.Password == "" {
		http.Error(w, "username and password are required", http.StatusBadRequest)
		return
	}

	ok, err := s.auth.Authenticate(req.Username, req.Password)
	if err != nil {
		http.Error(w, "authentication error", http.StatusInternalServerError)
		return
	}
	if !ok {
		http.Error(w, "invalid credentials", http.StatusUnauthorized)
		return
	}

	sessionID := s.sessions.Create(req.Username)
	s.setSessionCookie(w, sessionID)

	writeJSON(w, http.StatusOK, map[string]any{
		"username": req.Username,
	})
}

func (s *Server) handleLogout(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	cookie, err := r.Cookie(sessionCookieName)
	if err == nil {
		s.sessions.Delete(cookie.Value)
	}
	s.clearSessionCookie(w)
	writeJSON(w, http.StatusOK, map[string]any{"ok": true})
}

func (s *Server) handleIndex(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" {
		http.NotFound(w, r)
		return
	}

	if len(s.indexHTML) == 0 {
		http.Error(w, "frontend assets not built - run `npm run build`", http.StatusServiceUnavailable)
		return
	}

	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(s.indexHTML)
}

func (s *Server) handleMetrics(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()

	rows, err := s.db.QueryContext(ctx, `
		select
			queue_name,
			queue_length,
			newest_msg_age_sec,
			oldest_msg_age_sec,
			total_messages,
			scrape_time,
			queue_visible_length
		from absurd.metrics_all()
	`)
	if err != nil {
		http.Error(w, "failed to query metrics", http.StatusInternalServerError)
		return
	}
	defer rows.Close()

	metrics := make([]QueueMetrics, 0)
	for rows.Next() {
		var item queueMetricsRecord
		if err := rows.Scan(
			&item.QueueName,
			&item.QueueLength,
			&item.NewestMsgAgeSec,
			&item.OldestMsgAgeSec,
			&item.TotalMessages,
			&item.ScrapeTime,
			&item.QueueVisibleLength,
		); err != nil {
			http.Error(w, "failed to scan metrics", http.StatusInternalServerError)
			return
		}

		metrics = append(metrics, item.AsAPI())
	}
	if err := rows.Err(); err != nil {
		http.Error(w, "metrics query error", http.StatusInternalServerError)
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"queues": metrics,
	})
}

type loginRequest struct {
	Username string `json:"username"`
	Password string `json:"password"`
}

type queueMetricsRecord struct {
	QueueName          string
	QueueLength        int64
	NewestMsgAgeSec    sql.NullInt64
	OldestMsgAgeSec    sql.NullInt64
	TotalMessages      int64
	ScrapeTime         time.Time
	QueueVisibleLength int64
}

// QueueMetrics is the API representation of queue metrics.
type QueueMetrics struct {
	QueueName          string    `json:"queueName"`
	QueueLength        int64     `json:"queueLength"`
	NewestMsgAgeSec    *int64    `json:"newestMsgAgeSec,omitempty"`
	OldestMsgAgeSec    *int64    `json:"oldestMsgAgeSec,omitempty"`
	TotalMessages      int64     `json:"totalMessages"`
	ScrapeTime         time.Time `json:"scrapeTime"`
	QueueVisibleLength int64     `json:"queueVisibleLength"`
}

func (r queueMetricsRecord) AsAPI() QueueMetrics {
	return QueueMetrics{
		QueueName:          r.QueueName,
		QueueLength:        r.QueueLength,
		NewestMsgAgeSec:    nullableInt64(r.NewestMsgAgeSec),
		OldestMsgAgeSec:    nullableInt64(r.OldestMsgAgeSec),
		TotalMessages:      r.TotalMessages,
		ScrapeTime:         r.ScrapeTime,
		QueueVisibleLength: r.QueueVisibleLength,
	}
}

func nullableInt64(v sql.NullInt64) *int64 {
	if !v.Valid {
		return nil
	}
	value := v.Int64
	return &value
}

func (s *Server) handleTasks(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()

	queryValues := r.URL.Query()
	search := strings.TrimSpace(queryValues.Get("q"))
	statusFilter := strings.TrimSpace(queryValues.Get("status"))
	queueFilter := strings.TrimSpace(queryValues.Get("queue"))
	taskNameFilter := strings.TrimSpace(queryValues.Get("taskName"))
	taskIDFilter := strings.TrimSpace(queryValues.Get("taskId"))

	page := parsePositiveInt(queryValues.Get("page"), 1)
	perPage := parsePositiveInt(queryValues.Get("perPage"), 25)
	if perPage > 200 {
		perPage = 200
	}

	queueRows, err := s.db.QueryContext(ctx, `SELECT DISTINCT queue_name FROM absurd.run_catalog ORDER BY queue_name`)
	if err != nil {
		http.Error(w, "failed to query queues", http.StatusInternalServerError)
		return
	}
	defer queueRows.Close()

	var queueNames []string
	for queueRows.Next() {
		var queueName string
		if err := queueRows.Scan(&queueName); err != nil {
			http.Error(w, "failed to scan queue name", http.StatusInternalServerError)
			return
		}
		queueNames = append(queueNames, queueName)
	}
	if err := queueRows.Err(); err != nil {
		http.Error(w, "queue query error", http.StatusInternalServerError)
		return
	}

	statusSet := make(map[string]struct{})
	taskNameSet := make(map[string]struct{})
	var filtered []TaskSummary

	for _, queueName := range queueNames {
		if queueFilter != "" && queueName != queueFilter {
			continue
		}

		rtable := queueTableIdentifier("r", queueName)
		query := fmt.Sprintf(`
			SELECT
				task_id, run_id, queue_name, task_name, status,
				attempt, max_attempts, created_at, updated_at, completed_at
			FROM absurd.%s
			ORDER BY created_at DESC
		`, rtable)

		rows, err := s.db.QueryContext(ctx, query)
		if err != nil {
			continue // Skip queues that don't exist or have errors
		}

		for rows.Next() {
			var record taskSummaryRecord
			if err := rows.Scan(
				&record.TaskID,
				&record.RunID,
				&record.QueueName,
				&record.TaskName,
				&record.Status,
				&record.Attempt,
				&record.MaxAttempts,
				&record.CreatedAt,
				&record.UpdatedAt,
				&record.CompletedAt,
			); err != nil {
				rows.Close()
				http.Error(w, "failed to scan task", http.StatusInternalServerError)
				return
			}

			summary := record.AsAPI()

			if summary.Status != "" {
				statusSet[summary.Status] = struct{}{}
			}
			if summary.TaskName != "" {
				taskNameSet[summary.TaskName] = struct{}{}
			}

			if matchesTaskFilters(summary, search, statusFilter, queueFilter, taskNameFilter, taskIDFilter) {
				filtered = append(filtered, summary)
			}
		}
		rows.Close()
	}

	sort.Slice(filtered, func(i, j int) bool {
		return filtered[i].CreatedAt.After(filtered[j].CreatedAt)
	})

	total := len(filtered)
	if page < 1 {
		page = 1
	}
	start := (page - 1) * perPage
	if start > total {
		start = total
	}
	end := start + perPage
	if end > total {
		end = total
	}

	if queueNames == nil {
		queueNames = []string{}
	}

	response := TaskListResponse{
		Items:              filtered[start:end],
		Total:              total,
		Page:               page,
		PerPage:            perPage,
		AvailableStatuses:  sortedKeys(statusSet),
		AvailableQueues:    queueNames,
		AvailableTaskNames: sortedKeys(taskNameSet),
	}

	writeJSON(w, http.StatusOK, response)
}

func (s *Server) handleTaskDetail(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()

	// Extract run ID from URL path
	runID := strings.TrimPrefix(r.URL.Path, "/api/tasks/")
	if runID == "" {
		http.Error(w, "run ID required", http.StatusBadRequest)
		return
	}

	// Find which queue this task belongs to
	var queueName string
	err := s.db.QueryRowContext(ctx,
		`SELECT queue_name FROM absurd.run_catalog WHERE run_id = $1 LIMIT 1`,
		runID,
	).Scan(&queueName)
	if err == sql.ErrNoRows {
		http.Error(w, "task not found", http.StatusNotFound)
		return
	}
	if err != nil {
		http.Error(w, "failed to query task", http.StatusInternalServerError)
		return
	}

	// Query full task details from r_* table
	rtable := queueTableIdentifier("r", queueName)
	query := fmt.Sprintf(`
		SELECT
			task_id, run_id, queue_name, task_name, status, attempt, max_attempts,
			params, retry_strategy, headers, claimed_by, lease_expires_at,
			next_wake_at, wake_event, final_status, final_state,
			created_at, updated_at, completed_at
		FROM absurd.%s
		WHERE run_id = $1
		LIMIT 1
	`, rtable)

	var task taskDetailRecord
	err = s.db.QueryRowContext(ctx, query, runID).Scan(
		&task.TaskID,
		&task.RunID,
		&task.QueueName,
		&task.TaskName,
		&task.Status,
		&task.Attempt,
		&task.MaxAttempts,
		&task.Params,
		&task.RetryStrategy,
		&task.Headers,
		&task.ClaimedBy,
		&task.LeaseExpiresAt,
		&task.NextWakeAt,
		&task.WakeEvent,
		&task.FinalStatus,
		&task.FinalState,
		&task.CreatedAt,
		&task.UpdatedAt,
		&task.CompletedAt,
	)
	if err == sql.ErrNoRows {
		http.Error(w, "task not found", http.StatusNotFound)
		return
	}
	if err != nil {
		http.Error(w, "failed to query task details", http.StatusInternalServerError)
		return
	}

	// Query checkpoints from s_* table
	stable := queueTableIdentifier("s", queueName)
	checkpointQuery := fmt.Sprintf(`
		SELECT step_name, state, status, owner_run_id, ephemeral, expires_at, updated_at
		FROM absurd.%s
		WHERE item_type = 'checkpoint' AND owner_run_id = $1
		ORDER BY updated_at DESC
	`, stable)

	checkpointRows, err := s.db.QueryContext(ctx, checkpointQuery, runID)
	if err == nil {
		defer checkpointRows.Close()
		for checkpointRows.Next() {
			var cp checkpointStateRecord
			if err := checkpointRows.Scan(
				&cp.StepName,
				&cp.State,
				&cp.Status,
				&cp.OwnerRunID,
				&cp.Ephemeral,
				&cp.ExpiresAt,
				&cp.UpdatedAt,
			); err == nil {
				task.Checkpoints = append(task.Checkpoints, cp)
			}
		}
	}

	writeJSON(w, http.StatusOK, task.AsAPI())
}

func (s *Server) handleQueues(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()

	// Get all queues
	queueRows, err := s.db.QueryContext(ctx, `SELECT DISTINCT queue_name FROM absurd.run_catalog ORDER BY queue_name`)
	if err != nil {
		http.Error(w, "failed to query queues", http.StatusInternalServerError)
		return
	}
	defer queueRows.Close()

	var queues []QueueSummary
	for queueRows.Next() {
		var queueName string
		if err := queueRows.Scan(&queueName); err != nil {
			http.Error(w, "failed to scan queue name", http.StatusInternalServerError)
			return
		}

		// Count tasks by status for this queue
		rtable := queueTableIdentifier("r", queueName)
		countQuery := fmt.Sprintf(`
			SELECT
				COUNT(*) FILTER (WHERE status = 'pending') as pending_count,
				COUNT(*) FILTER (WHERE status = 'running') as running_count,
				COUNT(*) FILTER (WHERE status = 'sleeping') as sleeping_count,
				COUNT(*) FILTER (WHERE status = 'completed') as completed_count,
				COUNT(*) FILTER (WHERE status = 'failed') as failed_count
			FROM absurd.%s
		`, rtable)

		var summary QueueSummary
		summary.QueueName = queueName
		err := s.db.QueryRowContext(ctx, countQuery).Scan(
			&summary.PendingCount,
			&summary.RunningCount,
			&summary.SleepingCount,
			&summary.CompletedCount,
			&summary.FailedCount,
		)
		if err != nil {
			continue // Skip queues with errors
		}

		queues = append(queues, summary)
	}

	writeJSON(w, http.StatusOK, queues)
}

func (s *Server) handleQueueTasks(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()

	// Extract queue name from URL
	queueName := strings.TrimPrefix(r.URL.Path, "/api/queues/")
	queueName = strings.TrimSuffix(queueName, "/tasks")
	if queueName == "" {
		http.Error(w, "queue name required", http.StatusBadRequest)
		return
	}

	rtable := queueTableIdentifier("r", queueName)
	query := fmt.Sprintf(`
		SELECT
			task_id, run_id, queue_name, task_name, status,
			attempt, max_attempts, created_at, updated_at, completed_at
		FROM absurd.%s
		ORDER BY created_at DESC
	`, rtable)

	rows, err := s.db.QueryContext(ctx, query)
	if err != nil {
		http.Error(w, "failed to query queue tasks", http.StatusInternalServerError)
		return
	}
	defer rows.Close()

	var tasks []TaskSummary
	for rows.Next() {
		var task taskSummaryRecord
		if err := rows.Scan(
			&task.TaskID,
			&task.RunID,
			&task.QueueName,
			&task.TaskName,
			&task.Status,
			&task.Attempt,
			&task.MaxAttempts,
			&task.CreatedAt,
			&task.UpdatedAt,
			&task.CompletedAt,
		); err != nil {
			http.Error(w, "failed to scan task", http.StatusInternalServerError)
			return
		}
		tasks = append(tasks, task.AsAPI())
	}

	writeJSON(w, http.StatusOK, tasks)
}

func queueTableIdentifier(prefix, queueName string) string {
	return pq.QuoteIdentifier(prefix + "_" + strings.ToLower(queueName))
}

type taskSummaryRecord struct {
	TaskID      string
	RunID       string
	QueueName   string
	TaskName    string
	Status      string
	Attempt     int
	MaxAttempts sql.NullInt64
	CreatedAt   time.Time
	UpdatedAt   time.Time
	CompletedAt sql.NullTime
}

type taskDetailRecord struct {
	taskSummaryRecord
	Params         []byte
	RetryStrategy  []byte
	Headers        []byte
	ClaimedBy      sql.NullString
	LeaseExpiresAt sql.NullTime
	NextWakeAt     sql.NullTime
	WakeEvent      sql.NullString
	FinalStatus    sql.NullString
	FinalState     []byte
	Checkpoints    []checkpointStateRecord
}

type checkpointStateRecord struct {
	StepName   string
	State      []byte
	Status     string
	OwnerRunID sql.NullString
	Ephemeral  bool
	ExpiresAt  sql.NullTime
	UpdatedAt  time.Time
}

// TaskSummary is the API representation for task list views
type TaskSummary struct {
	TaskID      string     `json:"taskId"`
	RunID       string     `json:"runId"`
	QueueName   string     `json:"queueName"`
	TaskName    string     `json:"taskName"`
	Status      string     `json:"status"`
	Attempt     int        `json:"attempt"`
	MaxAttempts *int       `json:"maxAttempts,omitempty"`
	CreatedAt   time.Time  `json:"createdAt"`
	UpdatedAt   time.Time  `json:"updatedAt"`
	CompletedAt *time.Time `json:"completedAt,omitempty"`
}

// TaskDetail is the API representation for expanded task details
type TaskDetail struct {
	TaskSummary
	Params         json.RawMessage   `json:"params,omitempty"`
	RetryStrategy  json.RawMessage   `json:"retryStrategy,omitempty"`
	Headers        json.RawMessage   `json:"headers,omitempty"`
	ClaimedBy      *string           `json:"claimedBy,omitempty"`
	LeaseExpiresAt *time.Time        `json:"leaseExpiresAt,omitempty"`
	NextWakeAt     *time.Time        `json:"nextWakeAt,omitempty"`
	WakeEvent      *string           `json:"wakeEvent,omitempty"`
	FinalStatus    *string           `json:"finalStatus,omitempty"`
	FinalState     json.RawMessage   `json:"finalState,omitempty"`
	Checkpoints    []CheckpointState `json:"checkpoints"`
}

// CheckpointState is the API representation for checkpoint data
type CheckpointState struct {
	StepName   string          `json:"stepName"`
	State      json.RawMessage `json:"state"`
	Status     string          `json:"status"`
	OwnerRunID *string         `json:"ownerRunId,omitempty"`
	Ephemeral  bool            `json:"ephemeral"`
	ExpiresAt  *time.Time      `json:"expiresAt,omitempty"`
	UpdatedAt  time.Time       `json:"updatedAt"`
}

// QueueSummary is the API representation for queue list
type QueueSummary struct {
	QueueName      string `json:"queueName"`
	PendingCount   int64  `json:"pendingCount"`
	RunningCount   int64  `json:"runningCount"`
	SleepingCount  int64  `json:"sleepingCount"`
	CompletedCount int64  `json:"completedCount"`
	FailedCount    int64  `json:"failedCount"`
}

type TaskListResponse struct {
	Items              []TaskSummary `json:"items"`
	Total              int           `json:"total"`
	Page               int           `json:"page"`
	PerPage            int           `json:"perPage"`
	AvailableStatuses  []string      `json:"availableStatuses"`
	AvailableQueues    []string      `json:"availableQueues"`
	AvailableTaskNames []string      `json:"availableTaskNames"`
}

func (r taskSummaryRecord) AsAPI() TaskSummary {
	return TaskSummary{
		TaskID:      r.TaskID,
		RunID:       r.RunID,
		QueueName:   r.QueueName,
		TaskName:    r.TaskName,
		Status:      r.Status,
		Attempt:     r.Attempt,
		MaxAttempts: nullableInt(r.MaxAttempts),
		CreatedAt:   r.CreatedAt,
		UpdatedAt:   r.UpdatedAt,
		CompletedAt: nullableTime(r.CompletedAt),
	}
}

func (r taskDetailRecord) AsAPI() TaskDetail {
	checkpoints := make([]CheckpointState, 0, len(r.Checkpoints))
	for _, cp := range r.Checkpoints {
		checkpoints = append(checkpoints, CheckpointState{
			StepName:   cp.StepName,
			State:      cp.State,
			Status:     cp.Status,
			OwnerRunID: nullableString(cp.OwnerRunID),
			Ephemeral:  cp.Ephemeral,
			ExpiresAt:  nullableTime(cp.ExpiresAt),
			UpdatedAt:  cp.UpdatedAt,
		})
	}

	return TaskDetail{
		TaskSummary:    r.taskSummaryRecord.AsAPI(),
		Params:         r.Params,
		RetryStrategy:  nullableBytes(r.RetryStrategy),
		Headers:        nullableBytes(r.Headers),
		ClaimedBy:      nullableString(r.ClaimedBy),
		LeaseExpiresAt: nullableTime(r.LeaseExpiresAt),
		NextWakeAt:     nullableTime(r.NextWakeAt),
		WakeEvent:      nullableString(r.WakeEvent),
		FinalStatus:    nullableString(r.FinalStatus),
		FinalState:     nullableBytes(r.FinalState),
		Checkpoints:    checkpoints,
	}
}

func nullableInt(v sql.NullInt64) *int {
	if !v.Valid {
		return nil
	}
	value := int(v.Int64)
	return &value
}

func nullableTime(v sql.NullTime) *time.Time {
	if !v.Valid {
		return nil
	}
	return &v.Time
}

func nullableString(v sql.NullString) *string {
	if !v.Valid {
		return nil
	}
	return &v.String
}

func nullableBytes(v []byte) json.RawMessage {
	if len(v) == 0 {
		return nil
	}
	return v
}

func matchesTaskFilters(task TaskSummary, search string, status string, queue string, taskName string, taskID string) bool {
	if status != "" && !strings.EqualFold(task.Status, status) {
		return false
	}
	if queue != "" && task.QueueName != queue {
		return false
	}
	if taskName != "" && task.TaskName != taskName {
		return false
	}
	if taskID != "" && task.TaskID != taskID {
		return false
	}

	if search != "" {
		searchLower := strings.ToLower(search)
		if !strings.Contains(strings.ToLower(task.TaskID), searchLower) &&
			!strings.Contains(strings.ToLower(task.RunID), searchLower) &&
			!strings.Contains(strings.ToLower(task.QueueName), searchLower) &&
			!strings.Contains(strings.ToLower(task.TaskName), searchLower) {
			return false
		}
	}

	return true
}

func parsePositiveInt(value string, fallback int) int {
	if value == "" {
		return fallback
	}
	parsed, err := strconv.Atoi(value)
	if err != nil || parsed <= 0 {
		return fallback
	}
	return parsed
}

func sortedKeys(set map[string]struct{}) []string {
	if len(set) == 0 {
		return []string{}
	}
	keys := make([]string, 0, len(set))
	for key := range set {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

func writeJSON(w http.ResponseWriter, status int, payload any) {
	var buf bytes.Buffer
	if err := json.NewEncoder(&buf).Encode(payload); err != nil {
		http.Error(w, fmt.Sprintf("encode json: %v", err), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_, _ = w.Write(buf.Bytes())
}
