package server

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
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
	writeJSON(w, http.StatusOK, map[string]any{})
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

	queueNames, err := s.listQueueNames(ctx)
	if err != nil {
		log.Printf("handleTasks: failed to list queues: %v", err)
		http.Error(w, "failed to query queues", http.StatusInternalServerError)
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
		queueLiteral := pq.QuoteLiteral(queueName)
		query := fmt.Sprintf(`
			SELECT
				task_id, run_id, %s AS queue_name, task_name, status,
				attempt, max_attempts, created_at, updated_at, completed_at
			FROM absurd.%s
			ORDER BY created_at DESC
		`, queueLiteral, rtable)

		rows, err := s.db.QueryContext(ctx, query)
		if err != nil {
			log.Printf("handleTasks: failed to query tasks for queue %s: %v", queueName, err)
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
				log.Printf("handleTasks: failed to scan task in queue %s: %v", queueName, err)
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
		if err := rows.Err(); err != nil {
			log.Printf("handleTasks: row iteration error for queue %s: %v", queueName, err)
		}
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

	queueName, err := s.findQueueForRun(ctx, runID)
	if err == sql.ErrNoRows {
		http.Error(w, "task not found", http.StatusNotFound)
		return
	}
	if err != nil {
		log.Printf("handleTaskDetail: failed to locate queue for run %s: %v", runID, err)
		http.Error(w, "failed to query task", http.StatusInternalServerError)
		return
	}

	// Query full task details from r_* table
	rtable := queueTableIdentifier("r", queueName)
	queueLiteral := pq.QuoteLiteral(queueName)
	query := fmt.Sprintf(`
		SELECT
			task_id, run_id, %s AS queue_name, task_name, status, attempt, max_attempts,
			params, retry_strategy, headers, claimed_by, lease_expires_at,
			next_wake_at, wake_event, last_claimed_at, final_status, state,
			created_at, updated_at, completed_at
		FROM absurd.%s
		WHERE run_id = $1
		LIMIT 1
	`, queueLiteral, rtable)

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
		&task.LastClaimedAt,
		&task.FinalStatus,
		&task.State,
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

	// Query checkpoints from c_* table. Checkpoints belong to a specific run,
	// so fetch by owner_run_id to show only checkpoints for this run.
	ctable := queueTableIdentifier("c", queueName)
	wtable := queueTableIdentifier("w", queueName)
	etable := queueTableIdentifier("e", queueName)
	checkpointQuery := fmt.Sprintf(`
		SELECT step_name, state, status, owner_run_id, NULL::timestamptz AS expires_at, updated_at
		FROM absurd.%s
		WHERE owner_run_id = $1
		ORDER BY updated_at DESC
	`, ctable)

	checkpointRows, err := s.db.QueryContext(ctx, checkpointQuery, runID)
	if err != nil {
		log.Printf("handleTaskDetail: checkpoint query failed for run %s: %v", runID, err)
		http.Error(w, "failed to query checkpoints", http.StatusInternalServerError)
		return
	}
	defer checkpointRows.Close()
	for checkpointRows.Next() {
		var cp checkpointStateRecord
		if err := checkpointRows.Scan(
			&cp.StepName,
			&cp.State,
			&cp.Status,
			&cp.OwnerRunID,
			&cp.ExpiresAt,
			&cp.UpdatedAt,
		); err == nil {
			task.Checkpoints = append(task.Checkpoints, cp)
		}
	}

	waitQuery := fmt.Sprintf(`
		SELECT
			w.wait_type,
			w.wake_at,
			w.wake_event,
			w.step_name,
			w.payload,
			ev.payload,
			w.updated_at,
			ev.emitted_at
		FROM absurd.%[1]s AS w
		LEFT JOIN absurd.%[2]s AS ev
			ON ev.event_name = w.wake_event
		WHERE w.run_id = $1
		ORDER BY w.updated_at DESC
	`, wtable, etable)

	waitRows, err := s.db.QueryContext(ctx, waitQuery, runID)
	if err == nil {
		defer waitRows.Close()
		for waitRows.Next() {
			var wt waitStateRecord
			if err := waitRows.Scan(
				&wt.WaitType,
				&wt.WakeAt,
				&wt.WakeEvent,
				&wt.StepName,
				&wt.Payload,
				&wt.EventPayload,
				&wt.UpdatedAt,
				&wt.EmittedAt,
			); err == nil {
				task.WaitStates = append(task.WaitStates, wt)
			}
		}
	}

	writeJSON(w, http.StatusOK, task.AsAPI())
}

func (s *Server) handleQueues(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()

	type queueMeta struct {
		name      string
		createdAt *time.Time
	}

	queueMap := make(map[string]*queueMeta)

	metaRows, err := s.db.QueryContext(ctx, `SELECT queue_name, created_at FROM absurd.meta ORDER BY queue_name`)
	if err != nil {
		http.Error(w, "failed to query queues", http.StatusInternalServerError)
		return
	}
	defer metaRows.Close()

	for metaRows.Next() {
		var name string
		var createdAt time.Time
		if err := metaRows.Scan(&name, &createdAt); err != nil {
			http.Error(w, "failed to scan queue metadata", http.StatusInternalServerError)
			return
		}
		ts := createdAt
		queueMap[name] = &queueMeta{name: name, createdAt: &ts}
	}
	if err := metaRows.Err(); err != nil {
		http.Error(w, "failed to iterate queue metadata", http.StatusInternalServerError)
		return
	}

	allQueues, err := s.listQueueNames(ctx)
	if err != nil {
		log.Printf("handleQueues: failed to list queues: %v", err)
		http.Error(w, "failed to query queues", http.StatusInternalServerError)
		return
	}

	for _, name := range allQueues {
		if _, exists := queueMap[name]; !exists {
			queueMap[name] = &queueMeta{name: name}
		}
	}

	queueNames := make([]*queueMeta, 0, len(queueMap))
	for _, meta := range queueMap {
		queueNames = append(queueNames, meta)
	}
	sort.Slice(queueNames, func(i, j int) bool {
		return queueNames[i].name < queueNames[j].name
	})

	var queues []QueueSummary
	for _, meta := range queueNames {
		queueName := meta.name

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

		if meta.createdAt != nil {
			ts := *meta.createdAt
			summary.CreatedAt = &ts
		}

		queues = append(queues, summary)
	}

	writeJSON(w, http.StatusOK, queues)
}

func (s *Server) handleQueueResource(w http.ResponseWriter, r *http.Request) {
	path := strings.TrimPrefix(r.URL.Path, "/api/queues/")
	path = strings.Trim(path, "/")
	if path == "" {
		http.Error(w, "queue name required", http.StatusBadRequest)
		return
	}

	parts := strings.Split(path, "/")
	queueName := parts[0]
	if queueName == "" {
		http.Error(w, "queue name required", http.StatusBadRequest)
		return
	}

	if len(parts) == 1 {
		http.NotFound(w, r)
		return
	}

	switch parts[1] {
	case "tasks":
		s.handleQueueTasks(w, r, queueName)
	case "messages":
		s.handleQueueMessages(w, r, queueName)
	case "events":
		s.handleQueueEvents(w, r, queueName)
	default:
		http.NotFound(w, r)
	}
}

func (s *Server) handleQueueTasks(w http.ResponseWriter, r *http.Request, queueName string) {
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()

	rtable := queueTableIdentifier("r", queueName)
	queueLiteral := pq.QuoteLiteral(queueName)
	query := fmt.Sprintf(`
		SELECT
			task_id, run_id, %s AS queue_name, task_name, status,
			attempt, max_attempts, created_at, updated_at, completed_at
		FROM absurd.%s
		ORDER BY created_at DESC
	`, queueLiteral, rtable)

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

func (s *Server) handleQueueMessages(w http.ResponseWriter, r *http.Request, queueName string) {
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()

	limit := parsePositiveInt(r.URL.Query().Get("limit"), 50)
	if limit > 200 {
		limit = 200
	}

	qtable := queueTableIdentifier("q", queueName)
	rtable := queueTableIdentifier("r", queueName)
	query := fmt.Sprintf(`
		SELECT
			q.msg_id,
			q.read_ct,
			q.enqueued_at,
			q.vt,
			q.message,
			r.headers
		FROM absurd.%s AS q
		LEFT JOIN absurd.%s AS r
			ON r.run_id = q.msg_id
		ORDER BY q.enqueued_at ASC
		LIMIT $1
	`, qtable, rtable)

	rows, err := s.db.QueryContext(ctx, query, limit)
	if err != nil {
		http.Error(w, "failed to query queue messages", http.StatusInternalServerError)
		return
	}
	defer rows.Close()

	var messages []QueueMessage
	for rows.Next() {
		var record queueMessageRecord
		if err := rows.Scan(
			&record.MessageID,
			&record.ReadCount,
			&record.EnqueuedAt,
			&record.VisibleAt,
			&record.Message,
			&record.Headers,
		); err != nil {
			http.Error(w, "failed to scan queue message", http.StatusInternalServerError)
			return
		}
		messages = append(messages, record.AsAPI(queueName))
	}

	if err := rows.Err(); err != nil {
		http.Error(w, "failed to iterate queue messages", http.StatusInternalServerError)
		return
	}

	writeJSON(w, http.StatusOK, messages)
}

func (s *Server) handleQueueEvents(w http.ResponseWriter, r *http.Request, queueName string) {
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()

	limit := parsePositiveInt(r.URL.Query().Get("limit"), 100)
	if limit > 500 {
		limit = 500
	}

	eventName := strings.TrimSpace(r.URL.Query().Get("eventName"))

	events, err := s.fetchQueueEvents(ctx, queueName, limit, eventName)
	if err != nil {
		http.Error(w, "failed to query queue events", http.StatusInternalServerError)
		return
	}

	writeJSON(w, http.StatusOK, events)
}

func (s *Server) fetchQueueEvents(ctx context.Context, queueName string, limit int, eventName string) ([]QueueEvent, error) {
	if limit <= 0 {
		limit = 100
	}
	if limit > 1000 {
		limit = 1000
	}

	etable := queueTableIdentifier("e", queueName)

	var (
		params  []any
		clauses []string
	)

	if eventName != "" {
		params = append(params, eventName)
		clauses = append(clauses, fmt.Sprintf("event_name = $%d", len(params)))
	}

	params = append(params, limit)
	limitPos := len(params)

	whereClause := ""
	if len(clauses) > 0 {
		whereClause = "WHERE " + strings.Join(clauses, " AND ")
	}

	query := fmt.Sprintf(`
		SELECT
			event_name,
			payload,
			emitted_at,
			created_at
		FROM absurd.%s
		%s
		ORDER BY COALESCE(emitted_at, created_at) DESC
		LIMIT $%d
	`, etable, whereClause, limitPos)

	rows, err := s.db.QueryContext(ctx, query, params...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var events []QueueEvent
	for rows.Next() {
		var record queueEventRecord
		if err := rows.Scan(
			&record.EventName,
			&record.Payload,
			&record.EmittedAt,
			&record.CreatedAt,
		); err != nil {
			return nil, err
		}
		events = append(events, record.AsAPI(queueName))
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return events, nil
}

func (s *Server) handleEvents(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()

	limit := parsePositiveInt(r.URL.Query().Get("limit"), 100)
	if limit > 1000 {
		limit = 1000
	}

	queueFilter := strings.TrimSpace(r.URL.Query().Get("queue"))
	eventFilter := strings.TrimSpace(r.URL.Query().Get("eventName"))

	var events []QueueEvent

	if queueFilter != "" {
		queueEvents, err := s.fetchQueueEvents(ctx, queueFilter, limit, eventFilter)
		if err != nil {
			http.Error(w, "failed to query queue events", http.StatusInternalServerError)
			return
		}
		events = queueEvents
	} else {
		queueNames, err := s.listQueueNames(ctx)
		if err != nil {
			log.Printf("handleEvents: failed to list queues: %v", err)
			http.Error(w, "failed to query queues", http.StatusInternalServerError)
			return
		}

		for _, queueName := range queueNames {
			queueEvents, err := s.fetchQueueEvents(ctx, queueName, limit, eventFilter)
			if err != nil {
				continue
			}
			events = append(events, queueEvents...)
		}

		sort.Slice(events, func(i, j int) bool {
			ti := events[i].CreatedAt
			if events[i].EmittedAt != nil {
				ti = *events[i].EmittedAt
			}
			tj := events[j].CreatedAt
			if events[j].EmittedAt != nil {
				tj = *events[j].EmittedAt
			}
			return ti.After(tj)
		})

		if len(events) > limit {
			events = events[:limit]
		}
	}

	writeJSON(w, http.StatusOK, events)
}

func (s *Server) listQueueNames(ctx context.Context) ([]string, error) {
	names := make(map[string]struct{})

	metaRows, err := s.db.QueryContext(ctx, `SELECT queue_name FROM absurd.meta`)
	if err != nil {
		return nil, err
	}
	defer metaRows.Close()
	for metaRows.Next() {
		var name string
		if err := metaRows.Scan(&name); err != nil {
			return nil, err
		}
		if name != "" {
			names[name] = struct{}{}
		}
	}
	if err := metaRows.Err(); err != nil {
		return nil, err
	}

	tableRows, err := s.db.QueryContext(ctx, `
		SELECT table_name
		FROM information_schema.tables
		WHERE table_schema = 'absurd' AND table_name LIKE 'r\_%' ESCAPE '\'
	`)
	if err != nil {
		return nil, err
	}
	defer tableRows.Close()

	for tableRows.Next() {
		var tableName string
		if err := tableRows.Scan(&tableName); err != nil {
			return nil, err
		}
		if strings.HasPrefix(tableName, "r_") && len(tableName) > 2 {
			queueName := tableName[2:]
			names[queueName] = struct{}{}
		}
	}
	if err := tableRows.Err(); err != nil {
		return nil, err
	}

	queueNames := make([]string, 0, len(names))
	for name := range names {
		queueNames = append(queueNames, name)
	}
	sort.Strings(queueNames)
	return queueNames, nil
}

func (s *Server) findQueueForRun(ctx context.Context, runID string) (string, error) {
	queueNames, err := s.listQueueNames(ctx)
	if err != nil {
		return "", err
	}

	for _, queueName := range queueNames {
		rtable := queueTableIdentifier("r", queueName)
		query := fmt.Sprintf(`SELECT 1 FROM absurd.%s WHERE run_id = $1 LIMIT 1`, rtable)
		var dummy int
		err := s.db.QueryRowContext(ctx, query, runID).Scan(&dummy)
		switch {
		case err == nil:
			return queueName, nil
		case err == sql.ErrNoRows:
			continue
		default:
			log.Printf("findQueueForRun: query failed for queue %s: %v", queueName, err)
			continue
		}
	}

	return "", sql.ErrNoRows
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
	LastClaimedAt  sql.NullTime
	FinalStatus    sql.NullString
	State          []byte
	Checkpoints    []checkpointStateRecord
	WaitStates     []waitStateRecord
}

type checkpointStateRecord struct {
	StepName   string
	State      []byte
	Status     string
	OwnerRunID sql.NullString
	ExpiresAt  sql.NullTime
	UpdatedAt  time.Time
}

type waitStateRecord struct {
	WaitType     string
	WakeAt       sql.NullTime
	WakeEvent    sql.NullString
	StepName     sql.NullString
	Payload      []byte
	EventPayload []byte
	UpdatedAt    time.Time
	EmittedAt    sql.NullTime
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
	LastClaimedAt  *time.Time        `json:"lastClaimedAt,omitempty"`
	FinalStatus    *string           `json:"finalStatus,omitempty"`
	State          json.RawMessage   `json:"state,omitempty"`
	Checkpoints    []CheckpointState `json:"checkpoints"`
	Waits          []WaitState       `json:"waits"`
}

// CheckpointState is the API representation for checkpoint data
type CheckpointState struct {
	StepName   string          `json:"stepName"`
	State      json.RawMessage `json:"state"`
	Status     string          `json:"status"`
	OwnerRunID *string         `json:"ownerRunId,omitempty"`
	ExpiresAt  *time.Time      `json:"expiresAt,omitempty"`
	UpdatedAt  time.Time       `json:"updatedAt"`
}

// WaitState describes an active or historical wait for a run.
type WaitState struct {
	WaitType     string          `json:"waitType"`
	WakeAt       *time.Time      `json:"wakeAt,omitempty"`
	WakeEvent    *string         `json:"wakeEvent,omitempty"`
	StepName     *string         `json:"stepName,omitempty"`
	Payload      json.RawMessage `json:"payload,omitempty"`
	EventPayload json.RawMessage `json:"eventPayload,omitempty"`
	EmittedAt    *time.Time      `json:"emittedAt,omitempty"`
	UpdatedAt    time.Time       `json:"updatedAt"`
}

// QueueSummary is the API representation for queue list
type QueueSummary struct {
	QueueName      string     `json:"queueName"`
	CreatedAt      *time.Time `json:"createdAt,omitempty"`
	PendingCount   int64      `json:"pendingCount"`
	RunningCount   int64      `json:"runningCount"`
	SleepingCount  int64      `json:"sleepingCount"`
	CompletedCount int64      `json:"completedCount"`
	FailedCount    int64      `json:"failedCount"`
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

type queueMessageRecord struct {
	MessageID  string
	ReadCount  int
	EnqueuedAt time.Time
	VisibleAt  time.Time
	Message    []byte
	Headers    []byte
}

type QueueMessage struct {
	QueueName  string          `json:"queueName"`
	MessageID  string          `json:"messageId"`
	ReadCount  int             `json:"readCount"`
	EnqueuedAt time.Time       `json:"enqueuedAt"`
	VisibleAt  time.Time       `json:"visibleAt"`
	Message    json.RawMessage `json:"message,omitempty"`
	Headers    json.RawMessage `json:"headers,omitempty"`
}

type queueEventRecord struct {
	EventName string
	Payload   []byte
	EmittedAt sql.NullTime
	CreatedAt time.Time
}

type QueueEvent struct {
	QueueName string          `json:"queueName"`
	EventName string          `json:"eventName"`
	Payload   json.RawMessage `json:"payload,omitempty"`
	EmittedAt *time.Time      `json:"emittedAt,omitempty"`
	CreatedAt time.Time       `json:"createdAt"`
}

func (r queueMessageRecord) AsAPI(queueName string) QueueMessage {
	return QueueMessage{
		QueueName:  queueName,
		MessageID:  r.MessageID,
		ReadCount:  r.ReadCount,
		EnqueuedAt: r.EnqueuedAt,
		VisibleAt:  r.VisibleAt,
		Message:    nullableBytes(r.Message),
		Headers:    nullableBytes(r.Headers),
	}
}

func (r queueEventRecord) AsAPI(queueName string) QueueEvent {
	return QueueEvent{
		QueueName: queueName,
		EventName: r.EventName,
		Payload:   nullableBytes(r.Payload),
		EmittedAt: nullableTime(r.EmittedAt),
		CreatedAt: r.CreatedAt,
	}
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
			ExpiresAt:  nullableTime(cp.ExpiresAt),
			UpdatedAt:  cp.UpdatedAt,
		})
	}

	waits := make([]WaitState, 0, len(r.WaitStates))
	for _, wt := range r.WaitStates {
		waits = append(waits, WaitState{
			WaitType:     wt.WaitType,
			WakeAt:       nullableTime(wt.WakeAt),
			WakeEvent:    nullableString(wt.WakeEvent),
			StepName:     nullableString(wt.StepName),
			Payload:      nullableBytes(wt.Payload),
			EventPayload: nullableBytes(wt.EventPayload),
			EmittedAt:    nullableTime(wt.EmittedAt),
			UpdatedAt:    wt.UpdatedAt,
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
		LastClaimedAt:  nullableTime(r.LastClaimedAt),
		FinalStatus:    nullableString(r.FinalStatus),
		State:          nullableBytes(r.State),
		Checkpoints:    checkpoints,
		Waits:          waits,
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
