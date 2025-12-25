package absurd

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
)

type Context interface {
	context.Context

	incrementalCheckpointName(name string) string
	lookupCheckpoint(name string) ([]byte, error)
	persistCheckpoint(name string, value []byte) error
}

type internalContext struct {
	context.Context

	taskID       string
	conn         *pgx.Conn
	queue        string
	task         ClaimedTask
	claimTimeout time.Duration

	stepNameMu      sync.RWMutex
	stepNameCounter map[string]int

	checkpointMu    sync.Mutex
	checkpointCache map[string][]byte // Raw json.
}

func newInternalContext(
	ctx context.Context,
	task ClaimedTask,
	conn *pgx.Conn,
	queue string,
	claimTimeout time.Duration,
) *internalContext {
	return &internalContext{
		Context: ctx,

		stepNameMu:      sync.RWMutex{},
		stepNameCounter: make(map[string]int),

		checkpointMu:    sync.Mutex{},
		checkpointCache: make(map[string][]byte),

		conn:         conn,
		task:         task,
		claimTimeout: claimTimeout,
		queue:        queue,
	}
}

func (ictx *internalContext) incrementalCheckpointName(name string) string {
	ictx.stepNameMu.Lock()
	old, ok := ictx.stepNameCounter[name]
	if !ok {
		ictx.stepNameCounter[name] = 1
		ictx.stepNameMu.Unlock()
		return name
	} else {
		ictx.stepNameCounter[name] = old + 1
	}
	ictx.stepNameMu.Unlock()

	return fmt.Sprintf("%s#%d", name, old+1)
}

func (ictx *internalContext) lookupCheckpoint(
	name string,
) ([]byte, error) {
	// Fast path: Checkpoint was already cached.
	ictx.checkpointMu.Lock()
	if cached, ok := ictx.checkpointCache[name]; ok {
		ictx.checkpointMu.Unlock()
		return cached, nil
	}
	ictx.checkpointMu.Unlock()

	// Slow path: Get checkpoint from postgres.
	row := ictx.conn.QueryRow(
		ictx,
		`SELECT checkpoint_name, state, status, owner_run_id, updated_at
		 FROM absurd.get_task_checkpoint_state($1, $2, $3)`,
		ictx.queue,
		ictx.task.TaskID,
		name,
	)
	var result Checkpoint
	if err := row.Scan(
		&result.CheckpointName,
		&result.State,
		&result.Status,
		&result.OwnerRunID,
		&result.UpdatedAt,
	); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrCheckpointNotFound
		}
		return nil, err
	}

	// Cache result.
	ictx.checkpointMu.Lock()
	ictx.checkpointCache[name] = result.State
	ictx.checkpointMu.Unlock()

	return result.State, nil
}

func (ictx *internalContext) persistCheckpoint(name string, rawState []byte) error {
	_, err := ictx.conn.Exec(
		ictx,
		`SELECT absurd.set_task_checkpoint_state($1, $2, $3, $4, $5, $6)`,
		ictx.queue,
		ictx.task.TaskID,
		name,
		rawState,
		ictx.task.RunID,
		int(ictx.claimTimeout.Seconds()),
	)
	if err != nil {
		return err
	}

	// Cache raw state after write to postgres was successful.
	ictx.checkpointMu.Lock()
	ictx.checkpointCache[name] = rawState
	ictx.checkpointMu.Unlock()

	return nil
}

type StepFunc[R any] func() (R, error)

func Step[R any](
	ctx Context,
	name string,
	fn func() (R, error),
) (R, error) {
	var zero R

	checkpoint := ctx.incrementalCheckpointName(name)

	// Checkpoint was found in cache or postgres.
	rawState, err := ctx.lookupCheckpoint(checkpoint)
	if err == nil {
		var result R
		if err := json.Unmarshal(rawState, &result); err != nil {
			return zero, err
		}
		return result, nil
	}
	if !errors.Is(err, ErrCheckpointNotFound) {
		return zero, err
	}

	// Run step function and persist a successful run as checkpoint.
	state, err := fn()
	if err != nil {
		return zero, err
	}
	rawState, err = json.Marshal(state)
	if err != nil {
		return zero, err
	}
	if err := ctx.persistCheckpoint(checkpoint, rawState); err != nil {
		return zero, err
	}

	return state, nil
}
