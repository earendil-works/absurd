package absurd

import (
	"context"
	"database/sql"
	"fmt"
)

func completeTaskRun(ctx context.Context, db *sql.DB, queueName string, runID string, result any) error {
	raw, err := marshalJSON(result)
	if err != nil {
		return err
	}
	_, err = db.ExecContext(ctx, `SELECT absurd.complete_run($1, $2, $3)`, queueName, runID, string(raw))
	return err
}

func failTaskRun(ctx context.Context, db *sql.DB, queueName string, runID string, err error) error {
	serialized, marshalErr := marshalJSON(map[string]any{
		"name":    fmt.Sprintf("%T", err),
		"message": err.Error(),
	})
	if marshalErr != nil {
		return marshalErr
	}
	_, execErr := db.ExecContext(ctx, `SELECT absurd.fail_run($1, $2, $3, $4)`, queueName, runID, string(serialized), nil)
	return execErr
}
