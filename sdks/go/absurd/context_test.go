package absurd

import (
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestStep_Integration(t *testing.T) {
	t.Parallel()
	ctx := t.Context()

	uri, close := testutilPostgresContainer(t)
	defer close(t)

	queue := "test"
	abs, err := New(WithURI(uri), WithDefaultQueue(queue))
	require.NoErrorf(t, err, "cannot create absurd client")

	type taskParams struct {
		Message string `json:"message"`
	}

	// 1. Create queue.
	err = CreateQueue(ctx, abs, queue)
	require.NoErrorf(t, err, "cannot CreateQueue")

	// 2. Spawn a task.
	result, err := Spawn(ctx, abs, "step-test-task", taskParams{Message: "test"})
	require.NoErrorf(t, err, "cannot Spawn")
	assert.NotEmpty(t, result)

	// 3. Claim the task.
	tasks, err := ClaimTasks(ctx, abs, 1, 120*time.Second, "test-worker")
	require.NoErrorf(t, err, "cannot ClaimTasks")
	require.Lenf(t, tasks, 1, "should claim 1 task")

	task := tasks[0]
	conn, err := abs.base.connect(ctx)
	require.NoErrorf(t, err, "cannot connect to database")

	// 4. Create internal context.
	ictx := newInternalContext(ctx, task, conn, queue, 120*time.Second)

	// 5. Test Step function.
	t.Log("first step execution")
	executionCount := 0
	step1Result, err := Step(ictx, "step1", func() (string, error) {
		executionCount++
		return "result1", nil
	})
	require.NoErrorf(t, err, "first step1 execution should succeed")
	assert.Equal(t, "result1", step1Result)
	assert.Equal(t, 1, executionCount, "function should execute once")

	t.Log("different step name")
	step2Result, err := Step(ictx, "step2", func() (int, error) {
		executionCount++
		return 100, nil
	})
	require.NoErrorf(t, err, "step2 execution should succeed")
	assert.Equal(t, 100, step2Result)
	assert.Equal(t, 2, executionCount, "function should execute for new step")

	// 8. Test duplicate step name (incremental).
	t.Log("duplicate step name (incremental)")
	step1DuplicateResult, err := Step(ictx, "step1", func() (string, error) {
		executionCount++
		return "result1-duplicate", nil
	})
	require.NoErrorf(t, err, "duplicate step1 should succeed")
	assert.Equal(t, "result1-duplicate", step1DuplicateResult)
	assert.Equal(t, 3, executionCount, "function should execute for incremented name")

	// 9. Test error handling in step function.
	t.Log("error in step function")
	_, err = Step(ictx, "error-step", func() (string, error) {
		return "", errors.New("test error")
	})
	require.Error(t, err, "error step should return error")
	assert.Equal(t, "test error", err.Error())

	// 10. Create a new context with the same task to verify persistence in database.
	t.Log("verify persistence across contexts")
	ictx2 := newInternalContext(ctx, task, conn, queue, 120*time.Second)
	persistedResult, err := Step(ictx2, "step1", func() (string, error) {
		t.Fatal("should not execute - result should be loaded from database")
		return "", nil
	})
	require.NoErrorf(t, err, "persisted step1 should succeed")
	assert.Equal(t, "result1", persistedResult, "should load persisted result from database")
}
