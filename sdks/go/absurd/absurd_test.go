package absurd

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
)

// ----------
// Unit tests
// ----------

func TestCheckQueueName(t *testing.T) {
	tests := []struct {
		name        string
		queueName   string
		expectedErr error
	}{
		{
			name:      "valid queue name",
			queueName: "valid_QUEUE_NaMe_0123456798",
		},
		{
			name:        "empty queue name",
			queueName:   "",
			expectedErr: ErrQueueNameEmpty,
		},
		{
			name:        "invalid queue name",
			queueName:   "ínvalid_queue_name",
			expectedErr: ErrQueueNameCharsetInvalid,
		},
		{
			name:        "too long queue name",
			queueName:   "toooooooooooooooooooo_loooooooooooooooooooooooooong",
			expectedErr: ErrQueueNameTooLong,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := checkQueueName(tt.queueName)
			assert.ErrorIs(t, err, tt.expectedErr)
		})
	}
}

// -------------------------------
// Testcontainer integration tests
// -------------------------------

func TestAbsurd_New(t *testing.T) {
	t.Parallel()

	uri, close := testutilPostgresContainer(t)
	defer close(t)

	// Testing.
	_, err := New(WithURI(uri))
	assert.NoError(t, err)

	// TODO(hohmannr): Add testing for all viable options as separate test cases.
}

func TestQueueLifecycle(t *testing.T) {
	t.Parallel()
	ctx := t.Context()

	uri, close := testutilPostgresContainer(t)
	defer close(t)

	abs, err := New(WithURI(uri))
	require.NoErrorf(t, err, "cannot create absurd client")
	queue := "test"

	// Testing.
	// 1. Create queue.
	err = CreateQueue(ctx, abs, queue)
	require.NoErrorf(t, err, "cannot CreateQueue")

	// 2. List queues.
	t.Logf("list queues: %s", queue)
	queues, err := ListQueues(ctx, abs)
	require.NoErrorf(t, err, "cannot ListQueues")
	assert.Equal(t, []string{queue}, queues)

	// 3. Drop queue.
	t.Logf("list queues: %s", queue)
	err = DropQueue(ctx, abs, queue)
	require.NoErrorf(t, err, "cannot DropQueue")

	// 4. List queues again, expecting [queue] to not be present.
	t.Logf("list queues: %s", queue)
	queues, err = ListQueues(ctx, abs)
	require.NoErrorf(t, err, "cannot ListQueues")
	assert.Lenf(t, queues, 0, "too many queues found")
}

func TestSpawn(t *testing.T) {
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

	result, err := Spawn(
		ctx,
		abs,
		"test-task",
		taskParams{"test message"},
		WithFixedRetry(10*time.Second, 60*time.Second, 2),
	)
	require.NoErrorf(t, err, "cannot Spawn")
	assert.NotEmpty(t, result)

	// TODO: Add tests for options.
	// TODO: Add checks in postgres if tables/queues look like expected.
}

func TestCancel(t *testing.T) {
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

	// 2. Spawn a task to cancel.
	result, err := Spawn(
		ctx,
		abs,
		"cancel-test-task",
		taskParams{"test message"},
		WithFixedRetry(10*time.Second, 60*time.Second, 2),
	)
	require.NoErrorf(t, err, "cannot Spawn")
	require.NotEmptyf(t, result.TaskID, "task ID should not be empty")

	// 3. Cancel the task.
	err = CancelTask(ctx, abs, result.TaskID)
	require.NoErrorf(t, err, "cannot CancelTask")

	// TODO: Add tests for options.
	// TODO: Add checks in postgres if tables/queues look like expected.
}

func TestEmitEvent(t *testing.T) {
	t.Parallel()
	ctx := t.Context()

	uri, close := testutilPostgresContainer(t)
	defer close(t)

	queue := "test"
	abs, err := New(WithURI(uri), WithDefaultQueue(queue))
	require.NoErrorf(t, err, "cannot create absurd client")

	type eventPayload struct {
		Data string `json:"data"`
	}

	// 1. Create queue.
	err = CreateQueue(ctx, abs, queue)
	require.NoErrorf(t, err, "cannot CreateQueue")

	// 2. Emit event with payload on default queue.
	err = EmitEvent(ctx, abs, "test.event", eventPayload{"test data"})
	require.NoErrorf(t, err, "cannot EmitEvent with payload")

	// 3. Emit event without payload.
	err = EmitEvent(ctx, abs, "test.event.no-payload", nil)
	require.NoErrorf(t, err, "cannot EmitEvent without payload")

	// 4. Test with explicit queue option.
	queue2 := "test2"
	err = CreateQueue(ctx, abs, queue2)
	require.NoErrorf(t, err, "cannot CreateQueue for queue2")

	err = EmitEvent(
		ctx,
		abs,
		"test.event.queue2",
		eventPayload{"queue2 data"},
		WithEmitEventQueue(queue2),
	)
	require.NoErrorf(t, err, "cannot EmitEvent with explicit queue")

	// 5. Test empty event name validation.
	err = EmitEvent(ctx, abs, "", nil)
	require.ErrorIs(t, err, ErrEventNameEmpty, "should error on empty event name")

	// TODO: Add tests for options.
	// TODO: Add checks in postgres if tables/queues look like expected.
	// TODO: Add tests for event consumption via.
}

func TestClaimTasks(t *testing.T) {
	t.Parallel()
	ctx := t.Context()

	uri, close := testutilPostgresContainer(t)
	defer close(t)

	queue := "test"
	abs, err := New(WithURI(uri), WithDefaultQueue(queue))
	require.NoErrorf(t, err, "cannot create absurd client")

	type taskParams struct {
		Value int `json:"value"`
	}

	// 1. Create queue.
	err = CreateQueue(ctx, abs, queue)
	require.NoErrorf(t, err, "cannot CreateQueue")

	// 2. Spawn multiple tasks.
	var spawnedTaskIDs []string
	for i := 0; i < 3; i++ {
		result, err := Spawn(
			ctx,
			abs,
			"claim-test-task",
			taskParams{Value: i},
		)
		require.NoErrorf(t, err, "cannot Spawn task %d", i)
		spawnedTaskIDs = append(spawnedTaskIDs, result.TaskID)
	}

	// 3. Claim tasks with batch size of 2.
	claimed, err := ClaimTasks(ctx, abs, 2, 120*time.Second, "test-worker-1")
	require.NoErrorf(t, err, "cannot ClaimTasks")
	require.Lenf(t, claimed, 2, "should claim 2 tasks")

	// 4. Verify claimed tasks.
	for _, task := range claimed {
		t.Logf("claimed task: %s (name: %s, attempt: %d)", task.TaskID, task.TaskName, task.Attempt)
		assert.Equal(t, "claim-test-task", task.TaskName)
		assert.Equal(t, 1, task.Attempt)
		assert.NotEmpty(t, task.RunID)
		assert.Contains(t, spawnedTaskIDs, task.TaskID)
	}

	// 5. Claim remaining task.
	claimed, err = ClaimTasks(ctx, abs, 10, 120*time.Second, "test-worker-2")
	require.NoErrorf(t, err, "cannot ClaimTasks second batch")
	require.Lenf(t, claimed, 1, "should claim 1 remaining task")
}

// testutilPostgresContainer creates a docker test container for postgres:18. It returns the
// postgres connection uri and the function to terminate the container. If the container start fails
// it marks the test as failed.
func testutilPostgresContainer(t testing.TB) (string, func(t testing.TB)) {
	t.Helper()
	ctx := t.Context()

	// Postgres test container setup initialized with absurd.sql.
	container, err := postgres.Run(
		ctx,
		"postgres:18",
		postgres.WithUsername("absurd"),
		postgres.WithPassword("absurd"),
		postgres.WithOrderedInitScripts(
			filepath.Join("..", "..", "..", "sql", "absurd.sql"),
		),
		postgres.BasicWaitStrategies(),
	)
	require.NoErrorf(t, err, "cannot start postgres container")
	defer func() {
	}()

	// Build return args.
	uri, err := container.ConnectionString(ctx)
	require.NoError(t, err)

	closefn := func(t testing.TB) {
		err := testcontainers.TerminateContainer(container)
		require.NoErrorf(
			t,
			err,
			"cannot terminate postgres container: id=%s",
			container.GetContainerID(),
		)
	}

	return uri, closefn
}
