package absurd

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/jackc/pgx/v5"
)

// -----------------------
// Base config and options
// -----------------------

type BaseConfig struct {
	Queue       string
	MaxAttempts int

	// Either ConnConfig or conn can be set, never both.
	ConnConfig *pgx.ConnConfig
	Conn       *pgx.Conn
}

// defaultBaseConfig is the default value for the base absurd client.
var defaultBaseConfig = BaseConfig{
	Queue:       "default",
	ConnConfig:  mustParseConfig("postgres://localhost:5432/absurd"),
	MaxAttempts: 5,
}

// BaseOption is a function that configures [base] client. Currently supported options:
//   - [WithDefaultQueue]
//   - [WithConn]
//   - [WithURI]
//   - [FromEnv]
//   - [WithDefaultMaxAttempts]
type BaseOption func(c *BaseConfig) error

// connect calls pgx.ConnectConfig if [b.Conn] was not cached yet or set externally via [WithConn].
func (c *BaseConfig) connect(ctx context.Context) (*pgx.Conn, error) {
	// Fast path, when connection was already set.
	if c.Conn != nil {
		return c.Conn, nil
	}

	// Slow path where we need to connect to postgres first.
	if c.ConnConfig == nil {
		return nil, ErrNoPostgresConnectionConfigured
	}
	conn, err := pgx.ConnectConfig(ctx, c.ConnConfig)
	if err != nil {
		return nil, err
	}
	// Cache connection and clear config, to make a state, where both [b.Conn] and [b.ConnConfig]
	// are set, unrepresentable.
	c.Conn = conn
	c.ConnConfig = nil

	return conn, nil
}

func WithDefaultQueue(name string) BaseOption {
	return func(c *BaseConfig) error {
		c.Queue = name

		return nil
	}
}

func WithConn(conn *pgx.Conn) BaseOption {
	return func(c *BaseConfig) error {
		// Clear [b.connConfig], since this option enables [b.Conn].
		c.ConnConfig = nil
		c.Conn = conn

		return nil
	}
}

func WithURI(uri string) BaseOption {
	return func(c *BaseConfig) error {
		// Clear [b.conn], since this option enables [b.ConnConfig].
		c.Conn = nil
		connConfig, err := pgx.ParseConfig(uri)
		if err != nil {
			return err
		}
		c.ConnConfig = connConfig

		return nil
	}
}

func WithDefaultMaxAttempts(n int) BaseOption {
	return func(c *BaseConfig) error {
		c.MaxAttempts = n

		return nil
	}
}

func FromEnv() BaseOption {
	uri := os.Getenv("ABSURD_DATABASE_URL")
	return WithURI(uri)
}

// --------------------------
// Handle config and options
// --------------------------

type HandleConfig struct {
	Queue        string
	MaxAttempts  int
	Cancellation *CancellationPolicy
}

type CancellationPolicy struct {
	MaxDuration time.Duration
	MaxDelay    time.Duration
}

var defaultHandleConfig = HandleConfig{
	Queue:        "",  // Empty, since fallback is always [BaseConfig.Queue]
	MaxAttempts:  0,   // Zeroed, since fallback is awlays [BaseConfig.MaxAttempts]
	Cancellation: nil, // No cancellation policy by default.
}

// HandleOption is a function that configures [HandleConfig]. Currently supported options:
//   - [WithHandlerQueue]
//   - [WithHandlerMaxAttempts]
//   - [WithHandlerCancellation]
type HandleOption func(c *HandleConfig) error

func WithHandlerQueue(name string) HandleOption {
	return func(c *HandleConfig) error {
		c.Queue = name

		return nil
	}
}

func WithHandlerMaxAttempts(n int) HandleOption {
	return func(c *HandleConfig) error {
		if n < 1 {
			return ErrInvalidMaxAttempts
		}
		c.MaxAttempts = n

		return nil
	}
}

func WithHandlerCancellation(maxDuration, maxDelay time.Duration) HandleOption {
	return func(c *HandleConfig) error {
		c.Cancellation = &CancellationPolicy{
			MaxDuration: maxDuration,
			MaxDelay:    maxDelay,
		}
		// TODO: Add a validity check for CancellationPolicy, since this is a user facing function.

		return nil
	}
}

// ------------------------
// Spawn config and options
// ------------------------

type SpawnConfig struct {
	Queue         string
	MaxAttempts   int
	RetryStrategy *RetryStrategy
	Cancellation  *CancellationPolicy
	Headers       any
}

// defaultSpawnConfig is the default value for spawning a task.
var defaultSpawnConfig = SpawnConfig{
	Queue:       "", // Empty, since fallback is always [BaseConfig.Queue]
	MaxAttempts: 0,  // Zeroed, since fallback is awlays [BaseConfig.MaxAttempts]
	// NOTE(hohmannr): Is this also optional? What would then be the difference between nil and
	// "none"
	RetryStrategy: &RetryStrategy{
		Kind: RetryStrategyNone,
	},
	Cancellation: nil, // Optional.
	Headers:      nil, // Optional.
}

// SpawnOption is a function that configures [SpawnConfig]. Currently supported options:
//   - [WithSpawnQueue]
//   - [WithSpawnMaxAttempts]
//   - [WithExponentialRetry]
//   - [WithFixedRetry]
//   - [WithHeaders]
type SpawnOption func(c *SpawnConfig) error

func WithSpawnQueue(name string) SpawnOption {
	return func(c *SpawnConfig) error {
		c.Queue = name

		return nil
	}
}

func WithSpawnMaxAttempts(n int) SpawnOption {
	return func(c *SpawnConfig) error {
		if n < 1 {
			return ErrInvalidMaxAttempts
		}
		c.MaxAttempts = n

		return nil
	}
}

func WithExponentialRetry(base, max time.Duration, factor float64) SpawnOption {
	return func(c *SpawnConfig) error {
		c.RetryStrategy = &RetryStrategy{
			Kind:   RetryStrategyExponential,
			Base:   base,
			Max:    max,
			Factor: factor,
		}
		if err := c.RetryStrategy.Valid(); err != nil {
			return err
		}

		return nil
	}
}

func WithFixedRetry(base, max time.Duration, factor float64) SpawnOption {
	return func(c *SpawnConfig) error {
		c.RetryStrategy = &RetryStrategy{
			Kind:   RetryStrategyFixed,
			Base:   base,
			Max:    max,
			Factor: factor,
		}
		if err := c.RetryStrategy.Valid(); err != nil {
			return err
		}

		return nil
	}
}

func WithHeaders(headers any) SpawnOption {
	return func(c *SpawnConfig) error {
		c.Headers = headers

		return nil
	}
}

// -------------------------
// Cancel config and options
// -------------------------

type CancelConfig struct {
	Queue string
}

// defaultCancelConfig is the default value for cancelling a task.
var defaultCancelConfig = CancelConfig{
	Queue: "", // Empty, since fallback is always [BaseConfig.Queue]
}

// CancelOption is a function that configures [CancelConfig]. Currently supported options:
//   - [WithCancelQueue]
type CancelOption func(c *CancelConfig) error

func WithCancelQueue(name string) CancelOption {
	return func(c *CancelConfig) error {
		c.Queue = name

		return nil
	}
}

// -----------------------------
// EmitEvent config and options
// -----------------------------

type EmitEventConfig struct {
	Queue string
}

// defaultEmitEventConfig is the default value for emitting an event.
var defaultEmitEventConfig = EmitEventConfig{
	Queue: "", // Empty, since fallback is always [BaseConfig.Queue]
}

// EmitEventOption is a function that configures [EmitEventConfig]. Currently supported options:
//   - [WithEmitEventQueue]
type EmitEventOption func(c *EmitEventConfig) error

func WithEmitEventQueue(name string) EmitEventOption {
	return func(c *EmitEventConfig) error {
		c.Queue = name

		return nil
	}
}

// -----------------------
// Run config and options
// -----------------------

type RunConfig struct {
	WorkerID     string
	ClaimTimeout time.Duration
	BatchSize    int
	PollInterval time.Duration
}

// defaultRunConfig is the default value for running a worker.
var defaultRunConfig = RunConfig{
	WorkerID:     defaultWorkerID(), // <hostname>:<pid>
	ClaimTimeout: 120 * time.Second,
	BatchSize:    1,
	PollInterval: 250 * time.Millisecond,
}

// RunOption is a function that configures [RunConfig]. Currently supported options:
//   - [WithWorkerID]
//   - [WithClaimTimeout]
//   - [WithBatchSize]
//   - [WithPollInterval]
type RunOption func(c *RunConfig) error

func WithWorkerID(id string) RunOption {
	return func(c *RunConfig) error {
		c.WorkerID = id

		return nil
	}
}

func WithClaimTimeout(timeout time.Duration) RunOption {
	return func(c *RunConfig) error {
		// NOTE(hohmannr): Does this rounding and check even make sense?
		timeout = timeout.Round(time.Second)
		if timeout < time.Second {
			return ErrInvalidClaimTimeout
		}
		c.ClaimTimeout = timeout

		return nil
	}
}

func WithBatchSize(size int) RunOption {
	return func(c *RunConfig) error {
		if size < 1 {
			return ErrInvalidBatchSize
		}
		c.BatchSize = size

		return nil
	}
}

func WithPollInterval(interval time.Duration) RunOption {
	return func(c *RunConfig) error {
		if interval < 0 {
			return ErrInvalidPollInterval
		}
		c.PollInterval = interval

		return nil
	}
}

func defaultWorkerID() string {
	host, err := os.Hostname()
	if err != nil {
		host = "host" // NOTE(hohmannr): The typescript sdk does this, should "host" really be the fallback here?
	}

	return fmt.Sprintf("%s:%d", host, os.Getpid())
}

func mustParseConfig(uri string) *pgx.ConnConfig {
	config, err := pgx.ParseConfig(uri)
	if err != nil {
		// Panic on invariant.
		panic("absurd: provided postgres uri must be parsable")
	}

	return config
}
