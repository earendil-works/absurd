package absurd

import (
	"slices"
	"time"
)

type SpawnResult struct {
	TaskID  string
	RunID   string
	Attempt int
}

type ClaimedTask struct {
	RunID         string  `json:"run_id"`
	TaskID        string  `json:"task_id"`
	TaskName      string  `json:"task_name"`
	Attempt       int     `json:"attempt"`
	Params        any     `json:"params"`
	RetryStrategy any     `json:"retry_strategy"`
	MaxAttempts   *int    `json:"max_attempts"`
	Headers       any     `json:"headers"`
	WakeEvent     *string `json:"wake_event"`
	EventPayload  any     `json:"event_payload"`
}

type SpawnOptions struct {
	MaxAttempts   int                 `json:"max_attempts,omitempty"`
	Headers       any                 `json:"headers,omitempty"`
	RetryStrategy *RetryStrategyJSONB `json:"retry_strategy,omitempty"`
	Cancellation  *CancellationPolicy `json:"cancellation,omitempty"`
}

type Checkpoint struct {
	CheckpointName string
	// State is the json encoded checkpoint state.
	State      []byte
	Status     string
	OwnerRunID string
	UpdatedAt  time.Time
}

type RetryStrategy struct {
	Kind   RetryStrategyKind
	Base   time.Duration
	Factor float64
	Max    time.Duration
}

// NOTE(hohmannr): Serialization could also be handled via a custom UnmarshalJSON/MarshalJSON on
// [RetryStrategy], which might even be the better solution, but would probably need a external
// package like gjson/sjson.
type RetryStrategyJSONB struct {
	Kind RetryStrategyKind `json:"kind"`

	// NOTE(hohmannr): Why are these floats in the python SDK?
	BaseSeconds float64 `json:"base_seconds,omitempty"`
	MaxSeconds  float64 `json:"max_seconds,omitempty"`
	Factor      float64 `json:"factor,omitempty"`
}

func (r *RetryStrategy) JSONB() *RetryStrategyJSONB {
	if r == nil {
		return nil
	}

	return &RetryStrategyJSONB{
		Kind:        r.Kind,
		BaseSeconds: r.Base.Seconds(),
		Factor:      r.Factor,
		MaxSeconds:  r.Max.Seconds(),
	}
}

type RetryStrategyKind string

var (
	RetryStrategyNone        RetryStrategyKind = "none"
	RetryStrategyFixed       RetryStrategyKind = "fixed"
	RetryStrategyExponential RetryStrategyKind = "exponential"
)

var validRetryStrategyKinds = []RetryStrategyKind{
	RetryStrategyNone,
	RetryStrategyFixed,
	RetryStrategyExponential,
}

func (k RetryStrategyKind) String() string {
	switch k {
	default:
		return string(RetryStrategyNone) // "none" is the default value.
	case RetryStrategyExponential:
		return string(RetryStrategyExponential)
	case RetryStrategyFixed:
		return string(RetryStrategyFixed)
	}
}

func (r RetryStrategy) Valid() error {
	if r.Kind == RetryStrategyNone {
		return nil
	}

	if r.Base <= 0 {
		return ErrInvalidRetryBaseDuration
	}
	if r.Base > r.Max {
		return ErrInvalidRetryMaxDuration
	}
	if r.Factor < 1 {
		return ErrInvalidRetryFactor
	}

	if !slices.Contains(validRetryStrategyKinds, r.Kind) {
		return ErrInvalidRetryStrategyKind
	}

	return nil
}
