// Package bus implements a publish-subscribe pattern on top of go-que.
// It allows publishing messages to subjects which can be subscribed to by multiple queues.
// The subject matching follows NATS-style wildcards pattern.
package bus

import (
	"log/slog"
	"time"

	"github.com/cenkalti/backoff/v5"
	"github.com/tnclong/go-que"
)

// DefaultRetryPolicy provides a default retry policy for published messages.
var DefaultRetryPolicy = que.RetryPolicy{
	InitialInterval:        30 * time.Second,
	MaxInterval:            600 * time.Second,
	NextIntervalMultiplier: 2,
	IntervalRandomPercent:  33,
	MaxRetryCount:          3,
}

// DefaultPlanConfig provides default settings for subscription jobs.
var DefaultPlanConfig = PlanConfig{
	RetryPolicy:     DefaultRetryPolicy,
	RunAtDelta:      0, // Immediate execution
	UniqueLifecycle: que.Ignore,
}

// DefaultWorkerConfig provides default settings for workers.
var DefaultWorkerConfig = WorkerConfig{
	MaxLockPerSecond:          5,
	MaxBufferJobsCount:        0,
	MaxPerformPerSecond:       1000,
	MaxConcurrentPerformCount: 200,
}

// DefaultMaxEnqueuePerBatch defines the default maximum number of plans
// that can be enqueued in a single transaction.
var DefaultMaxEnqueuePerBatch = 100

// DefaultConsumeBackOffFactory generates the default backoff strategy for worker reconnection.
var DefaultConsumeBackOffFactory = func() backoff.BackOff {
	expBackoff := backoff.NewExponentialBackOff()
	expBackoff.InitialInterval = 1 * time.Second
	expBackoff.MaxInterval = 30 * time.Second
	expBackoff.Multiplier = 2.0
	return expBackoff
}

// WorkerConfig defines performance-related configuration for workers processing messages.
type WorkerConfig struct {
	// MaxLockPerSecond is maximum frequency of calls to Lock() method of Queue.
	// Lower number uses lower database CPU resources.
	MaxLockPerSecond float64 `json:"maxLockPerSecond"`

	// MaxBufferJobsCount is maximum number of jobs in channel that are waiting for
	// a goroutine to execute them.
	MaxBufferJobsCount int `json:"maxBufferJobsCount"`

	// MaxPerformPerSecond is maximum frequency of Perform executions.
	MaxPerformPerSecond float64 `json:"maxPerformPerSecond"`

	// MaxConcurrentPerformCount is maximum number of goroutines executing Perform simultaneously.
	MaxConcurrentPerformCount int `json:"maxConcurrentPerformCount"`
}

// PlanConfig defines how a queue processes messages for a specific subject pattern.
type PlanConfig struct {
	// RetryPolicy defines how to retry failed job executions.
	RetryPolicy que.RetryPolicy `json:"retryPolicy"`

	// RunAtDelta specifies the duration to delay job execution from the time of message receipt.
	// Zero means execute immediately, positive values mean delayed execution.
	RunAtDelta time.Duration `json:"runAtDelta"`

	// UniqueLifecycle controls the uniqueness behavior of the job.
	UniqueLifecycle que.UniqueLifecycle `json:"uniqueLifecycle"`
}

// Equal compares this PlanConfig with another and returns true if they are equivalent.
func (p PlanConfig) Equal(other PlanConfig) bool {
	if p.RunAtDelta != other.RunAtDelta || p.UniqueLifecycle != other.UniqueLifecycle {
		return false
	}

	return p.RetryPolicy.InitialInterval == other.RetryPolicy.InitialInterval &&
		p.RetryPolicy.MaxInterval == other.RetryPolicy.MaxInterval &&
		p.RetryPolicy.NextIntervalMultiplier == other.RetryPolicy.NextIntervalMultiplier &&
		p.RetryPolicy.IntervalRandomPercent == other.RetryPolicy.IntervalRandomPercent &&
		p.RetryPolicy.MaxRetryCount == other.RetryPolicy.MaxRetryCount
}

// ConsumeOption represents an option for customizing a worker.
type ConsumeOption func(*ConsumeOptions)

// ConsumeOptions holds all the options for creating a worker.
type ConsumeOptions struct {
	// WorkerConfig contains the performance-related settings for a worker.
	WorkerConfig WorkerConfig

	// Backoff configures the reconnection backoff strategy.
	// If nil, DefaultBackOffFactory() will be used.
	Backoff backoff.BackOff
}

// WithWorkerConfig sets the worker configuration for a worker.
func WithWorkerConfig(config WorkerConfig) ConsumeOption {
	return func(opts *ConsumeOptions) {
		opts.WorkerConfig = config
	}
}

// WithBackoff sets the backoff strategy for reconnection.
func WithBackoff(b backoff.BackOff) ConsumeOption {
	return func(o *ConsumeOptions) {
		o.Backoff = b
	}
}

// SubscribeOption represents an option for customizing a subscription.
type SubscribeOption func(*SubscribeOptions)

// SubscribeOptions holds all the options for creating a subscription.
type SubscribeOptions struct {
	// PlanConfig contains the settings for job execution.
	PlanConfig PlanConfig
}

// WithPlanConfig sets the job configuration for a subscription.
func WithPlanConfig(config PlanConfig) SubscribeOption {
	return func(opts *SubscribeOptions) {
		opts.PlanConfig = config
	}
}

// BusOptions configures the Bus implementation.
type BusOptions struct {
	// Migrate controls whether database migrations are run during initialization.
	Migrate bool

	// Logger is used for logging warnings and errors. If nil, a default logger will be used.
	Logger *slog.Logger

	// MaxEnqueuePerBatch limits the maximum number of plans that can be enqueued in a single transaction.
	// If the number of plans exceeds this limit, they will be split into multiple transactions.
	// If less than or equal to 0, DefaultMaxEnqueuePerBatch will be used.
	MaxEnqueuePerBatch int
}

type BusOption func(*BusOptions)

// WithLogger sets the logger for the Bus implementation.
func WithLogger(logger *slog.Logger) BusOption {
	return func(opts *BusOptions) {
		opts.Logger = logger
	}
}

// WithMigrate sets whether database migrations should be run during initialization.
func WithMigrate(migrate bool) BusOption {
	return func(opts *BusOptions) {
		opts.Migrate = migrate
	}
}

// WithMaxEnqueuePerBatch sets the maximum number of plans that can be enqueued in a single transaction.
// If less than or equal to 0, DefaultMaxEnqueuePerBatch will be used.
func WithMaxEnqueuePerBatch(max int) BusOption {
	return func(opts *BusOptions) {
		opts.MaxEnqueuePerBatch = max
	}
}

type PublishOptions struct {
	Header   Header
	UniqueID func(msg *Outbound) string
}

type PublishOption func(*PublishOptions)

func WithHeader(header Header) PublishOption {
	return func(opts *PublishOptions) {
		opts.Header = header
	}
}

func WithUniqueID(v string) PublishOption {
	return func(opts *PublishOptions) {
		opts.UniqueID = UniqueID(v)
	}
}

func WithPureUniqueID(v string) PublishOption {
	return func(opts *PublishOptions) {
		opts.UniqueID = PureUniqueID(v)
	}
}
