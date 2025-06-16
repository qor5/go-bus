package quex

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/cenkalti/backoff/v5"
	"github.com/pkg/errors"
	"github.com/qor5/go-que"
)

var (
	// ErrReconnectBackOffStopped is returned when the reconnection backoff policy indicates no more retries should be attempted.
	ErrReconnectBackOffStopped = errors.New("reconnect backoff policy indicates no more retries")

	// DefaultReconnectBackOffFactory generates the default reconnection backoff strategy for workers.
	DefaultReconnectBackOffFactory = func() backoff.BackOff {
		expBackOff := backoff.NewExponentialBackOff()
		expBackOff.InitialInterval = 1 * time.Second
		expBackOff.MaxInterval = 30 * time.Second
		expBackOff.Multiplier = 2.0
		return expBackOff
	}
)

// WorkerController provides methods to control a worker.
// It extends the Consumer interface with additional worker-specific functionality.
type WorkerController interface {
	// Stop stops the worker with the provided context for cancellation/timeout.
	// Returns an error if the shutdown failed or was interrupted.
	Stop(ctx context.Context) error
	// Done returns a channel that is closed when the worker is stopped.
	Done() <-chan struct{}
	// Err returns the error that caused the worker to stop, or nil if the worker is still active.
	Err() error
}

// StartWorkerOptions contains options for starting a worker.
type StartWorkerOptions struct {
	// Logger is used for logging worker events.
	// If nil, slog.Default() will be used.
	Logger *slog.Logger

	// ReconnectBackOff defines the backoff strategy for reconnection attempts.
	// If nil, DefaultReconnectBackOffFactory() will be used.
	ReconnectBackOff backoff.BackOff
}

// StartWorkerOption represents an option function for configuring a worker.
type StartWorkerOption func(*StartWorkerOptions)

// WithLogger sets the logger for the worker.
func WithLogger(logger *slog.Logger) StartWorkerOption {
	return func(opts *StartWorkerOptions) {
		opts.Logger = logger
	}
}

// WithReconnectBackOff sets the backoff strategy for worker reconnection.
func WithReconnectBackOff(b backoff.BackOff) StartWorkerOption {
	return func(opts *StartWorkerOptions) {
		opts.ReconnectBackOff = b
	}
}

// StartWorker starts a new worker with automatic reconnection capabilities.
// It returns a WorkerController that can be used to control the worker.
//
// The ctx parameter is only used to manage the startup process, not the worker's lifecycle.
// Currently, it only checks if the context is already canceled at startup time.
// It's included primarily to maintain good API design practices and for future extensibility.
// The worker will continue running until explicitly stopped with WorkerController.Stop.
//
// The workerOptions parameter defines the worker configuration, including queue, perform function, etc.
// Additional options can be provided using the StartWorkerOption functions.
func StartWorker(ctx context.Context, workerOptions que.WorkerOptions, options ...StartWorkerOption) (WorkerController, error) {
	if ctx.Err() != nil {
		return nil, errors.WithStack(ctx.Err())
	}

	// Initialize options with defaults
	opts := &StartWorkerOptions{}

	// Apply all option functions
	for _, option := range options {
		option(opts)
	}

	logger := opts.Logger
	if logger == nil {
		logger = slog.Default()
	}

	reconnectBackOff := opts.ReconnectBackOff
	if reconnectBackOff == nil {
		reconnectBackOff = DefaultReconnectBackOffFactory()
	}

	// Create initial worker
	worker, err := que.NewWorker(workerOptions)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create worker")
	}

	// Worker lifetime
	workerCtx, workerCancel := context.WithCancelCause(context.Background())
	workerDoneC := make(chan struct{})
	var workerMu sync.RWMutex

	// Start a goroutine to run the worker
	go func() (xerr error) {
		defer close(workerDoneC)
		// Ensure worker lifecycle ends when goroutine exits for any reason
		defer func() {
			if xerr != nil {
				logger.Error("worker stopped with error", "error", xerr)
			} else {
				logger.Info("worker stopped")
			}
			workerCancel(xerr)
		}()

		workerMu.RLock()
		currentWorker := worker
		workerMu.RUnlock()

		for {
			if workerCtx.Err() != nil {
				return nil
			}

			err := currentWorker.Run()
			if errors.Is(err, que.ErrWorkerStoped) {
				return nil
			}

			logger.Warn("worker error, will attempt to recreate", "error", err)

			nextBackOff := reconnectBackOff.NextBackOff()
			if nextBackOff == backoff.Stop {
				return errors.WithStack(ErrReconnectBackOffStopped)
			}

			select {
			case <-workerCtx.Done():
				return nil
			case <-time.After(nextBackOff):
			}

			newWorker, err := que.NewWorker(workerOptions)
			if err != nil {
				return errors.Wrap(err, "failed to recreate worker")
			}

			workerMu.Lock()
			if workerCtx.Err() != nil {
				workerMu.Unlock()
				return nil
			}
			worker = newWorker
			currentWorker = newWorker
			workerMu.Unlock()

			logger.Info("worker successfully recreated")

			reconnectBackOff.Reset()
		}
	}() // nolint:errcheck

	controller := &workerController{
		ctx:   workerCtx,
		doneC: workerDoneC,
	}
	controller.stop = func(ctx context.Context) error {
		// Always try to cancel the worker context
		workerCancel(nil)

		// Always wait for the worker to actually stop, regardless of who initiated the cancellation
		workerMu.RLock()
		workerToStop := worker
		workerMu.RUnlock()

		if workerToStop != nil {
			err := workerToStop.Stop(ctx)
			if err != nil {
				logger.Warn("error stopping worker", "error", err)
				return err
			}
		}

		// Wait for worker goroutine to finish - this ensures true idempotency
		select {
		case <-workerDoneC:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	return controller, nil
}

// workerController implements the WorkerController interface.
type workerController struct {
	ctx   context.Context
	stop  func(ctx context.Context) error
	doneC <-chan struct{}
}

func (c *workerController) Stop(ctx context.Context) error {
	return c.stop(ctx)
}

func (c *workerController) Done() <-chan struct{} {
	return c.doneC
}

func (c *workerController) Err() error {
	err := context.Cause(c.ctx)
	if errors.Is(err, context.Canceled) { // if no cause
		return nil // Normal shutdown, not an error
	}
	return err
}
