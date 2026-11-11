// Package quex forwards to github.com/qor5/go-que/supervisor, where this
// code now lives. Everything here is the same type, value or behaviour as
// there; it stays so that code importing it keeps building. New code should
// use supervisor.
package quex

import (
	"context"
	"log/slog"
	"slices"

	"github.com/cenkalti/backoff/v5"
	"github.com/qor5/go-que"
	"github.com/qor5/go-que/supervisor"
)

var (
	// ErrReconnectBackOffStopped is supervisor.ErrReconnectBackOffStopped
	// (the same value, so errors.Is matches either).
	ErrReconnectBackOffStopped = supervisor.ErrReconnectBackOffStopped

	// DefaultReconnectBackOffFactory generates the default reconnection
	// backoff strategy for workers started through this package, and for
	// go-bus's default worker config. Replacing it does not change
	// supervisor's own default.
	DefaultReconnectBackOffFactory = supervisor.DefaultReconnectBackOffFactory
)

// WorkerController is supervisor.WorkerController.
type WorkerController = supervisor.WorkerController

// StartWorkerOptions is supervisor.StartWorkerOptions.
type StartWorkerOptions = supervisor.StartWorkerOptions

// StartWorkerOption is supervisor.StartWorkerOption.
type StartWorkerOption = supervisor.StartWorkerOption

// WithLogger is supervisor.WithLogger.
func WithLogger(logger *slog.Logger) StartWorkerOption {
	return supervisor.WithLogger(logger)
}

// WithReconnectBackOff is supervisor.WithReconnectBackOff.
func WithReconnectBackOff(b backoff.BackOff) StartWorkerOption {
	return supervisor.WithReconnectBackOff(b)
}

// WithOnStop is supervisor.WithOnStop.
func WithOnStop(fn func(err error)) StartWorkerOption {
	return supervisor.WithOnStop(fn)
}

// StartWorker is supervisor.StartWorker, except that without a reconnect
// backoff it uses this package's DefaultReconnectBackOffFactory, as it did.
func StartWorker(ctx context.Context, workerOptions que.WorkerOptions, options ...StartWorkerOption) (WorkerController, error) {
	opts := &StartWorkerOptions{}
	for _, option := range options {
		option(opts)
	}
	if opts.ReconnectBackOff == nil {
		// clip first: appending must not write into the caller's slice
		options = append(slices.Clip(options), supervisor.WithReconnectBackOff(DefaultReconnectBackOffFactory()))
	}
	return supervisor.StartWorker(ctx, workerOptions, options...)
}
