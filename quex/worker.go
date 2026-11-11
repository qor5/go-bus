// Package quex forwards to github.com/qor5/go-que/supervisor, where this
// code now lives. Everything here is the same type, value or behaviour as
// there; it stays so that code importing it keeps building.
//
// Deprecated: use github.com/qor5/go-que/supervisor.
package quex

import (
	"context"
	"log/slog"

	"github.com/cenkalti/backoff/v5"
	"github.com/qor5/go-que"
	"github.com/qor5/go-que/supervisor"
)

var (
	// ErrReconnectBackOffStopped is supervisor.ErrReconnectBackOffStopped
	// (the same value, so errors.Is matches either).
	//
	// Deprecated: use supervisor.ErrReconnectBackOffStopped.
	ErrReconnectBackOffStopped = supervisor.ErrReconnectBackOffStopped

	// DefaultReconnectBackOffFactory generates the default reconnection
	// backoff strategy for workers started through this package, and for
	// go-bus's default worker config. Replacing it does not change
	// supervisor's own default.
	//
	// Deprecated: use supervisor.DefaultReconnectBackOffFactory.
	DefaultReconnectBackOffFactory = supervisor.DefaultReconnectBackOffFactory
)

// WorkerController is supervisor.WorkerController.
//
// Deprecated: use supervisor.WorkerController.
type WorkerController = supervisor.WorkerController

// StartWorkerOptions is supervisor.StartWorkerOptions.
//
// Deprecated: use supervisor.StartWorkerOptions.
type StartWorkerOptions = supervisor.StartWorkerOptions

// StartWorkerOption is supervisor.StartWorkerOption.
//
// Deprecated: use supervisor.StartWorkerOption.
type StartWorkerOption = supervisor.StartWorkerOption

// WithLogger is supervisor.WithLogger.
//
// Deprecated: use supervisor.WithLogger.
func WithLogger(logger *slog.Logger) StartWorkerOption {
	return supervisor.WithLogger(logger)
}

// WithReconnectBackOff is supervisor.WithReconnectBackOff.
//
// Deprecated: use supervisor.WithReconnectBackOff.
func WithReconnectBackOff(b backoff.BackOff) StartWorkerOption {
	return supervisor.WithReconnectBackOff(b)
}

// WithOnStop is supervisor.WithOnStop.
//
// Deprecated: use supervisor.WithOnStop.
func WithOnStop(fn func(err error)) StartWorkerOption {
	return supervisor.WithOnStop(fn)
}

// StartWorker is supervisor.StartWorker, except that without a reconnect
// backoff it uses this package's DefaultReconnectBackOffFactory, as it did.
//
// Deprecated: use supervisor.StartWorker.
func StartWorker(ctx context.Context, workerOptions que.WorkerOptions, options ...StartWorkerOption) (WorkerController, error) {
	opts := &StartWorkerOptions{}
	for _, option := range options {
		option(opts)
	}
	if opts.ReconnectBackOff == nil {
		options = append(options, supervisor.WithReconnectBackOff(DefaultReconnectBackOffFactory()))
	}
	return supervisor.StartWorker(ctx, workerOptions, options...)
}
