// Package quex forwards to github.com/qor5/go-que/supervisor, where this
// code now lives. Everything here is the same type, value or behaviour as
// there; it stays so that code importing it keeps building. New code should
// use supervisor.
package quex

import (
	"context"
	"log/slog"

	"github.com/cenkalti/backoff/v5"
	"github.com/pkg/errors"
	"github.com/qor5/go-que"
	"github.com/qor5/go-que/supervisor"
)

var (
	// ErrReconnectBackOffStopped is supervisor.ErrReconnectBackOffStopped
	// (the same value, so errors.Is matches either). Assigning to it has no
	// effect on what workers return.
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
// It applies each option once, and none when ctx is already done, as before.
func StartWorker(ctx context.Context, workerOptions que.WorkerOptions, options ...StartWorkerOption) (WorkerController, error) {
	if ctx.Err() != nil {
		return nil, errors.WithStack(ctx.Err())
	}
	// one option that applies the caller's onto supervisor's own options, so
	// whatever supervisor sets up first is kept
	return supervisor.StartWorker(ctx, workerOptions, func(o *StartWorkerOptions) {
		for _, option := range options {
			option(o)
		}
		if o.ReconnectBackOff == nil {
			o.ReconnectBackOff = DefaultReconnectBackOffFactory()
		}
	})
}
