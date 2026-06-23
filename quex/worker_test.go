package quex_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/cenkalti/backoff/v5"
	"github.com/pkg/errors"
	que "github.com/qor5/go-que"
	"github.com/stretchr/testify/require"

	"github.com/qor5/go-bus/quex"
)

// recordingOnStop captures the OnStop callback invocations.
type recordingOnStop struct {
	mu    sync.Mutex
	calls []error
}

func (r *recordingOnStop) fn(err error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.calls = append(r.calls, err)
}

func (r *recordingOnStop) Calls() []error {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]error(nil), r.calls...)
}

// failingMutex makes every Lock fail, so Worker.Run() returns an error.
type failingMutex struct{}

func (failingMutex) Lock(_ context.Context, _ string, _ int) ([]que.Job, error) {
	return nil, errors.New("simulated lock failure")
}
func (failingMutex) Unlock(_ context.Context, _ []int64) error { return nil }

// idleMutex never returns jobs and never errors, so the worker idles until Stop.
type idleMutex struct{}

func (idleMutex) Lock(_ context.Context, _ string, _ int) ([]que.Job, error) { return nil, nil }
func (idleMutex) Unlock(_ context.Context, _ []int64) error                  { return nil }

func noopPerform(_ context.Context, _ que.Job) error { return nil }

// TestStartWorker_OnStopReportsRealErrorOnAbnormalTermination verifies that
// when the worker gives up, OnStop fires once carrying the real Run() error
// (not the generic ErrReconnectBackOffStopped that Err() reports).
func TestStartWorker_OnStopReportsRealErrorOnAbnormalTermination(t *testing.T) {
	rec := &recordingOnStop{}
	ctrl, err := quex.StartWorker(context.Background(), que.WorkerOptions{
		Queue:                     "test-queue",
		Mutex:                     failingMutex{},
		Perform:                   noopPerform,
		MaxLockPerSecond:          100,
		MaxPerformPerSecond:       100,
		MaxConcurrentPerformCount: 1,
	},
		quex.WithReconnectBackOff(&backoff.StopBackOff{}), // give up on first failure
		quex.WithOnStop(rec.fn),
	)
	require.NoError(t, err)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = ctrl.Stop(ctx)
	})

	select {
	case <-ctrl.Done():
	case <-time.After(5 * time.Second):
		t.Fatal("worker should give up and close Done()")
	}
	require.Error(t, ctrl.Err())

	calls := rec.Calls()
	require.Len(t, calls, 1, "OnStop must fire exactly once on terminal failure")
	require.Error(t, calls[0])
	require.ErrorContains(t, calls[0], "simulated lock failure",
		"OnStop must carry the real Run() error, not ErrReconnectBackOffStopped")
}

// TestStartWorker_OnStopNilOnCleanStop verifies a graceful Stop() invokes
// OnStop once with a nil error.
func TestStartWorker_OnStopNilOnCleanStop(t *testing.T) {
	rec := &recordingOnStop{}
	ctrl, err := quex.StartWorker(context.Background(), que.WorkerOptions{
		Queue:                     "test-queue",
		Mutex:                     idleMutex{},
		Perform:                   noopPerform,
		MaxLockPerSecond:          100,
		MaxPerformPerSecond:       100,
		MaxConcurrentPerformCount: 1,
	}, quex.WithOnStop(rec.fn))
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	require.NoError(t, ctrl.Stop(ctx))
	<-ctrl.Done()
	require.NoError(t, ctrl.Err())

	calls := rec.Calls()
	require.Len(t, calls, 1, "OnStop must fire once on clean stop")
	require.NoError(t, calls[0], "clean stop must report a nil error")
}
