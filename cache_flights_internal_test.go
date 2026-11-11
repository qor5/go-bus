package bus

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// heldMetadata holds its first GetMetadata until released; every call
// honours ctx, as a database driver does.
type heldMetadata struct {
	Dialect
	started chan struct{}
	release chan struct{}
	n       int
}

func (d *heldMetadata) GetMetadata(ctx context.Context) (*Metadata, error) {
	d.n++
	if d.n == 1 {
		close(d.started)
		<-d.release
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return &Metadata{Version: 1}, nil
}

// The call that callers share does not end with the ctx of the caller that
// started it: a caller that gives up while waiting for it (its own deadline)
// must not fail the others sharing it.
func TestFreshFlightsSharedCallOutlivesTheCallerThatStartedIt(t *testing.T) {
	d := &heldMetadata{started: make(chan struct{}), release: make(chan struct{})}
	rc, err := NewRistrettoCache(nil)
	require.NoError(t, err)
	c := &cacheDialect{Dialect: d, cache: WrapRistrettoCache(rc)}
	go func() { _, _ = c.checkVersion(context.Background()) }() // running call
	<-d.started

	ctxA, cancelA := context.WithCancel(context.Background())
	aDone := make(chan error, 1)
	go func() { _, err := c.checkVersion(ctxA); aDone <- err }() // creates the next call with ctxA
	require.Eventually(t, func() bool { return waitingIn("(*freshFlights).do(") == 2 }, time.Second, time.Millisecond)

	bDone := make(chan error, 1)
	go func() { _, err := c.checkVersion(context.Background()); bDone <- err }() // healthy caller, shares next
	require.Eventually(t, func() bool { return waitingIn("(*freshFlights).do(") == 3 }, time.Second, time.Millisecond)

	cancelA()
	require.ErrorIs(t, <-aDone, context.Canceled)
	close(d.release)
	errB := <-bDone
	assert.NoError(t, errB, "a caller whose ctx is fine got another caller's cancellation")
}

// A shared call ends at the latest deadline of the callers waiting for it: a
// caller without a deadline does not keep a call that hangs (a dead
// connection) running past every deadline, as it did not when the call ran on
// the starting caller's ctx.
func TestFreshFlightsEndsAtTheLatestDeadline(t *testing.T) {
	var f freshFlights
	release := make(chan struct{})
	running := make(chan struct{})
	go func() {
		_, _ = f.do(context.Background(), "k", func(context.Context) (any, error) {
			close(running)
			<-release
			return "first", nil
		})
	}()
	<-running

	ended := make(chan error, 1)
	hang := func(ctx context.Context) (any, error) {
		<-ctx.Done()
		ended <- ctx.Err()
		return nil, ctx.Err()
	}
	// both wait for the next call; it hangs once it starts
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	go func() { _, _ = f.do(ctx, "k", hang) }()
	require.Eventually(t, func() bool { return waitingIn("(*freshFlights).do(") == 2 }, time.Second, time.Millisecond)
	joined := make(chan error, 1)
	go func() { _, err := f.do(context.Background(), "k", hang); joined <- err }()
	require.Eventually(t, func() bool { return waitingIn("(*freshFlights).do(") == 3 }, time.Second, time.Millisecond)
	close(release)

	select {
	case err := <-joined:
		require.Error(t, err)
	case <-time.After(3 * time.Second):
		t.Fatal("a caller without a deadline still waits for a call past every deadline")
	}
	require.Error(t, <-ended)
}

// A waiting call whose deadline passed before it started is not shared with a
// caller arriving afterwards: that caller gets a call of its own.
func TestFreshFlightsDoesNotShareACallPastItsDeadline(t *testing.T) {
	var f freshFlights
	release := make(chan struct{})
	running := make(chan struct{})
	go func() {
		_, _ = f.do(context.Background(), "k", func(context.Context) (any, error) {
			close(running)
			<-release
			return "first", nil
		})
	}()
	<-running

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	timedOut := make(chan error, 1)
	go func() {
		_, err := f.do(ctx, "k", func(ctx context.Context) (any, error) { return "timed out", ctx.Err() })
		timedOut <- err
	}()
	// a caller without a deadline keeps the waiting call from being dropped
	kept := make(chan error, 1)
	go func() {
		_, err := f.do(context.Background(), "k", func(ctx context.Context) (any, error) { return "kept", ctx.Err() })
		kept <- err
	}()
	require.Eventually(t, func() bool { return waitingIn("(*freshFlights).do(") == 3 }, time.Second, time.Millisecond)
	require.ErrorIs(t, <-timedOut, context.DeadlineExceeded)

	type result struct {
		v   any
		err error
	}
	got := make(chan result, 1)
	go func() {
		v, err := f.do(context.Background(), "k", func(ctx context.Context) (any, error) { return "fresh", ctx.Err() })
		got <- result{v, err}
	}()
	// this caller is in once the caller without a deadline got the expired
	// call's error, or is waiting next to it
	require.Eventually(t, func() bool {
		return len(kept) > 0 || waitingIn("(*freshFlights).do(") == 3
	}, time.Second, time.Millisecond)
	close(release)
	select {
	case r := <-got:
		require.NoError(t, r.err, "the caller shared a call cancelled at another caller's deadline")
	case <-time.After(3 * time.Second):
		t.Fatal("the caller got no result")
	}
}

// The earliest deadline among a call's callers does not end it while another
// caller's is still ahead.
func TestFreshFlightsRunsUntilTheLatestDeadline(t *testing.T) {
	var f freshFlights
	release := make(chan struct{})
	running := make(chan struct{})
	go func() {
		_, _ = f.do(context.Background(), "k", func(context.Context) (any, error) {
			close(running)
			<-release
			return "first", nil
		})
	}()
	<-running

	slow := func(ctx context.Context) (any, error) {
		select {
		case <-time.After(300 * time.Millisecond):
			return "done", nil
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	short, cancelShort := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancelShort()
	go func() { _, _ = f.do(short, "k", slow) }()
	require.Eventually(t, func() bool { return waitingIn("(*freshFlights).do(") == 2 }, time.Second, time.Millisecond)
	long, cancelLong := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelLong()
	type result struct {
		v   any
		err error
	}
	got := make(chan result, 1)
	go func() { v, err := f.do(long, "k", slow); got <- result{v, err} }()
	require.Eventually(t, func() bool { return waitingIn("(*freshFlights).do(") == 3 }, time.Second, time.Millisecond)
	close(release)

	r := <-got
	require.NoError(t, r.err, "the call ended at the earliest deadline, not the latest")
	assert.Equal(t, "done", r.v)
}
