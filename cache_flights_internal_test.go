package bus

import (
	"context"
	"sync/atomic"
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

// A shared call ends at the latest deadline of the callers waiting for it, a
// caller without a deadline counting as one of the limit after it arrived:
// a call that hangs (a dead connection) does not run on for good because one
// caller has no deadline, and that caller gets a deadline error, not a
// cancellation.
func TestFreshFlightsEndsAtTheLatestDeadline(t *testing.T) {
	f := freshFlights{limit: 300 * time.Millisecond}
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

	ended := make(chan time.Time, 1)
	hang := func(ctx context.Context) (any, error) {
		<-ctx.Done()
		ended <- time.Now()
		return nil, ctx.Err()
	}
	// both wait for the next call; it hangs once it starts
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	go func() { _, _ = f.do(ctx, "k", hang) }()
	require.Eventually(t, func() bool { return waitingIn("(*freshFlights).do(") == 2 }, time.Second, time.Millisecond)
	joined := make(chan error, 1)
	start := time.Now()
	go func() { _, err := f.do(context.Background(), "k", hang); joined <- err }()
	require.Eventually(t, func() bool { return waitingIn("(*freshFlights).do(") == 3 }, time.Second, time.Millisecond)
	close(release)

	select {
	case at := <-ended:
		assert.GreaterOrEqual(t, at.Sub(start), 250*time.Millisecond, "the call ended at the other caller's deadline")
	case <-time.After(3 * time.Second):
		t.Fatal("a call ran on for good because one caller has no deadline")
	}
	err := <-joined
	require.ErrorIs(t, err, context.DeadlineExceeded)
	assert.NotErrorIs(t, err, context.Canceled)
}

// A waiting call whose deadline passed before it started is not shared with a
// caller arriving afterwards: that caller gets a call of its own.
func TestFreshFlightsDoesNotShareACallPastItsDeadline(t *testing.T) {
	f := freshFlights{limit: 30 * time.Millisecond}
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
	// a caller without a deadline (and a limit shorter than the other's
	// deadline) keeps the waiting call from being dropped
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

// A caller without a deadline gets the result of a call slower than another
// caller's deadline: that deadline does not end the call.
func TestFreshFlightsOutlivesAnotherCallersDeadline(t *testing.T) {
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

	var calls atomic.Int32
	slow := func(ctx context.Context) (any, error) {
		calls.Add(1)
		select {
		case <-time.After(300 * time.Millisecond):
			return "ok", nil
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	type result struct {
		v   any
		err error
	}
	got := make(chan result, 1)
	go func() { v, err := f.do(context.Background(), "k", slow); got <- result{v, err} }()
	require.Eventually(t, func() bool { return waitingIn("(*freshFlights).do(") == 2 }, time.Second, time.Millisecond)
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	timedOut := make(chan error, 1)
	go func() { _, err := f.do(ctx, "k", slow); timedOut <- err }()
	require.Eventually(t, func() bool { return waitingIn("(*freshFlights).do(") == 3 }, time.Second, time.Millisecond)
	close(release)

	require.ErrorIs(t, <-timedOut, context.DeadlineExceeded)
	select {
	case r := <-got:
		require.NoError(t, r.err, "a caller without a deadline got another caller's")
		assert.Equal(t, "ok", r.v)
	case <-time.After(3 * time.Second):
		t.Fatal("the caller got no result")
	}
	assert.Equal(t, int32(1), calls.Load(), "the call ran once")
}

// A caller without a deadline gets a result while the calls take longer than
// the deadlines of a steady stream of other callers.
func TestFreshFlightsCallerWithoutDeadlineAmongShorterDeadlines(t *testing.T) {
	var f freshFlights
	slow := func(ctx context.Context) (any, error) {
		select {
		case <-time.After(150 * time.Millisecond):
			return "ok", nil
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	stop := make(chan struct{})
	defer close(stop)
	go func() {
		for {
			select {
			case <-stop:
				return
			case <-time.After(10 * time.Millisecond):
			}
			go func() {
				ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
				defer cancel()
				_, _ = f.do(ctx, "k", slow)
			}()
		}
	}()
	time.Sleep(50 * time.Millisecond)
	type result struct {
		v   any
		err error
	}
	got := make(chan result, 1)
	go func() { v, err := f.do(context.Background(), "k", slow); got <- result{v, err} }()
	select {
	case r := <-got:
		require.NoError(t, r.err)
		assert.Equal(t, "ok", r.v)
	case <-time.After(3 * time.Second):
		t.Fatal("a caller without a deadline never got a result")
	}
}

// A caller whose own deadline passes gets context.DeadlineExceeded, also when
// the call it waits for ends at that same deadline.
func TestFreshFlightsCallerGetsItsOwnDeadline(t *testing.T) {
	var f freshFlights
	hang := func(ctx context.Context) (any, error) {
		<-ctx.Done()
		return nil, ctx.Err()
	}
	for range 100 {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Millisecond)
		_, err := f.do(ctx, "k", hang)
		cancel()
		require.ErrorIs(t, err, context.DeadlineExceeded)
	}
}
