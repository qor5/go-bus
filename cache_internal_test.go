package bus

import (
	"bytes"
	"context"
	"runtime/pprof"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestFreshFlightsShareTheNextCall verifies that callers arriving while a
// call runs do not get its result, and share one call after it.
func TestFreshFlightsShareTheNextCall(t *testing.T) {
	var f freshFlights
	var calls atomic.Int64
	release := make(chan struct{})
	fn := func(context.Context) (any, error) {
		n := calls.Add(1)
		if n == 1 {
			<-release
		}
		return n, nil
	}
	ctx := context.Background()
	first := make(chan any, 1)
	go func() { v, _ := f.do(ctx, "k", fn); first <- v }()
	require.Eventually(t, func() bool { return calls.Load() == 1 }, time.Second, time.Millisecond)

	var wg sync.WaitGroup
	got := make(chan any, 5)
	for range 5 {
		wg.Add(1)
		go func() { defer wg.Done(); v, _ := f.do(ctx, "k", fn); got <- v }()
	}
	// All six wait for a result: the first and the five that joined the next call.
	require.Eventually(t, func() bool { return waitingIn("(*freshFlights).do(") == 6 }, time.Second, time.Millisecond)
	close(release)
	wg.Wait()
	close(got)

	assert.Equal(t, int64(1), <-first)
	for v := range got {
		assert.Equal(t, int64(2), v, "the call after the running one")
	}
	assert.Equal(t, int64(2), calls.Load(), "the waiting callers share one call")
}

// waitingIn counts the goroutines blocked in a select in fn.
func waitingIn(fn string) int {
	var buf bytes.Buffer
	_ = pprof.Lookup("goroutine").WriteTo(&buf, 2)
	n := 0
	for _, g := range strings.Split(buf.String(), "\n\n") {
		if strings.Contains(g, "[select") && strings.Contains(g, fn) {
			n++
		}
	}
	return n
}

// A call nobody waits for any more is cancelled; one that had not started is
// dropped, so a caller arriving afterwards starts a call of its own instead of
// sharing a cancelled one.
func TestFreshFlightsDropsACallEveryWaiterLeft(t *testing.T) {
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

	// a caller that gives up while waiting for the next call
	ctx, cancel := context.WithCancel(context.Background())
	left := make(chan error, 1)
	go func() {
		_, err := f.do(ctx, "k", func(ctx context.Context) (any, error) { return "abandoned", ctx.Err() })
		left <- err
	}()
	require.Eventually(t, func() bool { return waitingIn("(*freshFlights).do(") == 2 }, time.Second, time.Millisecond)
	cancel()
	require.ErrorIs(t, <-left, context.Canceled)

	// a caller arriving now gets a call of its own, not the abandoned one
	type result struct {
		v   any
		err error
	}
	got := make(chan result, 1)
	go func() {
		v, err := f.do(context.Background(), "k", func(ctx context.Context) (any, error) { return "fresh", ctx.Err() })
		got <- result{v, err}
	}()
	require.Eventually(t, func() bool { return waitingIn("(*freshFlights).do(") == 2 }, time.Second, time.Millisecond)
	close(release)
	select {
	case r := <-got:
		require.NoError(t, r.err)
		assert.Equal(t, "fresh", r.v)
	case <-time.After(3 * time.Second):
		t.Fatal("the caller got no result")
	}
}

// A running call whose callers have all given up is cancelled.
func TestFreshFlightsCancelsARunningCallEveryWaiterLeft(t *testing.T) {
	var f freshFlights
	ended := make(chan error, 1)
	ctx, cancel := context.WithCancel(context.Background())
	waited := make(chan error, 1)
	go func() {
		_, err := f.do(ctx, "k", func(ctx context.Context) (any, error) {
			<-ctx.Done() // a call that hangs until cancelled
			ended <- ctx.Err()
			return nil, ctx.Err()
		})
		waited <- err
	}()
	require.Eventually(t, func() bool { return waitingIn("(*freshFlights).do(") == 1 }, time.Second, time.Millisecond)
	cancel()
	require.ErrorIs(t, <-waited, context.Canceled)
	select {
	case err := <-ended:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(time.Second):
		t.Fatal("the running call was not cancelled after its only caller left")
	}
}
