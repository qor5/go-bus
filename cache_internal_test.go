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
	fn := func() (any, error) {
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
