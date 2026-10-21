package bus_test

import (
	"bytes"
	"context"
	"runtime/pprof"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/qor5/go-bus"
	"github.com/qor5/go-bus/pgbus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// gatedDialect holds a GetMetadata call, once armed, after it read the
// metadata, until released.
type gatedDialect struct {
	bus.Dialect
	mu      sync.Mutex
	armed   bool
	read    chan struct{}
	release chan struct{}
}

func (d *gatedDialect) GetMetadata(ctx context.Context) (*bus.Metadata, error) {
	meta, err := d.Dialect.GetMetadata(ctx)
	d.mu.Lock()
	armed := d.armed
	d.armed = false
	d.mu.Unlock()
	if armed {
		close(d.read)
		<-d.release
	}
	return meta, err
}

// goroutinesIn counts the goroutines whose stack has a frame of fn.
func goroutinesIn(fn string) int {
	var buf bytes.Buffer
	_ = pprof.Lookup("goroutine").WriteTo(&buf, 2)
	n := 0
	for _, g := range strings.Split(buf.String(), "\n\n") {
		if strings.Contains(g, fn) {
			n++
		}
	}
	return n
}

// TestCachedPublishSeesASubscriptionCommittedBeforeIt verifies that a
// Publish that starts after a Subscribe committed delivers to the new
// subscription, even when it finds the subject cached and a version check
// that read the metadata before the Subscribe committed is still running.
func TestCachedPublishSeesASubscriptionCommittedBeforeIt(t *testing.T) {
	cleanupAllTables()
	ctx := context.Background()
	d, err := pgbus.NewDialect(db)
	require.NoError(t, err)
	gate := &gatedDialect{read: make(chan struct{}), release: make(chan struct{})}
	b, err := bus.New(d, bus.WithDialectDecorator(func(_ context.Context, next bus.Dialect) (bus.Dialect, error) {
		gate.Dialect = next
		return gate, nil
	}))
	require.NoError(t, err)
	defer func() { _ = b.Close() }()
	publish := func() (*bus.Dispatch, error) {
		return b.Publish(ctx, &bus.Outbound{Message: bus.Message{Subject: "seen.topic", Payload: []byte(`"m"`)}})
	}

	_, err = b.Queue("old").Subscribe(ctx, "seen.topic")
	require.NoError(t, err)
	_, err = publish() // caches the subject's subscriptions
	require.NoError(t, err)

	gate.mu.Lock()
	gate.armed = true
	gate.mu.Unlock()
	go func() { _, _ = publish() }() // its version check reads the metadata, then waits
	<-gate.read
	_, err = b.Queue("new").Subscribe(ctx, "seen.topic")
	require.NoError(t, err)

	type result struct {
		d   *bus.Dispatch
		err error
	}
	after := make(chan result, 1)
	go func() { d, err := publish(); after <- result{d, err} }()
	// Wait until it is in its own version check, or waiting on the first.
	require.Eventually(t, func() bool { return goroutinesIn("(*cacheDialect).checkVersion(") >= 2 }, 10*time.Second, time.Millisecond)
	close(gate.release)

	r := <-after
	require.NoError(t, r.err)
	var queues []string
	for _, e := range r.d.Executions {
		queues = append(queues, e.Subscription.Queue())
	}
	assert.ElementsMatch(t, []string{"old", "new"}, queues, "a Publish after the Subscribe committed delivers to it")
}
