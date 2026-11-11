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
	"github.com/qor5/go-bus/bussql"
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

// gatedLookup holds a BySubject call, once armed, after it read the database.
type gatedLookup struct {
	bus.Dialect
	mu      sync.Mutex
	armed   bool
	read    chan struct{}
	release chan struct{}
}

func (d *gatedLookup) BySubject(ctx context.Context, s string) ([]bus.Subscription, error) {
	subs, err := d.Dialect.BySubject(ctx, s)
	d.mu.Lock()
	armed := d.armed
	d.armed = false
	d.mu.Unlock()
	if armed {
		close(d.read)
		<-d.release
	}
	return subs, err
}

// On a cache miss too, a Publish that starts after a Subscribe committed
// delivers to it, even while a lookup that read the database before the
// commit is still running (and is about to cache what it read).
func TestCacheMissPublishSeesASubscriptionCommittedBeforeIt(t *testing.T) {
	cleanupAllTables()
	ctx := context.Background()
	d, err := pgbus.NewDialect(db)
	require.NoError(t, err)
	gate := &gatedLookup{read: make(chan struct{}), release: make(chan struct{})}
	b, err := bus.New(d, bus.WithDialectDecorator(func(_ context.Context, next bus.Dialect) (bus.Dialect, error) {
		gate.Dialect = next
		return gate, nil
	}))
	require.NoError(t, err)
	defer func() { _ = b.Close() }()
	publish := func() (*bus.Dispatch, error) {
		return b.Publish(ctx, &bus.Outbound{Message: bus.Message{Subject: "miss.topic", Payload: []byte(`"m"`)}})
	}
	_, err = b.Queue("old").Subscribe(ctx, "miss.topic")
	require.NoError(t, err)
	_, err = publish() // learns the version, caches
	require.NoError(t, err)
	_, err = b.Queue("other").Subscribe(ctx, "unrelated.topic") // bumps version
	require.NoError(t, err)

	gate.mu.Lock()
	gate.armed = true
	gate.mu.Unlock()
	go func() { _, _ = publish() }() // version check clears the cache; its lookup reads, then waits
	<-gate.read
	_, err = b.Queue("new").Subscribe(ctx, "miss.topic")
	require.NoError(t, err)

	type result struct {
		d   *bus.Dispatch
		err error
	}
	after := make(chan result, 1)
	go func() { d, err := publish(); after <- result{d, err} }()
	require.Eventually(t, func() bool { return goroutinesIn("(*freshFlights).do(") >= 2 }, 10*time.Second, time.Millisecond)
	close(gate.release)
	r := <-after
	require.NoError(t, r.err)
	var queues []string
	for _, e := range r.d.Executions {
		queues = append(queues, e.Subscription.Queue())
	}
	assert.ElementsMatch(t, []string{"old", "new"}, queues, "a Publish after the Subscribe committed delivers to it")
}

// gatedSetCache holds one Set, once armed, until released.
type gatedSetCache struct {
	bus.Cache
	mu      sync.Mutex
	armed   bool
	in      chan struct{}
	release chan struct{}
	stored  chan struct{} // closed once the held Set is done
}

func (c *gatedSetCache) Set(k string, v []bus.Subscription) {
	c.mu.Lock()
	armed := c.armed
	c.armed = false
	c.mu.Unlock()
	if armed {
		close(c.in)
		<-c.release
		defer close(c.stored)
	}
	c.Cache.Set(k, v)
}

// A lookup that read the subscriptions under one metadata version and caches
// them after the version moved on must not leave them in place under the new
// version, where later Publishes would keep missing a new subscription.
func TestALookupCachedAfterTheVersionChangedIsNotServed(t *testing.T) {
	cleanupAllTables()
	ctx := context.Background()
	d, err := pgbus.NewDialect(db)
	require.NoError(t, err)
	rc, err := bus.NewRistrettoCache(nil)
	require.NoError(t, err)
	defer rc.Close()
	gc := &gatedSetCache{Cache: bus.WrapRistrettoCache(rc), in: make(chan struct{}), release: make(chan struct{}), stored: make(chan struct{})}
	b, err := bus.New(d, bus.WithCache(gc))
	require.NoError(t, err)
	defer func() { _ = b.Close() }()
	pub := func(s string) *bus.Dispatch {
		r, err := b.Publish(ctx, &bus.Outbound{Message: bus.Message{Subject: s, Payload: []byte(`"m"`)}})
		require.NoError(t, err)
		return r
	}
	_, err = b.Queue("old").Subscribe(ctx, "race.topic")
	require.NoError(t, err)
	_, err = b.Queue("z").Subscribe(ctx, "zzz.topic")
	require.NoError(t, err)
	pub("race.topic")
	_, err = b.Queue("other").Subscribe(ctx, "unrelated.topic") // bump
	require.NoError(t, err)

	gc.mu.Lock()
	gc.armed = true
	gc.mu.Unlock()
	go pub("race.topic") // clears, reads [old], passes the version guard, then Set is held
	<-gc.in
	pub("zzz.topic") // caches zzz under the current version
	_, err = b.Queue("new").Subscribe(ctx, "race.topic")
	require.NoError(t, err)
	pub("zzz.topic")  // version check: learns the new version, clears
	close(gc.release) // the held Set stores the stale list
	<-gc.stored

	for i := 0; i < 3; i++ {
		var queues []string
		for _, e := range pub("race.topic").Executions {
			queues = append(queues, e.Subscription.Queue())
		}
		assert.ElementsMatch(t, []string{"old", "new"}, queues, "publish #%d after the Subscribe", i)
	}
}

// A lookup shared among callers does not run on an executor other than a
// transaction that one of them carries (a *sql.Conn, say): the others would
// use a connection that is not theirs, and fail with it. Here the connection
// is closed; the lookup runs on the bus's own database.
func TestCachedLookupIgnoresAConnectionTheCallerCarries(t *testing.T) {
	cleanupAllTables()
	ctx := context.Background()
	b, err := pgbus.New(db)
	require.NoError(t, err)
	defer func() { _ = b.Close() }()
	_, err = b.Queue("conn").Subscribe(ctx, "conn.topic")
	require.NoError(t, err)

	conn, err := db.Conn(ctx)
	require.NoError(t, err)
	require.NoError(t, conn.Close())
	subs, err := b.BySubject(bussql.NewContext(ctx, conn), "conn.topic")
	require.NoError(t, err)
	assert.Len(t, subs, 1)
}
