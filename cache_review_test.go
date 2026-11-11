package bus_test

import (
	"context"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/qor5/go-bus"
	"github.com/qor5/go-bus/bussql"
	"github.com/qor5/go-bus/pgbus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func queuesOf(d *bus.Dispatch) []string {
	var qs []string
	for _, e := range d.Executions {
		qs = append(qs, e.Subscription.Queue())
	}
	return qs
}

// A Publish in a caller's transaction looks subscriptions up in that
// transaction: a lookup on another connection would wait forever for one when
// the pool is used up by such callers.
func TestCachedPublishInATransactionNeedsNoOtherConnection(t *testing.T) {
	cleanupAllTables()
	ctx := context.Background()
	d, err := pgbus.NewDialect(db)
	require.NoError(t, err)
	b, err := bus.New(d)
	require.NoError(t, err)
	defer func() { _ = b.Close() }()
	_, err = b.Queue("pool").Subscribe(ctx, "pool.topic")
	require.NoError(t, err)

	db.SetMaxOpenConns(1)
	defer db.SetMaxOpenConns(0)
	tx, err := db.BeginTx(ctx, nil)
	require.NoError(t, err)
	defer func() { _ = tx.Rollback() }()
	tctx, cancel := context.WithTimeout(bussql.NewContext(ctx, tx), 3*time.Second)
	defer cancel()
	_, err = b.Publish(tctx, &bus.Outbound{Message: bus.Message{Subject: "pool.topic", Payload: []byte(`"m"`)}})
	assert.NoError(t, err, "Publish in a tx with MaxOpenConns=1")
	require.NoError(t, tx.Rollback())
}

// A Publish in the transaction that subscribed delivers to that subscription.
func TestCachedPublishInATransactionSeesItsOwnSubscription(t *testing.T) {
	cleanupAllTables()
	ctx := context.Background()
	d, err := pgbus.NewDialect(db)
	require.NoError(t, err)
	b, err := bus.New(d)
	require.NoError(t, err)
	defer func() { _ = b.Close() }()
	_, err = b.Queue("rw_old").Subscribe(ctx, "rw.topic")
	require.NoError(t, err)
	pub := func(ctx context.Context) *bus.Dispatch {
		dd, err := b.Publish(ctx, &bus.Outbound{Message: bus.Message{Subject: "rw.topic", Payload: []byte(`"m"`)}})
		require.NoError(t, err)
		return dd
	}
	pub(ctx) // cache

	tx, err := db.BeginTx(ctx, nil)
	require.NoError(t, err)
	defer func() { _ = tx.Rollback() }()
	tctx := bussql.NewContext(ctx, tx)
	_, err = b.Queue("rw_new").Subscribe(tctx, "rw.topic")
	require.NoError(t, err)
	assert.ElementsMatch(t, []string{"rw_old", "rw_new"}, queuesOf(pub(tctx)), "Publish in the tx that subscribed")
}

// hookCache runs hook once, after reading a key containing match and before
// returning what it read.
type hookCache struct {
	bus.Cache
	mu    sync.Mutex
	match string
	hook  func()
}

func (c *hookCache) Get(key string) ([]bus.Subscription, bool) {
	v, ok := c.Cache.Get(key)
	c.mu.Lock()
	h := c.hook
	if h != nil && strings.Contains(key, c.match) {
		c.hook = nil
	} else {
		h = nil
	}
	c.mu.Unlock()
	if h != nil {
		h()
	}
	return v, ok
}

// A version check that another caller completes between this caller's cache
// read and its own check does not let it serve what it read under the old
// version.
func TestCachedPublishIgnoresACheckThatRanBetweenItsReadAndItsCheck(t *testing.T) {
	cleanupAllTables()
	ctx := context.Background()
	d, err := pgbus.NewDialect(db)
	require.NoError(t, err)
	rc, err := bus.NewRistrettoCache(nil)
	require.NoError(t, err)
	defer rc.Close()
	hc := &hookCache{Cache: bus.WrapRistrettoCache(rc)}
	b, err := bus.New(d, bus.WithCache(hc))
	require.NoError(t, err)
	defer func() { _ = b.Close() }()
	pub := func(s string) *bus.Dispatch {
		dd, err := b.Publish(ctx, &bus.Outbound{Message: bus.Message{Subject: s, Payload: []byte(`"m"`)}})
		require.NoError(t, err)
		return dd
	}
	_, err = b.Queue("gc_old").Subscribe(ctx, "gc.topic")
	require.NoError(t, err)
	_, err = b.Queue("gc_other").Subscribe(ctx, "other.topic")
	require.NoError(t, err)
	pub("gc.topic")
	pub("other.topic")
	pub("gc.topic")
	pub("other.topic") // both cached under the current version

	_, err = b.Queue("gc_new").Subscribe(ctx, "gc.topic")
	require.NoError(t, err) // committed before the Publish below starts

	hc.mu.Lock()
	hc.match = "gc.topic"
	hc.hook = func() { pub("other.topic") } // another publisher's check completes meanwhile
	hc.mu.Unlock()
	assert.ElementsMatch(t, []string{"gc_old", "gc_new"}, queuesOf(pub("gc.topic")))
}

// hangOnce makes the first GetMetadata after it is armed hang, as on a dead
// connection, until its ctx ends.
type hangOnce struct {
	bus.Dialect
	armed atomic.Bool
}

func (d *hangOnce) GetMetadata(ctx context.Context) (*bus.Metadata, error) {
	if d.armed.CompareAndSwap(true, false) {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(time.Minute):
		}
	}
	return d.Dialect.GetMetadata(ctx)
}

// A version check that hangs (a dead connection) is cancelled once every
// caller waiting for it has given up, so later Publishes are not held behind
// it until TCP gives up.
func TestAHungVersionCheckDoesNotHoldLaterPublishes(t *testing.T) {
	cleanupAllTables()
	ctx := context.Background()
	d, err := pgbus.NewDialect(db)
	require.NoError(t, err)
	h := &hangOnce{}
	b, err := bus.New(d, bus.WithDialectDecorator(func(_ context.Context, next bus.Dialect) (bus.Dialect, error) {
		h.Dialect = next
		return h, nil
	}))
	require.NoError(t, err)
	defer func() { _ = b.Close() }()
	_, err = b.Queue("hang").Subscribe(ctx, "hang.topic")
	require.NoError(t, err)
	pub := func() error {
		c, cancel := context.WithTimeout(ctx, time.Second)
		defer cancel()
		_, err := b.Publish(c, &bus.Outbound{Message: bus.Message{Subject: "hang.topic", Payload: []byte(`"m"`)}})
		return err
	}
	require.NoError(t, pub())
	require.NoError(t, pub())
	h.armed.Store(true)
	assert.Error(t, pub(), "the publish whose check hangs")
	fails := 0
	for i := 0; i < 5; i++ {
		if pub() != nil {
			fails++
		}
	}
	assert.Zero(t, fails, "later publishes (each 1s deadline) failing because of one hung check")
}
