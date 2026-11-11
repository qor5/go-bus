package bus

import (
	"context"
	"database/sql"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/dgraph-io/ristretto/v2"
	"github.com/pkg/errors"
	"github.com/rs/xid"

	"github.com/qor5/go-bus/bussql"
)

var _ Dialect = (*cacheDialect)(nil)

// DefaultRistrettoConfigFactory is a factory function that returns a default ristretto.Config[string, []Subscription].
var DefaultRistrettoConfigFactory = func() *ristretto.Config[string, []Subscription] {
	return &ristretto.Config[string, []Subscription]{
		NumCounters: 1e6,
		MaxCost:     1e5,
		BufferItems: 64,
		Cost:        func(value []Subscription) int64 { return int64(len(value)) },
	}
}

type ristrettoCache struct {
	scope string
	cache *ristretto.Cache[string, []Subscription]
}

func (c *ristrettoCache) Get(key string) (value []Subscription, ok bool) {
	return c.cache.Get(c.scope + ":" + key)
}

func (c *ristrettoCache) Set(key string, value []Subscription) {
	if c.cache.Set(c.scope+":"+key, value, 0) {
		c.cache.Wait()
	}
}

func (c *ristrettoCache) Del(key string) {
	c.cache.Del(c.scope + ":" + key)
	c.cache.Wait()
}

func (c *ristrettoCache) Clear() {
	c.cache.Clear()
}

// hasExpiredSubscriptions checks if any subscriptions in the slice are expired
func hasExpiredSubscriptions(subs []Subscription) bool {
	now := time.Now()
	for _, sub := range subs {
		if expiresAt := sub.ExpiresAt(); !expiresAt.IsZero() && now.After(expiresAt) {
			return true
		}
	}
	return false
}

// NewRistrettoCache creates a new ristretto cache.
func NewRistrettoCache(config *ristretto.Config[string, []Subscription]) (*ristretto.Cache[string, []Subscription], error) {
	if config == nil {
		config = DefaultRistrettoConfigFactory()
	}
	cache, err := ristretto.NewCache(config)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create ristretto cache")
	}
	return cache, nil
}

// WrapRistrettoCache wraps a ristretto cache into a Cache interface.
// This is useful when you want to use WithCache with a ristretto cache.
func WrapRistrettoCache(cache *ristretto.Cache[string, []Subscription]) Cache {
	return &ristrettoCache{
		scope: xid.New().String(),
		cache: cache,
	}
}

// RistrettoDecorator creates a decorator that adds caching functionality to the dialect.
func RistrettoDecorator(cache *ristretto.Cache[string, []Subscription]) DialectDecorator {
	if cache == nil {
		panic("cache instance cannot be nil")
	}
	return CacheDecorator(&ristrettoCache{
		scope: xid.New().String(),
		cache: cache,
	})
}

// Cache is a simple cache interface for caching subscription lookups.
type Cache interface {
	Get(key string) (value []Subscription, ok bool)
	Set(key string, value []Subscription)
	Del(key string)
	Clear()
}

// CacheDecorator creates a decorator that adds caching functionality to the dialect.
// This significantly improves performance for subscription lookups.
// The decorator automatically handles TTL-based subscription expiration by checking
// cached subscriptions' ExpiresAt() values and refreshing from database when needed.
// The caller is responsible for creating and closing the cache instance.
func CacheDecorator(cache Cache) DialectDecorator {
	if cache == nil {
		panic("cache instance cannot be nil")
	}
	return func(_ context.Context, d Dialect) (Dialect, error) {
		return &cacheDialect{
			Dialect: d,
			cache:   cache,
		}, nil
	}
}

// cacheDialect caches subscription lookups by subject
type cacheDialect struct {
	Dialect
	cache   Cache
	flights freshFlights
	version int64 // Database version, accessed atomically
}

// withoutTx drops the caller's transaction from ctx, for a call that callers
// share: the others are not part of that transaction, which may also be over
// by the time the call runs.
func withoutTx(ctx context.Context) context.Context {
	return bussql.NewContext(ctx, nil)
}

// txOf returns the transaction ctx carries, if any.
func txOf(ctx context.Context) *sql.Tx {
	tx, _ := bussql.FromContext(ctx, nil).(*sql.Tx)
	return tx
}

// checkVersion reads the metadata version, clearing the cache if it moved
// on, and returns the version it read.
func (c *cacheDialect) checkVersion(ctx context.Context) (int64, error) {
	val, err := c.flights.do(withoutTx(ctx), "checkVersion", func(ctx context.Context) (any, error) {
		meta, err := c.GetMetadata(ctx)
		if err != nil {
			return int64(0), errors.Wrap(err, "failed to get metadata")
		}
		if meta.Version != atomic.LoadInt64(&c.version) {
			atomic.StoreInt64(&c.version, meta.Version)
			c.cache.Clear()
		}
		return meta.Version, nil
	})
	if err != nil {
		return 0, err
	}
	return val.(int64), nil
}

// getCachedSubscriptionsIfValid retrieves cached subscriptions if they exist and are not expired.
// Returns (subscriptions, found) where found indicates if valid cached data was available.
// If expired subscriptions are found, they are automatically removed from cache.
func (c *cacheDialect) getCachedSubscriptionsIfValid(subject string) ([]Subscription, bool) {
	subs, found := c.cache.Get(subject)
	if !found {
		return nil, false
	}

	if !hasExpiredSubscriptions(subs) {
		return subs, true
	}

	// If expired subscriptions found, delete from cache
	c.cache.Del(subject)
	return nil, false
}

// bySubjectKey is the cache key of subject's subscriptions read under the
// metadata version: a lookup caches what it read under the version it began
// with, so a list read before a change is never served after it, even if it
// is stored after the change cleared the cache.
func bySubjectKey(version int64, subject string) string {
	return fmt.Sprintf("v%d:BySubject:%s", version, subject)
}

// BySubject retrieves subscriptions for a subject, using cache when possible
func (c *cacheDialect) BySubject(ctx context.Context, subject string) ([]Subscription, error) {
	if txOf(ctx) != nil {
		return c.bySubjectInTx(ctx, subject)
	}

	seen := atomic.LoadInt64(&c.version)
	subs, found := c.getCachedSubscriptionsIfValid(bySubjectKey(seen, subject))
	if found || seen == 0 {
		version, err := c.checkVersion(ctx)
		if err != nil {
			return nil, errors.Wrap(err, "failed to check version")
		}
		// compare with the version the cache was read under: another
		// caller's check may have moved c.version on in between
		if found && version == seen {
			return subs, nil
		}
	}

	// no second look at the cache here: freshFlights runs this only for
	// callers that arrived before it started, and what another call cached
	// may have been read before a Subscribe that committed before them
	val, err := c.flights.do(withoutTx(ctx), "BySubject:"+subject, func(ctx context.Context) (any, error) {
		version := atomic.LoadInt64(&c.version)
		subs, err := c.Dialect.BySubject(ctx, subject)
		if err != nil {
			return nil, err
		}
		c.cache.Set(bySubjectKey(version, subject), subs)
		return subs, nil
	})
	if err != nil {
		return nil, err
	}
	return val.([]Subscription), nil
}

// bySubjectInTx looks subject's subscriptions up for a caller in a
// transaction, on that transaction: a shared call on another connection
// could wait forever for one when such callers use up the pool, and would
// not see the transaction's own changes. The cache serves it while the
// version the transaction reads is the cached one; what it reads itself is
// not cached, as it may include the transaction's uncommitted changes.
func (c *cacheDialect) bySubjectInTx(ctx context.Context, subject string) ([]Subscription, error) {
	meta, err := c.GetMetadata(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "failed to get metadata")
	}
	if meta.Version == atomic.LoadInt64(&c.version) {
		if subs, found := c.getCachedSubscriptionsIfValid(bySubjectKey(meta.Version, subject)); found {
			return subs, nil
		}
	}
	return c.Dialect.BySubject(ctx, subject)
}

// freshFlights coalesces concurrent calls by key, like singleflight, except
// that a caller never gets the result of a call that started before it
// arrived: one that read the database earlier may miss a subscription
// committed before the caller began. A caller that finds a call running
// waits for the next one, which starts when the running one ends and which
// every caller arriving meanwhile shares.
//
// A call runs with the values of the ctx of the caller that started it, but
// does not end with that caller: it ends when it returns, when every caller
// waiting for it has given up, or at the latest deadline among them. So one
// caller's deadline does not fail the others, and a call that hangs (a dead
// connection) neither holds later callers once its own have left nor runs
// past every deadline because one caller has none.
type freshFlights struct {
	mu      sync.Mutex
	running map[string]*freshCall
	next    map[string]*freshCall
}

type freshCall struct {
	fn       func(context.Context) (any, error)
	ctx      context.Context
	cancel   context.CancelFunc
	waiters  int         // guarded by freshFlights.mu
	deadline time.Time   // guarded by freshFlights.mu
	timer    *time.Timer // guarded by freshFlights.mu
	done     chan struct{}
	val      any
	err      error
}

// wait counts one more caller waiting for the call and moves the call's
// deadline to the caller's if that is later. A caller without a deadline
// does not move it: the call then ends at the latest deadline of the others,
// so it never runs longer than it would have on any one caller's ctx. Called
// with freshFlights.mu held.
func (c *freshCall) wait(ctx context.Context) {
	c.waiters++
	d, ok := ctx.Deadline()
	if !ok || !d.After(c.deadline) {
		return
	}
	c.deadline = d
	if c.timer == nil {
		c.timer = time.AfterFunc(time.Until(d), c.cancel)
	} else {
		c.timer.Reset(time.Until(d))
	}
}

func newFreshCall(ctx context.Context, fn func(context.Context) (any, error)) *freshCall {
	cctx, cancel := context.WithCancel(context.WithoutCancel(ctx))
	return &freshCall{fn: fn, ctx: cctx, cancel: cancel, done: make(chan struct{})}
}

func (f *freshFlights) do(ctx context.Context, key string, fn func(context.Context) (any, error)) (any, error) {
	f.mu.Lock()
	if f.running == nil {
		f.running = map[string]*freshCall{}
		f.next = map[string]*freshCall{}
	}
	call := f.next[key]
	if call != nil && call.ctx.Err() != nil {
		// its deadline passed before it started: it fails at once, so its
		// callers get that error, and a caller arriving now gets a new call
		delete(f.next, key)
		call.err = call.ctx.Err()
		close(call.done)
		call = nil
	}
	switch {
	case f.running[key] == nil:
		call = newFreshCall(ctx, fn)
		f.running[key] = call
		go f.run(key, call)
	case call == nil:
		call = newFreshCall(ctx, fn)
		f.next[key] = call
	}
	call.wait(ctx)
	f.mu.Unlock()

	select {
	case <-ctx.Done():
		f.mu.Lock()
		call.waiters--
		if call.waiters == 0 {
			// nobody waits for it any more: end it, and if it has not
			// started, drop it so that a caller arriving now starts afresh
			// rather than share a cancelled call
			if f.next[key] == call {
				delete(f.next, key)
			}
			call.cancel()
		}
		f.mu.Unlock()
		return nil, ctx.Err()
	case <-call.done:
		return call.val, call.err
	}
}

func (f *freshFlights) run(key string, call *freshCall) {
	call.val, call.err = call.fn(call.ctx)
	call.cancel()
	close(call.done)

	f.mu.Lock()
	defer f.mu.Unlock()
	if call.timer != nil {
		call.timer.Stop()
	}
	next := f.next[key]
	delete(f.next, key)
	if next == nil {
		delete(f.running, key)
		return
	}
	f.running[key] = next
	go f.run(key, next)
}
