package bus

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/dgraph-io/ristretto/v2"
	"github.com/pkg/errors"
	"github.com/rs/xid"
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

func (c *cacheDialect) checkVersion(ctx context.Context) (bool, error) {
	val, err := c.flights.do(ctx, "checkVersion", func() (any, error) {
		meta, err := c.GetMetadata(ctx)
		if err != nil {
			return false, errors.Wrap(err, "failed to get metadata")
		}
		if meta.Version != atomic.LoadInt64(&c.version) {
			atomic.StoreInt64(&c.version, meta.Version)
			c.cache.Clear()
			return true, nil // Cache was cleared
		}

		return false, nil // Cache was not cleared
	})
	if err != nil {
		return false, err
	}
	return val.(bool), nil
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

// BySubject retrieves subscriptions for a subject, using cache when possible
func (c *cacheDialect) BySubject(ctx context.Context, subject string) ([]Subscription, error) {
	cacheKey := "BySubject:" + subject

	subs, found := c.getCachedSubscriptionsIfValid(cacheKey)
	if found || atomic.LoadInt64(&c.version) == 0 {
		cleared, err := c.checkVersion(ctx)
		if err != nil {
			return nil, errors.Wrap(err, "failed to check version")
		}
		if found && !cleared {
			return subs, nil
		}
	}

	val, err := c.flights.do(ctx, cacheKey, func() (any, error) {
		// Double-check cache after acquiring lock (another goroutine might have updated it)
		if subs, found := c.getCachedSubscriptionsIfValid(cacheKey); found {
			return subs, nil
		}

		currentVersion := atomic.LoadInt64(&c.version)

		subs, err := c.Dialect.BySubject(ctx, subject)
		if err != nil {
			return nil, err
		}

		// Only cache if version hasn't changed during query
		if currentVersion == atomic.LoadInt64(&c.version) {
			c.cache.Set(cacheKey, subs)
		}
		// else {
		// Version changed, data might be stale, don't cache
		// But still return the data we fetched to avoid another query
		// }

		return subs, nil
	})
	if err != nil {
		return nil, err
	}
	return val.([]Subscription), nil
}

// freshFlights coalesces concurrent calls by key, like singleflight, except
// that a caller never gets the result of a call that started before it
// arrived: one that read the database earlier may miss a subscription
// committed before the caller began. A caller that finds a call running
// waits for the next one, which starts when the running one ends and which
// every caller arriving meanwhile shares.
type freshFlights struct {
	mu      sync.Mutex
	running map[string]*freshCall
	next    map[string]*freshCall
}

type freshCall struct {
	fn   func() (any, error)
	done chan struct{}
	val  any
	err  error
}

func (f *freshFlights) do(ctx context.Context, key string, fn func() (any, error)) (any, error) {
	f.mu.Lock()
	if f.running == nil {
		f.running = map[string]*freshCall{}
		f.next = map[string]*freshCall{}
	}
	call := f.next[key]
	switch {
	case f.running[key] == nil:
		call = &freshCall{fn: fn, done: make(chan struct{})}
		f.running[key] = call
		go f.run(key, call)
	case call == nil:
		call = &freshCall{fn: fn, done: make(chan struct{})}
		f.next[key] = call
	}
	f.mu.Unlock()

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-call.done:
		return call.val, call.err
	}
}

func (f *freshFlights) run(key string, call *freshCall) {
	call.val, call.err = call.fn()
	close(call.done)

	f.mu.Lock()
	defer f.mu.Unlock()
	next := f.next[key]
	delete(f.next, key)
	if next == nil {
		delete(f.running, key)
		return
	}
	f.running[key] = next
	go f.run(key, next)
}
