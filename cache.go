package bus

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/dgraph-io/ristretto/v2"
	"github.com/pkg/errors"
	"github.com/rs/xid"
	"golang.org/x/sync/singleflight"
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
	sfg     singleflight.Group
	version int64 // Database version, accessed atomically
}

func (c *cacheDialect) checkVersion(ctx context.Context) (bool, error) {
	ch := c.sfg.DoChan("checkVersion", func() (any, error) {
		meta, err := c.Dialect.GetMetadata(ctx)
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

	select {
	case <-ctx.Done():
		return false, ctx.Err()
	case res := <-ch:
		return res.Val.(bool), res.Err
	}
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

	ch := c.sfg.DoChan(cacheKey, func() (any, error) {
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

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case res := <-ch:
		if res.Err != nil {
			return nil, res.Err
		}
		return res.Val.([]Subscription), nil
	}
}
