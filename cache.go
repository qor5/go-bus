package bus

import (
	"context"
	"sync/atomic"

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

func (c *ristrettoCache) Clear() {
	c.cache.Clear()
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
	Clear()
}

// TODO: 这个和 ttl 机制貌似相冲突，怎么应对呢？貌似 Subscription 还是可以反馈 ExpiresAt ，然后如果 cache 里的过期了就需要主动去查，否则不用重查
// CacheDecorator creates a decorator that adds caching functionality to the dialect.
// This significantly improves performance for subscription lookups.
// The caller is responsible for creating and closing the cache instance.
func CacheDecorator(cache Cache) DialectDecorator {
	if cache == nil {
		panic("cache instance cannot be nil")
	}
	return func(d Dialect) (Dialect, error) {
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

func (c *cacheDialect) checkVersion(ctx context.Context) error {
	ch := c.sfg.DoChan("version_check", func() (any, error) {
		meta, err := c.Dialect.GetMetadata(ctx)
		if err != nil {
			return nil, errors.Wrap(err, "failed to get metadata")
		}
		if meta.Version != atomic.LoadInt64(&c.version) {
			atomic.StoreInt64(&c.version, meta.Version)
			c.cache.Clear()
		}
		return nil, nil
	})

	select {
	case <-ctx.Done():
		return ctx.Err()
	case res := <-ch:
		return res.Err
	}
}

// BySubject retrieves subscriptions for a subject, using cache when possible
func (c *cacheDialect) BySubject(ctx context.Context, subject string) ([]Subscription, error) {
	if err := c.checkVersion(ctx); err != nil {
		return nil, errors.Wrap(err, "failed to check version")
	}

	if subs, found := c.cache.Get(subject); found {
		return subs, nil
	}

	ch := c.sfg.DoChan("subject:"+subject, func() (any, error) {
		if subs, found := c.cache.Get(subject); found {
			return subs, nil
		}

		// Capture current version before database operation
		currentVersion := atomic.LoadInt64(&c.version)

		subs, err := c.Dialect.BySubject(ctx, subject)
		if err != nil {
			return nil, err
		}

		// Check if version changed during query
		versionStillValid := currentVersion == atomic.LoadInt64(&c.version)
		if versionStillValid {
			c.cache.Set(subject, subs)
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
