package bus_test

import (
	"context"
	"testing"
	"time"

	"github.com/qor5/go-bus"
	"github.com/qor5/go-bus/pgbus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestCacheDecorator verifies the behavior and performance of the cache decorator
func TestCacheDecorator(t *testing.T) {
	cleanupAllTables()

	// Create a bus without cache first - used for comparison
	standardBus, err := pgbus.New(db, bus.WithMigrate(true), bus.WithoutCache())
	require.NoError(t, err, "Failed to create standard Bus instance")
	defer standardBus.Close()

	// Create a bus with custom cache
	ristrettoCache, err := bus.NewRistrettoCache(nil)
	require.NoError(t, err, "Failed to create cache instance")
	defer ristrettoCache.Close()

	cachedBus, err := pgbus.New(db,
		bus.WithMigrate(false), // Migration done already
		bus.WithCache(bus.WrapRistrettoCache(ristrettoCache)),
	)
	require.NoError(t, err, "Failed to create cached Bus instance")
	defer cachedBus.Close()

	ctx := context.Background()

	// Create test subscriptions
	queue1 := standardBus.Queue("cache-test-queue-1")
	queue2 := standardBus.Queue("cache-test-queue-2")

	// Add various subscription patterns
	patterns := []string{
		"test.cache.simple",
		"test.cache.*",
		"test.*.wildcard",
		"test.>",
	}

	for _, pattern := range patterns {
		_, err := queue1.Subscribe(ctx, pattern)
		require.NoError(t, err, "Failed to subscribe to pattern %s", pattern)
	}

	_, err = queue2.Subscribe(ctx, "other.topic")
	require.NoError(t, err, "Failed to subscribe to other topic")

	// Test 1: Check basic cache functionality
	t.Run("BasicCacheFunctionality", func(t *testing.T) {
		subject := "test.cache.example"

		// First call to cache-enabled bus (should miss cache)
		startTime := time.Now()
		subsCached1, err := cachedBus.BySubject(ctx, subject)
		firstCallDuration := time.Since(startTime)
		require.NoError(t, err, "Failed to get subscriptions with cached bus")

		// Get expected count by querying non-cached bus
		subsStandard, err := standardBus.BySubject(ctx, subject)
		require.NoError(t, err, "Failed to get subscriptions with standard bus")
		expectedCount := len(subsStandard)

		// Verify the matching subscription count
		assert.Equal(t, expectedCount, len(subsCached1),
			"Cached results should match standard results count")

		// Second call to cache-enabled bus (should hit cache)
		startTime = time.Now()
		subsCached2, err := cachedBus.BySubject(ctx, subject)
		secondCallDuration := time.Since(startTime)
		require.NoError(t, err, "Failed to get subscriptions with cached bus on second call")

		// Result should be same
		assert.Equal(t, len(subsCached1), len(subsCached2), "Cache results should match")

		// Results should match with standard bus
		assert.Equal(t, len(subsStandard), len(subsCached1), "Cached and non-cached results should match")

		// Second call should typically be faster due to cache, but with very fast
		// indexed queries, the cache overhead might outweigh benefits for simple operations
		t.Logf("First call: %v, Second call: %v", firstCallDuration, secondCallDuration)
	})

	// Test 2: Cache invalidation on subscription change
	t.Run("CacheInvalidation", func(t *testing.T) {
		subject := "test.cache.invalidation"

		// Get initial subscription count with standardBus to establish baseline
		subsStandardBefore, err := standardBus.BySubject(ctx, subject)
		require.NoError(t, err, "Failed to get standard subscriptions before change")

		// First call - will be cached
		subsBefore, err := cachedBus.BySubject(ctx, subject)
		require.NoError(t, err, "Failed to get subscriptions before invalidation")

		// Verify matching counts before change
		assert.Equal(t, len(subsStandardBefore), len(subsBefore),
			"Initial cached and standard results should match")

		// Modify subscriptions - add a new pattern that will match our test subject
		newPattern := "test.cache.invalidation"
		_, err = queue1.Subscribe(ctx, newPattern)
		require.NoError(t, err, "Failed to add new subscription")

		// Get subs with standard bus to verify the change and set expectations
		subsStandardAfter, err := standardBus.BySubject(ctx, subject)
		require.NoError(t, err, "Failed to get standard subscriptions after change")

		require.NotEqual(t, len(subsStandardAfter), len(subsStandardBefore), "Standard subscriptions should have changed")

		// Get subs with cached bus - should invalidate cache and see the new count
		subsAfter, err := cachedBus.BySubject(ctx, subject)
		require.NoError(t, err, "Failed to get cached subscriptions after change")

		// Results should match with the standard bus
		assert.Equal(t, len(subsStandardAfter), len(subsAfter),
			"Cache should be invalidated and match standard results")

		// The new count should not equal old count (we added a subscription)
		assert.NotEqual(t, len(subsBefore), len(subsAfter),
			"Results should be different after adding a subscription")
	})

	// Test 3: Cache isolation
	t.Run("CacheIsolation", func(t *testing.T) {
		// Different subjects should not affect each other in cache
		subject1 := "test.cache.isolation1"
		subject2 := "test.cache.isolation2"

		// Prime the cache for subject1
		_, err := cachedBus.BySubject(ctx, subject1)
		require.NoError(t, err, "Failed to prime cache for subject1")

		// Modify subscriptions in a way that affects subject2 but not subject1
		// Using an exact match pattern to ensure it matches subject2
		_, err = queue1.Subscribe(ctx, subject2)
		require.NoError(t, err, "Failed to add subscription for isolation test")

		// Get results for both subjects
		subsStandard1, err := standardBus.BySubject(ctx, subject1)
		require.NoError(t, err, "Failed to get standard results for subject1")

		subsStandard2, err := standardBus.BySubject(ctx, subject2)
		require.NoError(t, err, "Failed to get standard results for subject2")

		subsCached1, err := cachedBus.BySubject(ctx, subject1)
		require.NoError(t, err, "Failed to get cached results for subject1")

		subsCached2, err := cachedBus.BySubject(ctx, subject2)
		require.NoError(t, err, "Failed to get cached results for subject2")

		// Both cached results should match standard results
		assert.Equal(t, len(subsStandard1), len(subsCached1),
			"Cached results for subject1 should match standard")
		assert.Equal(t, len(subsStandard2), len(subsCached2),
			"Cached results for subject2 should match standard")

		// Also verify that subject2 results changed (now has at least one match)
		assert.Greater(t, len(subsCached2), 0,
			"Subject2 should now have at least one matching subscription")
	})

	// Cleanup
	_, err = db.Exec("DELETE FROM gobus_subscriptions WHERE queue LIKE 'cache-test-queue-%'")
	require.NoError(t, err, "Failed to clean up test data")
}

// TestCacheWithTTLIntegration tests that cache properly handles TTL subscriptions
func TestCacheWithTTLIntegration(t *testing.T) {
	ctx := context.Background()
	cleanupAllTables()

	// Create cache-enabled bus
	ristrettoCache, err := bus.NewRistrettoCache(nil)
	require.NoError(t, err)
	defer ristrettoCache.Close()
	cache := bus.WrapRistrettoCache(ristrettoCache)

	t.Run("Cache Returns Valid TTL Subscriptions", func(t *testing.T) {
		cachedBus, err := pgbus.New(db,
			bus.WithMigrate(false), // Migration done already
			bus.WithCache(cache),
		)
		require.NoError(t, err)
		defer cachedBus.Close()

		queue := cachedBus.Queue("test-cache-ttl-queue")

		// Create subscription with 300ms TTL
		_, err = queue.Subscribe(ctx, "test.cache-ttl",
			bus.WithTTL(300*time.Millisecond),
		)
		require.NoError(t, err)

		// First call - should populate cache
		subs1, err := cachedBus.BySubject(ctx, "test.cache-ttl")
		require.NoError(t, err)
		assert.Len(t, subs1, 1, "Should find subscription")

		// Verify the subscription has proper expiration
		expiresAt := subs1[0].ExpiresAt()
		assert.False(t, expiresAt.IsZero(), "TTL subscription should have expiration time")
		assert.True(t, time.Now().Before(expiresAt), "Subscription should not be expired yet")

		// Second call - should use cache (subscription still valid)
		subs2, err := cachedBus.BySubject(ctx, "test.cache-ttl")
		require.NoError(t, err)
		assert.Len(t, subs2, 1, "Should still find subscription from cache")
	})

	t.Run("Cache Refreshes When TTL Subscriptions Expire", func(t *testing.T) {
		cleanupAllTables()
		cachedBus, err := pgbus.New(db,
			bus.WithMigrate(false),
			bus.WithCache(cache),
		)
		require.NoError(t, err)
		defer cachedBus.Close()

		queue := cachedBus.Queue("test-cache-ttl-refresh-queue")

		// Create subscription with very short TTL (150ms)
		_, err = queue.Subscribe(ctx, "test.cache-refresh",
			bus.WithTTL(150*time.Millisecond),
		)
		require.NoError(t, err)

		// First call - should populate cache and find subscription
		subs1, err := cachedBus.BySubject(ctx, "test.cache-refresh")
		require.NoError(t, err)
		assert.Len(t, subs1, 1, "Should find subscription initially")

		// Wait for subscription to expire
		time.Sleep(200 * time.Millisecond)

		// Second call - cache should detect expiration and refresh from database
		// The database query should filter out expired subscriptions
		subs2, err := cachedBus.BySubject(ctx, "test.cache-refresh")
		require.NoError(t, err)
		assert.Len(t, subs2, 0, "Should not find expired subscription")
	})

	t.Run("Cache Handles Mixed TTL and Non-TTL Subscriptions", func(t *testing.T) {
		cleanupAllTables()
		cachedBus, err := pgbus.New(db,
			bus.WithMigrate(false),
			bus.WithCache(cache),
		)
		require.NoError(t, err)
		defer cachedBus.Close()

		queue := cachedBus.Queue("test-cache-mixed-queue")

		// Create permanent subscription (no TTL)
		_, err = queue.Subscribe(ctx, "test.cache-permanent")
		require.NoError(t, err)

		// Create TTL subscription that will expire soon (150ms)
		_, err = queue.Subscribe(ctx, "test.cache-expiring",
			bus.WithTTL(150*time.Millisecond),
		)
		require.NoError(t, err)

		// Both should match this subject
		subject := "test.cache-permanent"

		// First call - should find permanent subscription
		subs1, err := cachedBus.BySubject(ctx, subject)
		require.NoError(t, err)
		assert.Len(t, subs1, 1, "Should find permanent subscription")
		assert.Equal(t, "test.cache-permanent", subs1[0].Pattern())
		assert.True(t, subs1[0].ExpiresAt().IsZero(), "Permanent subscription should not have expiration time")

		// Also check the expiring subscription matches its own subject
		subs2, err := cachedBus.BySubject(ctx, "test.cache-expiring")
		require.NoError(t, err)
		assert.Len(t, subs2, 1, "Should find expiring subscription")
		assert.False(t, subs2[0].ExpiresAt().IsZero(), "Expiring subscription should have expiration time")

		// Wait for TTL subscription to expire
		time.Sleep(200 * time.Millisecond)

		// Check permanent subscription is still there
		subs3, err := cachedBus.BySubject(ctx, subject)
		require.NoError(t, err)
		assert.Len(t, subs3, 1, "Should still find permanent subscription")

		// Check expiring subscription is now gone
		subs4, err := cachedBus.BySubject(ctx, "test.cache-expiring")
		require.NoError(t, err)
		assert.Len(t, subs4, 0, "Should not find expired subscription")
	})

	t.Run("Cache Expiration Check Efficiency", func(t *testing.T) {
		cleanupAllTables()
		cachedBus, err := pgbus.New(db,
			bus.WithMigrate(false),
			bus.WithCache(cache),
		)
		require.NoError(t, err)
		defer cachedBus.Close()

		queue := cachedBus.Queue("test-cache-efficiency-queue")

		// Create subscription with long TTL (won't expire during test)
		_, err = queue.Subscribe(ctx, "test.cache-long-ttl",
			bus.WithTTL(2*time.Second),
		)
		require.NoError(t, err)

		subject := "test.cache-long-ttl"

		// First call - populates cache
		startTime := time.Now()
		subs1, err := cachedBus.BySubject(ctx, subject)
		firstCallDuration := time.Since(startTime)
		require.NoError(t, err)
		assert.Len(t, subs1, 1, "Should find subscription")

		// Subsequent calls should use cache (with expiration check)
		startTime = time.Now()
		subs2, err := cachedBus.BySubject(ctx, subject)
		secondCallDuration := time.Since(startTime)
		require.NoError(t, err)
		assert.Len(t, subs2, 1, "Should find cached subscription")

		// The second call should include expiration check but still be faster than database query
		// (though this is not strictly guaranteed, it's a performance characteristic)
		t.Logf("First call (DB): %v, Second call (cache with TTL check): %v",
			firstCallDuration, secondCallDuration)

		// Verify results are equivalent
		assert.Equal(t, subs1[0].ID(), subs2[0].ID(), "Cached result should match original")
		assert.Equal(t, subs1[0].Pattern(), subs2[0].Pattern(), "Cached pattern should match")
	})
}
