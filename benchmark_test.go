package bus_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/qor5/go-bus"
	"github.com/qor5/go-bus/pgbus"
	"github.com/stretchr/testify/require"
)

func setupBenchmarkTestData(b *testing.B, ctx context.Context, busSvc bus.Bus) {
	// Create a series of test subscriptions
	for i := 0; i < 10; i++ {
		queueName := fmt.Sprintf("benchmark-queue-%d", i)
		queue := busSvc.Queue(queueName)

		// Create multiple subscription patterns with distinct matching behaviors
		patterns := []string{
			fmt.Sprintf("benchmark.subject.%d.event.>", i),        // Multi-level wildcard for events
			fmt.Sprintf("benchmark.subject.%d.notification.*", i), // Single level wildcard for notifications
			fmt.Sprintf("benchmark.*.%d.status", i),               // Wildcard in middle token
		}

		for _, pattern := range patterns {
			_, err := queue.Subscribe(ctx, pattern)
			if err != nil {
				b.Fatalf("failed to create subscription: %v", err)
			}
		}
	}
}

// Common benchmark subjects - simulating normal topic distribution
var benchmarkSubjects = []string{
	"benchmark.subject.1.event.created",
	"benchmark.subject.2.notification.received",
	"benchmark.subject.3.event.updated",
	"benchmark.subject.4.notification.sent",
	"benchmark.subject.5.event.deleted",
	"benchmark.inbox.5.status",
	"benchmark.subject.7.event.processed",
	"benchmark.outbox.8.status",
	"benchmark.subject.9.notification.read",
	"benchmark.archive.5.status",
}

// Prepare benchmark environment
func prepareBenchmark(b *testing.B) (context.Context, bus.Bus, bus.Bus) {
	ctx := context.Background()

	// Clean up test data
	cleanupAllTables()

	// Bus without cache
	noCacheBus, err := pgbus.New(db, bus.WithMigrate(false), bus.WithoutCache())
	if err != nil {
		b.Fatalf("failed to create bus without cache: %v", err)
	}

	// Bus with cache - migration already done, no need to repeat
	ristrettoCache, err := bus.NewRistrettoCache(nil)
	require.NoError(b, err, "Failed to create cache instance")

	cacheBus, err := pgbus.New(db,
		bus.WithMigrate(false),
		bus.WithCache(bus.WrapRistrettoCache(ristrettoCache)),
	)
	if err != nil {
		b.Fatalf("failed to create bus with cache: %v", err)
	}

	// Set up test data
	setupBenchmarkTestData(b, ctx, noCacheBus)

	// Clean up data after test
	b.Cleanup(func() {
		cleanupAllTables()
		_ = noCacheBus.Close()
		_ = cacheBus.Close()
		ristrettoCache.Close()
	})

	return ctx, noCacheBus, cacheBus
}

// BenchmarkWithCacheRepeatedSubject tests cache performance when repeatedly querying the same subject
func BenchmarkWithCacheRepeatedSubject(b *testing.B) {
	ctx, _, cacheBus := prepareBenchmark(b)

	// Select a fixed subject for repeated queries
	subject := "benchmark.subject.5.event.deleted"

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := cacheBus.BySubject(ctx, subject)
		if err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkRealWorldScenario simulates real-world query patterns
func BenchmarkRealWorldScenario(b *testing.B) {
	ctx, noCacheBus, cacheBus := prepareBenchmark(b)

	// Run comparative tests with and without cache
	b.Run("WithoutCache", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			var subject string
			if i%5 == 0 {
				// 20% of queries use different subjects
				subject = benchmarkSubjects[i%len(benchmarkSubjects)]
			} else {
				// 80% of queries focus on hot subjects
				subject = "benchmark.subject.5.event.deleted"
			}

			_, err := noCacheBus.BySubject(ctx, subject)
			if err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("WithCache", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			var subject string
			if i%5 == 0 {
				// 20% of queries use different subjects
				subject = benchmarkSubjects[i%len(benchmarkSubjects)]
			} else {
				// 80% of queries focus on hot subjects
				subject = "benchmark.subject.5.event.deleted"
			}

			_, err := cacheBus.BySubject(ctx, subject)
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}

// BenchmarkSubscriptionChange tests the impact of subscription changes on cache performance
func BenchmarkSubscriptionChange(b *testing.B) {
	ctx, noCacheBus, cacheBus := prepareBenchmark(b)

	// Select a fixed subject for testing
	subject := "benchmark.subject.5.event.deleted"

	b.Run("WithoutCache", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			// Modify subscription every 100 operations
			if i > 0 && i%100 == 0 {
				// Add a new subscription to trigger version change
				queue := noCacheBus.Queue(fmt.Sprintf("benchmark-queue-temp-%d", i))
				// Use pattern that would match the test subject
				_, err := queue.Subscribe(ctx, "benchmark.subject.5.event.>")
				if err != nil {
					b.Fatal(err)
				}
			}

			_, err := noCacheBus.BySubject(ctx, subject)
			if err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("WithCache", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			// Modify subscription every 100 operations
			if i > 0 && i%100 == 0 {
				// Add a new subscription to trigger version change
				queue := noCacheBus.Queue(fmt.Sprintf("benchmark-queue-temp-%d", i))
				// Use pattern that would match the test subject
				_, err := queue.Subscribe(ctx, "benchmark.subject.5.event.>")
				if err != nil {
					b.Fatal(err)
				}
			}

			_, err := cacheBus.BySubject(ctx, subject)
			if err != nil {
				b.Fatal(err)
			}
		}
	})

	// Clean up additionally created temporary subscriptions
}

// setupSubscriptionsWithScale creates a specific number of test subscriptions
func setupSubscriptionsWithScale(b *testing.B, ctx context.Context, busSvc bus.Bus, scale int) {
	// Create a series of test subscriptions based on scale
	for i := 0; i < scale; i++ {
		queueName := fmt.Sprintf("benchmark-queue-scale-%d", i)
		queue := busSvc.Queue(queueName)

		// Create subscription patterns with distinct matching behaviors
		patterns := []string{
			fmt.Sprintf("benchmark.scale.%d.event.>", i),        // Multi-level wildcard
			fmt.Sprintf("benchmark.scale.%d.notification.*", i), // Single level wildcard
		}

		for _, pattern := range patterns {
			_, err := queue.Subscribe(ctx, pattern)
			if err != nil {
				b.Fatalf("failed to create subscription: %v", err)
			}
		}
	}
}

// BenchmarkDialectBySubject tests the performance of the underlying Dialect.BySubject method
func BenchmarkDialectBySubject(b *testing.B) {
	// Create dialect directly
	dialect, err := pgbus.NewDialect(db)
	if err != nil {
		b.Fatalf("failed to create dialect: %v", err)
	}

	// Define test scales (number of subscriptions to create)
	scales := []struct {
		name  string
		count int
	}{
		{"Small_10", 5},     // 10 subscriptions (5 queues × 2 patterns)
		{"Medium_100", 50},  // 100 subscriptions (50 queues × 2 patterns)
		{"Large_1000", 500}, // 1000 subscriptions (500 queues × 2 patterns)
	}

	// Define subjects to test
	testSubjects := []struct {
		name    string
		subject string
		scale   int // Which scale this subject is targeting (defaults to first if 0)
	}{
		{"ExactMatch", "benchmark.scale.1.event.created", 0},
		{"WildcardMatch", "benchmark.scale.5.notification.sent", 0},
		{"NoMatch", "benchmark.no.match.subject", 0},
		{"DeepMatch", "benchmark.scale.42.event.deeply.nested.path", 0},
	}

	// Test both cached and non-cached scenarios
	cacheScenarios := []struct {
		name     string
		useCache bool
	}{
		{"WithoutCache", false},
		{"WithCache", true},
	}

	for _, cacheScenario := range cacheScenarios {
		b.Run(cacheScenario.name, func(b *testing.B) {
			// Create appropriate bus based on cache scenario
			var testBus bus.Bus

			if cacheScenario.useCache {
				ristrettoCache, err := bus.NewRistrettoCache(nil)
				if err != nil {
					b.Fatalf("failed to create cache: %v", err)
				}
				defer ristrettoCache.Close()

				testBus, err = pgbus.New(db,
					bus.WithMigrate(false),
					bus.WithCache(bus.WrapRistrettoCache(ristrettoCache)),
				)
				if err != nil {
					b.Fatalf("failed to create cached bus: %v", err)
				}
				defer testBus.Close()
			} else {
				var err error
				testBus, err = pgbus.New(db, bus.WithMigrate(false), bus.WithoutCache())
				if err != nil {
					b.Fatalf("failed to create bus: %v", err)
				}
				defer testBus.Close()
			}

			for _, scale := range scales {
				b.Run(scale.name, func(b *testing.B) {
					// Clean up prior subscriptions
					cleanupAllTables()

					ctx := context.Background()

					// Setup subscriptions for this scale
					setupSubscriptionsWithScale(b, ctx, testBus, scale.count)

					for _, subject := range testSubjects {
						// If subject has specific scale requirement, skip if not matching
						if subject.scale > 0 && subject.scale != scale.count {
							continue
						}

						b.Run(subject.name, func(b *testing.B) {
							b.ResetTimer()
							for i := 0; i < b.N; i++ {
								if cacheScenario.useCache {
									_, err := testBus.BySubject(ctx, subject.subject)
									if err != nil {
										b.Fatal(err)
									}
								} else {
									_, err := dialect.BySubject(ctx, subject.subject)
									if err != nil {
										b.Fatal(err)
									}
								}
							}
						})
					}
				})
			}
		})
	}
}
