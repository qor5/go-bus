package bus_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/qor5/go-bus"
	"github.com/qor5/go-bus/pgbus"
	"github.com/stretchr/testify/require"
)

func setupTestData(b *testing.B, ctx context.Context, busSvc bus.Bus) {
	// Create a series of test subscriptions
	for i := 0; i < 10; i++ {
		queueName := fmt.Sprintf("benchmark-queue-%d", i)
		queue := busSvc.Queue(queueName)

		// Create multiple subscription patterns with distinct matching behaviors
		patterns := []string{
			fmt.Sprintf("benchmark.subject.%d.event.>", i),        // Multi-level wildcard for events
			fmt.Sprintf("benchmark.subject.%d.notification.*", i), // Single level wildcard for notifications
			fmt.Sprintf("benchmark.*.%d.status", i),               // Wildcard in middle segment
		}

		for _, pattern := range patterns {
			_, err := queue.Subscribe(ctx, pattern)
			if err != nil {
				b.Fatalf("failed to create subscription: %v", err)
			}
		}
	}
}

func cleanupTestData(b *testing.B, ctx context.Context) {
	// Clean up test data
	_, err := db.ExecContext(ctx, "DELETE FROM gobus_subscriptions WHERE queue LIKE 'benchmark-queue-%'")
	if err != nil {
		b.Fatalf("failed to clean up subscription data: %v", err)
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

	// Bus without cache - ensure migration is executed
	noCacheBus, err := pgbus.New(db, bus.WithMigrate(true))
	if err != nil {
		b.Fatalf("failed to create bus without cache: %v", err)
	}

	// Clean up test data
	cleanupTestData(b, ctx)

	// Bus with cache - migration already done, no need to repeat
	cache, err := bus.NewRistrettoCache(nil)
	require.NoError(b, err, "Failed to create cache instance")

	cacheBus, err := pgbus.New(db,
		bus.WithMigrate(false),
		bus.WithDialectDecorator(bus.RistrettoDecorator(cache)),
	)
	if err != nil {
		b.Fatalf("failed to create bus with cache: %v", err)
	}

	// Set up test data
	setupTestData(b, ctx, noCacheBus)

	// Clean up data after test
	b.Cleanup(func() {
		cleanupTestData(b, ctx)
		cache.Close()
	})

	return ctx, noCacheBus, cacheBus
}

// BenchmarkWithoutCache tests performance without cache
func BenchmarkWithoutCache(b *testing.B) {
	ctx, noCacheBus, _ := prepareBenchmark(b)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Use different subjects to simulate real queries
		subject := benchmarkSubjects[i%len(benchmarkSubjects)]
		_, err := noCacheBus.BySubject(ctx, subject)
		if err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkWithCache tests performance with cache
func BenchmarkWithCache(b *testing.B) {
	ctx, _, cacheBus := prepareBenchmark(b)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Use different subjects to simulate real queries
		subject := benchmarkSubjects[i%len(benchmarkSubjects)]
		_, err := cacheBus.BySubject(ctx, subject)
		if err != nil {
			b.Fatal(err)
		}
	}
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
	_, err := db.ExecContext(ctx, "DELETE FROM gobus_subscriptions WHERE queue LIKE 'benchmark-queue-temp-%'")
	if err != nil {
		b.Fatalf("failed to clean up temporary subscription data: %v", err)
	}
}
