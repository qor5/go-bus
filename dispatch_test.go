package bus_test

import (
	"context"
	"testing"

	"github.com/qor5/go-bus"
	"github.com/qor5/go-bus/pgbus"
	"github.com/qor5/go-que"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestDispatchMethods tests all methods of the Dispatch struct.
func TestDispatchMethods(t *testing.T) {
	t.Run("DispatchMethods_AllExecuted", func(t *testing.T) {
		cleanupAllTables()

		b, err := pgbus.New(db)
		require.NoError(t, err, "Failed to create Bus instance")

		ctx := context.Background()

		// Setup queues and subscriptions
		queue1 := b.Queue(testQueue)
		queue2 := b.Queue(testQueue2)

		_, err = queue1.Subscribe(ctx, "orders.new")
		require.NoError(t, err, "Failed to subscribe")

		_, err = queue2.Subscribe(ctx, "notifications.>")
		require.NoError(t, err, "Failed to subscribe")

		// Dispatch messages to create a proper dispatch result
		ordersMsg := &bus.Outbound{
			Message: bus.Message{
				Subject: "orders.new",
				Payload: []byte(`["order_payload"]`),
			},
		}

		notificationMsg := &bus.Outbound{
			Message: bus.Message{
				Subject: "notifications.user.login",
				Payload: []byte(`["notification_payload"]`),
			},
		}

		result, err := b.Dispatch(ctx, ordersMsg, notificationMsg)
		require.NoError(t, err, "Dispatch should not error")
		require.NotNil(t, result, "Dispatch result should not be nil")

		// Test MatchedCount
		assert.Equal(t, 2, result.MatchedCount(), "Should have 2 matched subscriptions")

		// Test ExecutedCount
		assert.Equal(t, 2, result.ExecutedCount(), "Should have 2 executed subscriptions")

		// Test JobIDs (existing test coverage but verify it works correctly)
		jobIDs := result.JobIDs()
		assert.Equal(t, 2, len(jobIDs), "Should have 2 job IDs")
		for _, id := range jobIDs {
			assert.Greater(t, id, int64(0), "Job ID should be positive")
		}

		// Test SkippedByOverlap - should be empty since no overlapping patterns
		skippedOverlap := result.SkippedByOverlap()
		assert.Empty(t, skippedOverlap, "Should have no skipped by overlap")

		// Test SkippedByConflict - should be empty in normal case
		skippedConflict := result.SkippedByConflict()
		assert.Empty(t, skippedConflict, "Should have no skipped by conflict")
	})

	t.Run("DispatchMethods_WithOverlappingPatterns", func(t *testing.T) {
		cleanupAllTables()

		b, err := pgbus.New(db)
		require.NoError(t, err, "Failed to create Bus instance")

		ctx := context.Background()

		// Setup queue with overlapping patterns
		queue1 := b.Queue(testQueue)

		_, err = queue1.Subscribe(ctx, "orders.new")
		require.NoError(t, err, "Failed to subscribe to exact pattern")

		_, err = queue1.Subscribe(ctx, "orders.*")
		require.NoError(t, err, "Failed to subscribe to wildcard pattern")

		// Dispatch message that matches both patterns
		msg := &bus.Outbound{
			Message: bus.Message{
				Subject: "orders.new",
				Payload: []byte(`["overlapping_payload"]`),
			},
		}

		result, err := b.Dispatch(ctx, msg)
		require.NoError(t, err, "Dispatch should not error")
		require.NotNil(t, result, "Dispatch result should not be nil")

		// Test MatchedCount - should include both matching subscriptions
		assert.Equal(t, 2, result.MatchedCount(), "Should have 2 matched subscriptions (overlapping)")

		// Test ExecutedCount - only first subscription should be executed
		assert.Equal(t, 1, result.ExecutedCount(), "Should have 1 executed subscription (first one)")

		// Test JobIDs - should only have one job ID
		jobIDs := result.JobIDs()
		assert.Equal(t, 1, len(jobIDs), "Should have 1 job ID")

		// Test SkippedByOverlap - should have one skipped execution
		skippedOverlap := result.SkippedByOverlap()
		assert.Equal(t, 1, len(skippedOverlap), "Should have 1 skipped by overlap")
		assert.Equal(t, bus.ExecutionStatusSkippedOverlap, skippedOverlap[0].Status, "Status should be skipped overlap")

		// Test SkippedByConflict - should be empty
		skippedConflict := result.SkippedByConflict()
		assert.Empty(t, skippedConflict, "Should have no skipped by conflict")
	})

	t.Run("DispatchMethods_EmptyDispatch", func(t *testing.T) {
		cleanupAllTables()

		b, err := pgbus.New(db)
		require.NoError(t, err, "Failed to create Bus instance")

		ctx := context.Background()

		// Dispatch with no messages
		result, err := b.Dispatch(ctx)
		require.NoError(t, err, "Empty dispatch should not error")
		require.NotNil(t, result, "Dispatch result should not be nil")

		// Test all methods on empty dispatch
		assert.Equal(t, 0, result.MatchedCount(), "Empty dispatch should have 0 matched count")
		assert.Equal(t, 0, result.ExecutedCount(), "Empty dispatch should have 0 executed count")
		assert.Empty(t, result.JobIDs(), "Empty dispatch should have no job IDs")
		assert.Empty(t, result.SkippedByOverlap(), "Empty dispatch should have no skipped by overlap")
		assert.Empty(t, result.SkippedByConflict(), "Empty dispatch should have no skipped by conflict")
	})

	t.Run("DispatchMethods_NoMatchingSubscriptions", func(t *testing.T) {
		cleanupAllTables()

		b, err := pgbus.New(db)
		require.NoError(t, err, "Failed to create Bus instance")

		ctx := context.Background()

		// Setup subscription that won't match our message
		queue1 := b.Queue(testQueue)
		_, err = queue1.Subscribe(ctx, "orders.new")
		require.NoError(t, err, "Failed to subscribe")

		// Dispatch message with different subject
		msg := &bus.Outbound{
			Message: bus.Message{
				Subject: "notifications.user.login",
				Payload: []byte(`["no_match_payload"]`),
			},
		}

		result, err := b.Dispatch(ctx, msg)
		require.NoError(t, err, "Dispatch should not error")
		require.NotNil(t, result, "Dispatch result should not be nil")

		// Test all methods when no subscriptions match
		assert.Equal(t, 0, result.MatchedCount(), "Should have 0 matched subscriptions")
		assert.Equal(t, 0, result.ExecutedCount(), "Should have 0 executed subscriptions")
		assert.Empty(t, result.JobIDs(), "Should have no job IDs")
		assert.Empty(t, result.SkippedByOverlap(), "Should have no skipped by overlap")
		assert.Empty(t, result.SkippedByConflict(), "Should have no skipped by conflict")
	})

	t.Run("DispatchMethods_WithUniqueConstraintConflict", func(t *testing.T) {
		cleanupAllTables()

		b, err := pgbus.New(db)
		require.NoError(t, err, "Failed to create Bus instance")

		ctx := context.Background()

		// Setup queue with unique constraint
		queue1 := b.Queue(testQueue)
		_, err = queue1.Subscribe(ctx, "orders.new", bus.WithPlanConfig(&bus.PlanConfig{
			UniqueLifecycle: que.Always,
		}))
		require.NoError(t, err, "Failed to subscribe with unique constraint")

		// Create message with unique ID
		msg := &bus.Outbound{
			Message: bus.Message{
				Subject: "orders.new",
				Payload: []byte(`["unique_payload"]`),
			},
			UniqueID: func(m *bus.Outbound) string {
				return "test-unique-id"
			},
		}

		// First dispatch should succeed
		result1, err := b.Dispatch(ctx, msg)
		require.NoError(t, err, "First dispatch should not error")
		assert.Equal(t, 1, result1.ExecutedCount(), "First dispatch should execute")
		assert.Equal(t, 1, len(result1.JobIDs()), "First dispatch should create job")

		// Second dispatch with same unique ID should create conflict
		result2, err := b.Dispatch(ctx, msg)
		require.NoError(t, err, "Second dispatch should not error")

		// Test methods on conflicted dispatch
		assert.Equal(t, 1, result2.MatchedCount(), "Should have 1 matched subscription")
		assert.Equal(t, 0, result2.ExecutedCount(), "Should have 0 executed subscriptions due to conflict")
		assert.Empty(t, result2.JobIDs(), "Should have no job IDs due to conflict")
		assert.Empty(t, result2.SkippedByOverlap(), "Should have no skipped by overlap")

		// Test SkippedByConflict - should have one skipped execution
		skippedConflict := result2.SkippedByConflict()
		assert.Equal(t, 1, len(skippedConflict), "Should have 1 skipped by conflict")
		assert.Equal(t, bus.ExecutionStatusSkippedConflict, skippedConflict[0].Status, "Status should be skipped conflict")
		assert.Nil(t, skippedConflict[0].JobID, "JobID should be nil for conflicted execution")
	})
}
