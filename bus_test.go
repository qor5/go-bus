package bus_test

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"testing"
	"time"

	"github.com/qor5/go-bus"
	"github.com/qor5/go-bus/pgbus"
	"github.com/qor5/go-que"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/theplant/testenv"
)

var db *sql.DB

func TestMain(m *testing.M) {
	env, err := testenv.New().DBEnable(true).SetUp()
	if err != nil {
		panic(err)
	}
	defer func() { _ = env.TearDown() }()

	db, err = env.DB.DB()
	if err != nil {
		panic(err)
	}

	// Initialize database schema directly - Migrate handles tables that may already exist
	ctx := context.Background()
	if err := pgbus.Migrate(ctx, db); err != nil {
		panic(fmt.Sprintf("Failed to migrate database: %v", err))
	}

	// Clear all data while preserving table structure
	cleanupAllTables()

	m.Run()
}

// cleanupAllTables clears all data from tables without dropping table structure,
// which improves test efficiency by avoiding repeated schema creation
func cleanupAllTables() {
	// Clear data while preserving table structure
	_, _ = db.Exec("DELETE FROM goque_jobs")
	_, _ = db.Exec("DELETE FROM gobus_subscriptions")
	_, _ = db.Exec("DELETE FROM gobus_metadata")

	// Restore initial metadata record to ensure consistent test environment
	_, _ = db.Exec("INSERT INTO gobus_metadata (version, updated_at, total_subscriptions) " +
		"SELECT 1, NOW(), 0 " +
		"WHERE NOT EXISTS (SELECT 1 FROM gobus_metadata)")
}

// Define multiple test queue names
const (
	testQueue        = "test_queue"
	testQueue2       = "test_queue2"
	testQueue3       = "test_queue3"
	nonExistentQueue = "non_existent_queue"
)

// TestBasicOperations verifies the basic operations of the bus implementation
// such as queue creation, subscription management and subject matching.
func TestBasicOperations(t *testing.T) {
	cleanupAllTables()

	b, err := pgbus.New(db)
	require.NoError(t, err, "Failed to create Bus instance")

	ctx := context.Background()

	// Test getting queues
	queue1 := b.Queue(testQueue)
	require.NotNil(t, queue1, "Queue should not be nil")

	queue2 := b.Queue(testQueue2)
	require.NotNil(t, queue2, "Queue should not be nil")

	// Test subscribing
	sub1, err := queue1.Subscribe(ctx, "test.topic")
	require.NoError(t, err, "Failed to subscribe")
	require.NotNil(t, sub1, "Subscription should not be nil")

	sub2, err := queue1.Subscribe(ctx, "test.*")
	require.NoError(t, err, "Failed to subscribe")
	require.NotNil(t, sub2, "Subscription should not be nil")

	sub3, err := queue2.Subscribe(ctx, "test.>")
	require.NoError(t, err, "Failed to subscribe")
	require.NotNil(t, sub3, "Subscription should not be nil")

	// Test getting subscriptions
	subs1, err := queue1.Subscriptions(ctx)
	require.NoError(t, err, "Failed to get subscriptions")
	assert.Equal(t, 2, len(subs1), "Should have 2 subscriptions")

	subs2, err := queue2.Subscriptions(ctx)
	require.NoError(t, err, "Failed to get subscriptions")
	assert.Equal(t, 1, len(subs2), "Should have 1 subscription")

	// Test BySubject
	matchingSubs, err := b.BySubject(ctx, "test.topic")
	require.NoError(t, err, "Failed to get matching subscriptions")
	assert.Equal(t, 3, len(matchingSubs), "Should have 3 matching subscriptions")

	nonMatchingSubs, err := b.BySubject(ctx, "other.topic")
	require.NoError(t, err, "Failed to get non-matching subscriptions")
	assert.Equal(t, 0, len(nonMatchingSubs), "Should have 0 matching subscriptions")

	// Test unsubscribe
	err = sub1.Unsubscribe(ctx)
	require.NoError(t, err, "Failed to unsubscribe")

	// Verify subscription was removed
	subsAfterUnsubscribe, err := queue1.Subscriptions(ctx)
	require.NoError(t, err, "Failed to get subscriptions after unsubscribe")
	assert.Equal(t, 1, len(subsAfterUnsubscribe), "Should have 1 subscription after unsubscribe")
}

// TestSubscriptionManagement tests detailed subscription operations
// across multiple queues, including updates and retrievals.
func TestSubscriptionManagement(t *testing.T) {
	cleanupAllTables()

	b, err := pgbus.New(db)
	require.NoError(t, err, "Failed to create Bus instance")

	ctx := context.Background()

	// Test subscribing to multiple queues
	t.Run("Subscribe_MultipleQueues", func(t *testing.T) {
		queuePatterns := map[string][]string{
			testQueue:  {"example.topic", "example.*", "example.>"},
			testQueue2: {"orders.new", "orders.*"},
			testQueue3: {"notifications.>"},
		}

		// Create subscriptions for multiple queues
		for queueName, patterns := range queuePatterns {
			queue := b.Queue(queueName)
			for _, pattern := range patterns {
				_, err := queue.Subscribe(ctx, pattern)
				require.NoError(t, err, "Failed to subscribe queue %s to pattern %s", queueName, pattern)
			}
		}

		// Verify subscriptions for each queue
		for queueName, expectedPatterns := range queuePatterns {
			queue := b.Queue(queueName)
			subs, err := queue.Subscriptions(ctx)
			require.NoError(t, err, "Failed to get subscriptions for queue %s", queueName)
			assert.Equal(t, len(expectedPatterns), len(subs), "Unexpected number of subscriptions for queue %s", queueName)

			// Validate subscription patterns
			patternMap := make(map[string]bool)
			for _, pattern := range expectedPatterns {
				patternMap[pattern] = true
			}

			for _, sub := range subs {
				assert.True(t, patternMap[sub.Pattern()], "Unexpected subscription pattern %s for queue %s", sub.Pattern(), queueName)
			}
		}

		// Verify non-existent queue returns empty list
		nonExistQueue := b.Queue(nonExistentQueue)
		subs, err := nonExistQueue.Subscriptions(ctx)
		require.NoError(t, err, "Subscriptions should not error for non-existent queue")
		assert.Empty(t, subs, "Subscription list should be empty for non-existent queue")
	})

	// Test subscription update when subscribing with the same pattern
	t.Run("Subscribe_Update", func(t *testing.T) {
		cleanupAllTables()
		b, err = pgbus.New(db)
		require.NoError(t, err, "Failed to create Bus instance")

		queue := b.Queue(testQueue)

		// Initial subscription
		sub1, err := queue.Subscribe(ctx, "example.topic")
		require.NoError(t, err, "First subscription should succeed")
		require.NotNil(t, sub1, "Subscription should not be nil")

		// Initial subscription ID
		initialID := sub1.ID()

		// Wait to ensure creation time will differ
		time.Sleep(10 * time.Millisecond)

		// Subscribe again with the same pattern
		sub2, err := queue.Subscribe(ctx, "example.topic")
		require.NoError(t, err, "Subscription update should succeed")
		require.NotNil(t, sub2, "Subscription should not be nil")

		// Verify same subscription ID (indicating update rather than new insertion)
		assert.Equal(t, initialID, sub2.ID(), "Subscription ID should remain the same")

		// Verify only one subscription exists
		subs, err := queue.Subscriptions(ctx)
		require.NoError(t, err, "Failed to get subscriptions")
		assert.Equal(t, 1, len(subs), "Should still have only one subscription")

		// Test PlanConfig update
		customPlan := &bus.PlanConfig{
			RunAtDelta: 200 * time.Millisecond,
			RetryPolicy: &que.RetryPolicy{
				InitialInterval:        2 * time.Second,
				MaxInterval:            20 * time.Second,
				NextIntervalMultiplier: 2.0,
				IntervalRandomPercent:  20,
				MaxRetryCount:          5,
			},
		}

		// Subscribe again with the same pattern but different PlanConfig
		sub3, err := queue.Subscribe(ctx, "example.topic", bus.WithPlanConfig(customPlan))
		require.NoError(t, err, "Subscription update with PlanConfig should succeed")
		require.NotNil(t, sub3, "Updated subscription should not be nil")

		// Verify new subscription ID (delete-and-recreate creates new record)
		assert.NotEqual(t, initialID, sub3.ID(), "Subscription ID should be new after PlanConfig update (delete-and-recreate)")

		// Verify PlanConfig was updated
		updatedConfig := sub3.PlanConfig()
		assert.Equal(t, customPlan.RunAtDelta, updatedConfig.RunAtDelta, "RunAtDelta should be updated")
		assert.Equal(t, customPlan.RetryPolicy.InitialInterval, updatedConfig.RetryPolicy.InitialInterval, "InitialInterval should be updated")
		assert.Equal(t, customPlan.RetryPolicy.MaxRetryCount, updatedConfig.RetryPolicy.MaxRetryCount, "MaxRetryCount should be updated")
	})

	// Test subscription pattern matching
	t.Run("BySubject_NoMatches", func(t *testing.T) {
		cleanupAllTables()
		b, err = pgbus.New(db)
		require.NoError(t, err, "Failed to create Bus instance")

		// Add some subscriptions
		queue1 := b.Queue(testQueue)
		queue2 := b.Queue(testQueue2)
		queue3 := b.Queue(testQueue3)

		_, err = queue1.Subscribe(ctx, "orders.*.confirmed")
		require.NoError(t, err, "Failed to subscribe")

		_, err = queue2.Subscribe(ctx, "payments.successful")
		require.NoError(t, err, "Failed to subscribe")

		_, err = queue3.Subscribe(ctx, "shipments.>")
		require.NoError(t, err, "Failed to subscribe")

		// Test subjects that don't match any patterns
		nonMatchingSubjects := []string{
			"products.new",
			"users.login",
			"orders", // doesn't match orders.*.confirmed
		}

		for _, subject := range nonMatchingSubjects {
			subs, err := b.BySubject(ctx, subject)
			require.NoError(t, err, "BySubject should not error for non-matching subject")
			assert.Empty(t, subs, "Should return empty subscription list for %s", subject)
		}
	})

	// Test subject matching across multiple queues
	t.Run("BySubject_CrossQueueMatches", func(t *testing.T) {
		cleanupAllTables()
		b, err = pgbus.New(db)
		require.NoError(t, err, "Failed to create Bus instance")

		// Create similar subscriptions across multiple queues
		queue1 := b.Queue(testQueue)
		queue2 := b.Queue(testQueue2)
		queue3 := b.Queue(testQueue3)

		_, err = queue1.Subscribe(ctx, "events.user.*")
		require.NoError(t, err, "Failed to subscribe to events.user.*")

		_, err = queue1.Subscribe(ctx, "events.admin.*")
		require.NoError(t, err, "Failed to subscribe to events.admin.*")

		_, err = queue2.Subscribe(ctx, "events.*.created")
		require.NoError(t, err, "Failed to subscribe to events.*.created")

		_, err = queue2.Subscribe(ctx, "events.*.updated")
		require.NoError(t, err, "Failed to subscribe to events.*.updated")

		_, err = queue3.Subscribe(ctx, "events.>")
		require.NoError(t, err, "Failed to subscribe to events.>")

		// Test cross-queue subscription matches
		tests := []struct {
			subject      string
			expectedSubs int
			queues       map[string]int // Expected matches per queue
		}{
			{
				subject:      "events.user.created",
				expectedSubs: 3, // Matches "events.user.*", "events.*.created", and "events.>"
				queues: map[string]int{
					testQueue:  1, // "events.user.*"
					testQueue2: 1, // "events.*.created"
					testQueue3: 1, // "events.>"
				},
			},
			{
				subject:      "events.admin.updated",
				expectedSubs: 3, // Matches "events.admin.*", "events.*.updated", and "events.>"
				queues: map[string]int{
					testQueue:  1, // "events.admin.*"
					testQueue2: 1, // "events.*.updated"
					testQueue3: 1, // "events.>"
				},
			},
			{
				subject:      "events.system.error",
				expectedSubs: 1, // Only matches "events.>"
				queues: map[string]int{
					testQueue3: 1, // "events.>"
				},
			},
		}

		for _, tc := range tests {
			subs, err := b.BySubject(ctx, tc.subject)
			require.NoError(t, err, "Failed to get subscriptions for subject %s", tc.subject)
			assert.Equal(t, tc.expectedSubs, len(subs), "Unexpected number of matches for %s", tc.subject)

			// Count matches per queue
			queueMatches := make(map[string]int)
			for _, sub := range subs {
				queueMatches[sub.Queue()]++
			}

			// Verify match count per queue meets expectations
			for queue, expectedCount := range tc.queues {
				assert.Equal(t, expectedCount, queueMatches[queue],
					"Queue %s should have %d matches for subject %s", queue, expectedCount, tc.subject)
			}
		}
	})
}

func setupBusAndQueues(t *testing.T) (bus.Bus, bus.Queue, bus.Queue) {
	cleanupAllTables()

	// Create a new Bus instance
	b, err := pgbus.New(db)
	require.NoError(t, err, "Failed to create Bus instance")

	return b, b.Queue(testQueue), b.Queue(testQueue2)
}

// TestPublish tests the publish functionality including job creation verification
// and multiple subscription matching scenarios.
func TestPublish(t *testing.T) {
	b, queue1, queue2 := setupBusAndQueues(t)

	ctx := context.Background()

	// Subscribe to patterns
	_, err := queue1.Subscribe(ctx, "orders.new")
	require.NoError(t, err, "Failed to subscribe")

	// Add a second subscription in the same queue with overlapping pattern
	_, err = queue1.Subscribe(ctx, "orders.*")
	require.NoError(t, err, "Failed to subscribe to wildcard pattern")

	_, err = queue1.Subscribe(ctx, "orders.*.processed")
	require.NoError(t, err, "Failed to subscribe")

	_, err = queue2.Subscribe(ctx, "notifications.>")
	require.NoError(t, err, "Failed to subscribe")

	t.Run("ValidPublish", func(t *testing.T) {
		// Clear previous jobs
		cleanupJobs(t)

		result, err := b.Publish(ctx, "orders.new", []byte(`["test_payload"]`))
		assert.NoError(t, err, "Valid publish should not error")
		assert.NotNil(t, result, "Publish result should not be nil")
		assert.Greater(t, len(result.JobIDs()), 0, "Should create at least one job")

		// Verify jobs were created - should create 1 jobs (for "orders.new")
		jobs := getQueueJobs(t, testQueue)
		assert.Equal(t, 1, len(jobs), "Should create exactly 1 job for overlapping subscriptions")

		// Verify job arguments contain the expected subject and payload
		for _, job := range jobs {
			msg, err := bus.InboundFromArgs(job.Args)
			require.NoError(t, err, "Failed to parse message from args")
			assert.Equal(t, "orders.new", msg.Subject, "Message subject mismatch")
			assert.Equal(t, []byte(`["test_payload"]`), msg.Payload, "Message payload mismatch")
		}
	})

	t.Run("WildcardMatch", func(t *testing.T) {
		// Clear previous jobs
		cleanupJobs(t)

		_, err = b.Publish(ctx, "orders.item123.processed", []byte(`["wildcard_payload"]`))
		assert.NoError(t, err, "Wildcard match publish should not error")

		// Verify jobs were created - should create 1 job in queue1
		jobs := getQueueJobs(t, testQueue)
		assert.Equal(t, 1, len(jobs), "Should create exactly 1 job for wildcard match")

		// Verify job arguments
		job := jobs[0]
		msg, err := bus.InboundFromArgs(job.Args)
		require.NoError(t, err, "Failed to parse message from args")
		assert.Equal(t, "orders.item123.processed", msg.Subject, "Message subject mismatch")
		assert.Equal(t, []byte(`["wildcard_payload"]`), msg.Payload, "Message payload mismatch")
		// Verify pattern is set
		assert.Contains(t, msg.Header.Get(bus.HeaderSubscriptionPattern), "orders.*", "Message pattern should contain the matching pattern")
	})

	t.Run("MultiLevelWildcardMatch", func(t *testing.T) {
		// Clear previous jobs
		cleanupJobs(t)

		_, err = b.Publish(ctx, "notifications.user.login", []byte(`["multilevel_payload"]`))
		assert.NoError(t, err, "Multi-level wildcard match publish should not error")

		// Verify jobs were created - should create 1 job in queue2
		jobs := getQueueJobs(t, testQueue2)
		assert.Equal(t, 1, len(jobs), "Should create exactly 1 job for multi-level wildcard match")

		// Verify job arguments
		job := jobs[0]
		msg, err := bus.InboundFromArgs(job.Args)
		require.NoError(t, err, "Failed to parse message from args")
		assert.Equal(t, "notifications.user.login", msg.Subject, "Message subject mismatch")
		assert.Equal(t, []byte(`["multilevel_payload"]`), msg.Payload, "Message payload mismatch")
		// Verify pattern is set
		assert.Equal(t, "notifications.>", msg.Header.Get(bus.HeaderSubscriptionPattern), "Message pattern should be the matching pattern")
	})

	// Test Dispatch API
	t.Run("Dispatch", func(t *testing.T) {
		// Clear previous jobs
		cleanupJobs(t)

		outbound := &bus.Outbound{
			Message: bus.Message{
				Subject: "orders.new",
				Header:  bus.Header{"test": []string{"value"}},
				Payload: []byte(`["message_test_payload"]`),
			},
		}

		_, err = b.Dispatch(ctx, outbound)
		assert.NoError(t, err, "Valid Dispatch should not error")

		// Verify jobs were created - should create 1 jobs (for "orders.new")
		jobs := getQueueJobs(t, testQueue)
		assert.Equal(t, 1, len(jobs), "Should create exactly 1 job for overlapping subscriptions")

		// Verify job arguments include header
		for _, job := range jobs {
			msg, err := bus.InboundFromArgs(job.Args)
			require.NoError(t, err, "Failed to parse message from args")
			assert.Equal(t, "orders.new", msg.Subject, "Message subject mismatch")
			assert.Equal(t, []byte(`["message_test_payload"]`), msg.Payload, "Message payload mismatch")
			assert.Equal(t, "value", msg.Header.Get("test"), "Message header should include test value")
		}
	})

	t.Run("BatchDispatch", func(t *testing.T) {
		// Clear previous jobs
		cleanupJobs(t)

		// Create multiple outbound messages targeting different queues
		ordersMsg := &bus.Outbound{
			Message: bus.Message{
				Subject: "orders.new",
				Header:  bus.Header{"batch": []string{"1"}},
				Payload: []byte(`["batch_order_payload"]`),
			},
		}

		notificationMsg := &bus.Outbound{
			Message: bus.Message{
				Subject: "notifications.user.login",
				Header:  bus.Header{"batch": []string{"2"}},
				Payload: []byte(`["batch_notification_payload"]`),
			},
		}

		// Another message matching orders.* pattern
		orderProcessedMsg := &bus.Outbound{
			Message: bus.Message{
				Subject: "orders.item456.processed",
				Header:  bus.Header{"batch": []string{"3"}},
				Payload: []byte(`["batch_processed_payload"]`),
			},
		}

		// Dispatch multiple messages in a single call
		_, err = b.Dispatch(ctx, ordersMsg, notificationMsg, orderProcessedMsg)
		assert.NoError(t, err, "Batch Dispatch should not error")

		// Verify jobs were created in queue1 (should have orders.new and orders.*.processed)
		queue1Jobs := getQueueJobs(t, testQueue)
		assert.Equal(t, 2, len(queue1Jobs), "Should create exactly 2 jobs in queue1")

		// Verify jobs were created in queue2 (should have notifications.user.login)
		queue2Jobs := getQueueJobs(t, testQueue2)
		assert.Equal(t, 1, len(queue2Jobs), "Should create exactly 1 job in queue2")

		// Verify job contents - Map subjects to payloads for easier verification
		messageMap := make(map[string][]byte)
		headerMap := make(map[string]string)

		for _, job := range queue1Jobs {
			msg, err := bus.InboundFromArgs(job.Args)
			require.NoError(t, err, "Failed to parse message from args")
			messageMap[msg.Subject] = msg.Payload
			headerMap[msg.Subject] = msg.Header.Get("batch")
		}

		for _, job := range queue2Jobs {
			msg, err := bus.InboundFromArgs(job.Args)
			require.NoError(t, err, "Failed to parse message from args")
			messageMap[msg.Subject] = msg.Payload
			headerMap[msg.Subject] = msg.Header.Get("batch")
		}

		// Verify all messages were delivered with correct content
		assert.Equal(t, []byte(`["batch_order_payload"]`), messageMap["orders.new"], "orders.new payload mismatch")
		assert.Equal(t, []byte(`["batch_processed_payload"]`), messageMap["orders.item456.processed"], "orders.item456.processed payload mismatch")
		assert.Equal(t, []byte(`["batch_notification_payload"]`), messageMap["notifications.user.login"], "notifications.user.login payload mismatch")

		// Verify headers were preserved
		assert.Equal(t, "1", headerMap["orders.new"], "orders.new header mismatch")
		assert.Equal(t, "3", headerMap["orders.item456.processed"], "orders.item456.processed header mismatch")
		assert.Equal(t, "2", headerMap["notifications.user.login"], "notifications.user.login header mismatch")
	})

	t.Run("EmptyBatchDispatch", func(t *testing.T) {
		// Clear previous jobs
		cleanupJobs(t)

		// Dispatch with empty slice should not error
		_, err = b.Dispatch(ctx)
		assert.NoError(t, err, "Empty Dispatch should not error")

		// Verify no jobs were created
		queue1Jobs := getQueueJobs(t, testQueue)
		queue2Jobs := getQueueJobs(t, testQueue2)
		assert.Equal(t, 0, len(queue1Jobs), "Should not create jobs in queue1 for empty dispatch")
		assert.Equal(t, 0, len(queue2Jobs), "Should not create jobs in queue2 for empty dispatch")
	})

	t.Run("BatchDispatchWithInvalidMessage", func(t *testing.T) {
		// Clear previous jobs
		cleanupJobs(t)

		// Create valid and invalid messages
		validMsg := &bus.Outbound{
			Message: bus.Message{
				Subject: "orders.new",
				Payload: []byte(`["valid_payload"]`),
			},
		}

		// Invalid message with empty subject
		invalidMsg := &bus.Outbound{
			Message: bus.Message{
				Subject: "", // Invalid empty subject
				Payload: []byte(`["invalid_payload"]`),
			},
		}

		// Dispatch should fail with invalid message
		_, err = b.Dispatch(ctx, validMsg, invalidMsg)
		assert.Error(t, err, "Batch Dispatch with invalid message should error")

		// Verify no jobs were created due to transaction rollback
		queue1Jobs := getQueueJobs(t, testQueue)
		assert.Equal(t, 0, len(queue1Jobs), "Should not create any jobs when batch contains invalid message")
	})

	t.Run("NoMatchingSubscriptions", func(t *testing.T) {
		// Clear previous jobs
		cleanupJobs(t)

		// Depending on the implementation, this may or may not return an error
		_, _ = b.Publish(ctx, "unknown.topic", []byte(`["test_payload"]`))

		// Verify no jobs were created
		queue1Jobs := getQueueJobs(t, testQueue)
		queue2Jobs := getQueueJobs(t, testQueue2)
		assert.Equal(t, 0, len(queue1Jobs), "Should not create jobs in queue1 for non-matching topic")
		assert.Equal(t, 0, len(queue2Jobs), "Should not create jobs in queue2 for non-matching topic")
	})

	t.Run("EmptyPayload", func(t *testing.T) {
		// Clear previous jobs
		cleanupJobs(t)

		// Depending on the implementation, this may or may not return an error
		_, err = b.Publish(ctx, "orders.new", []byte{})
		// Log errors if they occur, but don't assert error expectations
		if err != nil {
			t.Logf("Publish with empty payload returned: %v", err)
		}

		// Verify if jobs were created despite empty payload
		jobs := getQueueJobs(t, testQueue)
		if len(jobs) > 0 {
			t.Logf("Created %d jobs despite empty payload", len(jobs))
		}
	})
}

// Helper function to clean up jobs for testing
func cleanupJobs(t *testing.T) {
	_, err := db.Exec("DELETE FROM goque_jobs")
	require.NoError(t, err, "Failed to clean up jobs")
}

// Helper function to get jobs from a specific queue
func getQueueJobs(t *testing.T, queueName string) []jobInfo {
	// Query the database to get jobs for the specified queue
	rows, err := db.Query("SELECT id, args FROM goque_jobs WHERE queue = $1", queueName)
	require.NoError(t, err, "Failed to query jobs for queue "+queueName)
	defer rows.Close()

	var jobs []jobInfo
	for rows.Next() {
		var job jobInfo
		var args []byte
		err = rows.Scan(&job.ID, &args)
		require.NoError(t, err, "Failed to scan job row")

		// Parse args JSON into message structure
		job.Args = args
		jobs = append(jobs, job)
	}

	return jobs
}

// Simple job information structure
type jobInfo struct {
	ID   int
	Args []byte
}

// TestConsume tests message consumption functionality
func TestConsume(t *testing.T) {
	b, queue, _ := setupBusAndQueues(t)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Subscribe to a topic
	_, err := queue.Subscribe(ctx, "test.topic")
	require.NoError(t, err, "Failed to subscribe")

	// Channel to receive messages
	msgCh := make(chan *bus.Inbound, 1)

	// Start consuming
	consumer, err := queue.StartConsumer(ctx, func(ctx context.Context, msg *bus.Inbound) error {
		msgCh <- msg
		return msg.Done(ctx)
	})
	require.NoError(t, err, "Failed to start consumer")
	defer func() { _ = consumer.Stop(context.Background()) }()

	// Publish a message with header
	testPayload := []byte(`["consume_test_payload"]`)
	// Use mixed case header keys to test canonicalization
	testHeader := bus.Header{
		"test-header":     []string{"test"},
		"Content-Type":    []string{"application/json"},
		"x-custom-HEADER": []string{"value"},
	}
	_, err = b.Publish(ctx, "test.topic", testPayload, bus.WithHeader(testHeader))
	require.NoError(t, err, "Failed to publish message")

	// Wait for message or timeout
	select {
	case msg := <-msgCh:
		assert.Equal(t, "test.topic", msg.Subject, "Message subject mismatch")
		assert.Equal(t, testPayload, msg.Payload, "Message payload mismatch")

		// Check canonicalized headers
		assert.Equal(t, "test", msg.Header.Get("Test-Header"), "Message header value mismatch")
		assert.Equal(t, "application/json", msg.Header.Get("Content-Type"), "Content-Type header mismatch")
		assert.Equal(t, "value", msg.Header.Get("X-Custom-Header"), "Custom header value mismatch")
	case <-ctx.Done():
		t.Fatal("Timed out waiting for message")
	}
}

// TestConsumeWithOptions tests message consumption with custom options
func TestConsumeWithOptions(t *testing.T) {
	b, queue, _ := setupBusAndQueues(t)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Subscribe to a topic
	_, err := queue.Subscribe(ctx, "test.options")
	require.NoError(t, err, "Failed to subscribe")

	// Channel to receive messages
	msgCh := make(chan *bus.Inbound, 1)

	// Custom worker config
	workerConfig := &bus.WorkerConfig{
		MaxLockPerSecond:          5,
		MaxBufferJobsCount:        10,
		MaxPerformPerSecond:       5,
		MaxConcurrentPerformCount: 2,
	}

	// Start consuming with options
	consumer, err := queue.StartConsumer(ctx, func(ctx context.Context, msg *bus.Inbound) error {
		msgCh <- msg
		return msg.Done(ctx)
	}, bus.WithWorkerConfig(workerConfig))
	require.NoError(t, err, "Failed to start consumer with options")
	defer func() { _ = consumer.Stop(context.Background()) }()

	// Publish a message
	testPayload := []byte(`["options_test_payload"]`)
	_, err = b.Publish(ctx, "test.options", testPayload)
	require.NoError(t, err, "Failed to publish message")

	// Wait for message or timeout
	select {
	case msg := <-msgCh:
		assert.Equal(t, "test.options", msg.Subject, "Message subject mismatch")
		assert.Equal(t, testPayload, msg.Payload, "Message payload mismatch")
	case <-ctx.Done():
		t.Fatal("Timed out waiting for message")
	}
}

// TestMultipleConsumers tests starting multiple consumers on multiple queues
func TestMultipleConsumers(t *testing.T) {
	cleanupAllTables()

	b, queue, queue2 := setupBusAndQueues(t)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Set up message channels to track received messages
	queueMsgCh := make(chan *bus.Inbound, 2)
	queue2MsgCh := make(chan *bus.Inbound, 1)

	// Subscribe both queues to the same topic
	testTopic := "test.multiconsumer"
	_, err := queue.Subscribe(ctx, testTopic)
	require.NoError(t, err, "Failed to subscribe queue to test topic")

	_, err = queue2.Subscribe(ctx, testTopic)
	require.NoError(t, err, "Failed to subscribe queue2 to test topic")

	// Custom worker config to speed up tests
	workerConf := &bus.WorkerConfig{
		MaxLockPerSecond:          1000,
		MaxBufferJobsCount:        1000,
		MaxPerformPerSecond:       1000,
		MaxConcurrentPerformCount: 1000,
	}

	// Start two consumers on the first queue
	consumer1, err := queue.StartConsumer(ctx, func(ctx context.Context, msg *bus.Inbound) error {
		t.Logf("queue consumer1 received message")
		queueMsgCh <- msg
		return msg.Done(ctx)
	}, bus.WithWorkerConfig(workerConf))
	require.NoError(t, err, "Failed to start first consumer on queue")
	defer func() { _ = consumer1.Stop(context.Background()) }()

	consumer2, err := queue.StartConsumer(ctx, func(ctx context.Context, msg *bus.Inbound) error {
		t.Logf("queue consumer2 received message")
		queueMsgCh <- msg
		return msg.Done(ctx)
	}, bus.WithWorkerConfig(workerConf))
	require.NoError(t, err, "Failed to start second consumer on queue")
	defer func() { _ = consumer2.Stop(context.Background()) }()

	// Start one consumer on the second queue
	consumer3, err := queue2.StartConsumer(ctx, func(ctx context.Context, msg *bus.Inbound) error {
		t.Logf("queue2 consumer received message")
		queue2MsgCh <- msg
		return msg.Done(ctx)
	}, bus.WithWorkerConfig(workerConf))
	require.NoError(t, err, "Failed to start consumer on queue2")
	defer func() { _ = consumer3.Stop(context.Background()) }()

	// Verify all consumers can be started
	assert.NotNil(t, consumer1, "First consumer should not be nil")
	assert.NotNil(t, consumer2, "Second consumer should not be nil")
	assert.NotNil(t, consumer3, "Third consumer on queue2 should not be nil")

	// Publish a message to the test topic
	testPayload := []byte(`["multiconsumer_test"]`)
	_, err = b.Publish(ctx, testTopic, testPayload)
	require.NoError(t, err, "Failed to publish message")

	// Wait for messages to be received with a fixed timeout
	// This gives enough time to verify if multiple consumers receive the message
	var queueMsgCount int
	var queue2MsgCount int

	// Use a fixed timeout to give enough time for all potential messages
	waitTime := 1 * time.Second
	waitDeadline := time.After(waitTime)

	// Collect all messages until timeout
CollectLoop:
	for {
		select {
		case <-queueMsgCh:
			queueMsgCount++
			t.Logf("Received message on queue, count now: %d", queueMsgCount)
		case <-queue2MsgCh:
			queue2MsgCount++
			t.Logf("Received message on queue2, count now: %d", queue2MsgCount)
		case <-waitDeadline:
			t.Logf("Wait time elapsed, collection complete")
			break CollectLoop
		}
	}

	// Verify each queue received exactly one message
	assert.Equal(t, 1, queueMsgCount, "Queue should receive exactly one message (not multiple)")
	assert.Equal(t, 1, queue2MsgCount, "Queue2 should receive exactly one message")
}

// TestBusWithOptions tests creating a bus with custom options
func TestBusWithOptions(t *testing.T) {
	cleanupAllTables()

	dialect, err := pgbus.NewDialect(db)
	require.NoError(t, err, "Failed to create Bus with options")

	// Create Bus with custom options
	b, err := bus.New(dialect, bus.WithMigrate(true), bus.WithLogger(slog.Default()))
	require.NoError(t, err, "Failed to create Bus with options")

	// Test basic operations to verify it works
	queue := b.Queue(testQueue)
	require.NotNil(t, queue, "Queue should not be nil")

	ctx := context.Background()
	sub, err := queue.Subscribe(ctx, "test.options")
	require.NoError(t, err, "Failed to subscribe")
	require.NotNil(t, sub, "Subscription should not be nil")

	// Clean up
	err = sub.Unsubscribe(ctx)
	require.NoError(t, err, "Failed to unsubscribe")
}

// TestSubscriptionPlanConfig tests custom subscription plan configurations
func TestSubscriptionPlanConfig(t *testing.T) {
	cleanupAllTables()

	b, err := pgbus.New(db)
	require.NoError(t, err, "Failed to create Bus instance")

	ctx := context.Background()

	queue := b.Queue(testQueue)

	// Create custom plan config
	customPlan := &bus.PlanConfig{
		RunAtDelta: 500 * time.Millisecond,
		RetryPolicy: &que.RetryPolicy{
			InitialInterval:        1 * time.Second,
			MaxInterval:            10 * time.Second,
			NextIntervalMultiplier: 1.5,
			IntervalRandomPercent:  10,
			MaxRetryCount:          3,
		},
	}

	// Subscribe with custom plan
	sub, err := queue.Subscribe(ctx, "test.plan", bus.WithPlanConfig(customPlan))
	require.NoError(t, err, "Failed to subscribe with custom plan")
	require.NotNil(t, sub, "Subscription should not be nil")

	// Verify plan config was set
	assert.Equal(t, customPlan.RunAtDelta, sub.PlanConfig().RunAtDelta, "RunAtDelta mismatch")
	assert.Equal(t, customPlan.RetryPolicy.InitialInterval, sub.PlanConfig().RetryPolicy.InitialInterval, "InitialInterval mismatch")
	assert.Equal(t, customPlan.RetryPolicy.MaxRetryCount, sub.PlanConfig().RetryPolicy.MaxRetryCount, "MaxRetryCount mismatch")
}

// TestMultiQueueSubscription tests multiple queues subscribing to the same message
func TestMultiQueueSubscription(t *testing.T) {
	startTime := time.Now()
	t.Logf("[%s] TEST START: TestMultiQueueSubscription", time.Since(startTime))

	cleanupAllTables()
	t.Logf("[%s] Tables cleaned up", time.Since(startTime))

	b, err := pgbus.New(db)
	require.NoError(t, err, "Failed to create Bus instance")
	t.Logf("[%s] Bus instance created", time.Since(startTime))

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Create 4 different queues
	queue1 := b.Queue(testQueue)
	queue2 := b.Queue(testQueue2)
	queue3 := b.Queue(testQueue3)
	queue4 := b.Queue("test_queue4")
	t.Logf("[%s] Queues created", time.Since(startTime))

	// Set up message receipt channels for each queue
	msgCh1 := make(chan *bus.Inbound, 1)
	msgCh2 := make(chan *bus.Inbound, 1)
	msgCh3 := make(chan *bus.Inbound, 1)
	msgCh4 := make(chan *bus.Inbound, 1)

	// First set up consumers (as requested)
	consumeStartTime := time.Now()
	t.Logf("[%s] Setting up consumers...", time.Since(startTime))

	// Custom worker config to speed up tests
	workerConf := &bus.WorkerConfig{
		MaxLockPerSecond:          1000,
		MaxBufferJobsCount:        1000,
		MaxPerformPerSecond:       1000,
		MaxConcurrentPerformCount: 1000,
	}

	consumer1, err := queue1.StartConsumer(ctx, func(ctx context.Context, msg *bus.Inbound) error {
		t.Logf("[%s] Queue1 received message", time.Since(startTime))
		msgCh1 <- msg
		return msg.Done(ctx)
	}, bus.WithWorkerConfig(workerConf))
	require.NoError(t, err, "Failed to start consumer for queue1")
	defer func() { _ = consumer1.Stop(context.Background()) }()
	t.Logf("[%s] Consumer for queue1 setup", time.Since(startTime))

	consumer2, err := queue2.StartConsumer(ctx, func(ctx context.Context, msg *bus.Inbound) error {
		t.Logf("[%s] Queue2 received message", time.Since(startTime))
		msgCh2 <- msg
		return msg.Done(ctx)
	}, bus.WithWorkerConfig(workerConf))
	require.NoError(t, err, "Failed to start consumer for queue2")
	defer func() { _ = consumer2.Stop(context.Background()) }()
	t.Logf("[%s] Consumer for queue2 setup", time.Since(startTime))

	consumer3, err := queue3.StartConsumer(ctx, func(ctx context.Context, msg *bus.Inbound) error {
		t.Logf("[%s] Queue3 received message", time.Since(startTime))
		msgCh3 <- msg
		return msg.Done(ctx)
	}, bus.WithWorkerConfig(workerConf))
	require.NoError(t, err, "Failed to start consumer for queue3")
	defer func() { _ = consumer3.Stop(context.Background()) }()
	t.Logf("[%s] Consumer for queue3 setup", time.Since(startTime))

	consumer4, err := queue4.StartConsumer(ctx, func(ctx context.Context, msg *bus.Inbound) error {
		t.Logf("[%s] Queue4 received message", time.Since(startTime))
		msgCh4 <- msg
		return msg.Done(ctx)
	}, bus.WithWorkerConfig(workerConf))
	require.NoError(t, err, "Failed to start consumer for queue4")
	defer func() { _ = consumer4.Stop(context.Background()) }()
	t.Logf("[%s] All consumers setup complete in %s", time.Since(startTime), time.Since(consumeStartTime))

	// Subscribe with different patterns - 3 will match, 1 won't
	// Different subscription patterns that will match the same subject
	subscribeStartTime := time.Now()
	t.Logf("[%s] Setting up subscriptions...", time.Since(startTime))

	sub1, err := queue1.Subscribe(ctx, "event.*.created")
	require.NoError(t, err, "Failed to subscribe queue1")
	require.NotNil(t, sub1, "Subscription should not be nil")
	t.Logf("[%s] Subscription 1 setup", time.Since(startTime))

	sub2, err := queue2.Subscribe(ctx, "event.user.*")
	require.NoError(t, err, "Failed to subscribe queue2")
	require.NotNil(t, sub2, "Subscription should not be nil")
	t.Logf("[%s] Subscription 2 setup", time.Since(startTime))

	sub3, err := queue3.Subscribe(ctx, "event.>")
	require.NoError(t, err, "Failed to subscribe queue3")
	require.NotNil(t, sub3, "Subscription should not be nil")
	t.Logf("[%s] Subscription 3 setup", time.Since(startTime))

	// This pattern won't match our test subject
	sub4, err := queue4.Subscribe(ctx, "notification.>")
	require.NoError(t, err, "Failed to subscribe queue4")
	require.NotNil(t, sub4, "Subscription should not be nil")
	t.Logf("[%s] All subscriptions setup complete in %s", time.Since(startTime), time.Since(subscribeStartTime))

	// Verify subscriptions are properly registered
	const testSubject = "event.user.created"
	bySubjectStartTime := time.Now()
	matchingSubs, err := b.BySubject(ctx, testSubject)
	require.NoError(t, err, "Failed to get subscriptions for test subject")
	assert.Equal(t, 3, len(matchingSubs), "Should have 3 matching subscriptions for subject")
	t.Logf("[%s] BySubject call took %s", time.Since(startTime), time.Since(bySubjectStartTime))

	// Publish a message that matches 3 of the 4 patterns
	testPayload := []byte(`["multi_queue_payload"]`)
	testHeader := bus.Header{"content-type": []string{"application/json"}}

	t.Logf("[%s] Publishing first message...", time.Since(startTime))
	publishStartTime := time.Now()
	_, err = b.Dispatch(ctx, &bus.Outbound{
		Message: bus.Message{
			Subject: testSubject,
			Header:  testHeader,
			Payload: testPayload,
		},
	})
	require.NoError(t, err, "Failed to publish message")
	t.Logf("[%s] First publish took %s", time.Since(startTime), time.Since(publishStartTime))

	// Verify the three matching queues receive the message with correct pattern
	receiveStartTime := time.Now()
	t.Logf("[%s] Waiting for messages in queues...", time.Since(startTime))

	// Check queue1 received the message (matches event.*.created)
	t.Logf("[%s] Waiting for queue1 message...", time.Since(startTime))
	q1Start := time.Now()
	select {
	case msg := <-msgCh1:
		t.Logf("[%s] Queue1 message received after %s", time.Since(startTime), time.Since(q1Start))
		assert.Equal(t, testSubject, msg.Subject, "Message subject mismatch in queue1")
		assert.Equal(t, testPayload, msg.Payload, "Message payload mismatch in queue1")
		assert.Equal(t, "application/json", msg.Header.Get("Content-Type"), "Header mismatch in queue1")
		assert.Equal(t, "event.*.created", msg.Header.Get(bus.HeaderSubscriptionPattern), "Pattern mismatch in queue1")
	case <-ctx.Done():
		t.Fatal("Timed out waiting for message in queue1")
	}

	// Check queue2 received the message (matches event.user.*)
	t.Logf("[%s] Waiting for queue2 message...", time.Since(startTime))
	q2Start := time.Now()
	select {
	case msg := <-msgCh2:
		t.Logf("[%s] Queue2 message received after %s", time.Since(startTime), time.Since(q2Start))
		assert.Equal(t, testSubject, msg.Subject, "Message subject mismatch in queue2")
		assert.Equal(t, testPayload, msg.Payload, "Message payload mismatch in queue2")
		assert.Equal(t, "application/json", msg.Header.Get("Content-Type"), "Header mismatch in queue2")
		assert.Equal(t, "event.user.*", msg.Header.Get(bus.HeaderSubscriptionPattern), "Pattern mismatch in queue2")
	case <-ctx.Done():
		t.Fatal("Timed out waiting for message in queue2")
	}

	// Check queue3 received the message (matches event.>)
	t.Logf("[%s] Waiting for queue3 message...", time.Since(startTime))
	q3Start := time.Now()
	select {
	case msg := <-msgCh3:
		t.Logf("[%s] Queue3 message received after %s", time.Since(startTime), time.Since(q3Start))
		assert.Equal(t, testSubject, msg.Subject, "Message subject mismatch in queue3")
		assert.Equal(t, testPayload, msg.Payload, "Message payload mismatch in queue3")
		assert.Equal(t, "application/json", msg.Header.Get("Content-Type"), "Header mismatch in queue3")
		assert.Equal(t, "event.>", msg.Header.Get(bus.HeaderSubscriptionPattern), "Pattern mismatch in queue3")
	case <-ctx.Done():
		t.Fatal("Timed out waiting for message in queue3")
	}

	t.Logf("[%s] All matching queues received messages in %s", time.Since(startTime), time.Since(receiveStartTime))

	// Verify queue4 did not receive any message (timeout expected)
	t.Logf("[%s] Verifying queue4 did not receive message...", time.Since(startTime))
	q4TimeoutStart := time.Now()
	select {
	case <-msgCh4:
		t.Fatal("Queue4 should not receive any message")
	case <-time.After(100 * time.Millisecond):
		// Expected behavior - no message received
		t.Logf("[%s] Queue4 timeout verified after %s", time.Since(startTime), time.Since(q4TimeoutStart))
	}

	// Now unsubscribe one of the matching queues (queue2)
	// and verify it no longer receives messages
	t.Logf("[%s] Unsubscribing queue2...", time.Since(startTime))
	unsubStartTime := time.Now()
	err = sub2.Unsubscribe(ctx)
	require.NoError(t, err, "Failed to unsubscribe queue2")
	t.Logf("[%s] Unsubscribe took %s", time.Since(startTime), time.Since(unsubStartTime))

	// Clear channels
	t.Logf("[%s] Draining channels...", time.Since(startTime))
	drainChannel(msgCh1)
	drainChannel(msgCh2)
	drainChannel(msgCh3)
	drainChannel(msgCh4)
	t.Logf("[%s] Channels drained", time.Since(startTime))

	// Publish the same message again
	t.Logf("[%s] Publishing second message...", time.Since(startTime))
	publish2StartTime := time.Now()
	_, err = b.Publish(ctx, testSubject, testPayload)
	require.NoError(t, err, "Failed to publish second message")
	t.Logf("[%s] Second publish took %s", time.Since(startTime), time.Since(publish2StartTime))

	// Verify queue1 still receives message
	t.Logf("[%s] Waiting for queue1 message after unsubscribe...", time.Since(startTime))
	q1Start2 := time.Now()
	select {
	case msg := <-msgCh1:
		t.Logf("[%s] Queue1 second message received after %s", time.Since(startTime), time.Since(q1Start2))
		assert.Equal(t, testSubject, msg.Subject, "Message subject mismatch in queue1 after unsubscribe")
		assert.Equal(t, testPayload, msg.Payload, "Message payload mismatch in queue1 after unsubscribe")
	case <-ctx.Done():
		t.Fatal("Timed out waiting for message in queue1 after unsubscribe")
	}

	// Verify queue3 still receives message
	t.Logf("[%s] Waiting for queue3 message after unsubscribe...", time.Since(startTime))
	q3Start2 := time.Now()
	select {
	case msg := <-msgCh3:
		t.Logf("[%s] Queue3 second message received after %s", time.Since(startTime), time.Since(q3Start2))
		assert.Equal(t, testSubject, msg.Subject, "Message subject mismatch in queue3 after unsubscribe")
		assert.Equal(t, testPayload, msg.Payload, "Message payload mismatch in queue3 after unsubscribe")
	case <-ctx.Done():
		t.Fatal("Timed out waiting for message in queue3 after unsubscribe")
	}

	// Verify queue2 and queue4 no longer receives messages (timeout expected)
	t.Logf("[%s] Verifying queue2 and queue4 do not receive messages...", time.Since(startTime))
	q24TimeoutStart := time.Now()
	select {
	case <-msgCh2:
		t.Fatal("Queue2 should not receive messages after unsubscribe")
	case <-msgCh4:
		t.Fatal("Queue4 should still not receive any message")
	case <-time.After(100 * time.Millisecond):
		// Expected behavior - no message received
		t.Logf("[%s] Queue2 and Queue4 timeout verified after %s", time.Since(startTime), time.Since(q24TimeoutStart))
	}

	t.Logf("[%s] TEST COMPLETE: TestMultiQueueSubscription", time.Since(startTime))
}

// Helper function to drain a channel
func drainChannel(ch chan *bus.Inbound) {
	select {
	case <-ch:
		// Drain one message
	default:
		// Channel already empty
	}
}

// TestGetMetadata verifies metadata tracking functionality
func TestGetMetadata(t *testing.T) {
	cleanupAllTables()

	dialect, err := pgbus.NewDialect(db)
	require.NoError(t, err, "Failed to create dialect instance")

	b, err := bus.New(dialect)
	require.NoError(t, err, "Failed to create Bus instance")

	ctx := context.Background()

	metadata, err := dialect.GetMetadata(ctx)
	require.NoError(t, err, "Failed to get metadata")

	assert.NotNil(t, metadata, "Metadata should not be nil")
	assert.EqualValues(t, 0, metadata.TotalSubscriptions)
	assert.EqualValues(t, 1, metadata.Version)

	queue := b.Queue("test_queue")
	require.NotNil(t, queue, "Queue should not be nil")

	_, err = queue.Subscribe(ctx, "test.>", bus.WithPlanConfig(&bus.PlanConfig{
		RunAtDelta: 1 * time.Second,
	}))
	require.NoError(t, err, "Failed to subscribe to test.>")
	metadata, err = dialect.GetMetadata(ctx)
	require.NoError(t, err, "Failed to get metadata")
	assert.EqualValues(t, 1, metadata.TotalSubscriptions)
	assert.EqualValues(t, 2, metadata.Version)

	_, err = queue.Subscribe(ctx, "test.>") // without options
	require.NoError(t, err, "Failed to subscribe to test.>")
	metadata, err = dialect.GetMetadata(ctx)
	require.NoError(t, err, "Failed to get metadata")
	assert.EqualValues(t, 1, metadata.TotalSubscriptions)
	assert.EqualValues(t, 3, metadata.Version)

	sub, err := queue.Subscribe(ctx, "test.>")
	require.NoError(t, err, "Failed to subscribe to test.>")
	metadata, err = dialect.GetMetadata(ctx)
	require.NoError(t, err, "Failed to get metadata")
	assert.EqualValues(t, 1, metadata.TotalSubscriptions)
	assert.EqualValues(t, 3, metadata.Version)

	err = sub.Unsubscribe(ctx)
	require.NoError(t, err, "Failed to unsubscribe")
	metadata, err = dialect.GetMetadata(ctx)
	require.NoError(t, err, "Failed to get metadata")
	assert.EqualValues(t, 0, metadata.TotalSubscriptions)
	assert.EqualValues(t, 4, metadata.Version)
}

// TestIndexedQueryEdgeCases tests pattern matching edge cases with real database operations
func TestIndexedQueryEdgeCases(t *testing.T) {
	cleanupAllTables()

	ctx := context.Background()

	// Test cases with patterns and subjects
	tests := []struct {
		name        string
		patterns    []string // patterns to subscribe to
		subject     string   // subject to test
		shouldMatch bool     // whether subject should match any pattern
		description string
	}{
		// Basic exact matching
		{
			name:        "exact_match",
			patterns:    []string{"events.user.created"},
			subject:     "events.user.created",
			shouldMatch: true,
			description: "Exact pattern and subject should match",
		},
		{
			name:        "different_lengths_no_wildcard",
			patterns:    []string{"events.user"},
			subject:     "events.user.created",
			shouldMatch: false,
			description: "Subject longer than pattern without wildcards should not match",
		},
		{
			name:        "subject_shorter_than_pattern",
			patterns:    []string{"events.user.created"},
			subject:     "events.user",
			shouldMatch: false,
			description: "Subject shorter than pattern should not match",
		},

		// Single token wildcard (*) edge cases
		{
			name:        "single_wildcard_exact_length",
			patterns:    []string{"events.*.created"},
			subject:     "events.user.created",
			shouldMatch: true,
			description: "Single wildcard with exact token count should match",
		},
		{
			name:        "single_wildcard_extra_subject_tokens",
			patterns:    []string{"events.*"},
			subject:     "events.user.created",
			shouldMatch: false,
			description: "Single wildcard with extra subject tokens should not match",
		},
		{
			name:        "single_wildcard_missing_subject_tokens",
			patterns:    []string{"events.*.created"},
			subject:     "events.user",
			shouldMatch: false,
			description: "Single wildcard with missing subject tokens should not match",
		},

		// Multi-level wildcard (>) edge cases
		{
			name:        "multi_wildcard_one_extra_token",
			patterns:    []string{"events.>"},
			subject:     "events.user",
			shouldMatch: true,
			description: "Multi-level wildcard should match one extra token",
		},
		{
			name:        "multi_wildcard_many_extra_tokens",
			patterns:    []string{"events.>"},
			subject:     "events.user.created.successfully",
			shouldMatch: true,
			description: "Multi-level wildcard should match many extra tokens",
		},
		{
			name:        "multi_wildcard_exact_length",
			patterns:    []string{"events.user.>"},
			subject:     "events.user.created",
			shouldMatch: true,
			description: "Multi-level wildcard should match exactly one extra token",
		},
		{
			name:        "multi_wildcard_no_extra_tokens",
			patterns:    []string{"events.user.>"},
			subject:     "events.user",
			shouldMatch: false,
			description: "Multi-level wildcard requires at least one extra token",
		},
		{
			name:        "root_multi_wildcard",
			patterns:    []string{">"},
			subject:     "events",
			shouldMatch: true,
			description: "Root multi-level wildcard should match any subject",
		},
		{
			name:        "root_multi_wildcard_multiple_tokens",
			patterns:    []string{">"},
			subject:     "events.user.created",
			shouldMatch: true,
			description: "Root multi-level wildcard should match multiple tokens",
		},

		// Combined wildcards
		{
			name:        "combined_wildcards",
			patterns:    []string{"events.*.>"},
			subject:     "events.user.created.successfully",
			shouldMatch: true,
			description: "Combined single and multi-level wildcards should work",
		},
		{
			name:        "combined_wildcards_insufficient_tokens",
			patterns:    []string{"events.*.>"},
			subject:     "events.user",
			shouldMatch: false,
			description: "Combined wildcards require sufficient subject tokens",
		},

		// Maximum token edge cases
		{
			name:        "max_tokens_exact",
			patterns:    []string{"a.b.c.d.e.f.g.h.i.j.k.l.m.n.o.p"},
			subject:     "a.b.c.d.e.f.g.h.i.j.k.l.m.n.o.p",
			shouldMatch: true,
			description: "Maximum tokens should work for exact match",
		},
		{
			name:        "max_tokens_with_wildcard",
			patterns:    []string{"a.b.c.d.e.f.g.h.i.j.k.l.m.n.o.*"},
			subject:     "a.b.c.d.e.f.g.h.i.j.k.l.m.n.o.x",
			shouldMatch: true,
			description: "Maximum tokens with single wildcard should work",
		},

		// Multiple patterns test - ensure we test complex scenarios
		{
			name:        "multiple_patterns_with_mixed_matches",
			patterns:    []string{"events.user.*", "notifications.>", "orders.*.confirmed"},
			subject:     "events.user.created",
			shouldMatch: true,
			description: "Subject should match one of multiple patterns",
		},
		{
			name:        "multiple_patterns_no_matches",
			patterns:    []string{"events.admin.*", "notifications.>", "orders.*.confirmed"},
			subject:     "products.new",
			shouldMatch: false,
			description: "Subject should not match any of multiple patterns",
		},

		// Edge case with empty-like patterns
		{
			name:        "single_token_pattern_multi_token_subject",
			patterns:    []string{"events"},
			subject:     "events.user",
			shouldMatch: false,
			description: "Single token pattern should not match multi-token subject",
		},
		{
			name:        "single_token_exact_match",
			patterns:    []string{"events"},
			subject:     "events",
			shouldMatch: true,
			description: "Single token pattern should match single token subject exactly",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Clean up before each test case
			cleanupAllTables()

			// Create fresh bus instance
			testBus, err := pgbus.New(db)
			require.NoError(t, err, "Failed to create test Bus instance")

			// Create a test queue and subscribe to all patterns
			queue := testBus.Queue("edge_case_test_queue")
			for i, pattern := range tt.patterns {
				_, err := queue.Subscribe(ctx, pattern)
				require.NoError(t, err, "Failed to subscribe to pattern %s", pattern)
				t.Logf("Subscribed to pattern %d: %s", i+1, pattern)
			}

			// Test the subject matching
			matchingSubs, err := testBus.BySubject(ctx, tt.subject)
			require.NoError(t, err, "Failed to get matching subscriptions for subject %s", tt.subject)

			// Verify the expectation
			hasMatches := len(matchingSubs) > 0
			assert.Equal(t, tt.shouldMatch, hasMatches,
				"Subject '%s' match expectation failed. Expected: %v, Got: %v matches. Description: %s",
				tt.subject, tt.shouldMatch, len(matchingSubs), tt.description)

			if tt.shouldMatch && len(matchingSubs) > 0 {
				// Log which patterns matched for debugging
				for _, sub := range matchingSubs {
					t.Logf("Pattern '%s' matched subject '%s'", sub.Pattern(), tt.subject)
				}
			}

			t.Logf("Test case '%s': patterns=%v, subject='%s', expected=%v, actual=%v",
				tt.name, tt.patterns, tt.subject, tt.shouldMatch, hasMatches)
		})
	}
}

func TestTTLAndHeartbeat(t *testing.T) {
	ctx := context.Background()
	cleanupAllTables()

	b, err := pgbus.New(db)
	require.NoError(t, err)

	t.Run("TTL Subscription Creation", func(t *testing.T) {
		queue := b.Queue("test-ttl-queue")

		// Create subscription with 200ms TTL
		sub, err := queue.Subscribe(ctx, "test.ttl",
			bus.WithTTL(200*time.Millisecond),
		)
		require.NoError(t, err)
		require.NotNil(t, sub)

		// Verify subscription was created successfully
		subs, err := queue.Subscriptions(ctx)
		require.NoError(t, err)
		require.Len(t, subs, 1)
		assert.Equal(t, "test.ttl", subs[0].Pattern())
	})

	t.Run("Heartbeat Updates Expiration", func(t *testing.T) {
		queue := b.Queue("test-heartbeat-queue")

		// Create subscription with 300ms TTL
		sub, err := queue.Subscribe(ctx, "test.heartbeat",
			bus.WithTTL(300*time.Millisecond),
		)
		require.NoError(t, err)

		// Wait a bit and send heartbeat
		time.Sleep(100 * time.Millisecond)
		err = sub.Heartbeat(ctx)
		require.NoError(t, err)

		// Verify heartbeat was successful by ensuring subscription still exists
		subs, err := queue.Subscriptions(ctx)
		require.NoError(t, err)
		require.Len(t, subs, 1)
		assert.Equal(t, "test.heartbeat", subs[0].Pattern())

		// Wait 150ms (total elapsed ~250ms from heartbeat, should still be valid since TTL is 300ms)
		time.Sleep(150 * time.Millisecond)

		subs, err = queue.Subscriptions(ctx)
		require.NoError(t, err)
		assert.Len(t, subs, 1, "Subscription should still exist after heartbeat extended TTL")

		// Now wait for actual expiration (wait another 200ms to ensure it expires)
		time.Sleep(200 * time.Millisecond)

		subs, err = queue.Subscriptions(ctx)
		require.NoError(t, err)
		assert.Len(t, subs, 0, "Subscription should be expired now")
	})

	t.Run("Non-TTL Subscriptions Not Affected", func(t *testing.T) {
		cleanupAllTables() // Clean up before this test
		b, err := pgbus.New(db)
		require.NoError(t, err)

		// Create regular subscription without TTL
		queue := b.Queue("test-no-ttl-queue")
		_, err = queue.Subscribe(ctx, "test.no-ttl")
		require.NoError(t, err)

		// Create another subscription with TTL that will expire
		_, err = queue.Subscribe(ctx, "test.will-expire",
			bus.WithTTL(150*time.Millisecond),
		)
		require.NoError(t, err)

		// Initially should see both subscriptions
		subs, err := queue.Subscriptions(ctx)
		require.NoError(t, err)
		assert.Len(t, subs, 2, "Should initially see both subscriptions")

		// Wait for TTL subscription to expire
		time.Sleep(200 * time.Millisecond)

		// Verify expired subscription is now filtered from queries
		subs, err = queue.Subscriptions(ctx)
		require.NoError(t, err)
		assert.Len(t, subs, 1, "Should only see non-TTL subscription after expiration")
		assert.Equal(t, "test.no-ttl", subs[0].Pattern())
	})

	t.Run("Heartbeat Keeps Subscription Alive", func(t *testing.T) {
		cleanupAllTables() // Clean up before this test
		b, err := pgbus.New(db)
		require.NoError(t, err)

		queue := b.Queue("test-heartbeat-alive-queue")

		// Create subscription with 300ms TTL
		sub, err := queue.Subscribe(ctx, "test.heartbeat-alive",
			bus.WithTTL(300*time.Millisecond),
		)
		require.NoError(t, err)

		// Send heartbeat after 150ms (before expiration)
		time.Sleep(150 * time.Millisecond)
		err = sub.Heartbeat(ctx)
		require.NoError(t, err)

		// Wait another 150ms (total 300ms, but heartbeat extended TTL)
		time.Sleep(150 * time.Millisecond)

		// Subscription should still exist
		subs, err := queue.Subscriptions(ctx)
		require.NoError(t, err)
		assert.Len(t, subs, 1, "Subscription should still exist after heartbeat")

		// Now wait for actual expiration without heartbeat
		time.Sleep(350 * time.Millisecond)

		// Verify subscription is now filtered from queries (expired)
		subs, err = queue.Subscriptions(ctx)
		require.NoError(t, err)
		assert.Len(t, subs, 0, "Subscription should be filtered from queries after expiration")

		// Heartbeat should fail now (subscription was expired and then deleted)
		err = sub.Heartbeat(ctx)
		assert.Error(t, err, "Heartbeat should fail for expired/deleted subscription")
	})
}

// TestExpiredSubscriptionFiltering tests that expired subscriptions are properly filtered
// from all query methods
func TestExpiredSubscriptionFiltering(t *testing.T) {
	ctx := context.Background()
	cleanupAllTables()

	b, err := pgbus.New(db)
	require.NoError(t, err)

	queue := b.Queue("test-filtering-queue")

	t.Run("BySubject Filters Expired Subscriptions", func(t *testing.T) {
		// Create subscription with 150ms TTL
		_, err := queue.Subscribe(ctx, "test.expired",
			bus.WithTTL(150*time.Millisecond),
		)
		require.NoError(t, err)

		// Initially should find the subscription
		subs, err := b.BySubject(ctx, "test.expired")
		require.NoError(t, err)
		assert.Len(t, subs, 1, "Should find subscription before expiration")

		// Wait for expiration
		time.Sleep(200 * time.Millisecond)

		// Should not find the expired subscription
		subs, err = b.BySubject(ctx, "test.expired")
		require.NoError(t, err)
		assert.Len(t, subs, 0, "Should not find expired subscription")
	})

	t.Run("ByQueue Filters Expired Subscriptions", func(t *testing.T) {
		cleanupAllTables()
		b, err := pgbus.New(db)
		require.NoError(t, err)
		queue := b.Queue("test-filtering-queue2")

		// Create subscription with 150ms TTL
		_, err = queue.Subscribe(ctx, "test.queue-expired",
			bus.WithTTL(150*time.Millisecond),
		)
		require.NoError(t, err)

		// Initially should find the subscription
		subs, err := queue.Subscriptions(ctx)
		require.NoError(t, err)
		assert.Len(t, subs, 1, "Should find subscription before expiration")

		// Wait for expiration
		time.Sleep(200 * time.Millisecond)

		// Should not find the expired subscription
		subs, err = queue.Subscriptions(ctx)
		require.NoError(t, err)
		assert.Len(t, subs, 0, "Should not find expired subscription")
	})

	t.Run("Mixed Expired and Valid Subscriptions", func(t *testing.T) {
		cleanupAllTables()
		b, err := pgbus.New(db)
		require.NoError(t, err)
		queue := b.Queue("test-filtering-queue3")

		// Create one subscription with TTL that will expire
		_, err = queue.Subscribe(ctx, "test.will-expire",
			bus.WithTTL(150*time.Millisecond),
		)
		require.NoError(t, err)

		// Create one subscription without TTL (permanent)
		_, err = queue.Subscribe(ctx, "test.permanent")
		require.NoError(t, err)

		// Initially should find both subscriptions
		subs, err := queue.Subscriptions(ctx)
		require.NoError(t, err)
		assert.Len(t, subs, 2, "Should find both subscriptions initially")

		// Wait for first subscription to expire
		time.Sleep(200 * time.Millisecond)

		// Should only find the permanent subscription
		subs, err = queue.Subscriptions(ctx)
		require.NoError(t, err)
		assert.Len(t, subs, 1, "Should find only permanent subscription")
		assert.Equal(t, "test.permanent", subs[0].Pattern())
	})
}

// TestExpiredSubscriptionUpdatePrevention tests that expired subscriptions cannot be updated
func TestExpiredSubscriptionUpdatePrevention(t *testing.T) {
	ctx := context.Background()
	cleanupAllTables()

	b, err := pgbus.New(db)
	require.NoError(t, err)

	queue := b.Queue("test-update-prevention-queue")

	t.Run("Heartbeat Fails for Expired Subscription", func(t *testing.T) {
		// Create subscription with 150ms TTL
		sub, err := queue.Subscribe(ctx, "test.expired-heartbeat",
			bus.WithTTL(150*time.Millisecond),
		)
		require.NoError(t, err)

		// Wait for subscription to expire
		time.Sleep(200 * time.Millisecond)

		// Attempt to update heartbeat - should fail
		err = sub.Heartbeat(ctx)
		assert.Error(t, err, "Heartbeat should fail for expired subscription")
		assert.Contains(t, err.Error(), "subscription not found or expired")
	})

	t.Run("Valid Subscription Heartbeat Still Works", func(t *testing.T) {
		cleanupAllTables()
		b, err := pgbus.New(db)
		require.NoError(t, err)
		queue := b.Queue("test-update-prevention-queue2")

		// Create subscription with longer TTL
		sub, err := queue.Subscribe(ctx, "test.valid-heartbeat",
			bus.WithTTL(500*time.Millisecond),
		)
		require.NoError(t, err)

		// Heartbeat should succeed for valid subscription
		err = sub.Heartbeat(ctx)
		assert.NoError(t, err, "Heartbeat should succeed for valid subscription")

		// Wait a bit and try again
		time.Sleep(100 * time.Millisecond)
		err = sub.Heartbeat(ctx)
		assert.NoError(t, err, "Heartbeat should still succeed")
	})
}

// TestUpsertRevivesExpiredSubscription tests that Upsert handles expired subscriptions correctly
func TestUpsertRevivesExpiredSubscription(t *testing.T) {
	ctx := context.Background()
	cleanupAllTables()

	b, err := pgbus.New(db)
	require.NoError(t, err)

	queue := b.Queue("test-upsert-revive-queue")

	t.Run("Upsert Revives Expired Subscription", func(t *testing.T) {
		// Create subscription with 150ms TTL
		sub1, err := queue.Subscribe(ctx, "test.upsert-revive",
			bus.WithTTL(150*time.Millisecond),
		)
		require.NoError(t, err)

		originalID := sub1.ID()

		// Wait for subscription to expire
		time.Sleep(200 * time.Millisecond)

		// Verify subscription is no longer visible in queries (filtered as expired)
		subs, err := queue.Subscriptions(ctx)
		require.NoError(t, err)
		assert.Len(t, subs, 0, "Expired subscription should not be visible")

		// Now try to subscribe again with same pattern - should revive the expired subscription
		sub2, err := queue.Subscribe(ctx, "test.upsert-revive",
			bus.WithTTL(150*time.Millisecond),
		)
		require.NoError(t, err)

		newID := sub2.ID()

		// Should be a new subscription ID (delete-and-recreate creates new record)
		assert.NotEqual(t, originalID, newID, "Should create new subscription (delete-and-recreate strategy)")

		// Revived subscription should be visible
		subs, err = queue.Subscriptions(ctx)
		require.NoError(t, err)
		assert.Len(t, subs, 1, "Revived subscription should be visible")
		assert.Equal(t, newID, subs[0].ID(), "Should return the revived subscription")

		// Heartbeat should work on revived subscription
		err = sub2.Heartbeat(ctx)
		assert.NoError(t, err, "Heartbeat should work on revived subscription")

		// Wait for subscription to expire
		time.Sleep(200 * time.Millisecond)

		// Heartbeat should fail for expired subscription
		err = sub2.Heartbeat(ctx)
		assert.Error(t, err, "Heartbeat should fail for expired subscription")
		assert.Contains(t, err.Error(), "subscription not found or expired")

		sub3, err := queue.Subscribe(ctx, "test.upsert-revive",
			bus.WithTTL(150*time.Millisecond),
		)
		require.NoError(t, err)

		assert.NotEqual(t, sub3.ID(), newID, "Should create new subscription since the previous one was expired and deleted")
	})
}

// TestQueryJobsBySubscriptionID tests querying jobs by subscription ID from header
func TestQueryJobsBySubscriptionID(t *testing.T) {
	cleanupAllTables()

	b, err := pgbus.New(db)
	require.NoError(t, err, "Failed to create Bus instance")

	ctx := context.Background()

	// Set up queues and subscriptions
	queue1 := b.Queue(testQueue)
	queue2 := b.Queue(testQueue2)

	sub1, err := queue1.Subscribe(ctx, "orders.new")
	require.NoError(t, err, "Failed to subscribe")

	sub2, err := queue2.Subscribe(ctx, "notifications.>")
	require.NoError(t, err, "Failed to subscribe")

	// Publish messages to create jobs
	result, err := b.Publish(ctx, "orders.new", []byte(`["test_payload"]`))
	require.NoError(t, err, "Failed to publish message")
	require.NotNil(t, result, "Publish result should not be nil")

	result2, err := b.Publish(ctx, "notifications.user.login", []byte(`["notification_payload"]`))
	require.NoError(t, err, "Failed to publish message")
	require.NotNil(t, result2, "Publish result should not be nil")

	// Verify jobs were created using direct SQL queries since QueryJobsBySubscriptionID doesn't exist
	// Query jobs by subscription ID using SQL
	rows, err := db.QueryContext(ctx, `
		SELECT COUNT(*) 
		FROM goque_jobs 
		WHERE args::jsonb->0->'header'->'Subscription-Identifier'->>0 = $1
	`, sub1.ID())
	require.NoError(t, err, "Failed to query jobs by subscription ID")
	defer rows.Close()

	var count1 int
	require.True(t, rows.Next())
	err = rows.Scan(&count1)
	require.NoError(t, err, "Failed to scan count")
	assert.Equal(t, 1, count1, "Should find 1 job for sub1")

	// Query jobs for subscription 2
	rows2, err := db.QueryContext(ctx, `
		SELECT COUNT(*) 
		FROM goque_jobs 
		WHERE args::jsonb->0->'header'->'Subscription-Identifier'->>0 = $1
	`, sub2.ID())
	require.NoError(t, err, "Failed to query jobs by subscription ID")
	defer rows2.Close()

	var count2 int
	require.True(t, rows2.Next())
	err = rows2.Scan(&count2)
	require.NoError(t, err, "Failed to scan count")
	assert.Equal(t, 1, count2, "Should find 1 job for sub2")

	// Query jobs for non-existent subscription ID
	rows3, err := db.QueryContext(ctx, `
		SELECT COUNT(*) 
		FROM goque_jobs 
		WHERE args::jsonb->0->'header'->'Subscription-Identifier'->>0 = $1
	`, "99999")
	require.NoError(t, err, "Should not error for non-existent subscription ID")
	defer rows3.Close()

	var count3 int
	require.True(t, rows3.Next())
	err = rows3.Scan(&count3)
	require.NoError(t, err, "Failed to scan count")
	assert.Equal(t, 0, count3, "Should return 0 for non-existent subscription ID")
}

// TestSubscriptionDrain tests the Drain method functionality
func TestSubscriptionDrain(t *testing.T) {
	cleanupAllTables()

	b, err := pgbus.New(db)
	require.NoError(t, err, "Failed to create Bus instance")

	ctx := context.Background()

	t.Run("BasicDrainFunctionality", func(t *testing.T) {
		cleanupAllTables()

		// Create queues and subscriptions
		queue1 := b.Queue("drain_test_queue1")
		queue2 := b.Queue("drain_test_queue2")

		// Subscribe to different patterns
		sub1, err := queue1.Subscribe(ctx, "test.drain.*")
		require.NoError(t, err, "Failed to subscribe to pattern")

		sub2, err := queue2.Subscribe(ctx, "test.drain.specific")
		require.NoError(t, err, "Failed to subscribe to pattern")

		// Publish messages that will create jobs
		_, err = b.Publish(ctx, "test.drain.message1", []byte("payload1"))
		require.NoError(t, err, "Failed to publish message1")

		_, err = b.Publish(ctx, "test.drain.message2", []byte("payload2"))
		require.NoError(t, err, "Failed to publish message2")

		_, err = b.Publish(ctx, "test.drain.specific", []byte("payload3"))
		require.NoError(t, err, "Failed to publish message3")

		// Verify jobs were created
		jobsQueue1 := getQueueJobs(t, "drain_test_queue1")
		jobsQueue2 := getQueueJobs(t, "drain_test_queue2")
		assert.Len(t, jobsQueue1, 3, "Queue1 should have 3 jobs") // matches test.drain.*
		assert.Len(t, jobsQueue2, 1, "Queue2 should have 1 job")  // matches test.drain.specific

		// Test drain on subscription 1
		deletedCount1, err := sub1.Drain(ctx)
		require.NoError(t, err, "Drain should succeed")
		assert.Equal(t, 3, deletedCount1, "Should have deleted 3 jobs from subscription 1")

		// Verify jobs were deleted from queue1
		jobsQueue1After := getQueueJobs(t, "drain_test_queue1")
		assert.Len(t, jobsQueue1After, 0, "Queue1 should have no jobs after drain")

		// Verify queue2 jobs are unaffected
		jobsQueue2After := getQueueJobs(t, "drain_test_queue2")
		assert.Len(t, jobsQueue2After, 1, "Queue2 should still have 1 job")

		// Test drain on subscription 2
		deletedCount2, err := sub2.Drain(ctx)
		require.NoError(t, err, "Drain should succeed")
		assert.Equal(t, 1, deletedCount2, "Should have deleted 1 job from subscription 2")

		// Verify all jobs are now drained
		jobsQueue2Final := getQueueJobs(t, "drain_test_queue2")
		assert.Len(t, jobsQueue2Final, 0, "Queue2 should have no jobs after drain")
	})

	t.Run("DrainWithNoJobs", func(t *testing.T) {
		cleanupAllTables()

		queue := b.Queue("empty_drain_queue")
		sub, err := queue.Subscribe(ctx, "test.empty")
		require.NoError(t, err, "Failed to subscribe")

		// Drain when no jobs exist
		deletedCount, err := sub.Drain(ctx)
		require.NoError(t, err, "Drain should succeed even with no jobs")
		assert.Equal(t, 0, deletedCount, "Should have deleted 0 jobs")
	})

	t.Run("DrainWithPatternFiltering", func(t *testing.T) {
		cleanupAllTables()

		// Create two subscriptions with different patterns on the same queue
		queue := b.Queue("pattern_filter_queue")
		sub1, err := queue.Subscribe(ctx, "orders.*")
		require.NoError(t, err, "Failed to subscribe to orders.*")

		sub2, err := queue.Subscribe(ctx, "events.*")
		require.NoError(t, err, "Failed to subscribe to events.*")

		// Publish messages to different subjects
		_, err = b.Publish(ctx, "orders.new", []byte("order_payload"))
		require.NoError(t, err, "Failed to publish order message")

		_, err = b.Publish(ctx, "orders.updated", []byte("order_update_payload"))
		require.NoError(t, err, "Failed to publish order update message")

		_, err = b.Publish(ctx, "events.user_created", []byte("event_payload"))
		require.NoError(t, err, "Failed to publish event message")

		// Verify jobs were created
		allJobs := getQueueJobs(t, "pattern_filter_queue")
		assert.Len(t, allJobs, 3, "Should have 3 total jobs")

		// Drain only orders.* pattern
		deletedCount1, err := sub1.Drain(ctx)
		require.NoError(t, err, "Drain should succeed")
		assert.Equal(t, 2, deletedCount1, "Should have deleted 2 jobs matching orders.*")

		// Verify only event jobs remain
		remainingJobs := getQueueJobs(t, "pattern_filter_queue")
		assert.Len(t, remainingJobs, 1, "Should have 1 job remaining")

		// Drain events.* pattern
		deletedCount2, err := sub2.Drain(ctx)
		require.NoError(t, err, "Drain should succeed")
		assert.Equal(t, 1, deletedCount2, "Should have deleted 1 job matching events.*")

		// Verify all jobs are drained
		finalJobs := getQueueJobs(t, "pattern_filter_queue")
		assert.Len(t, finalJobs, 0, "Should have no jobs remaining")
	})

	t.Run("DrainMultipleJobsPartiallyLocked", func(t *testing.T) {
		cleanupAllTables()

		queue := b.Queue("partial_lock_queue")
		sub, err := queue.Subscribe(ctx, "test.partial")
		require.NoError(t, err, "Failed to subscribe")

		// Publish multiple messages
		for i := 0; i < 5; i++ {
			_, err = b.Publish(ctx, "test.partial", []byte(fmt.Sprintf("payload_%d", i)))
			require.NoError(t, err, "Failed to publish message %d", i)
		}

		// Verify all jobs were created
		jobs := getQueueJobs(t, "partial_lock_queue")
		require.Len(t, jobs, 5, "Should have 5 jobs")

		// Start transactions to hold advisory locks
		tx1, err := db.Begin()
		require.NoError(t, err, "Failed to start transaction 1")
		tx2, err := db.Begin()
		require.NoError(t, err, "Failed to start transaction 2")

		// Lock some jobs (simulate processing)
		_, err = tx1.Exec("SELECT pg_advisory_xact_lock($1)", jobs[1].ID)
		require.NoError(t, err, "Failed to lock job 1")
		_, err = tx2.Exec("SELECT pg_advisory_xact_lock($1)", jobs[3].ID)
		require.NoError(t, err, "Failed to lock job 3")

		// Drain should delete only unlocked jobs
		deletedCount, err := sub.Drain(ctx)
		require.NoError(t, err, "Drain should succeed")
		assert.Equal(t, 3, deletedCount, "Should have deleted 3 unlocked jobs")

		// Verify only locked jobs remain
		remainingJobs := getQueueJobs(t, "partial_lock_queue")
		assert.Len(t, remainingJobs, 2, "Should have 2 locked jobs remaining")

		// Release locks by committing transactions
		err = tx1.Commit()
		require.NoError(t, err, "Failed to commit transaction 1")
		err = tx2.Commit()
		require.NoError(t, err, "Failed to commit transaction 2")

		// Drain remaining jobs
		deletedCount2, err := sub.Drain(ctx)
		require.NoError(t, err, "Drain should succeed")
		assert.Equal(t, 2, deletedCount2, "Should have deleted 2 remaining jobs")

		// Verify all jobs are deleted
		finalJobs := getQueueJobs(t, "partial_lock_queue")
		assert.Len(t, finalJobs, 0, "All jobs should be deleted")
	})

	t.Run("DrainErrorHandling", func(t *testing.T) {
		cleanupAllTables()

		queue := b.Queue("error_test_queue")
		sub, err := queue.Subscribe(ctx, "test.error")
		require.NoError(t, err, "Failed to subscribe")

		// Test drain with cancelled context
		cancelledCtx, cancel := context.WithCancel(context.Background())
		cancel() // Cancel immediately

		_, err = sub.Drain(cancelledCtx)
		assert.Error(t, err, "Drain should fail with cancelled context")
	})

	t.Run("DrainWithJobStatusFiltering", func(t *testing.T) {
		cleanupAllTables()

		queue := b.Queue("status_filter_queue")
		sub, err := queue.Subscribe(ctx, "test.status")
		require.NoError(t, err, "Failed to subscribe")

		// Publish several messages to create jobs
		for i := 0; i < 5; i++ {
			_, err = b.Publish(ctx, "test.status", []byte(fmt.Sprintf("payload_%d", i)))
			require.NoError(t, err, "Failed to publish message %d", i)
		}

		// Verify all jobs were created
		jobs := getQueueJobs(t, "status_filter_queue")
		require.Len(t, jobs, 5, "Should have 5 jobs")

		// Test the filtering condition: (done_at IS NULL AND expired_at IS NULL AND retry_count = 0)
		// For a job to be INCLUDED in drain, ALL conditions must be true:
		// done_at IS NULL AND expired_at IS NULL AND retry_count = 0

		// Job 0: Set done_at - should NOT be drained (done_at IS NOT NULL)
		_, err = db.Exec("UPDATE goque_jobs SET done_at = NOW() WHERE id = $1", jobs[0].ID)
		require.NoError(t, err, "Failed to mark job 0 as done")

		// Job 1: Set expired_at - should NOT be drained (expired_at IS NOT NULL)
		_, err = db.Exec("UPDATE goque_jobs SET expired_at = NOW() WHERE id = $1", jobs[1].ID)
		require.NoError(t, err, "Failed to mark job 1 as expired")

		// Job 2: Set retry_count != 0 - should NOT be drained (retry_count != 0)
		_, err = db.Exec("UPDATE goque_jobs SET retry_count = 3 WHERE id = $1", jobs[2].ID)
		require.NoError(t, err, "Failed to set job 2 retry_count to 3")

		// Job 3: Set combination that excludes it - should NOT be drained
		_, err = db.Exec("UPDATE goque_jobs SET done_at = NOW(), expired_at = NOW(), retry_count = 5 WHERE id = $1", jobs[3].ID)
		require.NoError(t, err, "Failed to set job 3 to be excluded from drain")

		// Job 4: Default to meet all conditions - should be drained (done_at IS NULL, expired_at IS NULL, retry_count = 0)

		// Run drain - should delete only job 4
		deletedCount, err := sub.Drain(ctx)
		require.NoError(t, err, "Drain should succeed")
		assert.Equal(t, 1, deletedCount, "Should have deleted 1 job that meets the AND condition")

		// Verify jobs 0, 1, 2, 3 remain
		remainingJobs := getQueueJobs(t, "status_filter_queue")
		assert.Len(t, remainingJobs, 4, "Should have 4 jobs remaining")

		// Verify which jobs remain
		remainingIDs := make(map[int]bool)
		for _, job := range remainingJobs {
			remainingIDs[job.ID] = true
		}

		assert.True(t, remainingIDs[jobs[0].ID], "Job 0 (done) should remain")
		assert.True(t, remainingIDs[jobs[1].ID], "Job 1 (expired) should remain")
		assert.True(t, remainingIDs[jobs[2].ID], "Job 2 (retry_count != 0) should remain")
		assert.True(t, remainingIDs[jobs[3].ID], "Job 3 (multiple conditions) should remain")
		assert.False(t, remainingIDs[jobs[4].ID], "Job 4 (meets all conditions) should be deleted")
	})
}

// TestAutoDrainOnUnsubscribe tests that autoDrain automatically cleans up jobs when unsubscribing
func TestAutoDrainOnUnsubscribe(t *testing.T) {
	cleanupAllTables()

	b, err := pgbus.New(db)
	require.NoError(t, err, "Failed to create Bus instance")

	ctx := context.Background()

	t.Run("AutoDrainEnabled", func(t *testing.T) {
		cleanupAllTables()

		queue := b.Queue("auto_drain_enabled_queue")

		// Create subscription with autoDrain enabled
		sub, err := queue.Subscribe(ctx, "test.autodrain",
			bus.WithAutoDrain(true),
		)
		require.NoError(t, err, "Failed to subscribe with autoDrain")

		// Publish some messages to create jobs
		for i := 0; i < 3; i++ {
			_, err = b.Publish(ctx, "test.autodrain", []byte(fmt.Sprintf("payload_%d", i)))
			require.NoError(t, err, "Failed to publish message %d", i)
		}

		// Verify jobs were created
		jobs := getQueueJobs(t, "auto_drain_enabled_queue")
		assert.Len(t, jobs, 3, "Should have 3 jobs before unsubscribe")

		// Unsubscribe - should automatically drain jobs
		err = sub.Unsubscribe(ctx)
		require.NoError(t, err, "Failed to unsubscribe")

		// Verify jobs were automatically drained
		jobsAfter := getQueueJobs(t, "auto_drain_enabled_queue")
		assert.Len(t, jobsAfter, 0, "Jobs should be automatically drained after unsubscribe")
	})

	t.Run("AutoDrainDisabled", func(t *testing.T) {
		cleanupAllTables()

		queue := b.Queue("auto_drain_disabled_queue")

		// Create subscription without autoDrain (default is false)
		sub, err := queue.Subscribe(ctx, "test.no_autodrain")
		require.NoError(t, err, "Failed to subscribe without autoDrain")

		// Publish some messages to create jobs
		for i := 0; i < 3; i++ {
			_, err = b.Publish(ctx, "test.no_autodrain", []byte(fmt.Sprintf("payload_%d", i)))
			require.NoError(t, err, "Failed to publish message %d", i)
		}

		// Verify jobs were created
		jobs := getQueueJobs(t, "auto_drain_disabled_queue")
		assert.Len(t, jobs, 3, "Should have 3 jobs before unsubscribe")

		// Unsubscribe - should NOT automatically drain jobs
		err = sub.Unsubscribe(ctx)
		require.NoError(t, err, "Failed to unsubscribe")

		// Verify jobs were NOT automatically drained
		jobsAfter := getQueueJobs(t, "auto_drain_disabled_queue")
		assert.Len(t, jobsAfter, 3, "Jobs should remain after unsubscribe when autoDrain is disabled")
	})
}
