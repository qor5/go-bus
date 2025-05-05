package bus_test

import (
	"context"
	"database/sql"
	"log/slog"
	"testing"
	"time"

	"github.com/qor5/go-bus"
	"github.com/qor5/go-bus/pgbus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/theplant/testenv"
	"github.com/tnclong/go-que"
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

	// Ensure all potentially existing dependent tables are cleaned up before tests start
	cleanupAllTables()

	m.Run()
}

// cleanupAllTables safely drops all test-related tables with CASCADE option
// to handle any dependencies. Errors are intentionally ignored as tables
// might not exist during first run.
func cleanupAllTables() {
	// Safely drop tables, ignoring errors for non-existent tables
	_, _ = db.Exec("DROP TABLE IF EXISTS goque_jobs CASCADE")
	_, _ = db.Exec("DROP TABLE IF EXISTS gobus_subscriptions CASCADE")
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
		customPlan := bus.PlanConfig{
			RunAtDelta: 200 * time.Millisecond,
			RetryPolicy: que.RetryPolicy{
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

		// Verify same subscription ID (still an update, not a new subscription)
		assert.Equal(t, initialID, sub3.ID(), "Subscription ID should remain the same after PlanConfig update")

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

		err = b.Publish(ctx, "orders.new", []byte(`["test_payload"]`))
		assert.NoError(t, err, "Valid publish should not error")

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

		err = b.Publish(ctx, "orders.item123.processed", []byte(`["wildcard_payload"]`))
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
		assert.Contains(t, msg.Pattern, "orders.*", "Message pattern should contain the matching pattern")
	})

	t.Run("MultiLevelWildcardMatch", func(t *testing.T) {
		// Clear previous jobs
		cleanupJobs(t)

		err = b.Publish(ctx, "notifications.user.login", []byte(`["multilevel_payload"]`))
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
		assert.Equal(t, "notifications.>", msg.Pattern, "Message pattern should be the matching pattern")
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

		err = b.Dispatch(ctx, outbound)
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
		err = b.Dispatch(ctx, ordersMsg, notificationMsg, orderProcessedMsg)
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
		err = b.Dispatch(ctx)
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
		err = b.Dispatch(ctx, validMsg, invalidMsg)
		assert.Error(t, err, "Batch Dispatch with invalid message should error")

		// Verify no jobs were created due to transaction rollback
		queue1Jobs := getQueueJobs(t, testQueue)
		assert.Equal(t, 0, len(queue1Jobs), "Should not create any jobs when batch contains invalid message")
	})

	t.Run("NoMatchingSubscriptions", func(t *testing.T) {
		// Clear previous jobs
		cleanupJobs(t)

		// Depending on the implementation, this may or may not return an error
		_ = b.Publish(ctx, "unknown.topic", []byte(`["test_payload"]`))

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
		err = b.Publish(ctx, "orders.new", []byte{})
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
	defer func() { _ = consumer.Stop() }()

	// Publish a message with header
	testPayload := []byte(`["consume_test_payload"]`)
	// Use mixed case header keys to test canonicalization
	testHeader := bus.Header{
		"test-header":     []string{"test"},
		"Content-Type":    []string{"application/json"},
		"x-custom-HEADER": []string{"value"},
	}
	err = b.Publish(ctx, "test.topic", testPayload, bus.WithHeader(testHeader))
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
	workerConfig := bus.WorkerConfig{
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
	defer func() { _ = consumer.Stop() }()

	// Publish a message
	testPayload := []byte(`["options_test_payload"]`)
	err = b.Publish(ctx, "test.options", testPayload)
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
	workerConf := bus.WorkerConfig{
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
	defer func() { _ = consumer1.Stop() }()

	consumer2, err := queue.StartConsumer(ctx, func(ctx context.Context, msg *bus.Inbound) error {
		t.Logf("queue consumer2 received message")
		queueMsgCh <- msg
		return msg.Done(ctx)
	}, bus.WithWorkerConfig(workerConf))
	require.NoError(t, err, "Failed to start second consumer on queue")
	defer func() { _ = consumer2.Stop() }()

	// Start one consumer on the second queue
	consumer3, err := queue2.StartConsumer(ctx, func(ctx context.Context, msg *bus.Inbound) error {
		t.Logf("queue2 consumer received message")
		queue2MsgCh <- msg
		return msg.Done(ctx)
	}, bus.WithWorkerConfig(workerConf))
	require.NoError(t, err, "Failed to start consumer on queue2")
	defer func() { _ = consumer3.Stop() }()

	// Verify all consumers can be started
	assert.NotNil(t, consumer1, "First consumer should not be nil")
	assert.NotNil(t, consumer2, "Second consumer should not be nil")
	assert.NotNil(t, consumer3, "Third consumer on queue2 should not be nil")

	// Publish a message to the test topic
	testPayload := []byte(`["multiconsumer_test"]`)
	err = b.Publish(ctx, testTopic, testPayload)
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
	customPlan := bus.PlanConfig{
		RunAtDelta: 500 * time.Millisecond,
		RetryPolicy: que.RetryPolicy{
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
	workerConf := bus.WorkerConfig{
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
	defer func() { _ = consumer1.Stop() }()
	t.Logf("[%s] Consumer for queue1 setup", time.Since(startTime))

	consumer2, err := queue2.StartConsumer(ctx, func(ctx context.Context, msg *bus.Inbound) error {
		t.Logf("[%s] Queue2 received message", time.Since(startTime))
		msgCh2 <- msg
		return msg.Done(ctx)
	}, bus.WithWorkerConfig(workerConf))
	require.NoError(t, err, "Failed to start consumer for queue2")
	defer func() { _ = consumer2.Stop() }()
	t.Logf("[%s] Consumer for queue2 setup", time.Since(startTime))

	consumer3, err := queue3.StartConsumer(ctx, func(ctx context.Context, msg *bus.Inbound) error {
		t.Logf("[%s] Queue3 received message", time.Since(startTime))
		msgCh3 <- msg
		return msg.Done(ctx)
	}, bus.WithWorkerConfig(workerConf))
	require.NoError(t, err, "Failed to start consumer for queue3")
	defer func() { _ = consumer3.Stop() }()
	t.Logf("[%s] Consumer for queue3 setup", time.Since(startTime))

	consumer4, err := queue4.StartConsumer(ctx, func(ctx context.Context, msg *bus.Inbound) error {
		t.Logf("[%s] Queue4 received message", time.Since(startTime))
		msgCh4 <- msg
		return msg.Done(ctx)
	}, bus.WithWorkerConfig(workerConf))
	require.NoError(t, err, "Failed to start consumer for queue4")
	defer func() { _ = consumer4.Stop() }()
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
	err = b.Dispatch(ctx, &bus.Outbound{
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
		assert.Equal(t, "event.*.created", msg.Pattern, "Pattern mismatch in queue1")
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
		assert.Equal(t, "event.user.*", msg.Pattern, "Pattern mismatch in queue2")
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
		assert.Equal(t, "event.>", msg.Pattern, "Pattern mismatch in queue3")
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
	err = b.Publish(ctx, testSubject, testPayload)
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
