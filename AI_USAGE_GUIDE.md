# go-bus: AI Usage Guide

## Overview

PostgreSQL-based pub/sub message bus with NATS-style pattern matching, persistent queues, and customizable retry policies. Built on [go-que](https://github.com/tnclong/go-que).

**Core packages**: `bus` (interfaces), `pgbus` (PostgreSQL implementation), `quex` (worker utilities)

## Quick Reference

### Setup

```go
db, _ := sql.Open("postgres", "postgres://...")
bus, _ := pgbus.New(db)
defer bus.Close()       // Always close to release resources
queue := bus.Queue("my-queue")
```

### Pattern Matching

- Exact: `"orders.created"` → matches `"orders.created"` only
- Single wildcard: `"orders.*.status"` → matches `"orders.123.status"`
- Multi wildcard: `"orders.>"` → matches `"orders.created"`, `"orders.items.added"`, etc.

### Core Types

- `bus.Bus` - Message bus coordinator
- `bus.Queue` - Named queue for subscriptions
- `bus.Subscription` - Active subscription to a pattern
- `bus.Inbound` - Received message (call `Done(ctx)` or `Destroy(ctx)`)
- `bus.Outbound` - Message to publish
- `bus.Handler` - `func(ctx context.Context, msg *Inbound) error`

## Usage Examples

### 1. Basic Pub/Sub

```go
// Subscribe
queue.Subscribe(ctx, "events.>")

// Consume
consumer, _ := queue.StartConsumer(ctx, func(ctx context.Context, msg *bus.Inbound) error {
    log.Printf("Got: %s", msg.Subject)
    return msg.Done(ctx)
})
defer consumer.Stop(ctx)

// Publish
type UserEvent struct {
    ID string `json:"id"`
}

bus.Publish(ctx, &bus.Outbound{
    Message: bus.Message{
        Subject: "events.user.created",
        Payload: UserEvent{ID: "123"},
    },
})
```

### 2. Multiple Queues (Fan-out)

```go
// Different services subscribe to same events
ordersQueue := bus.Queue("orders-service")
ordersQueue.Subscribe(ctx, "user.>")

notificationsQueue := bus.Queue("notifications-service")
notificationsQueue.Subscribe(ctx, "user.>")

// Both queues receive this message
bus.Publish(ctx, &bus.Outbound{
    Message: bus.Message{
        Subject: "user.registered",
        Payload: json.RawMessage(`{"email":"user@example.com"}`),
    },
})
```

### 3. Selective Routing with Wildcards

```go
// Payment service only cares about payment events
paymentQueue := bus.Queue("payment-service")
paymentQueue.Subscribe(ctx, "payment.>")

// Analytics service wants all order and payment events
analyticsQueue := bus.Queue("analytics-service")
analyticsQueue.Subscribe(ctx, "order.>")
analyticsQueue.Subscribe(ctx, "payment.>")

bus.Publish(ctx, &bus.Outbound{Message: bus.Message{Subject: "payment.completed", Payload: data}}) // → payment-service, analytics-service
bus.Publish(ctx, &bus.Outbound{Message: bus.Message{Subject: "order.shipped", Payload: data}})     // → analytics-service only
```

### 4. Custom Retry Policy (Critical Jobs)

```go
criticalPlan := &bus.PlanConfig{
    RunAtDelta: 0, // Immediate
    RetryPolicy: &que.RetryPolicy{
        InitialInterval:        5 * time.Second,
        MaxInterval:            2 * time.Minute,
        NextIntervalMultiplier: 2.0,
        MaxRetryCount:          10,
    },
    UniqueLifecycle: que.Always, // Deduplicate
}

queue.Subscribe(ctx, "payment.>", bus.WithPlanConfig(criticalPlan))
```

### 5. Delayed Processing

```go
delayedPlan := &bus.PlanConfig{
    RunAtDelta: 5 * time.Minute, // Wait 5 minutes before processing
    RetryPolicy: bus.DefaultRetryPolicyFactory(),
}

queue.Subscribe(ctx, "reminder.>", bus.WithPlanConfig(delayedPlan))
bus.Publish(ctx, &bus.Outbound{
    Message: bus.Message{
        Subject: "reminder.send",
        Payload: map[string]string{"user": "123"},
    },
})
// Message won't be processed for 5 minutes
```

### 6. Broadcast to All Instances (Ephemeral Queues)

```go
// Each pod/instance creates unique queue
podQueue := bus.Queue(fmt.Sprintf("cache-invalidator-%s", uuid.New()))

sub, _ := podQueue.Subscribe(ctx, "cache.invalidate.>",
    bus.WithTTL(5*time.Minute),
    bus.WithAutoDrain(true))
defer sub.Unsubscribe(ctx)

// Keep alive with heartbeat
go func() {
    ticker := time.NewTicker(1 * time.Minute)
    defer ticker.Stop()
    for range ticker.C {
        sub.Heartbeat(ctx)
    }
}()

consumer, _ := podQueue.StartConsumer(ctx, func(ctx context.Context, msg *bus.Inbound) error {
    // All instances receive and process this
    cache.Invalidate(msg.Payload)
    return msg.Done(ctx)
})
defer consumer.Stop(ctx)
```

### 7. Batch Publish (Transactional)

```go
// All-or-nothing: all messages succeed or all fail
msgs := []*bus.Outbound{
    {
        Message: bus.Message{
            Subject: "order.created",
            Payload: map[string]string{"order_id": "123"},
        },
        UniqueID: bus.UniqueID("order-123"),
    },
    {
        Message: bus.Message{
            Subject: "inventory.reserved",
            Payload: json.RawMessage(`{"items":["..."]}`),
        },
        UniqueID: bus.UniqueID("inventory-123"),
    },
    {
        Message: bus.Message{
            Subject: "notification.queued",
            Payload: map[string]string{"user": "456"},
        },
    },
}

dispatch, _ := bus.Publish(ctx, msgs...)
log.Printf("Published %d messages", dispatch.ExecutedCount())
```

### 8. Message Deduplication

```go
// Prevent duplicate processing with UniqueID
for i := 0; i < 3; i++ {
    bus.Publish(ctx, &bus.Outbound{
        Message: bus.Message{
            Subject: "payment.process",
            Payload: map[string]string{"payment_id": "pay-123"},
        },
        UniqueID: bus.UniqueID("pay-123"),
    })
}
// Only one job created due to deduplication
```

### 9. Message Headers (Metadata)

```go
// Publish with headers
bus.Publish(ctx, &bus.Outbound{
    Message: bus.Message{
        Subject: "order.created",
        Header: bus.Header{
            "Content-Type":  []string{"application/json"},
            "X-Request-ID":  []string{"req-123"},
            "X-User-ID":     []string{"user-456"},
        },
        Payload: map[string]string{"order_id": "123"},
    },
})

// Read headers in consumer
consumer, _ := queue.StartConsumer(ctx, func(ctx context.Context, msg *bus.Inbound) error {
    requestID := msg.Header.Get("X-Request-ID")
    userID := msg.Header.Get("X-User-ID")
    log.Printf("Processing request %s for user %s", requestID, userID)
    return msg.Done(ctx)
})
```

### 10. Error Handling & Retry

```go
queue.StartConsumer(ctx, func(ctx context.Context, msg *bus.Inbound) error {
    var data OrderData
    if err := json.Unmarshal(msg.Payload, &data); err != nil {
        // Malformed message - discard without retry
        log.Printf("Invalid payload: %v", err)
        return msg.Destroy(ctx)
    }

    if err := processOrder(data); err != nil {
        if isTemporaryError(err) {
            // Retry according to subscription's retry policy
            return err
        }
        // Permanent failure - discard
        log.Printf("Permanent error: %v", err)
        return msg.Destroy(ctx)
    }

    // Success
    return msg.Done(ctx)
})
```

### 11. Performance Tuning

```go
workerConfig := &bus.WorkerConfig{
    MaxLockPerSecond:          10,   // Poll DB 10x/sec
    MaxBufferJobsCount:        50,   // Buffer 50 jobs
    MaxPerformPerSecond:       500,  // Process up to 500/sec
    MaxConcurrentPerformCount: 100,  // 100 concurrent handlers
}

consumer, _ := queue.StartConsumer(ctx, handler,
    bus.WithWorkerConfig(workerConfig))
```

### 12. Caching for High-Throughput

```go
// Default: Bus includes built-in cache automatically
bus, _ := pgbus.New(db)
defer bus.Close()

// Custom cache (if you need to share cache across instances)
bus, _ := pgbus.New(db, bus.WithCache(cache))
defer bus.Close()

// Disable cache if needed
bus, _ := pgbus.New(db, bus.WithoutCache())
defer bus.Close()
```

### 13. Monitoring & Debugging

```go
// Find which queues will receive a message
subs, _ := bus.BySubject(ctx, "order.created")
for _, sub := range subs {
    log.Printf("Queue %s will receive (pattern: %s)",
        sub.Queue(), sub.Pattern())
}

// List all subscriptions for a queue
allSubs, _ := queue.Subscriptions(ctx)
for _, sub := range allSubs {
    log.Printf("Subscribed to: %s (ID: %s)",
        sub.Pattern(), sub.ID())
}

// Check publish results
dispatch, _ := bus.Publish(ctx, &bus.Outbound{Message: bus.Message{Subject: "test.event", Payload: data}})
log.Printf("Matched: %d, Executed: %d, Skipped: %d",
    dispatch.MatchedCount(),
    dispatch.ExecutedCount(),
    len(dispatch.SkippedByConflict()))
```

### 14. Graceful Shutdown

```go
func main() {
    ctx, cancel := context.WithCancel(context.Background())
    defer cancel()

    // Setup
    bus, _ := pgbus.New(db)
    defer bus.Close() // Close bus to release cache resources

    queue := bus.Queue("my-service")
    queue.Subscribe(ctx, "events.>")

    consumer, _ := queue.StartConsumer(ctx, handler)

    // Handle shutdown
    sigChan := make(chan os.Signal, 1)
    signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

    <-sigChan
    log.Println("Shutting down...")

    // Stop consumer with timeout
    shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 30*time.Second)
    defer shutdownCancel()

    if err := consumer.Stop(shutdownCtx); err != nil {
        log.Printf("Error stopping consumer: %v", err)
    }
}
```

### 15. Saga Pattern (Distributed Transaction)

```go
// Orchestrator publishes saga steps
saga := []*bus.Outbound{
    {Message: bus.Message{Subject: "saga.reserve-inventory", Payload: orderData}},
    {Message: bus.Message{Subject: "saga.charge-payment", Payload: paymentData}},
    {Message: bus.Message{Subject: "saga.send-confirmation", Payload: notifData}},
}
bus.Publish(ctx, saga...)

// Each service subscribes to its step
inventoryQueue.Subscribe(ctx, "saga.reserve-inventory")
paymentQueue.Subscribe(ctx, "saga.charge-payment")
notificationQueue.Subscribe(ctx, "saga.send-confirmation")

// On failure, publish compensation events
if err := reserveInventory(); err != nil {
    bus.Publish(ctx, &bus.Outbound{Message: bus.Message{Subject: "saga.compensate.release-inventory", Payload: data}})
    return msg.Destroy(ctx)
}
```

## Key Configuration

### Bus Options

- `bus.WithLogger(logger)` - Custom logger
- `bus.WithMigrate(false)` - Skip auto-migration
- `bus.WithMaxEnqueuePerBatch(n)` - Batch size (default: 100)
- `bus.WithCache(cache)` - Custom cache (default: built-in Ristretto cache)
- `bus.WithoutCache()` - Disable caching
- `bus.WithDialectDecorator(...)` - Add metrics/logging decorators

### Subscribe Options

- `bus.WithPlanConfig(config)` - Retry policy, delays, deduplication
- `bus.WithTTL(duration)` - Auto-expire subscription
- `bus.WithAutoDrain(true)` - Auto-cleanup on unsubscribe

## Common Pitfalls

1. **Overlapping patterns in same queue**: Use separate queues for `"orders.>"` and `"orders.created"`
2. **Missing acknowledgment**: Always call `msg.Done(ctx)` or `msg.Destroy(ctx)`
3. **Consumer lifecycle**: Call `consumer.Stop(ctx)` in cleanup
4. **Bus lifecycle**: Always call `bus.Close()` to release cache resources
5. **Ephemeral queues**: Use `WithAutoDrain(true)` and call `Unsubscribe()`
6. **TTL subscriptions**: Call `Heartbeat()` periodically
7. **Subject validation**: Only lowercase, no wildcards in subjects (patterns can have wildcards)

## Dependencies

- PostgreSQL 9.5+
- `github.com/jackc/pgx/v5/stdlib` or compatible driver
- `github.com/qor5/go-que`
- Optional: `github.com/dgraph-io/ristretto/v2` (caching)

## Defaults

- Retry: 3 attempts, 30s→600s exponential backoff
- Worker: 5 locks/sec, 200 concurrent jobs
- Pattern limit: 16 tokens max
