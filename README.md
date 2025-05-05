# go-bus

[中文版本](./README_ZH.md)

A simple and reliable PostgreSQL-based publish/subscribe message bus system for Go applications. Built on top of [github.com/tnclong/go-que](https://github.com/tnclong/go-que).

## Features

✨ **Flexible topic subscription patterns**: Support for NATS-style topic matching with exact matching, single-level wildcards (`*`), and multi-level wildcards (`>`)  
✨ **Persistent message queue**: PostgreSQL-based storage ensures reliable message delivery  
✨ **Multiple queue support**: Multiple queues can subscribe to the same topic patterns  
✨ **Custom retry strategies**: Each subscription can configure its own message processing retry strategy  
✨ **Message header support**: Support for custom message metadata  
✨ **Context propagation**: Full integration with Go's context package

## Installation

```bash
go get github.com/qor5/go-bus
```

For proper functionality, you need to add the following replace directive to your `go.mod` file:

```go
replace github.com/tnclong/go-que => github.com/molon/go-que v0.0.0-20250504113152-4941cc99f7e9
```

## Quick Start

### Creating a Bus Instance

```go
import (
    "database/sql"
    "github.com/qor5/go-bus/pgbus"
    _ "github.com/lib/pq"
)

// Connect to PostgreSQL
db, err := sql.Open("postgres", "postgres://user:password@localhost/dbname?sslmode=disable")
if err != nil {
    log.Fatalf("Failed to connect to database: %v", err)
}

// Create a new bus instance
bus, err := pgbus.New(db)
if err != nil {
    log.Fatalf("Failed to create bus: %v", err)
}
```

### Creating Subscriptions

```go
ctx := context.Background()

// Get a queue
queue := bus.Queue("my_service_queue")

// Create subscriptions - supporting various patterns
exactSub, err := queue.Subscribe(ctx, "orders.created")                // Exact match
wildcardSub, err := queue.Subscribe(ctx, "products.*.category.*.info") // Single-level wildcard at multiple positions
multiLevelSub, err := queue.Subscribe(ctx, "notifications.>")          // Multi-level wildcard

// Subscription with custom configuration
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

customSub, err := queue.Subscribe(ctx, "payments.processed", bus.WithPlanConfig(customPlan))
```

### Publishing Messages

```go
// Basic publish
err = bus.Publish(ctx, "orders.created", []byte(`{"id": "12345", "total": 99.99}`))

// Publishing with unique ID (for deduplication)
err = bus.Publish(ctx, "orders.created", []byte(`{"id": "12345", "total": 99.99}`), bus.WithUniqueID("order-12345"))

// Publishing with headers
err = bus.Publish(ctx, "orders.created", []byte(`{"id": "12345", "total": 99.99}`), bus.WithHeader(bus.Header{
    "Content-Type": []string{"application/json"},
    "X-Request-ID": []string{"req-123456"},
}))

// Publishing with an Outbound object
outbound := &bus.Outbound{
    Message: bus.Message{
        Subject: "orders.created",
        Header:  bus.Header{"Content-Type": []string{"application/json"}},
        Payload: []byte(`{"id": "12345", "total": 99.99}`),
    },
    UniqueID: bus.UniqueID("order-12345"), // Optional unique ID for message deduplication
}
err = bus.Dispatch(ctx, outbound)

// Publishing multiple messages at once
outbound1 := &bus.Outbound{
    Message: bus.Message{
        Subject: "orders.created",
        Payload: []byte(`{"id": "12345", "total": 99.99}`),
    },
    UniqueID: bus.UniqueID("order-12345"),
}
outbound2 := &bus.Outbound{
    Message: bus.Message{
        Subject: "notifications.sent",
        Payload: []byte(`{"user_id": "user123", "message": "Your order has been created"}`),
    },
    UniqueID: bus.UniqueID("notification-user123-order-created"),
}
// Dispatch supports publishing multiple outbound messages in a single call
err = bus.Dispatch(ctx, outbound1, outbound2)
```

### Consuming Messages

```go
// Basic consumption
consumer, err := queue.StartConsumer(ctx, func(ctx context.Context, msg *bus.Inbound) error {
    fmt.Printf("Received message: subject=%s, payload=%s\n", msg.Subject, string(msg.Payload))

    // Reading headers
    if contentType := msg.Header.Get("Content-Type"); contentType != "" {
        fmt.Printf("Content-Type: %s\n", contentType)
    }

    // Mark message as done after processing
    return msg.Done(ctx)
})
if err != nil {
    log.Fatalf("Failed to start consumer: %v", err)
}
// Ensure consumer is stopped when done
defer consumer.Stop()

// Consumption with custom worker configuration
workerConfig := bus.WorkerConfig{
    MaxLockPerSecond:          5,
    MaxBufferJobsCount:        10,
    MaxPerformPerSecond:       5,
    MaxConcurrentPerformCount: 2,
}

consumer, err := queue.StartConsumer(ctx, func(ctx context.Context, msg *bus.Inbound) error {
    // Process message...

    // If you want to discard the message, use Destroy instead of Done
    return msg.Destroy(ctx)
}, bus.WithWorkerConfig(workerConfig))
if err != nil {
    log.Fatalf("Failed to start consumer with options: %v", err)
}
defer consumer.Stop()
```

### Finding Matching Subscriptions

```go
// Find subscriptions matching a specific subject
subs, err := bus.BySubject(ctx, "orders.created")
for _, sub := range subs {
    fmt.Printf("Queue %s matches with pattern %s\n", sub.Queue(), sub.Pattern())
}

// Get all subscriptions for a specific queue
queueSubs, err := queue.Subscriptions(ctx)
for _, sub := range queueSubs {
    fmt.Printf("Pattern: %s, Created at: %s\n", sub.Pattern(), sub.CreatedAt())
}
```

### Unsubscribing

```go
// Unsubscribe from a specific subscription
err = subscription.Unsubscribe(ctx)
```

## Advanced Usage

### Using Random Queue Names for Distributed Broadcast Reception

In distributed environments (like Kubernetes), when you need to ensure that each instance receives the same broadcast message, you can create queues with random names for each instance. This way, each instance will independently receive the same message, achieving a broadcast effect.

```go
import (
    "github.com/google/uuid"
    "github.com/qor5/go-bus/pgbus"
    "context"
)

// Create a unique queue name for each service instance (like a K8s Pod)
podQueueName := fmt.Sprintf("broadcast-receiver-%s", uuid.New().String())
podQueue := bus.Queue(podQueueName)

// Start consuming messages (starts workers but doesn't block)
consumer, err := podQueue.StartConsumer(ctx, func(ctx context.Context, msg *bus.Inbound) error {
    log.Printf("Instance %s received broadcast message: %s - %s",
        podQueueName, msg.Subject, string(msg.Payload))
    return msg.Done(ctx)
})
if err != nil {
    log.Printf("Failed to start consumer: %v", err)
}
defer consumer.Stop()

// Subscribe to broadcast topics
sub, err := podQueue.Subscribe(ctx, "broadcast.events.>")
if err != nil {
    log.Fatalf("Failed to create broadcast subscription: %v", err)
}
defer func() {
    if err := sub.Unsubscribe(context.Background()); err != nil {
        log.Printf("Failed to unsubscribe: %v", err)
    }
}()

// Other service blocking logic
```

This pattern is particularly useful for:

- Broadcasting configuration changes or system notifications to all service instances
- Ensuring each instance in the cluster independently processes the same message, implementing a reliable broadcast mechanism
- Implementing event-driven system-wide notifications in microservice architectures

Each instance creates a queue with a unique name, so each message is processed independently by each subscribed instance, achieving a true broadcast effect.

## Topic Pattern Explanation

go-bus supports three types of topic matching patterns, following the NATS messaging system style:

1. **Exact Match**: Matches the exact topic string

   - Example: `orders.created` only matches `orders.created`

2. **Single-Level Wildcard (`*`)**: Matches any string in a single level

   - Example: `products.*.category.*.info` matches `products.xyz.category.abc.info` and `products.123.category.456.info`, but not `products.category.info` or `products.xyz.category.abc.def.info`

3. **Multi-Level Wildcard (`>`)**: Matches zero or more levels
   - Example: `orders.>` matches `orders.created`, `orders.updated`, and `orders.items.created`

## Important Notes

### Avoid Overlapping Subscription Patterns

Do not subscribe to potentially overlapping patterns in the same queue. When a message matches multiple subscriptions in a queue, the system will only use the configuration (such as retry strategy) from the earliest created subscription and ignore others.

#### Problem Example

Suppose you create these two subscriptions in the same queue:

```go
// First created subscription - using default configuration
sub1, err := queue.Subscribe(ctx, "orders.>")

// Later created subscription - with custom retry strategy
customPlan := bus.PlanConfig{
    RetryPolicy: que.RetryPolicy{
        MaxRetryCount: 10,
        // Other configurations...
    },
}
sub2, err := queue.Subscribe(ctx, "orders.created", bus.WithPlanConfig(customPlan))
```

When publishing a message with the subject `orders.created`:

- The message matches both patterns: `orders.>` and `orders.created`
- The system will use the configuration from `sub1` (the earlier created `orders.>` subscription)
- The custom retry strategy in `sub2` (MaxRetryCount: 10) will be ignored

#### Correct Approach

To avoid this issue, use different queues for potentially overlapping patterns:

```go
// First queue handles general orders events
queue1 := bus.Queue("orders_general_queue")
sub1, err := queue1.Subscribe(ctx, "orders.>")

// Second queue specifically handles orders.created events with custom configuration
queue2 := bus.Queue("orders_created_queue")
customPlan := bus.PlanConfig{
    RetryPolicy: que.RetryPolicy{
        MaxRetryCount: 10,
        // Other configurations...
    },
}
sub2, err := queue2.Subscribe(ctx, "orders.created", bus.WithPlanConfig(customPlan))
```

This way, the two subscriptions will process messages independently in their respective queues, and each configuration will be effective.

## License

This project is licensed under the [MIT License](LICENSE).

## Acknowledgments

This project is based on [github.com/tnclong/go-que](https://github.com/tnclong/go-que) - a high-performance PostgreSQL backend job queue.
