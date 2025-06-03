# go-bus

一个基于 PostgreSQL 的简单可靠发布/订阅消息总线系统，为 Go 应用程序提供可靠的消息传递功能。基于 [tnclong/go-que](https://github.com/tnclong/go-que) 实现。

## 特性

✨ **灵活的主题订阅模式**：支持 NATS 风格的主题匹配，包括精确匹配、单级通配符 (`*`) 和多级通配符 (`>`)  
✨ **持久化消息队列**：基于 PostgreSQL 存储，确保消息可靠传递  
✨ **多队列支持**：允许多个队列订阅相同主题模式  
✨ **自定义重试策略**：每个订阅都可配置自己的消息处理重试策略  
✨ **消息头部支持**：支持自定义消息元数据  
✨ **上下文传递**：与 Go 的 context 包完全集成

## 安装

```bash
go get github.com/qor5/go-bus
```

## 快速开始

### 创建总线实例

```go
import (
    "database/sql"
    "github.com/qor5/go-bus/pgbus"
    _ "github.com/lib/pq"
)

// 连接到 PostgreSQL
db, err := sql.Open("postgres", "postgres://user:password@localhost/dbname?sslmode=disable")
if err != nil {
    log.Fatalf("连接数据库失败: %v", err)
}

// 创建总线实例
bus, err := pgbus.New(db)
if err != nil {
    log.Fatalf("创建总线失败: %v", err)
}
```

### 消费消息

```go
// 获取队列
queue := bus.Queue("my_service_queue")

// 基本消费
consumer, err := queue.StartConsumer(ctx, func(ctx context.Context, msg *bus.Inbound) error {
    fmt.Printf("收到消息: 主题=%s, 载荷=%s\n", msg.Subject, string(msg.Payload))

    // 读取头部信息
    if contentType := msg.Header.Get("Content-Type"); contentType != "" {
        fmt.Printf("Content-Type: %s\n", contentType)
    }

    // 处理完成后标记消息为已完成
    return msg.Done(ctx)
})
if err != nil {
    log.Fatalf("启动消费者失败: %v", err)
}
// 确保在完成后停止消费者
defer consumer.Stop(context.Background())

// 带自定义配置的消费
workerConfig := bus.WorkerConfig{
    MaxLockPerSecond:          5,
    MaxBufferJobsCount:        10,
    MaxPerformPerSecond:       5,
    MaxConcurrentPerformCount: 2,
}

consumer, err := queue.StartConsumer(ctx, func(ctx context.Context, msg *bus.Inbound) error {
    // 处理消息...
    fmt.Printf("处理消息: %s\n", string(msg.Payload))

    // 如果需要丢弃消息，可以使用 Destroy 而不是 Done
    return msg.Destroy(ctx)
}, bus.WithWorkerConfig(workerConfig))
if err != nil {
    log.Fatalf("启动自定义消费者失败: %v", err)
}
defer consumer.Stop(context.Background())
```

### 创建订阅

```go
// 创建订阅 - 支持多种模式
exactSub, err := queue.Subscribe(ctx, "orders.created")                // 精确匹配
wildcardSub, err := queue.Subscribe(ctx, "products.*.category.*.info") // 单级通配符支持多处匹配
multiLevelSub, err := queue.Subscribe(ctx, "notifications.>")          // 多级通配符

// 带自定义配置的订阅
customPlan := bus.PlanConfig{
    RunAtDelta: 200 * time.Millisecond,
    RetryPolicy: &que.RetryPolicy{
        InitialInterval:        2 * time.Second,
        MaxInterval:            20 * time.Second,
        NextIntervalMultiplier: 2.0,
        IntervalRandomPercent:  20,
        MaxRetryCount:          5,
    },
}

customSub, err := queue.Subscribe(ctx, "payments.processed", bus.WithPlanConfig(&customPlan))

// 取消特定订阅
// 这个方法通常是确认不需要订阅的时候才执行，并非应该伴随程序退出执行。因为 go-bus 设计上是支持离线消息的。
err = customSub.Unsubscribe(ctx)
```

### 发布消息

```go
// 基本发布
_, err = bus.Publish(ctx, "orders.created", []byte(`{"id": "12345", "total": 99.99}`))

// 带唯一ID的发布（用于消息去重）
_, err = bus.Publish(ctx, "orders.created", []byte(`{"id": "12345", "total": 99.99}`), bus.WithUniqueID("order-12345"))

// 带头部信息的发布
_, err = bus.Publish(ctx, "orders.created", []byte(`{"id": "12345", "total": 99.99}`), bus.WithHeader(bus.Header{
    "Content-Type": []string{"application/json"},
    "X-Request-ID": []string{"req-123456"},
}))

// 使用 Outbound 对象发布
outbound := &bus.Outbound{
    Message: bus.Message{
        Subject: "orders.created",
        Header:  bus.Header{"Content-Type": []string{"application/json"}},
        Payload: []byte(`{"id": "12345", "total": 99.99}`),
    },
    UniqueID: bus.UniqueID("order-12345"), // 可选的唯一ID，用于消息去重
}
_, err = bus.Dispatch(ctx, outbound)

// 一次性发布多条消息
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
        Payload: []byte(`{"user_id": "user123", "message": "您的订单已创建"}`),
    },
    UniqueID: bus.UniqueID("notification-user123-order-created"),
}
// Dispatch 方法支持在一次调用中发布多个 outbound 消息
_, err = bus.Dispatch(ctx, outbound1, outbound2)
```

### 查找匹配订阅

```go
// 查找匹配特定主题的所有订阅
subs, err := bus.BySubject(ctx, "orders.created")
for _, sub := range subs {
    fmt.Printf("队列 %s 匹配通过模式 %s\n", sub.Queue(), sub.Pattern())
}

// 获取特定队列的所有订阅
queueSubs, err := queue.Subscriptions(ctx)
for _, sub := range queueSubs {
    fmt.Printf("模式: %s, ID: %s\n", sub.Pattern(), sub.ID())
}
```

## 高级用法

### 使用消费者实例专属队列名实现分布式广播接收

在分布式环境（如 Kubernetes）中，当需要确保集群中的每个实例都能接收到相同的广播消息时，可以为每个实例创建具有消费者实例专属名称的队列。这样，每个实例都能独立接收到相同的消息，从而实现广播效果。

```go
import (
    "github.com/google/uuid"
    "github.com/qor5/go-bus/pgbus"
    "context"
)

// 为每个服务实例（如K8s Pod）创建唯一队列名
podQueueName := fmt.Sprintf("broadcast-receiver-%s", uuid.New().String())
podQueue := bus.Queue(podQueueName)

// 开始消费消息
consumer, err := podQueue.StartConsumer(ctx, func(ctx context.Context, msg *bus.Inbound) error {
    log.Printf("实例 %s 收到广播消息: %s - %s",
        podQueueName, msg.Subject, string(msg.Payload))
    return msg.Destory(ctx)
})
if err != nil {
    log.Printf("启动消费者失败: %v", err)
}
defer consumer.Stop(context.Background())

// 订阅广播主题
// 使用 bus.WithAutoDrain(true) 确保当取消订阅时自动清理所有待处理的作业
// 这对于临时队列来说非常重要，避免留下无用的任务
sub, err := podQueue.Subscribe(ctx, "broadcast.events.>", bus.WithAutoDrain(true))
if err != nil {
    log.Fatalf("创建广播订阅失败: %v", err)
}
defer func() {
    // 因为 podQueue 是一次性的，所以应该在程序退出时即刻取消订阅
    if err := sub.Unsubscribe(context.Background()); err != nil {
        log.Printf("取消订阅失败: %v", err)
    }
}()

// 其他服务阻塞逻辑
```

这种模式特别适用于：

- 需要向所有服务实例广播配置更改或系统通知
- 确保集群中的每个实例都能独立处理相同的消息，实现可靠的广播机制
- 在微服务架构中实现事件驱动的系统范围通知

每个实例创建的队列名都是唯一的，因此每条消息会被每个订阅的实例独立处理一次，实现真正的广播效果。

## 主题模式说明

go-bus 支持三种主题匹配模式，遵循 NATS 消息系统的风格：

1. **精确匹配**：完全匹配主题字符串

   - 例如：`orders.created` 只匹配 `orders.created`

2. **单级通配符 (`*`)**：匹配一个层级的任意字符串

   - 例如：`products.*.category.*.info` 匹配 `products.xyz.category.abc.info` 和 `products.123.category.456.info`，但不匹配 `products.category.info` 或 `products.xyz.category.abc.def.info`

3. **多级通配符 (`>`)**：匹配零个或多个层级
   - 例如：`orders.>` 匹配 `orders.created`、`orders.updated` 和 `orders.items.created`

## 注意事项

### 避免重叠的订阅模式

不要为同一个队列订阅可能重叠的模式（pattern）。当一条消息匹配到一个队列的多个订阅时，系统只会使用创建时间最早的那个订阅的配置（例如重试策略等），而忽略其他配置。

#### 问题示例

假设在同一个队列中创建了以下两个订阅：

```go
// 首先创建的订阅 - 使用默认配置
sub1, err := queue.Subscribe(ctx, "orders.>")

// 后来创建的订阅 - 使用自定义重试策略
customPlan := bus.PlanConfig{
    RetryPolicy: &que.RetryPolicy{
        MaxRetryCount: 10,
        // 其他配置...
    },
}
sub2, err := queue.Subscribe(ctx, "orders.created", bus.WithPlanConfig(&customPlan))
```

当发布一条 `orders.created` 主题的消息时：

- 这条消息同时匹配 `orders.>` 和 `orders.created` 两个模式
- 系统将使用较早创建的 `sub1`（`orders.>`）的默认配置
- `sub2` 中配置的自定义重试策略（MaxRetryCount: 10）将被忽略

#### 正确做法

为避免上述问题，应该使用不同的队列来处理可能重叠的模式：

```go
// 第一个队列处理通用的 orders 事件
queue1 := bus.Queue("orders_general_queue")
sub1, err := queue1.Subscribe(ctx, "orders.>")

// 第二个队列专门处理 orders.created 事件，使用自定义配置
queue2 := bus.Queue("orders_created_queue")
customPlan := bus.PlanConfig{
    RetryPolicy: &que.RetryPolicy{
        MaxRetryCount: 10,
        // 其他配置...
    },
}
sub2, err := queue2.Subscribe(ctx, "orders.created", bus.WithPlanConfig(&customPlan))
```

这样，两个订阅会分别在各自的队列中独立处理消息，各自的配置都能生效。

## 许可证

本项目采用 [MIT 许可证](LICENSE) 授权。

## 致谢

本项目基于 [tnclong/go-que](https://github.com/tnclong/go-que) - 一个高性能的 PostgreSQL 后端作业队列
