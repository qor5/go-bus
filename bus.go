package bus

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/pkg/errors"
	"github.com/qor5/go-bus/quex"
	"github.com/qor5/go-que"
)

var _ Queue = (*QueueImpl)(nil)

// QueueImpl implements the Queue interface.
type QueueImpl struct {
	name string
	b    *BusImpl
}

// Subscribe registers the queue to receive messages published to subjects matching the pattern.
func (q *QueueImpl) Subscribe(ctx context.Context, pattern string, opts ...SubscribeOption) (Subscription, error) {
	subscribeOpts := &SubscribeOptions{
		PlanConfig: DefaultPlanConfigFactory(),
	}

	for _, opt := range opts {
		opt(subscribeOpts)
	}

	sub, err := q.b.dialect.Upsert(ctx, q.name, pattern, subscribeOpts)
	if err != nil {
		return nil, err
	}

	return sub, nil
}

// Subscriptions returns all subscriptions for the queue.
func (q *QueueImpl) Subscriptions(ctx context.Context) ([]Subscription, error) {
	return q.b.dialect.ByQueue(ctx, q.name)
}

// StartConsumer starts a new message consumer for this queue.
// The returned Consumer must be stopped by the caller when no longer needed.
// The ctx parameter is only used to manage the startup process, not the Consumer's lifecycle.
func (q *QueueImpl) StartConsumer(ctx context.Context, handler Handler, options ...ConsumeOption) (Consumer, error) {
	opts := &ConsumeOptions{
		WorkerConfig: DefaultWorkerConfigFactory(),
	}

	for _, opt := range options {
		opt(opts)
	}

	workerOptions := que.WorkerOptions{
		Queue:              q.name,
		Mutex:              q.b.dialect.GoQue().Mutex(),
		MaxLockPerSecond:   opts.WorkerConfig.MaxLockPerSecond,
		MaxBufferJobsCount: opts.WorkerConfig.MaxBufferJobsCount,
		Perform: func(ctx context.Context, job que.Job) error {
			inbound, err := InboundFromJob(job)
			if err != nil {
				return err
			}
			return handler(ctx, inbound)
		},
		MaxPerformPerSecond:       opts.WorkerConfig.MaxPerformPerSecond,
		MaxConcurrentPerformCount: opts.WorkerConfig.MaxConcurrentPerformCount,
	}

	workerOpts := []quex.StartWorkerOption{
		quex.WithLogger(q.b.logger),
	}
	if opts.WorkerConfig.ReconnectBackOff != nil {
		workerOpts = append(workerOpts, quex.WithReconnectBackOff(opts.WorkerConfig.ReconnectBackOff))
	}
	controller, err := quex.StartWorker(ctx, workerOptions, workerOpts...)
	if err != nil {
		return nil, err
	}
	return &consumer{controller: controller}, nil
}

// consumer adapts a quex.WorkerController to the Consumer interface
// and ensures consistent error handling, particularly for stopped errors.
type consumer struct {
	controller quex.WorkerController
}

func (w *consumer) Stop(ctx context.Context) error {
	err := w.controller.Stop(ctx)
	if errors.Is(err, quex.ErrWorkerStopped) {
		return errors.WithStack(ErrConsumerStopped)
	}
	return err
}

func (w *consumer) Done() <-chan struct{} {
	return w.controller.Done()
}

func (w *consumer) Err() error {
	err := w.controller.Err()
	if errors.Is(err, quex.ErrWorkerStopped) {
		return errors.WithStack(ErrConsumerStopped)
	}
	return err
}

var _ Bus = (*BusImpl)(nil)

// BusImpl is a generic implementation of the Bus interface.
type BusImpl struct {
	dialect            Dialect
	mu                 sync.RWMutex
	queues             map[string]Queue
	logger             *slog.Logger
	maxEnqueuePerBatch int // Maximum number of plans that can be enqueued in a single transaction
}

// New creates a new Bus instance with the given dialect and options.
//
// dialect is the database dialect used for storing subscriptions.
// Different database backends can be supported by implementing this interface.
// A PostgreSQL implementation is provided in the pgbus package.
func New(dialect Dialect, opts ...BusOption) (Bus, error) {
	busOpts := &BusOptions{
		Migrate: true,
	}

	for _, opt := range opts {
		opt(busOpts)
	}

	if dialect == nil {
		return nil, errors.New("dialect is required")
	}
	if dialect.GoQue() == nil {
		return nil, errors.New("go-que implementation is required")
	}

	// Apply dialect decorator if provided
	if busOpts.DialectDecorator != nil {
		var err error
		dialect, err = busOpts.DialectDecorator(dialect)
		if err != nil {
			return nil, errors.Wrap(err, "failed to apply dialect decorator")
		}
	}

	b := &BusImpl{
		dialect:            dialect,
		queues:             make(map[string]Queue),
		logger:             busOpts.Logger,
		maxEnqueuePerBatch: busOpts.MaxEnqueuePerBatch,
	}

	if b.logger == nil {
		b.logger = slog.Default()
	}

	if busOpts.Migrate {
		if err := dialect.Migrate(context.Background()); err != nil {
			return nil, err
		}
	}

	return b, nil
}

// Queue returns a queue with the specified name.
func (b *BusImpl) Queue(name string) Queue {
	b.mu.RLock()
	if q, ok := b.queues[name]; ok {
		b.mu.RUnlock()
		return q
	}
	b.mu.RUnlock()

	b.mu.Lock()
	defer b.mu.Unlock()

	if q, ok := b.queues[name]; ok {
		return q
	}

	queue := &QueueImpl{
		name: name,
		b:    b,
	}

	b.queues[name] = queue
	return queue
}

// Publish sends a payload to all queues with subscriptions matching the subject.
func (b *BusImpl) Publish(ctx context.Context, subject string, payload []byte, opts ...PublishOption) error {
	publishOpts := &PublishOptions{}
	for _, opt := range opts {
		opt(publishOpts)
	}

	return b.Dispatch(ctx, &Outbound{
		Message: Message{
			Subject: subject,
			Header:  publishOpts.Header,
			Payload: payload,
		},
		UniqueID: publishOpts.UniqueID,
	})
}

// Dispatch sends outbound messages to all queues with subscriptions matching the subject.
// All messages are processed in a single transaction.
func (b *BusImpl) Dispatch(ctx context.Context, msgs ...*Outbound) (xerr error) {
	if len(msgs) == 0 {
		return nil
	}

	var allPlans []que.Plan

	for _, m := range msgs {
		if err := ValidateSubject(m.Subject); err != nil {
			return err
		}

		subscriptions, err := b.BySubject(ctx, m.Subject)
		if err != nil {
			return err
		}

		if len(subscriptions) == 0 {
			continue
		}

		queuesSeen := make(map[string][]string)

		var uniqueID *string
		for _, sub := range subscriptions {
			queueName := sub.Queue()
			pattern := sub.Pattern()

			queuesSeen[queueName] = append(queuesSeen[queueName], pattern)
			if len(queuesSeen[queueName]) > 1 {
				continue
			}

			msgRaw, err := m.Message.ToRaw(sub)
			if err != nil {
				return errors.Wrap(err, "failed to convert message to raw format")
			}

			planConfig := sub.PlanConfig()
			plan := que.Plan{
				Queue:           queueName,
				Args:            que.Args(msgRaw),
				RunAt:           time.Now().Add(planConfig.RunAtDelta),
				RetryPolicy:     planConfig.RetryPolicy,
				UniqueLifecycle: planConfig.UniqueLifecycle,
			}

			if planConfig.UniqueLifecycle != que.Ignore {
				if uniqueID == nil {
					f := m.UniqueID
					if f == nil {
						f = FallbackUniqueID
					}
					id := f(m)
					if id == "" {
						return errors.New("unique id is required")
					}
					uniqueID = &id
				}
				plan.UniqueID = uniqueID
			}

			allPlans = append(allPlans, plan)
		}

		for queue, patterns := range queuesSeen {
			if len(patterns) > 1 {
				b.logger.Error("queue has overlapping patterns; only the first subscription will be triggered",
					"queue", queue,
					"subject", m.Subject,
					"patterns", patterns,
					"error", ErrOverlappingPatterns)
			}
		}
	}

	if len(allPlans) == 0 {
		return nil
	}

	tx, err := b.dialect.BeginTx(ctx, nil)
	if err != nil {
		return errors.Wrap(err, "failed to begin transaction")
	}

	panicked := true
	defer func() {
		if panicked || xerr != nil {
			_ = tx.Rollback()
		}
	}()

	err = func() error {
		batchSize := DefaultMaxEnqueuePerBatch
		if b.maxEnqueuePerBatch > 0 {
			batchSize = b.maxEnqueuePerBatch
		}

		ctx := que.WithSkipConflict(ctx)
		goq := b.dialect.GoQue()
		for i := 0; i < len(allPlans); i += batchSize {
			end := i + batchSize
			if end > len(allPlans) {
				end = len(allPlans)
			}

			batch := allPlans[i:end]
			if _, err := goq.Enqueue(ctx, tx, batch...); err != nil {
				return errors.Wrap(err, "failed to enqueue jobs batch")
			}
		}
		return nil
	}()
	panicked = false
	if err != nil {
		return err
	}

	if err = tx.Commit(); err != nil {
		return errors.Wrap(err, "failed to commit transaction")
	}

	return nil
}

// BySubject returns all subscriptions with patterns matching the given subject.
func (b *BusImpl) BySubject(ctx context.Context, subject string) ([]Subscription, error) {
	return b.dialect.BySubject(ctx, subject)
}

// CleanupExpiredSubscriptions removes subscriptions that have exceeded their TTL.
func (b *BusImpl) CleanupExpiredSubscriptions(ctx context.Context) (int64, error) {
	return b.dialect.CleanupExpiredSubscriptions(ctx)
}
