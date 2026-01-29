package bus

import (
	"context"
	"fmt"
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
	subscribeOpts := &SubscribeOptions{}

	for _, opt := range opts {
		opt(subscribeOpts)
	}

	if subscribeOpts.PlanConfig == nil {
		subscribeOpts.PlanConfig = DefaultPlanConfigFactory()
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
	opts := &ConsumeOptions{}

	for _, opt := range options {
		opt(opts)
	}

	if opts.WorkerConfig == nil {
		opts.WorkerConfig = DefaultWorkerConfigFactory()
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
	return quex.StartWorker(ctx, workerOptions, workerOpts...)
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

func descOfSubscription(sub Subscription) string {
	return fmt.Sprintf("(queue: %s, pattern: %s, id: %s)", sub.Queue(), sub.Pattern(), sub.ID())
}

// Publish sends outbound messages to all queues with subscriptions matching the subject.
// All messages are processed in a single transaction.
func (b *BusImpl) Publish(ctx context.Context, msgs ...*Outbound) (_ *Dispatch, xerr error) {
	if len(msgs) == 0 {
		return &Dispatch{
			Executions: []*SubscriptionExecution{},
		}, nil
	}

	now := time.Now()

	var allExecutions []*SubscriptionExecution
	for _, m := range msgs {
		if err := ValidateSubject(m.Subject); err != nil {
			return nil, err
		}

		subscriptions, err := b.BySubject(ctx, m.Subject)
		if err != nil {
			return nil, err
		}

		if len(subscriptions) == 0 {
			continue
		}

		queuesSeen := make(map[string][]Subscription)

		var uniqueID *string
		for _, sub := range subscriptions {
			execution := &SubscriptionExecution{
				Subscription: sub,
			}
			allExecutions = append(allExecutions, execution)

			queueName := sub.Queue()
			queuesSeen[queueName] = append(queuesSeen[queueName], sub)
			// Only execute the first subscription per queue (handle overlaps)
			if len(queuesSeen[queueName]) > 1 {
				execution.Status = ExecutionStatusSkippedOverlap
				continue
			}

			execution.Status = ExecutionStatusExecuted

			msgRaw, err := m.Message.ToRaw(sub)
			if err != nil {
				return nil, errors.Wrapf(err, "failed to convert message to raw format for subscription %s: %v", descOfSubscription(sub), m.Message)
			}

			planConfig := sub.PlanConfig()
			if planConfig == nil {
				return nil, errors.Errorf("plan config is required for subscription %s", descOfSubscription(sub))
			}
			var retryPolicy que.RetryPolicy
			if planConfig.RetryPolicy != nil {
				retryPolicy = *planConfig.RetryPolicy
			}
			plan := que.Plan{
				Queue:           queueName,
				Args:            que.Args(msgRaw),
				RunAt:           now.Add(planConfig.RunAtDelta),
				RetryPolicy:     retryPolicy,
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
						return nil, errors.New("unique id is required")
					}
					uniqueID = &id
				}
				plan.UniqueID = uniqueID
			}

			execution.Plan = &plan
		}

		// Log overlap warnings
		for queue, subs := range queuesSeen {
			if len(subs) > 1 {
				patterns := make([]string, len(subs))
				for i, sub := range subs {
					patterns[i] = sub.Pattern()
				}
				b.logger.Warn("queue has overlapping patterns; only the first subscription will be triggered",
					"queue", queue,
					"subject", m.Subject,
					"patterns", patterns,
					"error", ErrOverlappingPatterns)
			}
		}
	}

	toBeExecuted := make([]*SubscriptionExecution, 0, len(allExecutions))
	for _, execution := range allExecutions {
		if execution.Status == ExecutionStatusExecuted {
			toBeExecuted = append(toBeExecuted, execution)
		}
	}

	if len(toBeExecuted) == 0 {
		return &Dispatch{
			Executions: allExecutions,
		}, nil
	}

	tx, err := b.dialect.BeginTx(ctx, nil)
	if err != nil {
		return nil, errors.Wrap(err, "failed to begin transaction")
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
		for i := 0; i < len(toBeExecuted); i += batchSize {
			end := i + batchSize
			if end > len(toBeExecuted) {
				end = len(toBeExecuted)
			}
			batch := toBeExecuted[i:end]

			plans := make([]que.Plan, len(batch))
			for j, execution := range batch {
				plans[j] = *execution.Plan
			}

			jobIDs, err := goq.Enqueue(ctx, tx, plans...)
			if err != nil {
				return errors.Wrap(err, "failed to enqueue jobs batch")
			}

			for j, execution := range batch {
				jobID := jobIDs[j]
				if jobID != que.SkippedConflictID {
					execution.JobID = &jobID
				} else {
					execution.Status = ExecutionStatusSkippedConflict
					execution.JobID = nil
				}
			}
		}
		return nil
	}()
	panicked = false
	if err != nil {
		return nil, err
	}

	if err = tx.Commit(); err != nil {
		return nil, errors.Wrap(err, "failed to commit transaction")
	}

	return &Dispatch{
		Executions: allExecutions,
	}, nil
}

// BySubject returns all subscriptions with patterns matching the given subject.
func (b *BusImpl) BySubject(ctx context.Context, subject string) ([]Subscription, error) {
	return b.dialect.BySubject(ctx, subject)
}
