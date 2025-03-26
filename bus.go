package bus

import (
	"context"
	"log/slog"
	"maps"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cenkalti/backoff/v5"
	"github.com/pkg/errors"
	"github.com/rs/xid"
	"github.com/tnclong/go-que"
	"golang.org/x/sync/errgroup"
)

var _ Queue = (*QueueImpl)(nil)

// QueueImpl implements the Queue interface.
type QueueImpl struct {
	name   string
	b      *BusImpl
	mu     sync.RWMutex       // Protects worker state
	worker *que.Worker        // Current active worker
	cancel context.CancelFunc // Cancel function for worker context
	doneC  chan struct{}      // Channel that closes when worker is fully stopped
	closed int32              // Whether the queue is closed (0=open, 1=closed), using atomic operations
}

// Subscribe registers the queue to receive messages published to subjects matching the pattern.
func (q *QueueImpl) Subscribe(ctx context.Context, pattern string, opts ...SubscribeOption) (Subscription, error) {
	if atomic.LoadInt32(&q.closed) == 1 {
		return nil, ErrQueueClosed
	}

	subscribeOpts := &SubscribeOptions{
		PlanConfig: DefaultPlanConfig,
	}

	for _, opt := range opts {
		opt(subscribeOpts)
	}

	sub, err := q.b.dialect.Upsert(ctx, q.name, pattern, subscribeOpts.PlanConfig)
	if err != nil {
		return nil, err
	}

	return sub, nil
}

// Subscriptions returns all subscriptions for the queue.
func (q *QueueImpl) Subscriptions(ctx context.Context) ([]Subscription, error) {
	if atomic.LoadInt32(&q.closed) == 1 {
		return nil, ErrQueueClosed
	}

	return q.b.dialect.ByQueue(ctx, q.name)
}

// Consume registers a worker to process messages for this queue.
func (q *QueueImpl) Consume(_ context.Context, handler Handler, opts ...ConsumeOption) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	// Check if queue is closed
	if atomic.LoadInt32(&q.closed) == 1 {
		return ErrQueueClosed
	}

	if q.worker != nil {
		// Check if worker is already running
		select {
		case <-q.doneC:
			// Worker is fully closed, can proceed to create new worker
		default:
			return ErrWorkerAlreadyRunning
		}
	}

	consumeOpts := &ConsumeOptions{
		WorkerConfig: DefaultWorkerConfig,
	}

	for _, opt := range opts {
		opt(consumeOpts)
	}

	consumingCtx, consumingCancel := context.WithCancel(context.Background())
	doneC := make(chan struct{})

	q.cancel = consumingCancel
	q.doneC = doneC

	var bkoff backoff.BackOff
	if consumeOpts.Backoff != nil {
		bkoff = consumeOpts.Backoff
	} else {
		bkoff = DefaultConsumeBackOffFactory()
	}

	startWorker := func() (*que.Worker, error) {
		return que.NewWorker(que.WorkerOptions{
			Queue:              q.name,
			Mutex:              q.b.dialect.GoQue().Mutex(),
			MaxLockPerSecond:   consumeOpts.WorkerConfig.MaxLockPerSecond,
			MaxBufferJobsCount: consumeOpts.WorkerConfig.MaxBufferJobsCount,
			Perform: func(ctx context.Context, job que.Job) error {
				inboundMsg, err := InboundFromArgs(job.Plan().Args)
				if err != nil {
					return err
				}
				inboundMsg.Job = job
				return handler(ctx, inboundMsg)
			},
			MaxPerformPerSecond:       consumeOpts.WorkerConfig.MaxPerformPerSecond,
			MaxConcurrentPerformCount: consumeOpts.WorkerConfig.MaxConcurrentPerformCount,
		})
	}

	worker, err := startWorker()
	if err != nil {
		consumingCancel()
		close(doneC)
		return errors.Wrap(err, "failed to create worker")
	}

	q.worker = worker

	go func() {
		defer close(doneC)

		q.mu.RLock()
		currentWorker := q.worker
		q.mu.RUnlock()

		for {
			select {
			case <-consumingCtx.Done():
				return
			default:
			}

			err := currentWorker.Run()
			if err == que.ErrWorkerStoped || atomic.LoadInt32(&q.closed) == 1 {
				return
			}

			q.b.logger.Warn("worker error, will attempt to reconnect",
				"queue", q.name,
				"error", err)

			nextBackoff := bkoff.NextBackOff()
			if nextBackoff == backoff.Stop {
				q.b.logger.Warn("backoff policy indicates no more retries, exiting worker",
					"queue", q.name)
				return
			}

			select {
			case <-consumingCtx.Done():
				return
			case <-time.After(nextBackoff):
			}

			if atomic.LoadInt32(&q.closed) == 1 {
				return
			}

			newWorker, err := startWorker()
			if err != nil {
				q.b.logger.Error("failed to recreate worker, exiting worker goroutine",
					"queue", q.name,
					"error", err)
				return // Exit without retry if startWorker fails
			}

			q.mu.Lock()
			q.worker = newWorker
			currentWorker = newWorker
			q.mu.Unlock()

			q.b.logger.Info("worker successfully reconnected", "queue", q.name)

			bkoff.Reset()
		}
	}()

	go func() {
		<-consumingCtx.Done()

		q.mu.RLock()
		workerToStop := q.worker
		q.mu.RUnlock()

		if workerToStop == nil {
			return
		}

		err := workerToStop.Stop(context.Background())
		if err != nil {
			q.b.logger.Warn("error stopping worker", "queue", q.name, "error", err)
		}
	}()

	return nil
}

// Close closes the queue and stops any running workers.
func (q *QueueImpl) Close() error {
	if !atomic.CompareAndSwapInt32(&q.closed, 0, 1) {
		return nil
	}

	q.mu.RLock()
	var cancel context.CancelFunc
	var doneC chan struct{}

	if q.cancel != nil {
		cancel = q.cancel
		doneC = q.doneC
	}
	q.mu.RUnlock()

	if cancel != nil {
		cancel()
		if doneC != nil {
			<-doneC
		}
	}

	return nil
}

var _ Bus = (*BusImpl)(nil)

// BusImpl is a generic implementation of the Bus interface.
type BusImpl struct {
	dialect            Dialect
	mu                 sync.RWMutex
	queues             map[string]Queue
	closed             int32 // Whether the bus is closed (0=open, 1=closed), using atomic operations
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

	b := &BusImpl{
		dialect:            dialect,
		queues:             make(map[string]Queue),
		closed:             0, // Initialize as open
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

	if atomic.LoadInt32(&b.closed) == 1 {
		return nil
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

var UniqueIDGenerator = func() string {
	return "_gobus_:" + xid.New().String()
}

// Dispatch sends outbound messages to all queues with subscriptions matching the subject.
// All messages are processed in a single transaction.
func (b *BusImpl) Dispatch(ctx context.Context, msgs ...*Outbound) (xerr error) {
	if atomic.LoadInt32(&b.closed) == 1 {
		return ErrBusClosed
	}

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

		msgRaw := m.Message.ToRaw()
		queuesSeen := make(map[string][]string)

		var uniqueID *string
		for _, sub := range subscriptions {
			queueName := sub.Queue()
			pattern := sub.Pattern()

			queuesSeen[queueName] = append(queuesSeen[queueName], pattern)
			if len(queuesSeen[queueName]) > 1 {
				continue
			}

			planConfig := sub.PlanConfig()

			plan := que.Plan{
				Queue:           queueName,
				Args:            ArgsFromMessageRaw(msgRaw, pattern),
				RunAt:           time.Now().Add(planConfig.RunAtDelta),
				RetryPolicy:     planConfig.RetryPolicy,
				UniqueLifecycle: planConfig.UniqueLifecycle,
			}

			if planConfig.UniqueLifecycle != que.Ignore {
				if uniqueID == nil {
					id := strings.TrimSpace(m.UniqueID)
					if id == "" {
						id = UniqueIDGenerator()
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
		if len(allPlans) > batchSize {
			for i := 0; i < len(allPlans); i += batchSize {
				end := i + batchSize
				if end > len(allPlans) {
					end = len(allPlans)
				}

				batch := allPlans[i:end]
				_, err = b.dialect.GoQue().Enqueue(ctx, tx, batch...)
				if err != nil {
					return errors.Wrap(err, "failed to enqueue jobs batch")
				}
			}
		} else {
			_, err = b.dialect.GoQue().Enqueue(ctx, tx, allPlans...)
			if err != nil {
				return errors.Wrap(err, "failed to enqueue jobs")
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
	if atomic.LoadInt32(&b.closed) == 1 {
		return nil, ErrBusClosed
	}

	return b.dialect.BySubject(ctx, subject)
}

// Close closes the bus and releases any associated resources.
func (b *BusImpl) Close() error {
	if !atomic.CompareAndSwapInt32(&b.closed, 0, 1) {
		return nil
	}

	b.mu.RLock()
	queues := maps.Clone(b.queues)
	b.mu.RUnlock()

	g := new(errgroup.Group)

	for _, q := range queues {
		q := q
		g.Go(func() error {
			return q.Close()
		})
	}

	return g.Wait()
}
