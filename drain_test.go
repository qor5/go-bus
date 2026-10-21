package bus_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"testing"
	"time"

	"github.com/qor5/go-bus"
	"github.com/qor5/go-bus/bussql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// publishMany publishes n messages on subject.
func publishMany(t *testing.T, b bus.Bus, subject string, n int) {
	t.Helper()
	outs := make([]*bus.Outbound, n)
	for i := range outs {
		outs[i] = &bus.Outbound{Message: bus.Message{Subject: subject, Payload: json.RawMessage(`"m"`)}}
	}
	_, err := b.Publish(context.Background(), outs...)
	require.NoError(t, err)
}

func queueJobs(t *testing.T, queue string) int {
	t.Helper()
	var n int
	require.NoError(t, db.QueryRow(`SELECT count(*) FROM goque_jobs WHERE queue = $1`, queue).Scan(&n))
	return n
}

// TestDrainLocksOnlyTheJobsItDeletes verifies that Drain and Unsubscribe with
// auto-drain take advisory locks only on the jobs of their own pattern, not
// on every job of the queue: the locks last until the transaction ends, and
// enough of them exhaust the lock table.
func TestDrainLocksOnlyTheJobsItDeletes(t *testing.T) {
	for name, drain := range map[string]func(t *testing.T, ctx context.Context, sub bus.Subscription) error{
		"Drain": func(t *testing.T, ctx context.Context, sub bus.Subscription) error {
			n, err := sub.Drain(ctx)
			assert.Equal(t, 10, n)
			return err
		},
		"Unsubscribe": func(_ *testing.T, ctx context.Context, sub bus.Subscription) error { return sub.Unsubscribe(ctx) },
	} {
		t.Run(name, func(t *testing.T) {
			b, _, _ := setupBusAndQueues(t)
			defer func() { _ = b.Close() }()
			ctx := context.Background()
			q := b.Queue("locks")
			sub, err := q.Subscribe(ctx, "mine.topic", bus.WithAutoDrain(true))
			require.NoError(t, err)
			_, err = q.Subscribe(ctx, "other.topic")
			require.NoError(t, err)
			publishMany(t, b, "mine.topic", 10)
			publishMany(t, b, "other.topic", 300)

			// In a transaction of the test's, so the locks can be counted.
			tx, err := db.Begin()
			require.NoError(t, err)
			defer func() { _ = tx.Rollback() }()
			require.NoError(t, drain(t, bussql.NewContext(ctx, tx), sub))
			var locks int
			require.NoError(t, tx.QueryRow(`SELECT count(*) FROM pg_locks WHERE locktype = 'advisory' AND pid = pg_backend_pid()`).Scan(&locks))
			assert.Equal(t, 10, locks, "a lock per deleted job")
			require.NoError(t, tx.Commit())
			assert.Equal(t, 300, queueJobs(t, "locks"))
		})
	}
}

// TestDrainALongQueue verifies that Drain and Unsubscribe with auto-drain
// finish a queue whose jobs are more than one transaction can lock.
func TestDrainALongQueue(t *testing.T) {
	if testing.Short() {
		t.Skip("publishes 20000 messages")
	}
	for name, drain := range map[string]func(t *testing.T, ctx context.Context, sub bus.Subscription) error{
		"Drain": func(t *testing.T, ctx context.Context, sub bus.Subscription) error {
			n, err := sub.Drain(ctx)
			assert.Equal(t, 20000, n)
			return err
		},
		"Unsubscribe": func(_ *testing.T, ctx context.Context, sub bus.Subscription) error { return sub.Unsubscribe(ctx) },
	} {
		t.Run(name, func(t *testing.T) {
			b, _, _ := setupBusAndQueues(t)
			defer func() { _ = b.Close() }()
			ctx := context.Background()
			sub, err := b.Queue("long").Subscribe(ctx, "long.topic", bus.WithAutoDrain(true))
			require.NoError(t, err)
			publishMany(t, b, "long.topic", 20000)

			require.NoError(t, drain(t, ctx, sub))
			assert.Zero(t, queueJobs(t, "long"))
		})
	}
}

// lockWaiting counts the sessions of the test database waiting for a lock.
func lockWaiting(t *testing.T) int {
	t.Helper()
	var n int
	require.NoError(t, db.QueryRow(`SELECT count(*) FROM pg_stat_activity WHERE datname = current_database() AND wait_event_type = 'Lock'`).Scan(&n))
	return n
}

// holdJob holds a row lock on the job of queue at offset (in id order), in a
// transaction the test ends.
func holdJob(t *testing.T, queue string, offset int) *sql.Tx {
	t.Helper()
	tx, err := db.Begin()
	require.NoError(t, err)
	t.Cleanup(func() { _ = tx.Rollback() })
	_, err = tx.Exec(`SELECT id FROM goque_jobs WHERE id = (SELECT id FROM goque_jobs WHERE queue = $1 ORDER BY id OFFSET $2 LIMIT 1) FOR UPDATE`, queue, offset)
	require.NoError(t, err)
	return tx
}

// TestUnsubscribeSparesASubscriptionMadeDuringTheDrain verifies that a
// Subscribe to the same queue and pattern made while Unsubscribe drains in
// batches waits for it, and that its jobs are not drained: jobs carry only the
// queue and the pattern.
func TestUnsubscribeSparesASubscriptionMadeDuringTheDrain(t *testing.T) {
	b, _, _ := setupBusAndQueues(t)
	defer func() { _ = b.Close() }()
	ctx := context.Background()
	q := b.Queue("again")
	sub, err := q.Subscribe(ctx, "again.topic", bus.WithAutoDrain(true))
	require.NoError(t, err)
	publishMany(t, b, "again.topic", 1500)
	blocker := holdJob(t, "again", 999) // the last job of the first batch

	unsubscribed := make(chan error, 1)
	go func() { unsubscribed <- sub.Unsubscribe(ctx) }()
	require.Eventually(t, func() bool { return lockWaiting(t) >= 1 }, 10*time.Second, 5*time.Millisecond)
	subscribed := make(chan error, 1)
	go func() { // other options: the same ones would find the old subscription and keep it
		_, err := q.Subscribe(ctx, "again.topic", bus.WithAutoDrain(true), bus.WithTTL(time.Minute))
		subscribed <- err
	}()
	require.Eventually(t, func() bool { return lockWaiting(t) >= 2 }, 10*time.Second, 5*time.Millisecond, "the new Subscribe waits for the batch")
	require.NoError(t, blocker.Commit())
	require.NoError(t, <-unsubscribed)
	require.NoError(t, <-subscribed)
	publishMany(t, b, "again.topic", 1)

	assert.Len(t, mustSubscriptions(t, q), 1)
	assert.Equal(t, 501, queueJobs(t, "again"), "the drain stopped at the new subscription; its job is kept")
}

func mustSubscriptions(t *testing.T, q bus.Queue) []bus.Subscription {
	t.Helper()
	subs, err := q.Subscriptions(context.Background())
	require.NoError(t, err)
	return subs
}

// TestUnsubscribeResumesAnInterruptedDrain verifies that an Unsubscribe whose
// drain is cut short leaves the subscription expired but not deleted, and
// that calling it again finishes.
func TestUnsubscribeResumesAnInterruptedDrain(t *testing.T) {
	b, _, _ := setupBusAndQueues(t)
	defer func() { _ = b.Close() }()
	ctx := context.Background()
	q := b.Queue("cut")
	sub, err := q.Subscribe(ctx, "cut.topic", bus.WithAutoDrain(true))
	require.NoError(t, err)
	publishMany(t, b, "cut.topic", 1500)
	blocker := holdJob(t, "cut", 1200) // in the second batch

	short, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
	defer cancel()
	require.Error(t, sub.Unsubscribe(short))
	require.NoError(t, blocker.Rollback())
	assert.Empty(t, mustSubscriptions(t, q), "expired at once")
	var rows int
	require.NoError(t, db.QueryRow(`SELECT count(*) FROM gobus_subscriptions WHERE queue = 'cut' AND deleted_at IS NULL`).Scan(&rows))
	assert.Equal(t, 1, rows, "but not deleted yet")
	assert.Equal(t, 500, queueJobs(t, "cut"), "the first batch was drained")

	require.NoError(t, sub.Unsubscribe(ctx))
	assert.Empty(t, mustSubscriptions(t, q))
	assert.Zero(t, queueJobs(t, "cut"))
}

// TestDrainKeepsAJobRetriedDuringTheDrain verifies that a job a worker retried
// after the drain read it, but before the drain deleted it, is kept.
func TestDrainKeepsAJobRetriedDuringTheDrain(t *testing.T) {
	b, _, _ := setupBusAndQueues(t)
	defer func() { _ = b.Close() }()
	ctx := context.Background()
	sub, err := b.Queue("retry").Subscribe(ctx, "retry.topic")
	require.NoError(t, err)
	publishMany(t, b, "retry.topic", 3)
	worker, err := db.Begin()
	require.NoError(t, err)
	defer func() { _ = worker.Rollback() }()
	_, err = worker.Exec(`UPDATE goque_jobs SET retry_count = 1 WHERE id = (SELECT min(id) FROM goque_jobs WHERE queue = 'retry')`)
	require.NoError(t, err)

	type result struct {
		n   int
		err error
	}
	drained := make(chan result, 1)
	go func() { n, err := sub.Drain(ctx); drained <- result{n, err} }()
	require.Eventually(t, func() bool { return lockWaiting(t) >= 1 }, 10*time.Second, 5*time.Millisecond)
	require.NoError(t, worker.Commit())
	r := <-drained
	require.NoError(t, r.err)
	assert.Equal(t, 2, r.n)
	assert.Equal(t, 1, queueJobs(t, "retry"), "the retried job is kept")
}

// TestSubscribeDuringUnsubscribeKeepsTheSubscription verifies that a
// Subscribe with the same options as a subscription being unsubscribed
// leaves a live subscription once both are done, rather than handing back
// the old one that the unsubscribe then removes.
func TestSubscribeDuringUnsubscribeKeepsTheSubscription(t *testing.T) {
	b, _, _ := setupBusAndQueues(t)
	defer func() { _ = b.Close() }()
	ctx := context.Background()
	q := b.Queue("same")
	subscribe := func() (bus.Subscription, error) { return q.Subscribe(ctx, "same.topic", bus.WithAutoDrain(true)) }
	sub, err := subscribe()
	require.NoError(t, err)
	publishMany(t, b, "same.topic", 1500)
	blocker := holdJob(t, "same", 999) // the last job of the first batch

	unsubscribed := make(chan error, 1)
	go func() { unsubscribed <- sub.Unsubscribe(ctx) }()
	require.Eventually(t, func() bool { return lockWaiting(t) >= 1 }, 10*time.Second, 5*time.Millisecond)
	subscribed := make(chan error, 1)
	go func() { _, err := subscribe(); subscribed <- err }()
	// The Subscribe returns at once, or waits for the batch.
	require.Eventually(t, func() bool { return len(subscribed) == 1 || lockWaiting(t) >= 2 }, 10*time.Second, 5*time.Millisecond)
	require.NoError(t, blocker.Commit())
	require.NoError(t, <-unsubscribed)
	require.NoError(t, <-subscribed)

	assert.Len(t, mustSubscriptions(t, q), 1, "the later Subscribe wins")
}

// TestSubscribeWaitsForAnUnsubscribeThatCameFirst verifies that a Subscribe
// with the same options does not return a subscription an Unsubscribe that
// came first is about to remove: it waits for the row, and then replaces it.
func TestSubscribeWaitsForAnUnsubscribeThatCameFirst(t *testing.T) {
	b, _, _ := setupBusAndQueues(t)
	defer func() { _ = b.Close() }()
	ctx := context.Background()
	q := b.Queue("first")
	subscribe := func() (bus.Subscription, error) { return q.Subscribe(ctx, "first.topic") }
	sub, err := subscribe()
	require.NoError(t, err)
	holder, err := db.Begin()
	require.NoError(t, err)
	defer func() { _ = holder.Rollback() }()
	_, err = holder.Exec(`SELECT id FROM gobus_subscriptions WHERE queue = 'first' AND deleted_at IS NULL FOR UPDATE`)
	require.NoError(t, err)

	unsubscribed := make(chan error, 1)
	go func() { unsubscribed <- sub.Unsubscribe(ctx) }()
	require.Eventually(t, func() bool { return lockWaiting(t) >= 1 }, 10*time.Second, 5*time.Millisecond)
	subscribed := make(chan error, 1)
	go func() { _, err := subscribe(); subscribed <- err }()
	require.Eventually(t, func() bool { return len(subscribed) == 1 || lockWaiting(t) >= 2 }, 10*time.Second, 5*time.Millisecond)
	require.NoError(t, holder.Commit())
	require.NoError(t, <-unsubscribed)
	require.NoError(t, <-subscribed)

	assert.Len(t, mustSubscriptions(t, q), 1, "the later Subscribe wins")
}
