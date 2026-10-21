package bus_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/qor5/go-bus"
	"github.com/qor5/go-bus/bussql"
	"github.com/qor5/go-bus/pgbus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// expire makes the live subscriptions of queue look expired a second ago.
func expire(t *testing.T, queue string) {
	t.Helper()
	_, err := db.Exec(`UPDATE gobus_subscriptions SET expires_at = NOW() - INTERVAL '1 second' WHERE queue = $1 AND deleted_at IS NULL`, queue)
	require.NoError(t, err)
}

func pendingJobs(t *testing.T, queue string) int {
	t.Helper()
	var n int
	require.NoError(t, db.QueryRow(`SELECT count(*) FROM goque_jobs WHERE queue = $1`, queue).Scan(&n))
	return n
}

func liveSubscriptions(t *testing.T, queue string) int {
	t.Helper()
	var n int
	require.NoError(t, db.QueryRow(`SELECT count(*) FROM gobus_subscriptions WHERE queue = $1 AND deleted_at IS NULL`, queue).Scan(&n))
	return n
}

// TestUnsubscribeExpired verifies that the subscriptions of a crashed owner,
// expired without a heartbeat, are unsubscribed as Unsubscribe would: drained
// only with auto-drain, jobs already retried kept, live and TTL-less
// subscriptions untouched.
func TestUnsubscribeExpired(t *testing.T) {
	b, _, _ := setupBusAndQueues(t)
	ctx := context.Background()
	sub := func(queue string, opts ...bus.SubscribeOption) {
		_, err := b.Queue(queue).Subscribe(ctx, "expire.topic", opts...)
		require.NoError(t, err)
	}
	sub("drained", bus.WithTTL(time.Minute), bus.WithAutoDrain(true))
	sub("kept", bus.WithTTL(time.Minute))
	sub("alive", bus.WithTTL(time.Minute), bus.WithAutoDrain(true))
	sub("forever", bus.WithAutoDrain(true))
	for range 3 {
		_, err := b.Publish(ctx, &bus.Outbound{Message: bus.Message{Subject: "expire.topic", Payload: json.RawMessage(`"m"`)}})
		require.NoError(t, err)
	}
	_, err := db.Exec(`UPDATE goque_jobs SET retry_count = 1 WHERE id = (SELECT min(id) FROM goque_jobs WHERE queue = 'drained')`)
	require.NoError(t, err)
	expire(t, "drained")
	expire(t, "kept")

	n, err := b.(*bus.BusImpl).UnsubscribeExpired(ctx)
	require.NoError(t, err)
	assert.Equal(t, 2, n)
	assert.Zero(t, liveSubscriptions(t, "drained"))
	assert.Equal(t, 1, pendingJobs(t, "drained"), "drained, but the retried job is kept")
	assert.Zero(t, liveSubscriptions(t, "kept"))
	assert.Equal(t, 3, pendingJobs(t, "kept"), "no auto-drain: its jobs are kept")
	for _, q := range []string{"alive", "forever"} {
		assert.Equal(t, 1, liveSubscriptions(t, q), q)
		assert.Equal(t, 3, pendingJobs(t, q), q)
	}

	n, err = b.(*bus.BusImpl).UnsubscribeExpired(ctx)
	require.NoError(t, err)
	assert.Zero(t, n, "nothing left to do")
}

// TestUnsubscribeExpiredWorksInBatches verifies that one call handles at most
// its limit of subscriptions and drains a long queue to the end in batches.
func TestUnsubscribeExpiredWorksInBatches(t *testing.T) {
	b, _, _ := setupBusAndQueues(t)
	ctx := context.Background()
	d, err := pgbus.NewDialect(db)
	require.NoError(t, err)
	for i := range 3 {
		_, err := b.Queue(fmt.Sprint("batch", i)).Subscribe(ctx, "batch.topic", bus.WithTTL(time.Minute), bus.WithAutoDrain(true))
		require.NoError(t, err)
	}
	outs := make([]*bus.Outbound, 2500)
	for i := range outs {
		outs[i] = &bus.Outbound{Message: bus.Message{Subject: "batch.topic", Payload: json.RawMessage(`"m"`)}}
	}
	_, err = b.Publish(ctx, outs...)
	require.NoError(t, err)
	for i := range 3 {
		expire(t, fmt.Sprint("batch", i))
	}

	n, err := d.UnsubscribeExpired(ctx, 2)
	require.NoError(t, err)
	assert.Equal(t, 2, n, "at most the limit a call")
	n, err = d.UnsubscribeExpired(ctx, 2)
	require.NoError(t, err)
	assert.Equal(t, 1, n)
	for i := range 3 {
		assert.Zero(t, pendingJobs(t, fmt.Sprint("batch", i)), "drained past several batches")
	}
}

func unsubscribeExpired(ctx context.Context, b bus.Bus) (int, error) {
	return b.(*bus.BusImpl).UnsubscribeExpired(ctx)
}

// publishN publishes n messages on subject.
func publishN(t *testing.T, b bus.Bus, subject string, n int) {
	t.Helper()
	outs := make([]*bus.Outbound, n)
	for i := range outs {
		outs[i] = &bus.Outbound{Message: bus.Message{Subject: subject, Payload: json.RawMessage(`"m"`)}}
	}
	_, err := b.Publish(context.Background(), outs...)
	require.NoError(t, err)
}

// lockWaiters counts the sessions of the test database waiting for a lock.
func lockWaiters(t *testing.T) int {
	t.Helper()
	var n int
	require.NoError(t, db.QueryRow(`SELECT count(*) FROM pg_stat_activity WHERE datname = current_database() AND wait_event_type = 'Lock'`).Scan(&n))
	return n
}

func waitForLockWaiters(t *testing.T, n int) {
	t.Helper()
	require.Eventually(t, func() bool { return lockWaiters(t) >= n }, 10*time.Second, 5*time.Millisecond)
}

// lockJob holds a row lock on the job of queue at offset (in id order), in a
// transaction the test ends.
func lockJob(t *testing.T, queue string, offset int) *sql.Tx {
	t.Helper()
	tx, err := db.Begin()
	require.NoError(t, err)
	t.Cleanup(func() { _ = tx.Rollback() })
	_, err = tx.Exec(`SELECT id FROM goque_jobs WHERE id = (SELECT id FROM goque_jobs WHERE queue = $1 ORDER BY id OFFSET $2 LIMIT 1) FOR UPDATE`, queue, offset)
	require.NoError(t, err)
	return tx
}

func maxJobID(t *testing.T) int64 {
	t.Helper()
	var id sql.NullInt64
	require.NoError(t, db.QueryRow(`SELECT max(id) FROM goque_jobs`).Scan(&id))
	return id.Int64
}

func jobsAfter(t *testing.T, queue string, id int64) int {
	t.Helper()
	var n int
	require.NoError(t, db.QueryRow(`SELECT count(*) FROM goque_jobs WHERE queue = $1 AND id > $2`, queue, id).Scan(&n))
	return n
}

// TestUnsubscribeExpiredSparesASubscriptionMadeDuringTheDrain verifies that
// the drain of an expired subscription never deletes the jobs of a
// subscription to the same queue and pattern made while it runs: jobs carry no
// subscription id, only the queue and the pattern.
func TestUnsubscribeExpiredSparesASubscriptionMadeDuringTheDrain(t *testing.T) {
	b, _, _ := setupBusAndQueues(t)
	ctx := context.Background()
	q := b.Queue("respawn")
	subscribe := func() error {
		_, err := q.Subscribe(ctx, "respawn.topic", bus.WithTTL(time.Minute), bus.WithAutoDrain(true))
		return err
	}
	require.NoError(t, subscribe())
	publishN(t, b, "respawn.topic", 1500)
	expire(t, "respawn")
	blocker := lockJob(t, "respawn", 999) // the last job of the drain's first batch

	unsubscribed := make(chan error, 1)
	go func() { _, err := unsubscribeExpired(ctx, b); unsubscribed <- err }()
	waitForLockWaiters(t, 1)

	// The owner comes back: the subscription is made at once, or waits for
	// the drain to let go of the old one.
	subscribed := make(chan error, 1)
	go func() { subscribed <- subscribe() }()
	var before int64
	made := false
	require.Eventually(t, func() bool {
		select {
		case err := <-subscribed:
			require.NoError(t, err)
			before = maxJobID(t)
			publishN(t, b, "respawn.topic", 1)
			made = true
			return true
		default:
			return lockWaiters(t) >= 2
		}
	}, 10*time.Second, 5*time.Millisecond)
	require.NoError(t, blocker.Commit())
	require.NoError(t, <-unsubscribed)
	if !made {
		require.NoError(t, <-subscribed)
		before = maxJobID(t)
		publishN(t, b, "respawn.topic", 1)
	}

	assert.Equal(t, 1, liveSubscriptions(t, "respawn"))
	assert.Equal(t, 1, jobsAfter(t, "respawn", before), "the new subscription's job is kept")
}

// TestUnsubscribeExpiredKeepsAJobRetriedDuringTheDrain verifies that a job a
// worker retried after the drain read it, but before the drain deleted it, is
// kept, as Drain keeps a retried job.
func TestUnsubscribeExpiredKeepsAJobRetriedDuringTheDrain(t *testing.T) {
	b, _, _ := setupBusAndQueues(t)
	ctx := context.Background()
	_, err := b.Queue("retried").Subscribe(ctx, "retried.topic", bus.WithTTL(time.Minute), bus.WithAutoDrain(true))
	require.NoError(t, err)
	publishN(t, b, "retried.topic", 3)
	expire(t, "retried")
	worker, err := db.Begin()
	require.NoError(t, err)
	defer func() { _ = worker.Rollback() }()
	_, err = worker.Exec(`UPDATE goque_jobs SET retry_count = 1 WHERE id = (SELECT min(id) FROM goque_jobs WHERE queue = 'retried')`)
	require.NoError(t, err)

	unsubscribed := make(chan error, 1)
	go func() { _, err := unsubscribeExpired(ctx, b); unsubscribed <- err }()
	waitForLockWaiters(t, 1)
	require.NoError(t, worker.Commit())
	require.NoError(t, <-unsubscribed)

	assert.Zero(t, liveSubscriptions(t, "retried"))
	assert.Equal(t, 1, pendingJobs(t, "retried"), "the retried job is kept")
}

// TestUnsubscribeExpiredResumesAnInterruptedDrain verifies that a drain cut
// short (here by the caller's deadline) is finished by the next call, rather
// than leaving the rest of the jobs behind for good.
func TestUnsubscribeExpiredResumesAnInterruptedDrain(t *testing.T) {
	b, _, _ := setupBusAndQueues(t)
	ctx := context.Background()
	_, err := b.Queue("resume").Subscribe(ctx, "resume.topic", bus.WithTTL(time.Minute), bus.WithAutoDrain(true))
	require.NoError(t, err)
	publishN(t, b, "resume.topic", 1500)
	expire(t, "resume")
	blocker := lockJob(t, "resume", 999)

	short, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
	defer cancel()
	_, err = unsubscribeExpired(short, b)
	require.Error(t, err)
	require.NoError(t, blocker.Rollback())

	n, err := unsubscribeExpired(ctx, b)
	require.NoError(t, err)
	assert.Equal(t, 1, n)
	assert.Zero(t, liveSubscriptions(t, "resume"))
	assert.Zero(t, pendingJobs(t, "resume"), "the drain is finished")
}

// TestUnsubscribeExpiredCommitsOnItsOwn verifies that UnsubscribeExpired does
// not join a transaction the caller's ctx carries: its batches are committed
// as they go, whatever becomes of the caller's transaction.
func TestUnsubscribeExpiredCommitsOnItsOwn(t *testing.T) {
	b, _, _ := setupBusAndQueues(t)
	ctx := context.Background()
	_, err := b.Queue("own").Subscribe(ctx, "own.topic", bus.WithTTL(time.Minute), bus.WithAutoDrain(true))
	require.NoError(t, err)
	publishN(t, b, "own.topic", 3)
	expire(t, "own")

	tx, err := db.Begin()
	require.NoError(t, err)
	n, err := unsubscribeExpired(bussql.NewContext(ctx, tx), b)
	require.NoError(t, err)
	require.Equal(t, 1, n)
	require.NoError(t, tx.Rollback())

	assert.Zero(t, liveSubscriptions(t, "own"))
	assert.Zero(t, pendingJobs(t, "own"))
}

// TestUnsubscribeExpiredSkipsRunningJobs verifies that the drain keeps a job
// a worker is performing (it holds the job's advisory lock).
func TestUnsubscribeExpiredSkipsRunningJobs(t *testing.T) {
	b, _, _ := setupBusAndQueues(t)
	ctx := context.Background()
	_, err := b.Queue("running").Subscribe(ctx, "running.topic", bus.WithTTL(time.Minute), bus.WithAutoDrain(true))
	require.NoError(t, err)
	publishN(t, b, "running.topic", 3)
	expire(t, "running")
	worker, err := db.Conn(ctx)
	require.NoError(t, err)
	defer func() { _ = worker.Close() }()
	var locked bool
	require.NoError(t, worker.QueryRowContext(ctx, `SELECT pg_try_advisory_lock(min(id)) FROM goque_jobs WHERE queue = 'running'`).Scan(&locked))
	require.True(t, locked)
	defer func() { _, _ = worker.ExecContext(ctx, `SELECT pg_advisory_unlock_all()`) }()

	n, err := unsubscribeExpired(ctx, b)
	require.NoError(t, err)
	assert.Equal(t, 1, n)
	assert.Equal(t, 1, pendingJobs(t, "running"), "the job being performed is kept")
}

// TestUnsubscribeExpiredDrainsOnlyItsPattern verifies that the drain leaves
// the jobs of the queue's other subscriptions alone.
func TestUnsubscribeExpiredDrainsOnlyItsPattern(t *testing.T) {
	b, _, _ := setupBusAndQueues(t)
	ctx := context.Background()
	q := b.Queue("mixed")
	_, err := q.Subscribe(ctx, "gone.topic", bus.WithTTL(time.Minute), bus.WithAutoDrain(true))
	require.NoError(t, err)
	_, err = q.Subscribe(ctx, "stays.topic", bus.WithAutoDrain(true))
	require.NoError(t, err)
	publishN(t, b, "gone.topic", 3)
	publishN(t, b, "stays.topic", 2)
	_, err = db.Exec(`UPDATE gobus_subscriptions SET expires_at = NOW() - INTERVAL '1 second' WHERE pattern = 'gone.topic'`)
	require.NoError(t, err)

	n, err := unsubscribeExpired(ctx, b)
	require.NoError(t, err)
	assert.Equal(t, 1, n)
	assert.Equal(t, 1, liveSubscriptions(t, "mixed"))
	assert.Equal(t, 2, pendingJobs(t, "mixed"), "the other subscription's jobs are kept")
}

// TestUnsubscribeExpiredOldestFirst verifies that a call limited to fewer
// subscriptions than expired handles the longest expired first.
func TestUnsubscribeExpiredOldestFirst(t *testing.T) {
	b, _, _ := setupBusAndQueues(t)
	ctx := context.Background()
	d, err := pgbus.NewDialect(db)
	require.NoError(t, err)
	// Subscribed in this order, expired a, c, b from the oldest.
	for queue, ago := range map[string]int{"a": 3, "b": 1, "c": 2} {
		_, err := b.Queue(queue).Subscribe(ctx, "old.topic", bus.WithTTL(time.Minute))
		require.NoError(t, err)
		_, err = db.Exec(`UPDATE gobus_subscriptions SET expires_at = NOW() - $2 * INTERVAL '1 minute' WHERE queue = $1`, queue, ago)
		require.NoError(t, err)
	}
	for _, queue := range []string{"a", "c", "b"} {
		n, err := d.UnsubscribeExpired(ctx, 1)
		require.NoError(t, err)
		require.Equal(t, 1, n)
		assert.Zero(t, liveSubscriptions(t, queue), queue)
	}
}

// TestUnsubscribeExpiredSkipsASubscriptionReplacedMeanwhile verifies that a
// subscription replaced (by a new Subscribe) after a call found it expired is
// left alone: the new one is live, and so are its jobs.
func TestUnsubscribeExpiredSkipsASubscriptionReplacedMeanwhile(t *testing.T) {
	b, _, _ := setupBusAndQueues(t)
	ctx := context.Background()
	subscribe := func(queue string) {
		_, err := b.Queue(queue).Subscribe(ctx, queue+".topic", bus.WithTTL(time.Minute), bus.WithAutoDrain(true))
		require.NoError(t, err)
	}
	subscribe("first")
	subscribe("second")
	publishN(t, b, "first.topic", 1)
	publishN(t, b, "second.topic", 2)
	_, err := db.Exec(`UPDATE gobus_subscriptions SET expires_at = NOW() - INTERVAL '2 seconds' WHERE queue = 'first'`)
	require.NoError(t, err)
	expire(t, "second")
	blocker := lockJob(t, "first", 0) // holds the call on the first subscription

	unsubscribed := make(chan error, 1)
	var n int
	go func() { var err error; n, err = unsubscribeExpired(ctx, b); unsubscribed <- err }()
	waitForLockWaiters(t, 1)
	subscribe("second") // its owner comes back
	publishN(t, b, "second.topic", 1)
	require.NoError(t, blocker.Commit())
	require.NoError(t, <-unsubscribed)

	assert.Equal(t, 1, n)
	assert.Zero(t, liveSubscriptions(t, "first"))
	assert.Equal(t, 1, liveSubscriptions(t, "second"))
	assert.Equal(t, 3, pendingJobs(t, "second"), "the live subscription's jobs are kept")
}

// TestUnsubscribeExpiredNeedsADialectThatCan verifies that a dialect hiding
// the method (a decorator that does not forward it) makes UnsubscribeExpired
// fail with ErrNotSupported, rather than quietly do nothing.
func TestUnsubscribeExpiredNeedsADialectThatCan(t *testing.T) {
	cleanupAllTables()
	hide := bus.WithDialectDecorator(func(_ context.Context, next bus.Dialect) (bus.Dialect, error) {
		return struct{ bus.Dialect }{next}, nil
	})
	for name, opts := range map[string][]bus.BusOption{
		"cached":   {hide},
		"uncached": {hide, bus.WithCache(nil)},
	} {
		t.Run(name, func(t *testing.T) {
			d, err := pgbus.NewDialect(db)
			require.NoError(t, err)
			b, err := bus.New(d, opts...)
			require.NoError(t, err)
			defer func() { _ = b.Close() }()

			u, ok := b.(bus.ExpiredUnsubscriber)
			require.True(t, ok)
			_, err = u.UnsubscribeExpired(context.Background())
			require.ErrorIs(t, err, bus.ErrNotSupported)
		})
	}
}
