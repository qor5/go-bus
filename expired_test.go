package bus_test

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/qor5/go-bus"
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
