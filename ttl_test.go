package bus_test

import (
	"context"
	"testing"
	"time"

	"github.com/qor5/go-bus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestSubscribeWithALongTTL verifies that a TTL longer than 2^31
// microseconds (about 35m47s) is stored, and renewed by Heartbeat.
func TestSubscribeWithALongTTL(t *testing.T) {
	b, _, _ := setupBusAndQueues(t)
	defer func() { _ = b.Close() }()
	ctx := context.Background()
	sub, err := b.Queue("long-ttl").Subscribe(ctx, "long.ttl", bus.WithTTL(24*time.Hour))
	require.NoError(t, err)
	assert.WithinDuration(t, time.Now().Add(24*time.Hour), sub.ExpiresAt(), time.Minute)

	_, err = db.Exec(`UPDATE gobus_subscriptions SET expires_at = NOW() + INTERVAL '1 minute' WHERE queue = 'long-ttl' AND deleted_at IS NULL`)
	require.NoError(t, err)
	require.NoError(t, sub.Heartbeat(ctx))
	subs := mustSubscriptions(t, b.Queue("long-ttl"))
	require.Len(t, subs, 1)
	assert.WithinDuration(t, time.Now().Add(24*time.Hour), subs[0].ExpiresAt(), time.Minute, "renewed for the whole TTL")
}
