package bus

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// heldMetadata holds its first GetMetadata until released; every call
// honours ctx, as a database driver does.
type heldMetadata struct {
	Dialect
	started chan struct{}
	release chan struct{}
	n       int
}

func (d *heldMetadata) GetMetadata(ctx context.Context) (*Metadata, error) {
	d.n++
	if d.n == 1 {
		close(d.started)
		<-d.release
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return &Metadata{Version: 1}, nil
}

// The call that callers share does not end with the ctx of the caller that
// started it: a caller that gives up while waiting for it (its own deadline)
// must not fail the others sharing it.
func TestFreshFlightsSharedCallOutlivesTheCallerThatStartedIt(t *testing.T) {
	d := &heldMetadata{started: make(chan struct{}), release: make(chan struct{})}
	rc, err := NewRistrettoCache(nil)
	require.NoError(t, err)
	c := &cacheDialect{Dialect: d, cache: WrapRistrettoCache(rc)}
	go func() { _, _ = c.checkVersion(context.Background()) }() // running call
	<-d.started

	ctxA, cancelA := context.WithCancel(context.Background())
	aDone := make(chan error, 1)
	go func() { _, err := c.checkVersion(ctxA); aDone <- err }() // creates the next call with ctxA
	require.Eventually(t, func() bool { return waitingIn("(*freshFlights).do(") == 2 }, time.Second, time.Millisecond)

	bDone := make(chan error, 1)
	go func() { _, err := c.checkVersion(context.Background()); bDone <- err }() // healthy caller, shares next
	require.Eventually(t, func() bool { return waitingIn("(*freshFlights).do(") == 3 }, time.Second, time.Millisecond)

	cancelA()
	require.ErrorIs(t, <-aDone, context.Canceled)
	close(d.release)
	errB := <-bDone
	assert.NoError(t, errB, "a caller whose ctx is fine got another caller's cancellation")
}
