// Package bus implements a publish-subscribe pattern on top of go-que.
// It allows publishing messages to subjects which can be subscribed to by multiple queues.
// The subject matching follows NATS-style wildcards pattern:
// - '*' matches any single token in a subject (e.g., "foo.*.baz" matches "foo.bar.baz")
// - '>' matches one or more tokens at the end of a subject (e.g., "foo.>" matches "foo.bar" and "foo.bar.baz")
//
// The package uses a pluggable architecture with the Dialect interface to support
// different database backends. A PostgreSQL implementation is provided in the pgbus subpackage.
package bus

import (
	"context"
	"encoding/json"
	"net/http"
	"time"

	"github.com/qor5/go-bus/quex"
	"github.com/qor5/go-que"
)

// Header constants for message metadata
const (
	HeaderSubscriptionPattern    = "Subscription-Pattern"    // The subscription pattern that matched this message
	HeaderSubscriptionIdentifier = "Subscription-Identifier" // The subscription identifier that received this message
)

type Header = http.Header

// Message represents a message in the publish-subscribe system.
type Message struct {
	// Subject is the topic this message is published to.
	Subject string `json:"subject"`

	// Header is the header of the message.
	Header Header `json:"header"`

	// Payload is the actual content of the message.
	//
	// When publishing (Outbound), the following payload types are supported:
	//   - Any Go value that is JSON-marshalable (e.g., structs, maps, slices, scalars)
	//   - json.RawMessage (used as-is, without additional marshaling)
	//   - []byte (treated as raw JSON bytes, equivalent to json.RawMessage)
	//
	// When receiving (Inbound), InboundFromArgs assigns the raw JSON payload as a
	// json.RawMessage to Inbound.Payload (i.e., msg.Payload on *Inbound). For
	// convenience and backwards compatibility, this same raw payload is also
	// populated into Message.Payload. Inbound handlers should primarily use
	// Inbound.Payload/msg.Payload (on the Inbound value) as the source of raw JSON
	// and unmarshal it into concrete types as needed.
	Payload any `json:"payload"`
}

type Outbound struct {
	// Message is the message content.
	Message

	// UniqueID is the unique identifier for this message.
	// If set, it will be used for message deduplication.
	UniqueID func(msg *Outbound) string `json:"-"`
}

type Inbound struct {
	// Message is the message content.
	Message

	// Payload is the raw JSON payload of the message.
	Payload json.RawMessage `json:"payload"`

	// Job is the job that received the message.
	que.Job `json:"-"`
}

// Handler represents a function that processes messages.
type Handler func(ctx context.Context, msg *Inbound) error

// Consumer represents a message consumer that can be stopped.
type Consumer interface {
	quex.WorkerController
}

// Queue represents a message queue that can subscribe to subjects.
type Queue interface {
	// Subscribe registers the queue to receive messages published to subjects matching the pattern.
	// Pattern supports NATS-style wildcards: '*' for a single token, '>' for multiple trailing tokens.
	Subscribe(ctx context.Context, pattern string, opts ...SubscribeOption) (Subscription, error)

	// Subscriptions returns all subscriptions for the queue.
	Subscriptions(ctx context.Context) ([]Subscription, error)

	// StartConsumer starts a new message consumer for this queue.
	// The ctx parameter is only used to manage the startup process, not the Consumer's lifecycle.
	// The returned Consumer must be stopped by the caller when no longer needed.
	StartConsumer(ctx context.Context, handler Handler, opts ...ConsumeOption) (Consumer, error)
}

// Bus provides publish-subscribe capabilities on top of go-que.
// It manages subject-queue mappings and handles subject pattern matching.
type Bus interface {
	// Queue returns a queue with the specified name.
	Queue(name string) Queue

	// Publish sends outbound messages to all queues with subscriptions matching the subject.
	// All messages are processed in a single transaction.
	Publish(ctx context.Context, msgs ...*Outbound) (*Dispatch, error)

	// BySubject returns all subscriptions with patterns matching the given subject.
	BySubject(ctx context.Context, subject string) ([]Subscription, error)
}

// Subscription represents an active subscription to a subject pattern.
type Subscription interface {
	// ID returns the unique identifier of the subscription.
	ID() string

	// Queue returns the name of the queue that receives messages.
	Queue() string

	// Pattern returns the subject pattern this subscription matches against.
	Pattern() string

	// PlanConfig returns the plan configuration for this subscription.
	PlanConfig() *PlanConfig

	// Unsubscribe removes this subscription.
	// If autoDrain was enabled in the subscription options, pending jobs will be automatically cleaned up.
	// This method is usually executed when the subscription is not needed, and is not supposed to be executed with the exit of the program.
	// This is because go-bus is designed to support offline messages.
	Unsubscribe(ctx context.Context) error

	// Heartbeat updates the heartbeat timestamp for this subscription.
	// This method should be called periodically to prevent TTL-based cleanup.
	Heartbeat(ctx context.Context) error

	// ExpiresAt returns the expiration time for this subscription.
	// Returns zero time if the subscription never expires (no TTL).
	ExpiresAt() time.Time

	// Drain removes all pending jobs that are not currently being processed.
	// This method is useful for cleaning up the queue without affecting jobs that are already running.
	// Returns the number of jobs that were deleted.
	Drain(ctx context.Context) (int, error)
}
