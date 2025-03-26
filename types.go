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
	"io"
	"net/http"

	"github.com/tnclong/go-que"
)

// Subscription represents an active subscription that can be unsubscribed.
type Subscription interface {
	// ID returns the unique identifier of the subscription.
	ID() int64

	// Queue returns the name of the queue that receives messages.
	Queue() string

	// Pattern returns the subject pattern this subscription matches against.
	Pattern() string

	// PlanConfig returns the configuration plan for this subscription.
	PlanConfig() PlanConfig

	// Unsubscribe removes this subscription.
	Unsubscribe(ctx context.Context) error
}

type Header = http.Header

// Message represents a message in the publish-subscribe system.
type Message struct {
	// Subject is the topic this message is published to.
	Subject string `json:"subject"`

	// Header is the header of the message.
	Header Header `json:"header,omitempty"`

	// Payload is the actual content of the message.
	Payload []byte `json:"payload,omitempty"`
}

type Outbound struct {
	// Message is the message content.
	Message

	// UniqueID is the unique identifier for this message.
	// If set, it will be used for message deduplication.
	UniqueID string `json:"uniqueId,omitempty"`
}

type Inbound struct {
	// Job is the job that received the message.
	que.Job `json:"-"`

	// Message is the message content.
	Message

	// Pattern is the pattern this message matches against.
	Pattern string `json:"pattern"`
}

// Handler represents a function that processes messages.
type Handler func(ctx context.Context, msg *Inbound) error

// Queue represents a message queue that can subscribe to subjects.
type Queue interface {
	io.Closer

	// Subscribe registers the queue to receive messages published to subjects matching the pattern.
	// Pattern supports NATS-style wildcards: '*' for a single token, '>' for multiple trailing tokens.
	Subscribe(ctx context.Context, pattern string, opts ...SubscribeOption) (Subscription, error)

	// Subscriptions returns all subscriptions for the queue.
	Subscriptions(ctx context.Context) ([]Subscription, error)

	// Consume registers the queue to receive messages published to subjects matching the pattern.
	Consume(ctx context.Context, handler Handler, opts ...ConsumeOption) error
}

// Bus provides publish-subscribe capabilities on top of go-que.
// It manages subject-queue mappings and handles subject pattern matching.
type Bus interface {
	io.Closer

	// Queue returns a queue with the specified name.
	Queue(name string) Queue

	// Publish sends a payload to all queues with subscriptions matching the subject.
	Publish(ctx context.Context, subject string, payload []byte, opts ...PublishOption) error

	// Dispatch sends outbound messages to all queues with subscriptions matching the subject.
	// All messages are processed in a single transaction.
	Dispatch(ctx context.Context, msgs ...*Outbound) error

	// BySubject returns all subscriptions with patterns matching the given subject.
	BySubject(ctx context.Context, subject string) ([]Subscription, error)
}
