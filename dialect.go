package bus

import (
	"context"
	"database/sql"

	"github.com/tnclong/go-que"
)

// Dialect defines the interface for database-specific implementations of the message bus.
// It abstracts storage operations and allows for different backend databases to be used.
type Dialect interface {
	// Migrate performs database migrations to initialize required tables.
	Migrate(ctx context.Context) error

	// GoQue returns the underlying GoQue instance.
	GoQue() que.Queue

	// BySubject finds all subscriptions with patterns matching the given subject.
	BySubject(ctx context.Context, subject string) ([]Subscription, error)

	// ByQueue returns all subscriptions for a specific queue.
	ByQueue(ctx context.Context, queue string) ([]Subscription, error)

	// Upsert creates or updates a subscription.
	Upsert(ctx context.Context, queue, pattern string, planConfig PlanConfig) (Subscription, error)

	// Delete removes a subscription.
	Delete(ctx context.Context, queue, pattern string) error

	// BeginTx starts a transaction.
	BeginTx(ctx context.Context, opts *sql.TxOptions) (*sql.Tx, error)
}
