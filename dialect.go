package bus

import (
	"context"
	"database/sql"
	"time"

	"github.com/qor5/go-bus/bussql"
	"github.com/qor5/go-que"
)

type (
	Metadata struct {
		Version            int64
		UpdatedAt          time.Time
		TotalSubscriptions int64
	}
)

// Dialect defines the interface for database-specific implementations of the message bus.
// It abstracts storage operations and allows for different backend databases to be used.
type Dialect interface {
	// Migrate performs database migrations to initialize required tables.
	Migrate(ctx context.Context) error

	// GoQue returns the underlying GoQue instance.
	GoQue() que.Queue

	// GetMetadata retrieves the current bus metadata.
	GetMetadata(ctx context.Context) (*Metadata, error)

	// BySubject finds all subscriptions with patterns matching the given subject.
	BySubject(ctx context.Context, subject string) ([]Subscription, error)

	// ByQueue returns all subscriptions for a specific queue.
	ByQueue(ctx context.Context, queue string) ([]Subscription, error)

	// Upsert creates or updates a subscription with the provided options.
	Upsert(ctx context.Context, queue, pattern string, opts *SubscribeOptions) (Subscription, error)

	// ExecTx executes fn within a database transaction.
	// If the context already contains a transaction (via bussql.NewContext), that transaction
	// is reused (with a savepoint if enabled). Otherwise, a new transaction is started.
	// The transaction is automatically committed if fn returns nil, or rolled back on error.
	ExecTx(ctx context.Context, fn func(ctx context.Context, tx *sql.Tx) error, opts ...bussql.TransactionOption) error
}
