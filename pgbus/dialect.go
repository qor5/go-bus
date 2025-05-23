package pgbus

import (
	"context"
	"database/sql"
	"encoding/json"
	"math/rand"
	"strings"
	"time"

	"github.com/pkg/errors"
	"github.com/qor5/go-bus"
	"github.com/qor5/go-que"
	"github.com/qor5/go-que/pg"
)

var _ bus.Dialect = (*Dialect)(nil)

// Dialect is a PostgreSQL-specific implementation of the bus.Dialect interface.
// It provides database operations for the message bus using PostgreSQL.
type Dialect struct {
	db  *sql.DB
	goq que.Queue
}

// NewDialect creates a new PostgreSQL-specific implementation of the bus.Dialect interface.
// It requires a valid PostgreSQL database connection.
func NewDialect(db *sql.DB) (*Dialect, error) {
	goq, err := pg.NewWithOptions(pg.Options{DB: db, DBMigrate: false})
	if err != nil {
		return nil, errors.Wrap(err, "failed to create GoQue instance")
	}
	return &Dialect{
		db:  db,
		goq: goq,
	}, nil
}

// GoQue returns the underlying GoQue instance.
func (d *Dialect) GoQue() que.Queue {
	return d.goq
}

// BeginTx starts a transaction.
func (d *Dialect) BeginTx(ctx context.Context, opts *sql.TxOptions) (*sql.Tx, error) {
	return d.db.BeginTx(ctx, opts)
}

var MaxMigrationAttempts = 10

// GetMetadata retrieves the current bus metadata
func (d *Dialect) GetMetadata(ctx context.Context) (*bus.Metadata, error) {
	var meta bus.Metadata
	err := d.db.QueryRowContext(ctx,
		"SELECT version, updated_at, total_subscriptions FROM gobus_metadata LIMIT 1").
		Scan(&meta.Version, &meta.UpdatedAt, &meta.TotalSubscriptions)
	if err != nil {
		return nil, errors.Wrap(err, "failed to query metadata")
	}
	return &meta, nil
}

// Migrate creates the subscriptions table and indexes.
func Migrate(ctx context.Context, db *sql.DB) error {
	f := func() error {
		err := pg.Migrate(db)
		if err != nil {
			return errors.Wrap(err, "failed to migrate go-que table")
		}

		_, err = db.ExecContext(ctx, `
    CREATE TABLE IF NOT EXISTS gobus_subscriptions (
        id SERIAL PRIMARY KEY,
        queue VARCHAR(100) NOT NULL CHECK (char_length(TRIM(queue)) > 0 AND char_length(queue) <= 100),
        pattern VARCHAR(255) NOT NULL,
        regex_pattern VARCHAR(1024) NOT NULL,
        plan JSONB NOT NULL DEFAULT '{}'::jsonb,
        created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
        updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
        UNIQUE (queue, pattern)
    );
    
    -- Index for queue lookups
    CREATE INDEX IF NOT EXISTS gobus_subscriptions_queue_idx 
        ON gobus_subscriptions (queue);
        
    -- Index for created_at and updated_at
    CREATE INDEX IF NOT EXISTS gobus_subscriptions_created_at_idx
        ON gobus_subscriptions (created_at);
    CREATE INDEX IF NOT EXISTS gobus_subscriptions_updated_at_idx
        ON gobus_subscriptions (updated_at);
        
    CREATE TABLE IF NOT EXISTS gobus_metadata (
        version BIGINT NOT NULL DEFAULT 1,
        updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
        total_subscriptions BIGINT NOT NULL DEFAULT 0
    );
    
    -- Insert initial metadata row if it doesn't exist
    INSERT INTO gobus_metadata (version, updated_at, total_subscriptions)
    SELECT 1, NOW(), (SELECT COUNT(1) FROM gobus_subscriptions)
    WHERE NOT EXISTS (SELECT 1 FROM gobus_metadata);
    `)
		if err != nil {
			return errors.Wrap(err, "failed to create subscriptions table and indexes")
		}
		return nil
	}
	for attempts := 0; attempts < MaxMigrationAttempts; attempts++ {
		err := f()
		if err == nil {
			return nil
		}
		if strings.Contains(err.Error(), `duplicate key value violates unique constraint "pg_class_relname_nsp_index"`) ||
			strings.Contains(err.Error(), "already exists (SQLSTATE 42P07)") {
			select {
			case <-ctx.Done():
				return errors.Wrap(ctx.Err(), "failed to migrate")
			case <-time.After(time.Duration(100+rand.Intn(100)) * time.Millisecond):
			}
			continue
		}
		return err
	}
	return nil
}

// Migrate creates the subscriptions table and indexes.
func (d *Dialect) Migrate(ctx context.Context) error {
	return Migrate(ctx, d.db)
}

// scanSubscriptionsInternal scans rows and converts them to subscription objects
func (d *Dialect) scanSubscriptions(rows *sql.Rows) ([]bus.Subscription, error) {
	var subscriptions []bus.Subscription
	for rows.Next() {
		var sub subscription
		var planData []byte

		if err := rows.Scan(
			&sub.id,
			&sub.queue,
			&sub.pattern,
			&sub.regexPattern,
			&planData,
			&sub.createdAt,
			&sub.updatedAt,
		); err != nil {
			return nil, errors.Wrap(err, "failed to scan subscription")
		}

		sub.d = d

		if err := json.Unmarshal(planData, &sub.plan); err != nil {
			return nil, errors.Wrap(err, "failed to deserialize subscription plan")
		}

		subscriptions = append(subscriptions, &sub)
	}

	if err := rows.Err(); err != nil {
		return nil, errors.Wrap(err, "error iterating subscriptions")
	}

	return subscriptions, nil
}

// BySubject finds all subscriptions that match the given subject.
func (d *Dialect) BySubject(ctx context.Context, subject string) ([]bus.Subscription, error) {
	if err := bus.ValidateSubject(subject); err != nil {
		return nil, err
	}

	rows, err := d.db.QueryContext(
		ctx,
		`SELECT id, queue, pattern, regex_pattern, plan, created_at, updated_at
		 FROM gobus_subscriptions
		 WHERE $1 ~ regex_pattern
		 ORDER BY id ASC`,
		subject,
	)
	if err != nil {
		return nil, errors.Wrap(err, "failed to query subscriptions")
	}
	defer rows.Close()

	return d.scanSubscriptions(rows)
}

// ByQueue finds all subscriptions for a specific queue.
func (d *Dialect) ByQueue(ctx context.Context, queue string) ([]bus.Subscription, error) {
	if strings.TrimSpace(queue) == "" {
		return nil, errors.Wrap(bus.ErrInvalidQueue, "queue cannot be empty")
	}

	rows, err := d.db.QueryContext(
		ctx,
		`SELECT id, queue, pattern, regex_pattern, plan, created_at, updated_at
         FROM gobus_subscriptions
         WHERE queue = $1
         ORDER BY id ASC`,
		queue,
	)
	if err != nil {
		return nil, errors.Wrap(err, "failed to query subscriptions")
	}
	defer rows.Close()

	return d.scanSubscriptions(rows)
}

func (d *Dialect) byQueueAndPattern(ctx context.Context, queue, pattern string) (*subscription, error) {
	rows, err := d.db.QueryContext(
		ctx,
		`SELECT id, queue, pattern, regex_pattern, plan, created_at, updated_at
         FROM gobus_subscriptions
         WHERE queue = $1 AND pattern = $2
         ORDER BY id ASC`,
		queue, pattern,
	)
	if err != nil {
		return nil, errors.Wrap(err, "failed to query subscription")
	}
	defer rows.Close()

	subs, err := d.scanSubscriptions(rows)
	if err != nil {
		return nil, err
	}

	if len(subs) == 0 {
		return nil, bus.ErrSubscriptionNotFound
	}

	return subs[0].(*subscription), nil
}

// Upsert creates or updates a subscription.
func (d *Dialect) Upsert(ctx context.Context, queue, pattern string, planConfig bus.PlanConfig) (_ bus.Subscription, xerr error) {
	if strings.TrimSpace(queue) == "" {
		return nil, errors.Wrap(bus.ErrInvalidQueue, "queue name cannot be empty")
	}

	regexPattern, err := bus.ToRegexPattern(pattern)
	if err != nil {
		return nil, err
	}

	planData, err := json.Marshal(planConfig)
	if err != nil {
		return nil, errors.Wrap(err, "failed to serialize subscription plan")
	}

	existingSub, err := d.byQueueAndPattern(ctx, queue, pattern)
	if err != nil && !errors.Is(err, bus.ErrSubscriptionNotFound) {
		return nil, err
	}

	// Skip update if subscription exists and is identical
	if existingSub != nil {
		if existingSub.pattern == pattern &&
			existingSub.queue == queue &&
			existingSub.regexPattern == regexPattern &&
			planConfig.Equal(existingSub.plan) {
			return existingSub, nil
		}
	}

	tx, err := d.BeginTx(ctx, nil)
	if err != nil {
		return nil, errors.Wrap(err, "failed to begin transaction")
	}
	defer func() {
		if xerr != nil {
			_ = tx.Rollback()
		}
	}()

	rows, err := tx.QueryContext(
		ctx,
		`INSERT INTO gobus_subscriptions 
         (queue, pattern, regex_pattern, plan, created_at, updated_at) 
         VALUES ($1, $2, $3, $4, NOW(), NOW())
         ON CONFLICT (queue, pattern) 
         DO UPDATE SET 
            regex_pattern = EXCLUDED.regex_pattern,
            plan = EXCLUDED.plan,
            updated_at = NOW()
		RETURNING id, queue, pattern, regex_pattern, plan, created_at, updated_at`,
		queue, pattern, regexPattern, planData,
	)
	if err != nil {
		return nil, errors.Wrap(err, "failed to insert or update subscription")
	}
	defer rows.Close()

	subs, err := d.scanSubscriptions(rows)
	if err != nil {
		return nil, err
	}

	if err := d.updateMetadata(ctx, tx); err != nil {
		return nil, err
	}

	if err = tx.Commit(); err != nil {
		return nil, errors.Wrap(err, "failed to commit transaction")
	}

	if len(subs) == 0 {
		return nil, bus.ErrSubscriptionNotFound
	}

	return subs[0], nil
}

// updateMetadata updates the version and total_subscriptions count in gobus_metadata table
func (d *Dialect) updateMetadata(ctx context.Context, tx *sql.Tx) error {
	result, err := tx.ExecContext(ctx,
		`UPDATE gobus_metadata SET 
            version = version + 1, 
            updated_at = NOW(),
            total_subscriptions = (SELECT COUNT(1) FROM gobus_subscriptions)`)
	if err != nil {
		return errors.Wrap(err, "failed to update metadata")
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return errors.Wrap(err, "failed to get affected rows")
	}

	if rowsAffected != 1 {
		return errors.New("failed to update metadata, no rows affected")
	}

	return nil
}

// Delete removes a subscription for the given queue and pattern.
func (d *Dialect) Delete(ctx context.Context, queue, pattern string) (xerr error) {
	if strings.TrimSpace(queue) == "" {
		return errors.Wrap(bus.ErrInvalidQueue, "queue name cannot be empty")
	}

	if err := bus.ValidatePattern(pattern); err != nil {
		return err
	}

	tx, err := d.BeginTx(ctx, nil)
	if err != nil {
		return errors.Wrap(err, "failed to begin transaction")
	}
	defer func() {
		if xerr != nil {
			_ = tx.Rollback()
		}
	}()

	_, err = tx.ExecContext(
		ctx,
		`DELETE FROM gobus_subscriptions 
         WHERE queue = $1 AND pattern = $2`,
		queue, pattern,
	)
	if err != nil {
		return errors.Wrap(err, "failed to delete subscription")
	}

	if err := d.updateMetadata(ctx, tx); err != nil {
		return err
	}

	if err = tx.Commit(); err != nil {
		return errors.Wrap(err, "failed to commit transaction")
	}

	return nil
}

var _ bus.Subscription = (*subscription)(nil)

// subscription is an implementation of the Subscription interface.
type subscription struct {
	id           int64
	queue        string
	pattern      string
	d            *Dialect
	regexPattern string
	plan         bus.PlanConfig
	createdAt    time.Time
	updatedAt    time.Time
}

// ID returns the unique identifier of the subscription.
func (s *subscription) ID() int64 {
	return s.id
}

// Queue returns the name of the queue that receives messages.
func (s *subscription) Queue() string {
	return s.queue
}

// Pattern returns the subject pattern this subscription matches against.
func (s *subscription) Pattern() string {
	return s.pattern
}

// PlanConfig returns the plan configuration for this subscription.
func (s *subscription) PlanConfig() bus.PlanConfig {
	return s.plan
}

// Unsubscribe removes this subscription.
func (s *subscription) Unsubscribe(ctx context.Context) error {
	return s.d.Delete(ctx, s.queue, s.pattern)
}
