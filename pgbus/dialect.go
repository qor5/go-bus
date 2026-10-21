package pgbus

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/pkg/errors"
	"github.com/qor5/go-bus"
	"github.com/qor5/go-bus/bussql"
	"github.com/qor5/go-que"
	"github.com/qor5/go-que/pg"
)

// QueryStrategy defines the query strategy for BySubject operations
type QueryStrategy string

const (
	QueryStrategyIndexed QueryStrategy = "INDEXED" // Use indexed lookup optimization
	QueryStrategyRegexp  QueryStrategy = "REGEXP"  // Use regexp-based query
)

var _ bus.Dialect = (*Dialect)(nil)

// Dialect is a PostgreSQL-specific implementation of the bus.Dialect interface.
// It provides database operations for the message bus using PostgreSQL.
type Dialect struct {
	db            *sql.DB
	goq           que.Queue
	queryStrategy QueryStrategy
}

// NewDialect creates a new PostgreSQL-specific implementation of the bus.Dialect interface.
// It requires a valid PostgreSQL database connection.
func NewDialect(db *sql.DB) (*Dialect, error) {
	goq, err := pg.NewWithOptions(pg.Options{DB: db, DBMigrate: false})
	if err != nil {
		return nil, errors.Wrap(err, "failed to create GoQue instance")
	}
	return &Dialect{
		db:            db,
		goq:           goq,
		queryStrategy: QueryStrategyIndexed, // Default to indexed strategy
	}, nil
}

// WithQueryStrategy sets the query strategy for BySubject operations
func (d *Dialect) WithQueryStrategy(strategy QueryStrategy) *Dialect {
	d.queryStrategy = strategy
	return d
}

// GoQue returns the underlying GoQue instance.
func (d *Dialect) GoQue() que.Queue {
	return d.goq
}

// ExecTx executes fn within a database transaction.
// If the context already contains a transaction (via bussql.NewContext), that transaction
// is reused (with a savepoint if enabled). Otherwise, a new transaction is started.
// The transaction is stored in context via bussql.NewContext for nested calls.
func (d *Dialect) ExecTx(ctx context.Context, fn func(ctx context.Context, tx *sql.Tx) error, opts ...bussql.TransactionOption) error {
	exec := bussql.FromContext(ctx, d.db)
	return bussql.Transaction(ctx, exec, func(ctx context.Context, tx *sql.Tx) error {
		return fn(bussql.NewContext(ctx, tx), tx)
	}, opts...)
}

var MaxMigrationAttempts = 10

// GetMetadata retrieves the current bus metadata
func (d *Dialect) GetMetadata(ctx context.Context) (*bus.Metadata, error) {
	var meta bus.Metadata
	exec := bussql.FromContext(ctx, d.db)
	err := exec.QueryRowContext(ctx,
		"SELECT version, updated_at, total_subscriptions FROM gobus_metadata LIMIT 1").
		Scan(&meta.Version, &meta.UpdatedAt, &meta.TotalSubscriptions)
	if err != nil {
		return nil, errors.Wrap(err, "failed to query metadata")
	}
	return &meta, nil
}

// Migrate creates the subscriptions table and indexes.
func (d *Dialect) Migrate(ctx context.Context) error {
	return Migrate(ctx, d.db)
}

// scanSubscriptions scans rows and converts them to subscription objects
func (d *Dialect) scanSubscriptions(rows *sql.Rows) ([]bus.Subscription, error) {
	var subscriptions []bus.Subscription
	for rows.Next() {
		var sub subscription
		var optionsData []byte
		var deletedAt sql.NullTime // Read but don't store in struct
		var expiresAt sql.NullTime // Read expires_at, may be NULL
		if err := rows.Scan(
			&sub.id,
			&sub.queue,
			&sub.pattern,
			&sub.regexPattern,
			&optionsData,
			&sub.createdAt,
			&sub.updatedAt,
			&deletedAt, // Read deleted_at but don't use it
			&expiresAt, // Scan expires_at into NullTime
			&sub.tokens[0],
			&sub.tokens[1],
			&sub.tokens[2],
			&sub.tokens[3],
			&sub.tokens[4],
			&sub.tokens[5],
			&sub.tokens[6],
			&sub.tokens[7],
			&sub.tokens[8],
			&sub.tokens[9],
			&sub.tokens[10],
			&sub.tokens[11],
			&sub.tokens[12],
			&sub.tokens[13],
			&sub.tokens[14],
			&sub.tokens[15],
		); err != nil {
			return nil, errors.Wrap(err, "failed to scan subscription")
		}

		sub.d = d

		if err := json.Unmarshal(optionsData, &sub.subscribeOptions); err != nil {
			return nil, errors.Wrap(err, "failed to deserialize subscription options")
		}

		// Set expires_at from scanned NullTime
		if expiresAt.Valid {
			sub.expiresAt = expiresAt.Time
		} else {
			sub.expiresAt = time.Time{} // Zero time for no expiration
		}

		subscriptions = append(subscriptions, &sub)
	}

	if err := rows.Err(); err != nil {
		return nil, errors.Wrap(err, "error iterating subscriptions")
	}

	return subscriptions, nil
}

// compareTokens compares two token arrays for equality
func compareTokens(a, b [bus.MaxPatternTokens]*string) bool {
	for i := 0; i < bus.MaxPatternTokens; i++ {
		// Both nil
		if a[i] == nil && b[i] == nil {
			continue
		}
		// One nil, one not nil
		if (a[i] == nil) != (b[i] == nil) {
			return false
		}
		// Both not nil, compare values
		if *a[i] != *b[i] {
			return false
		}
	}
	return true
}

// compareSubscribeOptions compares two SubscribeOptions for equality
func compareSubscribeOptions(a, b *bus.SubscribeOptions) bool {
	if a == nil || b == nil {
		return a == b
	}

	return a.TTL == b.TTL &&
		a.AutoDrain == b.AutoDrain &&
		a.PlanConfig.Equal(b.PlanConfig)
}

// parsePatternTokens splits a pattern into tokens for indexed lookup optimization
// When encountering ">", it fills all subsequent positions with ">" to enable
// precise matching without regex validation
func parsePatternTokens(pattern string) [bus.MaxPatternTokens]*string {
	var tokens [bus.MaxPatternTokens]*string
	parts := strings.Split(pattern, ".")

	for i, part := range parts {
		if i >= bus.MaxPatternTokens {
			break
		}

		part := part

		// Store the actual token value
		tokens[i] = &part

		// If this token is ">", fill all remaining positions with ">"
		if part == ">" {
			for j := i + 1; j < bus.MaxPatternTokens; j++ {
				tokens[j] = &part
			}
			break
		}
	}

	return tokens
}

const (
	// Simple expiration check using pre-computed expires_at field
	excludeExpired = "(expires_at IS NULL OR NOW() < expires_at)"
	excludeDeleted = "deleted_at IS NULL"
	excludeClause  = excludeDeleted + " AND " + excludeExpired
)

// buildSubscriptionQuery constructs the base SELECT query for subscriptions
// with a customizable WHERE clause, excluding expired and soft-deleted subscriptions
func buildSubscriptionQuery(where string) string {
	if where != "" {
		where = fmt.Sprintf("(%s) AND %s", where, excludeClause)
	} else {
		where = excludeClause
	}
	return fmt.Sprintf(`
		SELECT id, queue, pattern, regex_pattern, options, created_at, updated_at, deleted_at,
		       expires_at,
		       token_0, token_1, token_2, token_3, token_4, token_5, token_6, token_7,
		       token_8, token_9, token_10, token_11, token_12, token_13, token_14, token_15
		FROM gobus_subscriptions
		WHERE %s
		ORDER BY id ASC`,
		where)
}

// buildIndexedWhereClause constructs WHERE clause and arguments for indexed query optimization
func buildIndexedWhereClause(subject string) (string, []any) {
	subjectParts := strings.Split(subject, ".")
	subjectLen := len(subjectParts)

	var conditions []string
	var args []any
	argIndex := 1

	// Only need to check up to subjectLen + 1 positions
	// since pattern boundary validation is handled at subjectLen + 1
	var previousColName string
	for i := 0; i < min(subjectLen+1, bus.MaxPatternTokens); i++ {
		colName := fmt.Sprintf("token_%d", i)

		if i < subjectLen {
			// For positions within subject length: exact match OR "*" OR ">"
			part := subjectParts[i]
			condition := fmt.Sprintf("(%s = $%d OR %s = '*' OR %s = '>')",
				colName, argIndex, colName, colName)
			conditions = append(conditions, condition)
			args = append(args, part)
			argIndex++
		} else if i == subjectLen {
			// For position immediately after subject: current position must be NULL (pattern doesn't extend beyond subject)
			// OR previous position must be ">" (multi-level wildcard can match remaining tokens)
			condition := fmt.Sprintf("(%s IS NULL OR %s = '>')", colName, previousColName)
			conditions = append(conditions, condition)
		}
		previousColName = colName
	}

	return strings.Join(conditions, " AND "), args
}

// BySubject finds all subscriptions that match the given subject.
func (d *Dialect) BySubject(ctx context.Context, subject string) ([]bus.Subscription, error) {
	if err := bus.ValidateSubject(subject); err != nil {
		return nil, err
	}

	var where string
	var args []any

	switch d.queryStrategy {
	case QueryStrategyRegexp:
		where = "$1 ~ regex_pattern"
		args = []any{subject}
	case QueryStrategyIndexed:
		where, args = buildIndexedWhereClause(subject)
	default:
		return nil, errors.New("unknown query strategy")
	}

	query := buildSubscriptionQuery(where)
	exec := bussql.FromContext(ctx, d.db)
	rows, err := exec.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to query subscriptions with strategy %s", d.queryStrategy)
	}
	defer func() { _ = rows.Close() }()

	return d.scanSubscriptions(rows)
}

// ByQueue finds all subscriptions for a specific queue.
func (d *Dialect) ByQueue(ctx context.Context, queue string) ([]bus.Subscription, error) {
	if strings.TrimSpace(queue) == "" {
		return nil, errors.Wrap(bus.ErrInvalidQueue, "queue cannot be empty")
	}

	query := buildSubscriptionQuery("queue = $1")
	exec := bussql.FromContext(ctx, d.db)
	rows, err := exec.QueryContext(ctx, query, queue)
	if err != nil {
		return nil, errors.Wrap(err, "failed to query subscriptions")
	}
	defer func() { _ = rows.Close() }()

	return d.scanSubscriptions(rows)
}

// byQueueAndPattern finds the live subscription of queue and pattern and locks
// its row until tx ends.
func (d *Dialect) byQueueAndPattern(ctx context.Context, tx *sql.Tx, queue, pattern string) (*subscription, error) {
	query := buildSubscriptionQuery("queue = $1 AND pattern = $2") + " FOR UPDATE"
	rows, err := tx.QueryContext(ctx, query, queue, pattern)
	if err != nil {
		return nil, errors.Wrap(err, "failed to query subscription")
	}
	defer func() { _ = rows.Close() }()

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
func (d *Dialect) Upsert(ctx context.Context, queue, pattern string, opts *bus.SubscribeOptions) (bus.Subscription, error) {
	if strings.TrimSpace(queue) == "" {
		return nil, errors.Wrap(bus.ErrInvalidQueue, "queue name cannot be empty")
	}

	regexPattern, err := bus.ToRegexPattern(pattern)
	if err != nil {
		return nil, err
	}

	optionsData, err := json.Marshal(opts)
	if err != nil {
		return nil, errors.Wrap(err, "failed to serialize subscription options")
	}

	// Parse pattern into tokens for indexed lookup optimization
	tokens := parsePatternTokens(pattern)

	// Calculate TTL in microseconds with validation
	var ttlMicroseconds int64 = 0
	if opts != nil && opts.TTL > 0 {
		// Validate that TTL can be accurately represented in microseconds
		if opts.TTL%time.Microsecond != 0 {
			return nil, errors.New("TTL must be divisible by time.Microsecond")
		}
		ttlMicroseconds = int64(opts.TTL.Microseconds())
	}

	var resultSub bus.Subscription
	err = d.ExecTx(ctx, func(ctx context.Context, tx *sql.Tx) error {
		// Locked, so that an Unsubscribe either waits for this transaction or
		// has expired the subscription already, which then is replaced here.
		existingSub, err := d.byQueueAndPattern(ctx, tx, queue, pattern)
		if err != nil && !errors.Is(err, bus.ErrSubscriptionNotFound) {
			return err
		}

		// Skip update if subscription exists and is identical
		if existingSub != nil &&
			existingSub.pattern == pattern &&
			existingSub.queue == queue &&
			existingSub.regexPattern == regexPattern &&
			compareSubscribeOptions(opts, existingSub.subscribeOptions) &&
			compareTokens(existingSub.tokens, tokens) {
			resultSub = existingSub
			return nil
		}

		// Soft delete any existing subscription (including expired ones)
		// This preserves history and eliminates all conflict scenarios
		_, err = tx.ExecContext(ctx,
			`UPDATE gobus_subscriptions 
             SET deleted_at = NOW(), updated_at = NOW() 
             WHERE queue = $1 AND pattern = $2 AND deleted_at IS NULL`,
			queue, pattern)
		if err != nil {
			return errors.Wrap(err, "failed to soft delete existing subscription")
		}

		// Create a completely new subscription record
		rows, err := tx.QueryContext(
			ctx,
			`INSERT INTO gobus_subscriptions 
             (queue, pattern, regex_pattern, options, created_at, updated_at,
              expires_at,
              token_0, token_1, token_2, token_3, token_4, token_5, token_6, token_7,
              token_8, token_9, token_10, token_11, token_12, token_13, token_14, token_15) 
             VALUES ($1, $2, $3, $4, NOW(), NOW(), 
                     CASE WHEN $5::bigint <= 0 THEN NULL 
                          ELSE NOW() + ($5::bigint * INTERVAL '1 microsecond') 
                     END,
                     $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21)
            RETURNING id, queue, pattern, regex_pattern, options, created_at, updated_at, deleted_at,
                      expires_at,
                      token_0, token_1, token_2, token_3, token_4, token_5, token_6, token_7,
                      token_8, token_9, token_10, token_11, token_12, token_13, token_14, token_15`,
			queue, pattern, regexPattern, optionsData, ttlMicroseconds,
			tokens[0], tokens[1], tokens[2], tokens[3],
			tokens[4], tokens[5], tokens[6], tokens[7],
			tokens[8], tokens[9], tokens[10], tokens[11],
			tokens[12], tokens[13], tokens[14], tokens[15],
		)
		if err != nil {
			return errors.Wrap(err, "failed to insert or update subscription")
		}
		defer func() { _ = rows.Close() }()

		subs, err := d.scanSubscriptions(rows)
		if err != nil {
			return err
		}

		if err := d.updateMetadata(ctx, tx); err != nil {
			return err
		}

		if len(subs) == 0 {
			return bus.ErrSubscriptionNotFound
		}

		resultSub = subs[0]
		return nil
	})
	if err != nil {
		return nil, err
	}

	return resultSub, nil
}

// updateMetadata updates the version and total_subscriptions count in gobus_metadata table
func (d *Dialect) updateMetadata(ctx context.Context, tx *sql.Tx) error {
	result, err := tx.ExecContext(ctx,
		`UPDATE gobus_metadata SET 
            version = version + 1, 
            updated_at = NOW(),
            total_subscriptions = (SELECT COUNT(1) FROM gobus_subscriptions WHERE deleted_at IS NULL)`)
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

// delete performs soft delete of a subscription for the given queue and pattern.
// If autoDrain is enabled in the subscription options, it first drains the
// subscription's pending jobs, in batches (see unsubscribeInBatches).
func (d *Dialect) delete(ctx context.Context, queue, pattern string) error {
	if strings.TrimSpace(queue) == "" {
		return errors.Wrap(bus.ErrInvalidQueue, "queue name cannot be empty")
	}

	if err := bus.ValidatePattern(pattern); err != nil {
		return err
	}

	// First expire the subscription: a lookup in the database no longer
	// matches it, Heartbeat cannot revive it, and a Subscribe replaces it
	// rather than return it (the epoch, not NOW(): a Subscribe that waited
	// for this row compares with its own, earlier NOW()). If the drain then
	// fails, the subscription stays expired and calling Unsubscribe again
	// finishes it. The metadata version is bumped once, by the soft delete.
	var id int64
	err := bussql.FromContext(ctx, d.db).QueryRowContext(ctx,
		`UPDATE gobus_subscriptions SET expires_at = 'epoch', updated_at = NOW()
		 WHERE queue = $1 AND pattern = $2 AND deleted_at IS NULL
		 RETURNING id`, queue, pattern).Scan(&id)
	if errors.Is(err, sql.ErrNoRows) {
		return nil // deleting a subscription that does not exist succeeds
	}
	if err != nil {
		return errors.Wrap(err, "failed to expire subscription")
	}
	_, err = d.unsubscribeInBatches(ctx, "id = $1", id)
	return err
}

// updateHeartbeat updates the expiration timestamp for a subscription.
func (d *Dialect) updateHeartbeat(ctx context.Context, queue, pattern string) error {
	if strings.TrimSpace(queue) == "" {
		return errors.Wrap(bus.ErrInvalidQueue, "queue name cannot be empty")
	}

	if err := bus.ValidatePattern(pattern); err != nil {
		return err
	}

	exec := bussql.FromContext(ctx, d.db)
	result, err := exec.ExecContext(ctx,
		`UPDATE gobus_subscriptions 
         SET expires_at = CASE 
                            WHEN (options->>'ttl')::bigint <= 0 THEN NULL 
                            ELSE NOW() + (((options->>'ttl')::bigint / 1000) * INTERVAL '1 microsecond') 
                          END,
             updated_at = NOW()
         WHERE queue = $1 AND pattern = $2 AND `+excludeClause,
		queue, pattern)
	if err != nil {
		return errors.Wrap(err, "failed to update heartbeat")
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return errors.Wrap(err, "failed to get affected rows")
	}

	if rowsAffected == 0 {
		return errors.Wrap(bus.ErrSubscriptionNotFound, "subscription not found or expired")
	}

	return nil
}

// drain removes all pending jobs for a specific subscription.
// This method uses PostgreSQL advisory locks to safely clean up jobs without affecting running jobs.
// It filters jobs based on the subscription's pattern using the HeaderSubscriptionPattern header.
// It deletes drainBatch jobs a transaction (see drainBatchInTx), so a failure
// leaves the batches before it deleted.
// Returns the number of jobs that were deleted.
func (d *Dialect) drain(ctx context.Context, queue, pattern string) (int, error) {
	if strings.TrimSpace(queue) == "" {
		return 0, errors.Wrap(bus.ErrInvalidQueue, "queue name cannot be empty")
	}

	if err := bus.ValidatePattern(pattern); err != nil {
		return 0, err
	}

	total := 0
	var after int64
	for {
		var picked, deleted int
		err := d.ExecTx(ctx, func(ctx context.Context, tx *sql.Tx) error {
			var err error
			picked, deleted, after, err = d.drainBatchInTx(ctx, tx, queue, pattern, after)
			return err
		})
		if err != nil {
			return total, err
		}
		total += deleted
		if picked < drainBatch {
			return total, nil
		}
	}
}

var _ bus.Subscription = (*subscription)(nil)

// subscription is an implementation of the Subscription interface.
type subscription struct {
	id               uint
	queue            string
	pattern          string
	d                *Dialect
	regexPattern     string
	subscribeOptions *bus.SubscribeOptions
	createdAt        time.Time
	updatedAt        time.Time
	expiresAt        time.Time                     // Pre-computed expiration time, Zero means never expires
	tokens           [bus.MaxPatternTokens]*string // Parsed tokens for this subscription
}

// ID returns the unique identifier of the subscription.
func (s *subscription) ID() string {
	return fmt.Sprintf("%d", s.id)
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
func (s *subscription) PlanConfig() *bus.PlanConfig {
	if s.subscribeOptions == nil {
		return nil
	}
	return s.subscribeOptions.PlanConfig
}

// Unsubscribe removes this subscription.
// If autoDrain was enabled, pending jobs will be automatically cleaned up.
func (s *subscription) Unsubscribe(ctx context.Context) error {
	return s.d.delete(ctx, s.queue, s.pattern)
}

// Heartbeat updates the heartbeat timestamp for this subscription.
func (s *subscription) Heartbeat(ctx context.Context) error {
	return s.d.updateHeartbeat(ctx, s.queue, s.pattern)
}

// ExpiresAt returns the expiration time for this subscription.
// Returns zero time if the subscription never expires (no TTL).
func (s *subscription) ExpiresAt() time.Time {
	return s.expiresAt
}

// Drain removes all pending jobs that are not currently being processed.
// This method is useful for cleaning up the queue without affecting jobs that are already running.
// Returns the number of jobs that were deleted.
func (s *subscription) Drain(ctx context.Context) (int, error) {
	return s.d.drain(ctx, s.queue, s.pattern)
}

// drainBatch bounds the jobs one drain statement deletes, and so the advisory
// locks one transaction takes: draining a long queue in one statement can run
// out of lock table memory.
const drainBatch = 1000

// drainBatchInTx deletes up to drainBatch pending jobs of queue and pattern
// with ids above after, in id order. It returns how many jobs it looked at,
// how many it deleted and the last id it looked at. It skips a job being
// performed (its advisory lock is held) and a job already retried. The
// advisory lock is taken only on the jobs of the batch, and the job conditions
// are repeated on the DELETE, which is all PostgreSQL rechecks on a row a
// worker changed after the batch was read.
func (d *Dialect) drainBatchInTx(ctx context.Context, tx *sql.Tx, queue, pattern string, after int64) (picked, deleted int, last int64, err error) {
	var lastID sql.NullInt64
	err = tx.QueryRowContext(ctx, `
		WITH batch AS (
			SELECT id FROM goque_jobs
			WHERE queue = $1
			  AND args::jsonb->0->'header'->'`+bus.HeaderSubscriptionPattern+`'->>0 = $2
			  AND done_at IS NULL AND expired_at IS NULL AND retry_count = 0
			  AND id > $3
			ORDER BY id
			LIMIT $4
		), locked AS (
			SELECT id FROM batch WHERE pg_try_advisory_xact_lock(id)
		), deleted AS (
			DELETE FROM goque_jobs
			WHERE id IN (SELECT id FROM locked)
			  AND done_at IS NULL AND expired_at IS NULL AND retry_count = 0
			RETURNING id
		)
		SELECT (SELECT COUNT(*) FROM batch), (SELECT COUNT(*) FROM deleted), (SELECT MAX(id) FROM batch)`,
		queue, pattern, after, drainBatch).Scan(&picked, &deleted, &lastID)
	if err != nil {
		return 0, 0, 0, errors.Wrap(err, "failed to drain subscription jobs")
	}
	return picked, deleted, lastID.Int64, nil
}

// unsubscribeInBatches unsubscribes the live subscription that cond (with
// args) picks, if any, and reports whether it did. If the subscription asked
// for auto-drain, its pending jobs are deleted first, drainBatch at a time,
// each batch in a transaction of its own, and it is soft-deleted in the
// transaction of the last batch.
//
// Each transaction starts by locking the subscription's row. A Subscribe to
// the same queue and pattern, which soft-deletes that row, so waits for the
// batch, and the next batch no longer finds the subscription and stops: jobs
// carry only the queue and the pattern, so draining on would delete the new
// subscription's jobs. Until the soft delete the subscription stays as it
// was, so a drain cut short (an error, a deadline) is resumed by the next
// call.
func (d *Dialect) unsubscribeInBatches(ctx context.Context, cond string, args ...any) (bool, error) {
	var after int64
	for {
		var done, more bool
		err := d.ExecTx(ctx, func(ctx context.Context, tx *sql.Tx) error {
			var id int64
			var queue, pattern string
			var optionsData []byte
			err := tx.QueryRowContext(ctx,
				`SELECT id, queue, pattern, options FROM gobus_subscriptions
				 WHERE deleted_at IS NULL AND (`+cond+`)
				 FOR UPDATE`, args...).Scan(&id, &queue, &pattern, &optionsData)
			if errors.Is(err, sql.ErrNoRows) {
				return nil // unsubscribed or replaced meanwhile
			}
			if err != nil {
				return errors.Wrap(err, "failed to lock subscription")
			}
			var options bus.SubscribeOptions
			if json.Unmarshal(optionsData, &options) == nil && options.AutoDrain {
				picked, _, last, err := d.drainBatchInTx(ctx, tx, queue, pattern, after)
				if err != nil {
					return err
				}
				if picked == drainBatch {
					after, more = last, true
					return nil
				}
			}
			if _, err := tx.ExecContext(ctx,
				`UPDATE gobus_subscriptions SET deleted_at = NOW(), updated_at = NOW() WHERE id = $1`, id); err != nil {
				return errors.Wrap(err, "failed to soft delete subscription")
			}
			done = true
			return d.updateMetadata(ctx, tx)
		})
		if err != nil || !more {
			return done, err
		}
	}
}
