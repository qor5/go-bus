package pgbus

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"math/rand"
	"strings"
	"time"

	"github.com/pkg/errors"
	"github.com/qor5/go-bus"
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
        deleted_at TIMESTAMP WITH TIME ZONE, -- Soft delete timestamp, NULL means not deleted
        ttl_milliseconds INTEGER NOT NULL DEFAULT 0, -- TTL in milliseconds, 0 means no expiration
        heartbeat_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(), -- Last heartbeat timestamp
        -- Pattern token columns for optimized indexed lookups
        token_0 VARCHAR(64),
        token_1 VARCHAR(64),
        token_2 VARCHAR(64),
        token_3 VARCHAR(64),
        token_4 VARCHAR(64),
        token_5 VARCHAR(64),
        token_6 VARCHAR(64),
        token_7 VARCHAR(64),
        token_8 VARCHAR(64),
        token_9 VARCHAR(64),
        token_10 VARCHAR(64),
        token_11 VARCHAR(64),
        token_12 VARCHAR(64),
        token_13 VARCHAR(64),
        token_14 VARCHAR(64),
        token_15 VARCHAR(64)
    );
    
    -- Partial unique constraint: only for non-deleted records
    CREATE UNIQUE INDEX IF NOT EXISTS gobus_subscriptions_queue_pattern_idx 
        ON gobus_subscriptions (queue, pattern) WHERE deleted_at IS NULL;
    
    -- Index for queue lookups (only for active subscriptions)
    CREATE INDEX IF NOT EXISTS gobus_subscriptions_queue_idx 
        ON gobus_subscriptions (queue) WHERE deleted_at IS NULL;
        
    -- Index for soft delete status (no condition needed)
    CREATE INDEX IF NOT EXISTS gobus_subscriptions_deleted_at_idx
        ON gobus_subscriptions (deleted_at);
        
    -- Index for created_at and updated_at (no condition needed for management)
    CREATE INDEX IF NOT EXISTS gobus_subscriptions_created_at_idx
        ON gobus_subscriptions (created_at);
    CREATE INDEX IF NOT EXISTS gobus_subscriptions_updated_at_idx
        ON gobus_subscriptions (updated_at);
        
    -- Index for TTL cleanup - filter records with TTL enabled and not deleted
    CREATE INDEX IF NOT EXISTS gobus_subscriptions_ttl_cleanup_idx
        ON gobus_subscriptions (ttl_milliseconds) WHERE ttl_milliseconds > 0 AND deleted_at IS NULL;
        
    -- Optimized indexes for pattern token lookups (only active records)
    CREATE INDEX IF NOT EXISTS gobus_subscriptions_token_0_idx ON gobus_subscriptions (token_0) WHERE deleted_at IS NULL;
    CREATE INDEX IF NOT EXISTS gobus_subscriptions_token_1_idx ON gobus_subscriptions (token_1) WHERE deleted_at IS NULL;
    CREATE INDEX IF NOT EXISTS gobus_subscriptions_token_2_idx ON gobus_subscriptions (token_2) WHERE deleted_at IS NULL;
    CREATE INDEX IF NOT EXISTS gobus_subscriptions_token_3_idx ON gobus_subscriptions (token_3) WHERE deleted_at IS NULL;
    CREATE INDEX IF NOT EXISTS gobus_subscriptions_token_4_idx ON gobus_subscriptions (token_4) WHERE deleted_at IS NULL;
    CREATE INDEX IF NOT EXISTS gobus_subscriptions_token_5_idx ON gobus_subscriptions (token_5) WHERE deleted_at IS NULL;
    CREATE INDEX IF NOT EXISTS gobus_subscriptions_token_6_idx ON gobus_subscriptions (token_6) WHERE deleted_at IS NULL;
    CREATE INDEX IF NOT EXISTS gobus_subscriptions_token_7_idx ON gobus_subscriptions (token_7) WHERE deleted_at IS NULL;
    CREATE INDEX IF NOT EXISTS gobus_subscriptions_token_8_idx ON gobus_subscriptions (token_8) WHERE deleted_at IS NULL;
    CREATE INDEX IF NOT EXISTS gobus_subscriptions_token_9_idx ON gobus_subscriptions (token_9) WHERE deleted_at IS NULL;
    CREATE INDEX IF NOT EXISTS gobus_subscriptions_token_10_idx ON gobus_subscriptions (token_10) WHERE deleted_at IS NULL;
    CREATE INDEX IF NOT EXISTS gobus_subscriptions_token_11_idx ON gobus_subscriptions (token_11) WHERE deleted_at IS NULL;
    CREATE INDEX IF NOT EXISTS gobus_subscriptions_token_12_idx ON gobus_subscriptions (token_12) WHERE deleted_at IS NULL;
    CREATE INDEX IF NOT EXISTS gobus_subscriptions_token_13_idx ON gobus_subscriptions (token_13) WHERE deleted_at IS NULL;
    CREATE INDEX IF NOT EXISTS gobus_subscriptions_token_14_idx ON gobus_subscriptions (token_14) WHERE deleted_at IS NULL;
    CREATE INDEX IF NOT EXISTS gobus_subscriptions_token_15_idx ON gobus_subscriptions (token_15) WHERE deleted_at IS NULL;
        
    CREATE TABLE IF NOT EXISTS gobus_metadata (
        version BIGINT NOT NULL DEFAULT 1,
        updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
        total_subscriptions BIGINT NOT NULL DEFAULT 0
    );
    
    -- Insert initial metadata row if it doesn't exist
    INSERT INTO gobus_metadata (version, updated_at, total_subscriptions)
    SELECT 1, NOW(), (SELECT COUNT(1) FROM gobus_subscriptions WHERE deleted_at IS NULL)
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

// scanSubscriptions scans rows and converts them to subscription objects
func (d *Dialect) scanSubscriptions(rows *sql.Rows) ([]bus.Subscription, error) {
	var subscriptions []bus.Subscription
	for rows.Next() {
		var sub subscription
		var planData []byte
		var deletedAt sql.NullTime // Read but don't store in struct
		if err := rows.Scan(
			&sub.id,
			&sub.queue,
			&sub.pattern,
			&sub.regexPattern,
			&planData,
			&sub.createdAt,
			&sub.updatedAt,
			&deletedAt, // Read deleted_at but don't use it
			&sub.ttlMilliseconds,
			&sub.heartbeatAt,
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
	// TODO: 这个可能对索引不友好，需要优化。并且目前索引里像 token 那些需要加 WHERE 吗？
	excludeExpired = "(ttl_milliseconds <= 0 OR (NOW() - heartbeat_at) <= (ttl_milliseconds * INTERVAL '1 millisecond'))"
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
		SELECT id, queue, pattern, regex_pattern, plan, created_at, updated_at, deleted_at,
		       ttl_milliseconds, heartbeat_at,
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
	rows, err := d.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to query subscriptions with strategy %s", d.queryStrategy)
	}
	defer rows.Close()

	return d.scanSubscriptions(rows)
}

// ByQueue finds all subscriptions for a specific queue.
func (d *Dialect) ByQueue(ctx context.Context, queue string) ([]bus.Subscription, error) {
	if strings.TrimSpace(queue) == "" {
		return nil, errors.Wrap(bus.ErrInvalidQueue, "queue cannot be empty")
	}

	query := buildSubscriptionQuery("queue = $1")
	rows, err := d.db.QueryContext(ctx, query, queue)
	if err != nil {
		return nil, errors.Wrap(err, "failed to query subscriptions")
	}
	defer rows.Close()

	return d.scanSubscriptions(rows)
}

func (d *Dialect) byQueueAndPattern(ctx context.Context, queue, pattern string) (*subscription, error) {
	query := buildSubscriptionQuery("queue = $1 AND pattern = $2")
	rows, err := d.db.QueryContext(ctx, query, queue, pattern)
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
func (d *Dialect) Upsert(ctx context.Context, queue, pattern string, opts *bus.SubscribeOptions) (_ bus.Subscription, xerr error) {
	if strings.TrimSpace(queue) == "" {
		return nil, errors.Wrap(bus.ErrInvalidQueue, "queue name cannot be empty")
	}

	regexPattern, err := bus.ToRegexPattern(pattern)
	if err != nil {
		return nil, err
	}

	planData, err := json.Marshal(opts.PlanConfig)
	if err != nil {
		return nil, errors.Wrap(err, "failed to serialize subscription plan")
	}

	// Parse pattern into tokens for indexed lookup optimization
	tokens := parsePatternTokens(pattern)

	// Calculate TTL in milliseconds with validation
	var ttlMilliseconds int64 = 0
	if opts != nil && opts.TTL > 0 {
		// Validate that TTL can be accurately represented in milliseconds
		if opts.TTL%time.Millisecond != 0 {
			return nil, errors.New("TTL must be divisible by time.Millisecond")
		}
		ttlMilliseconds = int64(opts.TTL.Milliseconds())
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
			opts.PlanConfig.Equal(existingSub.plan) &&
			compareTokens(existingSub.tokens, tokens) &&
			ttlMilliseconds == existingSub.ttlMilliseconds {
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
	// TODO: 需要确保删除已经过期的订阅先
	rows, err := tx.QueryContext(
		ctx,
		`INSERT INTO gobus_subscriptions 
         (queue, pattern, regex_pattern, plan, created_at, updated_at,
          ttl_milliseconds, heartbeat_at,
          token_0, token_1, token_2, token_3, token_4, token_5, token_6, token_7,
          token_8, token_9, token_10, token_11, token_12, token_13, token_14, token_15) 
         VALUES ($1, $2, $3, $4, NOW(), NOW(), $5, NOW(), $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21)
         ON CONFLICT (queue, pattern) WHERE deleted_at IS NULL
         DO UPDATE SET 
            regex_pattern = EXCLUDED.regex_pattern,
            plan = EXCLUDED.plan,
            updated_at = NOW(),
            ttl_milliseconds = EXCLUDED.ttl_milliseconds,
            heartbeat_at = NOW(),
            token_0 = EXCLUDED.token_0,
            token_1 = EXCLUDED.token_1,
            token_2 = EXCLUDED.token_2,
            token_3 = EXCLUDED.token_3,
            token_4 = EXCLUDED.token_4,
            token_5 = EXCLUDED.token_5,
            token_6 = EXCLUDED.token_6,
            token_7 = EXCLUDED.token_7,
            token_8 = EXCLUDED.token_8,
            token_9 = EXCLUDED.token_9,
            token_10 = EXCLUDED.token_10,
            token_11 = EXCLUDED.token_11,
            token_12 = EXCLUDED.token_12,
            token_13 = EXCLUDED.token_13,
            token_14 = EXCLUDED.token_14,
            token_15 = EXCLUDED.token_15
		RETURNING id, queue, pattern, regex_pattern, plan, created_at, updated_at, deleted_at,
		          ttl_milliseconds, heartbeat_at,
		          token_0, token_1, token_2, token_3, token_4, token_5, token_6, token_7,
		          token_8, token_9, token_10, token_11, token_12, token_13, token_14, token_15`,
		queue, pattern, regexPattern, planData, ttlMilliseconds,
		tokens[0], tokens[1], tokens[2], tokens[3],
		tokens[4], tokens[5], tokens[6], tokens[7],
		tokens[8], tokens[9], tokens[10], tokens[11],
		tokens[12], tokens[13], tokens[14], tokens[15],
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
func (d *Dialect) delete(ctx context.Context, queue, pattern string) (xerr error) {
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
		`UPDATE gobus_subscriptions 
         SET deleted_at = NOW(), updated_at = NOW() 
         WHERE queue = $1 AND pattern = $2 AND deleted_at IS NULL`,
		queue, pattern,
	)
	if err != nil {
		return errors.Wrap(err, "failed to soft delete subscription")
	}

	if err := d.updateMetadata(ctx, tx); err != nil {
		return err
	}

	if err = tx.Commit(); err != nil {
		return errors.Wrap(err, "failed to commit transaction")
	}

	return nil
}

// updateHeartbeat updates the heartbeat timestamp for a subscription.
func (d *Dialect) updateHeartbeat(ctx context.Context, queue, pattern string) error {
	if strings.TrimSpace(queue) == "" {
		return errors.Wrap(bus.ErrInvalidQueue, "queue name cannot be empty")
	}

	if err := bus.ValidatePattern(pattern); err != nil {
		return err
	}

	result, err := d.db.ExecContext(ctx,
		`UPDATE gobus_subscriptions 
         SET heartbeat_at = NOW() 
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

// CleanupExpiredSubscriptions soft deletes subscriptions that have exceeded their TTL.
// Returns the number of subscriptions that were marked as deleted.
func (d *Dialect) CleanupExpiredSubscriptions(ctx context.Context) (_ int64, xerr error) {
	tx, err := d.BeginTx(ctx, nil)
	if err != nil {
		return 0, errors.Wrap(err, "failed to begin transaction")
	}
	defer func() {
		if xerr != nil {
			_ = tx.Rollback()
		}
	}()

	result, err := tx.ExecContext(ctx,
		`UPDATE gobus_subscriptions 
         SET deleted_at = NOW(), updated_at = NOW()
         WHERE ttl_milliseconds > 0 
           AND (NOW() - heartbeat_at) > (ttl_milliseconds * INTERVAL '1 millisecond')
           AND deleted_at IS NULL`)
	if err != nil {
		return 0, errors.Wrap(err, "failed to soft delete expired subscriptions")
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return 0, errors.Wrap(err, "failed to get affected rows")
	}

	if rowsAffected > 0 {
		if err := d.updateMetadata(ctx, tx); err != nil {
			return 0, err
		}
	}

	if err = tx.Commit(); err != nil {
		return 0, errors.Wrap(err, "failed to commit transaction")
	}

	return rowsAffected, nil
}

var _ bus.Subscription = (*subscription)(nil)

// subscription is an implementation of the Subscription interface.
type subscription struct {
	id              int64
	queue           string
	pattern         string
	d               *Dialect
	regexPattern    string
	plan            bus.PlanConfig
	createdAt       time.Time
	updatedAt       time.Time
	ttlMilliseconds int64                         // TTL in milliseconds, 0 means no expiration
	heartbeatAt     time.Time                     // Last heartbeat timestamp
	tokens          [bus.MaxPatternTokens]*string // Parsed tokens for this subscription
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
	return s.d.delete(ctx, s.queue, s.pattern)
}

// Heartbeat updates the heartbeat timestamp for this subscription.
func (s *subscription) Heartbeat(ctx context.Context) error {
	return s.d.updateHeartbeat(ctx, s.queue, s.pattern)
}

// ExpiresAt returns the expiration time for this subscription.
// Returns zero time if the subscription never expires (no TTL).
func (s *subscription) ExpiresAt() time.Time {
	if s.ttlMilliseconds <= 0 {
		return time.Time{}
	}

	return s.heartbeatAt.Add(time.Duration(s.ttlMilliseconds) * time.Millisecond)
}
