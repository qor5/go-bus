package pgbus

import (
	"context"
	"database/sql"
	"math/rand"
	"strings"
	"time"

	"github.com/pkg/errors"
	"github.com/qor5/go-bus"
	"github.com/qor5/go-que/pg"
)

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
        expires_at TIMESTAMP WITH TIME ZONE, -- Pre-computed expiration time, NULL means never expires
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
    
	 -- Index for created_at, updated_at, deleted_at
    CREATE INDEX IF NOT EXISTS gobus_subscriptions_deleted_at_idx
        ON gobus_subscriptions (deleted_at);
    CREATE INDEX IF NOT EXISTS gobus_subscriptions_created_at_idx
        ON gobus_subscriptions (created_at);
    CREATE INDEX IF NOT EXISTS gobus_subscriptions_updated_at_idx
        ON gobus_subscriptions (updated_at);

    -- Partial unique constraint: only for non-deleted records
    CREATE UNIQUE INDEX IF NOT EXISTS gobus_subscriptions_queue_pattern_idx 
        ON gobus_subscriptions (queue, pattern) WHERE deleted_at IS NULL;
    
    -- Index for queue lookups (only for active subscriptions)
    CREATE INDEX IF NOT EXISTS gobus_subscriptions_queue_idx 
        ON gobus_subscriptions (queue) WHERE deleted_at IS NULL;
        
    -- Index for expires_at field - enables fast expiration checks
    CREATE INDEX IF NOT EXISTS gobus_subscriptions_expires_at_idx
        ON gobus_subscriptions (expires_at) WHERE deleted_at IS NULL;
        
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
    
    -- Create drain function for safe job cleanup with advisory locks
    CREATE OR REPLACE FUNCTION gobus_drain_subscription_jobs(p_queue VARCHAR, p_pattern VARCHAR)
    RETURNS INTEGER AS $$
    DECLARE
        job_record RECORD;
        lock_acquired BOOLEAN;
        deleted_count INTEGER := 0;
    BEGIN
        -- Loop through all jobs for this queue that match the subscription pattern
        -- Only process jobs that are truly pending (not done, not expired, and retry_count = 0)
        FOR job_record IN 
            SELECT id FROM goque_jobs 
            WHERE queue = p_queue 
              AND args::jsonb->0->'header'->'`+bus.HeaderSubscriptionPattern+`'->>0 = p_pattern
              AND (done_at IS NULL AND expired_at IS NULL AND retry_count = 0)
            ORDER BY id
        LOOP
            -- Try to acquire an exclusive advisory lock on the job ID
            -- This ensures we don't interfere with jobs that might be picked up
            SELECT pg_try_advisory_lock(job_record.id) INTO lock_acquired;
            
            IF lock_acquired THEN
                -- Delete the job
                DELETE FROM goque_jobs WHERE id = job_record.id;
                deleted_count := deleted_count + 1;
                
                -- Release the advisory lock
                PERFORM pg_advisory_unlock(job_record.id);
            END IF;
            -- If lock couldn't be acquired, skip this job (it's likely being processed)
        END LOOP;
        
        RETURN deleted_count;
    END;
    $$ LANGUAGE plpgsql;
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
