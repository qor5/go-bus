package bussql

import (
	"context"
	"database/sql"
)

// Executor defines the common interface for database operations.
// Both *sql.DB and *sql.Tx implement this interface, allowing code to work
// with either a direct database connection or within a transaction context.
type Executor interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}

var (
	_ Executor = (*sql.DB)(nil)
	_ Executor = (*sql.Tx)(nil)
)

// ctxKeyExecutor is the context key for storing an Executor.
type ctxKeyExecutor struct{}

// FromContext retrieves an Executor from the context.
// If no Executor is found in the context, it returns the provided fallback.
// This is useful for propagating transactions through the call stack.
//
// Example:
//
//	func doSomething(ctx context.Context, db *sql.DB) error {
//	    exec := bustx.FromContext(ctx, db)
//	    _, err := exec.ExecContext(ctx, "INSERT INTO ...")
//	    return err
//	}
func FromContext(ctx context.Context, fallback Executor) Executor {
	if executor, ok := ctx.Value(ctxKeyExecutor{}).(Executor); ok {
		return executor
	}
	return fallback
}

// NewContext returns a new context with the given Executor attached.
// Use this to propagate a transaction through the call stack, allowing
// nested functions to participate in the same transaction.
//
// Example:
//
//	bustx.Transaction(ctx, db, func(ctx context.Context, tx *sql.Tx) error {
//	    ctx = bustx.NewContext(ctx, tx)
//	    return doSomething(ctx, db) // doSomething will use tx via FromContext
//	})
func NewContext(ctx context.Context, executor Executor) context.Context {
	return context.WithValue(ctx, ctxKeyExecutor{}, executor)
}
