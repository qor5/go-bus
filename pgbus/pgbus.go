package pgbus

import (
	"database/sql"

	"github.com/qor5/go-bus"
)

// New creates a new Bus instance backed by PostgreSQL.
// It sets up the required database dialect and go-que implementation,
// and automatically performs database migrations.
func New(db *sql.DB) (bus.Bus, error) {
	dialect, err := NewDialect(db)
	if err != nil {
		return nil, err
	}

	return bus.New(dialect)
}
