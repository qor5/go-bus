package bussql_test

import (
	"context"
	"database/sql"
	"testing"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/qor5/x/v3/gormx"
)

// dsn reaches the PostgreSQL the test suite starts for this package.
var dsn string

func TestMain(m *testing.M) {
	ctx := context.Background()
	suite := gormx.MustStartTestSuite(ctx)
	defer func() { _ = suite.Stop(ctx) }()
	dsn = suite.DSN()
	m.Run()
}

// setupTestDB opens a connection pool of the test's own on a fresh, empty
// users table. The tests in this package do not run in parallel, so
// recreating the table at the start of each keeps them apart. The caller
// closes the pool.
func setupTestDB(t *testing.T) *sql.DB {
	t.Helper()
	db := openOtherDB(t)
	if _, err := db.Exec(`DROP TABLE IF EXISTS users`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`CREATE TABLE users (id SERIAL PRIMARY KEY, name TEXT, balance INTEGER)`); err != nil {
		t.Fatal(err)
	}
	return db
}

// openOtherDB opens another connection pool on the same database.
func openOtherDB(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatal(err)
	}
	return db
}
