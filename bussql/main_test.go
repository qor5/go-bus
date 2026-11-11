package bussql_test

import (
	"context"
	"database/sql"
	"fmt"
	"net/url"
	"strings"
	"testing"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/qor5/x/v3/gormx"
)

// dsn and admin reach the PostgreSQL the test suite starts.
var (
	dsn   string
	admin *sql.DB
)

func TestMain(m *testing.M) {
	ctx := context.Background()
	suite := gormx.MustStartTestSuite(ctx)
	defer func() { _ = suite.Stop(ctx) }()

	var err error
	if admin, err = suite.DB().DB(); err != nil {
		panic(err)
	}
	dsn = suite.DSN()
	m.Run()
}

// setupTestDB opens a database of the test's own: a schema holding an empty
// users table, reached through a connection pool whose search_path is that
// schema. The caller closes it; the schema is dropped when the test ends.
func setupTestDB(t *testing.T) *sql.DB {
	t.Helper()
	schema := fmt.Sprintf("bussql_%d", time.Now().UnixNano())
	if _, err := admin.Exec("CREATE SCHEMA " + schema); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _, _ = admin.Exec("DROP SCHEMA " + schema + " CASCADE") })

	db, err := sql.Open("pgx", withSearchPath(t, schema))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`CREATE TABLE users (id SERIAL PRIMARY KEY, name TEXT, balance INTEGER)`); err != nil {
		t.Fatal(err)
	}
	return db
}

// openOtherDB opens a second pool on the same database, for tests that only
// need a distinct *sql.DB.
func openOtherDB(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatal(err)
	}
	return db
}

func withSearchPath(t *testing.T, schema string) string {
	t.Helper()
	if !strings.Contains(dsn, "://") { // key=value form
		return dsn + " search_path=" + schema
	}
	u, err := url.Parse(dsn)
	if err != nil {
		t.Fatal(err)
	}
	q := u.Query()
	q.Set("search_path", schema)
	u.RawQuery = q.Encode()
	return u.String()
}
