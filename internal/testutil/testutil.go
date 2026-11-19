package testutil

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/dahai80/fusion-ops/internal/store"
)

// Fresh returns a pool to a freshly recreated test database with migrations
// applied. Skips the test when no local PostgreSQL is reachable.
func Fresh(t *testing.T) *pgxpool.Pool {
	t.Helper()
	ctx := context.Background()
	dsn := os.Getenv("FUSION_OPS_TEST_DATABASE_URL")
	if dsn == "" {
		dsn = "postgres://127.0.0.1:5432/fusion_ops_test?sslmode=disable"
	}
	adminDSN := os.Getenv("FUSION_OPS_TEST_ADMIN_URL")
	if adminDSN == "" {
		adminDSN = "postgres://127.0.0.1:5432/postgres?sslmode=disable"
	}

	pool, err := store.Connect(ctx, dsn)
	if err != nil {
		t.Skipf("test database unavailable: %v", err)
	}

	admin, err := pgx.Connect(ctx, adminDSN)
	if err != nil {
		pool.Close()
		t.Skipf("admin connection unavailable: %v", err)
	}
	dbname := dbOf(dsn)
	if _, err := admin.Exec(ctx, fmt.Sprintf(`DROP DATABASE IF EXISTS %s WITH (FORCE)`, dbname)); err != nil {
		pool.Close()
		t.Fatalf("drop test database: %v", err)
	}
	if _, err := admin.Exec(ctx, fmt.Sprintf(`CREATE DATABASE %s`, dbname)); err != nil {
		pool.Close()
		t.Fatalf("create test database: %v", err)
	}
	if err := admin.Close(ctx); err != nil {
		pool.Close()
		t.Fatalf("close admin connection: %v", err)
	}
	pool.Close()

	pool, err = store.Connect(ctx, dsn)
	if err != nil {
		t.Fatalf("reconnect after recreate: %v", err)
	}
	t.Cleanup(pool.Close)

	mdir := migrationsDir()
	if err := store.Migrate(ctx, pool, mdir); err != nil {
		t.Fatalf("migrate test database: %v", err)
	}
	return pool
}

func dbOf(dsn string) string {
	u, err := pgx.ParseConfig(dsn)
	if err != nil {
		return "fusion_ops_test"
	}
	if u.Database == "" {
		return "fusion_ops_test"
	}
	return u.Database
}

func migrationsDir() string {
	for _, cand := range []string{"migrations", "../migrations", "../../migrations", "../../../migrations"} {
		if entries, err := os.ReadDir(cand); err == nil && len(entries) > 0 {
			abs, _ := filepath.Abs(cand)
			return abs
		}
	}
	return "migrations"
}
