package testutil

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/dahai80/fusion-ops/internal/store"
)

// Fresh returns a pool to a freshly recreated per-package test database with
// migrations applied. Database name = fusion_ops_test_<package dir>, so
// parallel `go test ./...` packages never drop each other's databases.
// Skips the test when no local PostgreSQL is reachable.
func Fresh(t *testing.T) *pgxpool.Pool {
	t.Helper()
	ctx := context.Background()

	base := os.Getenv("FUSION_OPS_TEST_DATABASE_URL")
	if base == "" {
		base = "postgres://127.0.0.1:5432/postgres?sslmode=disable"
	}
	dbname := packageTestDB(t)
	dsn := withDB(base, dbname)

	adminDSN := os.Getenv("FUSION_OPS_TEST_ADMIN_URL")
	if adminDSN == "" {
		adminDSN = "postgres://127.0.0.1:5432/postgres?sslmode=disable"
	}

	admin, err := pgx.Connect(ctx, adminDSN)
	if err != nil {
		t.Skipf("admin connection unavailable: %v", err)
	}
	if _, err := admin.Exec(ctx, fmt.Sprintf(`DROP DATABASE IF EXISTS %s WITH (FORCE)`, dbname)); err != nil {
		t.Fatalf("drop test database: %v", err)
	}
	if _, err := admin.Exec(ctx, fmt.Sprintf(`CREATE DATABASE %s`, dbname)); err != nil {
		t.Fatalf("create test database: %v", err)
	}
	if err := admin.Close(ctx); err != nil {
		t.Fatalf("close admin connection: %v", err)
	}

	pool, err := store.Connect(ctx, dsn)
	if err != nil {
		t.Fatalf("connect test database: %v", err)
	}
	t.Cleanup(pool.Close)

	mdir := migrationsDir()
	if err := store.Migrate(ctx, pool, mdir); err != nil {
		t.Fatalf("migrate test database: %v", err)
	}
	return pool
}

func packageTestDB(t *testing.T) string {
	t.Helper()
	pc, _, _, ok := runtime.Caller(2)
	name := "default"
	if ok {
		file := runtime.FuncForPC(pc).Name()
		// caller package dir from the test file path is more stable via
		// func name: github.com/dahai80/fusion-ops/internal/cluster.TestX
		if i := strings.LastIndexByte(file, '.'); i >= 0 {
			name = file[:i]
			if j := strings.LastIndexByte(name, '/'); j >= 0 {
				name = name[j+1:]
			}
		}
	}
	return "fusion_ops_test_" + sanitize(name)
}

func sanitize(s string) string {
	out := strings.Map(func(r rune) rune {
		if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') || r == '_' {
			return r
		}
		return -1
	}, s)
	if out == "" {
		return "default"
	}
	return out
}

func withDB(dsn, dbname string) string {
	// String surgery: replace the path segment after host with dbname,
	// preserving any query string.
	if i := strings.LastIndexByte(dsn, '/'); i >= 0 {
		tail := dsn[i+1:]
		if q := strings.IndexByte(tail, '?'); q >= 0 {
			return dsn[:i+1] + dbname + tail[q:]
		}
		return dsn[:i+1] + dbname
	}
	return dsn
}

// TestDBName exposes the current package's test database name (for
// role-isolation tests that build their own DSNs).
func TestDBName(t *testing.T) string {
	t.Helper()
	return packageTestDB(t)
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
