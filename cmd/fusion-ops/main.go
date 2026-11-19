package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/dahai80/fusion-ops/internal/api"
	"github.com/dahai80/fusion-ops/internal/cluster"
	"github.com/dahai80/fusion-ops/internal/store"
	"github.com/jackc/pgx/v5/pgxpool"
)

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func main() {
	dsn := envOr("FUSION_OPS_DATABASE_URL", "postgres://127.0.0.1:5432/fusion_ops?sslmode=disable")
	addr := envOr("FUSION_OPS_LISTEN", "127.0.0.1:11400")
	interval := 15 * time.Minute
	if v := os.Getenv("FUSION_OPS_CLUSTER_INTERVAL"); v != "" {
		if parsed, err := time.ParseDuration(v); err == nil && parsed > 0 {
			interval = parsed
		} else {
			slog.Warn("invalid FUSION_OPS_CLUSTER_INTERVAL, using default", "value", v)
		}
	}
	threshold := int64(5)
	if v := os.Getenv("FUSION_OPS_THRESHOLD"); v != "" {
		if parsed, err := strconv.ParseInt(v, 10, 64); err == nil && parsed > 0 {
			threshold = parsed
		}
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	pool, err := store.Connect(ctx, dsn)
	if err != nil {
		slog.Error("startup: database unavailable", "error", err)
		os.Exit(1)
	}
	defer pool.Close()

	// Boot migration gate: app roles hold DML only (no CREATE), so if the
	// schema is already up to date this is a no-op; otherwise a real apply is
	// attempted and must succeed (deploy runbook runs cmd/migrate as admin).
	upToDate, err := store.SchemaUpToDate(ctx, pool, envOr("FUSION_OPS_MIGRATIONS_DIR", "migrations"))
	if err != nil {
		slog.Error("startup: migration state check failed", "error", err)
		os.Exit(1)
	}
	if !upToDate {
		if err := store.Migrate(ctx, pool, envOr("FUSION_OPS_MIGRATIONS_DIR", "migrations")); err != nil {
			slog.Error("startup: migration failed", "error", err)
			os.Exit(1)
		}
	}

	// Fail-closed privilege probe (issue #1 req 3): refuse to serve when the
	// role lacks full DML on required tables.
	conn, err := pool.Acquire(ctx)
	if err != nil {
		slog.Error("startup: acquire for probe failed", "error", err)
		os.Exit(1)
	}
	if err := store.ProbePrivileges(ctx, conn); err != nil {
		conn.Release()
		slog.Error("startup: privilege probe failed, refusing to start", "error", err)
		os.Exit(1)
	}
	if err := store.ProbeCanary(ctx, conn); err != nil {
		conn.Release()
		slog.Error("startup: privilege canary failed, refusing to start", "error", err)
		os.Exit(1)
	}
	conn.Release()
	slog.Info("startup: privilege probe passed")

	go clusterLoop(ctx, pool, interval, threshold)

	if err := api.NewServer(pool).Run(addr); err != nil {
		slog.Error("server exited", "error", err)
		os.Exit(1)
	}
}

func clusterLoop(ctx context.Context, pool *pgxpool.Pool, interval time.Duration, threshold int64) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			n, err := cluster.Run(ctx, pool)
			if err != nil {
				slog.Error("clustering job failed", "error", err)
				continue
			}
			if n > 0 {
				slog.Info("clustering pass done", "clusters_upserted", n, "threshold", threshold)
			}
		}
	}
}
