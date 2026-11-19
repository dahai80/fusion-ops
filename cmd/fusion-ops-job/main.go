package main

import (
	"context"
	"flag"
	"log/slog"
	"os"
	"time"

	"github.com/dahai80/fusion-ops/internal/cluster"
	"github.com/dahai80/fusion-ops/internal/retention"
	"github.com/dahai80/fusion-ops/internal/store"
)

// fusion-ops-job runs a single maintenance pass and exits. Supervisor T2
// tier schedules it (retention nightly off-peak, clustering periodic).
// Usage: fusion-ops-job -job=retention|cluster|migrate
func main() {
	dsn := os.Getenv("FUSION_OPS_DATABASE_URL")
	if dsn == "" {
		dsn = "postgres://127.0.0.1:5432/fusion_ops?sslmode=disable"
	}
	job := flag.String("job", "retention", "job to run: retention|cluster|migrate")
	dir := flag.String("dir", "migrations", "migrations directory (migrate job)")
	flag.Parse()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	pool, err := store.Connect(ctx, dsn)
	if err != nil {
		slog.Error("job: database unavailable", "job", *job, "error", err)
		os.Exit(1)
	}
	defer pool.Close()

	switch *job {
	case "migrate":
		if err := store.Migrate(ctx, pool, *dir); err != nil {
			slog.Error("job: migrate failed", "error", err)
			os.Exit(1)
		}
	case "retention":
		res, err := retention.Run(ctx, pool)
		if err != nil {
			slog.Error("job: retention failed", "error", err)
			os.Exit(1)
		}
		slog.Info("job: retention done", "events_deleted", res.EventsDeleted,
			"prompts_cleared", res.PromptsCleared, "drop_counters_rolled_up", res.DropsRolledUp)
	case "cluster":
		n, err := cluster.Run(ctx, pool)
		if err != nil {
			slog.Error("job: cluster failed", "error", err)
			os.Exit(1)
		}
		slog.Info("job: cluster done", "clusters_upserted", n)
	default:
		slog.Error("job: unknown job", "job", *job)
		os.Exit(2)
	}
}
