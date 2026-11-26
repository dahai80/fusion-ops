package main

import (
	"context"
	"encoding/json"
	"flag"
	"log/slog"
	"os"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/dahai80/fusion-ops/internal/clean"
)

// fusion-ops-clean: scheduled junk/log sweep with dry-run + audit (issue #15).
// Deletions are recorded in fusion_ops.cleanup_audit (path, bytes, decision,
// reason) so every run is reviewable; dry-run is the default until the
// operator sets "dry_run": false in the config.

func main() {
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stderr, nil)))

	configPath := flag.String("config", "/opt/fusion-ops/env/clean-config.json", "policy JSON path")
	dryRun := flag.Bool("dry-run", false, "force dry-run regardless of config (first runs)")
	dsn := flag.String("dsn", os.Getenv("FUSION_OPS_DATABASE_URL"), "ops DB DSN for the audit log (empty = log-only audit)")
	flag.Parse()

	policy, err := clean.LoadConfig(*configPath)
	if err != nil {
		slog.Error("config rejected", "path", *configPath, "error", err)
		os.Exit(1)
	}
	if *dryRun {
		policy.DryRun = true
	}

	var pool *pgxpool.Pool
	if *dsn != "" {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		pool, err = pgxpool.New(ctx, *dsn)
		cancel()
		if err != nil {
			slog.Error("audit DB unavailable; refusing to run non-dry without audit trail", "error", err)
			if !policy.DryRun {
				os.Exit(1)
			}
			pool = nil
		} else {
			defer pool.Close()
		}
	} else if !policy.DryRun {
		slog.Error("real run requires FUSION_OPS_DATABASE_URL for the deletion audit")
		os.Exit(1)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
	defer cancel()
	start := time.Now()
	res := clean.Run(ctx, policy, func(a clean.Action, decision string) {
		slog.Info("clean decision", "decision", decision, "path", a.Path, "bytes", a.Bytes, "reason", a.Reason)
		if pool != nil {
			_, err := pool.Exec(ctx, `INSERT INTO cleanup_audit (path, bytes, decision, reason, dry_run) VALUES ($1, $2, $3, $4, $5)`,
				a.Path, a.Bytes, decision, a.Reason, policy.DryRun)
			if err != nil {
				slog.Error("audit insert failed", "path", a.Path, "error", err)
			}
		}
	})

	summary, _ := json.Marshal(map[string]any{
		"dry_run": res.DryRun, "deleted": len(res.Deleted), "kept": len(res.Kept),
		"deleted_bytes": res.DeletedBytes, "elapsed": time.Since(start).String(),
	})
	slog.Info("clean run complete", "summary", string(summary))
}
