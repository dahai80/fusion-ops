package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/dahai80/fusion-ops/internal/clean"
)

// fusion-ops-clean: scheduled junk/log sweep with audit (issue #15) and
// two-stage confirmation (issue #17). Stage 1 (plan): scan, audit
// would_delete, write a plan manifest with a sha256 over the candidate
// set — nothing is deleted. Stage 2 (confirm): a second invocation with
// -confirm whose re-scanned candidate set hashes to the reviewed plan;
// any workspace change invalidates the plan and the run refuses.

func main() {
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stderr, nil)))

	configPath := flag.String("config", "/opt/fusion-ops/env/clean-config.json", "policy JSON path")
	planPath := flag.String("plan", "", "plan manifest path (default: clean-plan.json next to the config)")
	confirm := flag.Bool("confirm", false, "second stage: delete exactly the reviewed plan's candidate set")
	dryRun := flag.Bool("dry-run", false, "force dry-run regardless of config (first runs)")
	dsn := flag.String("dsn", os.Getenv("FUSION_OPS_DATABASE_URL"), "ops DB DSN for the audit log (empty = log-only audit)")
	flag.Parse()

	if *planPath == "" {
		*planPath = filepath.Join(filepath.Dir(*configPath), "clean-plan.json")
	}

	policy, err := clean.LoadConfig(*configPath)
	if err != nil {
		slog.Error("config rejected", "path", *configPath, "error", err)
		os.Exit(1)
	}
	if *dryRun {
		policy.DryRun = true
	}
	if policy.QuarantineDir == "" {
		policy.QuarantineDir = filepath.Join(filepath.Dir(*configPath), "quarantine")
	}
	if policy.QuarantineDays <= 0 {
		policy.QuarantineDays = 7
	}
	if *confirm && policy.DryRun {
		slog.Error("-confirm requires \"dry_run\": false in the config")
		os.Exit(1)
	}

	planPhase := policy.DryRun || !*confirm

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

	auditFn := func(a clean.Action, decision string) {
		slog.Info("clean decision", "decision", decision, "path", a.Path, "bytes", a.Bytes, "reason", a.Reason)
		if pool != nil {
			_, err := pool.Exec(context.Background(), `INSERT INTO cleanup_audit (path, bytes, decision, reason, dry_run) VALUES ($1, $2, $3, $4, $5)`,
				a.Path, a.Bytes, decision, a.Reason, policy.DryRun)
			if err != nil {
				slog.Error("audit insert failed", "path", a.Path, "error", err)
			}
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
	defer cancel()
	start := time.Now()

	var summary map[string]any
	if planPhase {
		plan, res := clean.PlanRun(ctx, policy, auditFn)
		summary = map[string]any{
			"dry_run": res.DryRun, "candidates": len(res.Deleted), "kept": len(res.Kept),
			"candidate_bytes": res.DeletedBytes, "elapsed": time.Since(start).String(),
		}
		if !policy.DryRun {
			raw, err := json.MarshalIndent(plan, "", "  ")
			if err != nil {
				slog.Error("plan marshal failed", "error", err)
				os.Exit(1)
			}
			if err := os.WriteFile(*planPath, raw, 0o600); err != nil {
				slog.Error("plan write failed", "path", *planPath, "error", err)
				os.Exit(1)
			}
			summary["plan"] = *planPath
			summary["plan_hash"] = plan.Hash
			slog.Info("plan written; nothing deleted — review the plan, then rerun with -confirm", "plan_hash", plan.Hash, "candidates", plan.Count, "total_bytes", plan.TotalBytes)
		}
	} else {
		raw, err := os.ReadFile(*planPath)
		if err != nil {
			slog.Error("no plan to confirm; run once without -confirm first", "path", *planPath, "error", err)
			os.Exit(1)
		}
		var plan clean.Plan
		if err := json.Unmarshal(raw, &plan); err != nil || plan.Hash == "" {
			slog.Error("plan manifest unreadable", "path", *planPath, "error", err)
			os.Exit(1)
		}
		res, err := clean.ConfirmApply(ctx, policy, auditFn, plan.Hash)
		if err != nil {
			slog.Error("confirm refused", "error", err)
			os.Exit(1)
		}
		summary = map[string]any{
			"dry_run": res.DryRun, "deleted": len(res.Deleted), "kept": len(res.Kept),
			"deleted_bytes": res.DeletedBytes, "elapsed": time.Since(start).String(),
		}
		if err := os.Remove(*planPath); err != nil {
			slog.Warn("plan cleanup failed", "path", *planPath, "error", err)
		}
	}

	out, _ := json.Marshal(summary)
	slog.Info("clean run complete", "summary", string(out))
	fmt.Fprintln(os.Stderr, "done")
}
