package main

import (
	"context"
	"flag"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/dahai80/fusion-ops/internal/probe"
	"github.com/dahai80/fusion-ops/internal/store"
	"github.com/jackc/pgx/v5/pgxpool"
)

// fusion-ops-probe is the blackbox site-health probe daemon (issue #4).
// Runs on the Mac side, outside the VPS. Registered as a supervisor T2 job
// (upstream request on fusion-supervisor#137). Verdict state seeds from
// ops_events history, so one-shot invocations continue FAIL episodes.

var (
	state = map[string]*probe.Verdict{}
	prev  = map[string]probe.Result{}
)

func main() {
	once := flag.Bool("once", false, "run one probe round and exit")
	flag.Parse()

	cfg := probe.LoadConfig()
	client := &http.Client{Timeout: probe.RequestTimeout()}

	dsn := os.Getenv("FUSION_OPS_DATABASE_URL")
	if dsn == "" {
		dsn = "postgres://127.0.0.1:5432/fusion_ops?sslmode=disable"
	}

	if *once {
		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cancel()
		pool, err := store.Connect(ctx, dsn)
		if err != nil {
			slog.Warn("probe once: database unavailable, results not persisted", "error", err)
			runRound(cfg, client, probe.Store{})
			return
		}
		defer pool.Close()
		st := buildStore(pool)
		seedState(ctx, st)
		runRound(cfg, client, st)
		return
	}

	interval := 5 * time.Minute
	if v := os.Getenv("FUSION_OPS_PROBE_INTERVAL"); v != "" {
		if parsed, err := time.ParseDuration(v); err == nil && parsed > 0 {
			interval = parsed
		} else {
			slog.Warn("invalid FUSION_OPS_PROBE_INTERVAL, using default", "value", v)
		}
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	pool, err := store.Connect(ctx, dsn)
	if err != nil {
		slog.Error("probe: database unavailable", "error", err)
		os.Exit(1)
	}
	defer pool.Close()
	st := buildStore(pool)
	seedState(ctx, st)

	slog.Info("probe daemon started", "site", cfg.SiteBase, "interval", interval)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	runRound(cfg, client, st)
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			runRound(cfg, client, st)
		}
	}
}

func buildStore(pool *pgxpool.Pool) probe.Store {
	return probe.Store{
		Exec: func(ctx context.Context, sql string, args ...any) error {
			_, err := pool.Exec(ctx, sql, args...)
			return err
		},
		Query: func(ctx context.Context, sql string, args ...any) ([]probe.Result, error) {
			rows, err := pool.Query(ctx, sql, args...)
			if err != nil {
				return nil, err
			}
			defer rows.Close()
			out := []probe.Result{}
			for rows.Next() {
				var r probe.Result
				var ok bool
				if err := rows.Scan(&r.Name, &ok, &r.LatencyMS, &r.Detail, &r.TS); err != nil {
					return nil, err
				}
				r.OK = ok
				out = append(out, r)
			}
			return out, rows.Err()
		},
		QueryStrings: func(ctx context.Context, sql string, args ...any) ([]string, error) {
			rows, err := pool.Query(ctx, sql, args...)
			if err != nil {
				return nil, err
			}
			defer rows.Close()
			out := []string{}
			for rows.Next() {
				var s string
				if err := rows.Scan(&s); err != nil {
					return nil, err
				}
				out = append(out, s)
			}
			return out, rows.Err()
		},
	}
}

// seedState loads the last round's results so a fresh process continues any
// in-progress FAIL episode instead of resetting the debounce window.
func seedState(ctx context.Context, st probe.Store) {
	last, err := st.LoadLastResults(ctx)
	if err != nil {
		slog.Warn("probe: cannot load last round for seeding", "error", err)
		return
	}
	active, err := st.LoadActiveAlerts(ctx)
	if err != nil {
		slog.Warn("probe: cannot load active alerts for seeding", "error", err)
		active = nil
	}
	state = probe.SeedState(last, active)
	for name, v := range state {
		if v.Fails > 0 || v.Alerted {
			slog.Info("probe: seeded episode state", "probe", name, "prior_fails", v.Fails, "alerted", v.Alerted)
		}
	}
}

func runRound(cfg probe.Config, client *http.Client, st probe.Store) {
	ctx := context.Background()
	results, counted := probe.RunRound(cfg, client)
	for _, r := range results {
		slog.Info("probe result", "probe", r.Name, "ok", r.OK, "latency_ms", r.LatencyMS, "detail", r.Detail)
	}
	st.AppendEvents(ctx, "health_probe", results)

	var alerts, recoveries []probe.Result
	if counted {
		alerts, recoveries = probe.ApplyRound(state, results)
	}
	for _, a := range alerts {
		// Alert payload = the 2 consecutive failing results (issue #4 req 4).
		pair := []probe.Result{prev[a.Name], a}
		st.AppendEvents(ctx, "health_alert", pair)
		probe.DeliverAlert(cfg, "fail", a.Name, pair)
	}
	for _, rec := range recoveries {
		probe.DeliverAlert(cfg, "recover", rec.Name, []probe.Result{rec})
	}
	for _, r := range results {
		prev[r.Name] = r
	}
}
