package retention_test

import (
	"context"
	"testing"

	"github.com/dahai80/fusion-ops/internal/retention"
	"github.com/dahai80/fusion-ops/internal/testutil"
)

func TestRetentionDeletesOldEvents(t *testing.T) {
	pool := testutil.Fresh(t)
	ctx := context.Background()

	for i := 0; i < 3; i++ {
		if _, err := pool.Exec(ctx,
			`INSERT INTO ops_events (ts, source, event_type) VALUES (now() - interval '100 days', 'gateway', 'http_error')`); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := pool.Exec(ctx,
		`INSERT INTO ops_events (ts, source, event_type) VALUES (now() - interval '10 days', 'gateway', 'http_error')`); err != nil {
		t.Fatal(err)
	}

	res, err := retention.Run(ctx, pool)
	if err != nil {
		t.Fatalf("retention run: %v", err)
	}
	if res.EventsDeleted != 3 {
		t.Fatalf("expected 3 events deleted, got %d", res.EventsDeleted)
	}
	var remain int
	if err := pool.QueryRow(ctx, "SELECT COUNT(*) FROM ops_events").Scan(&remain); err != nil {
		t.Fatal(err)
	}
	if remain != 1 {
		t.Fatalf("recent event must survive, remain=%d", remain)
	}
}

func TestRetentionClearsOldPromptTextKeepsHash(t *testing.T) {
	pool := testutil.Fresh(t)
	ctx := context.Background()

	if _, err := pool.Exec(ctx,
		`INSERT INTO ops_prompts (ts, source, prompt_hash, prompt_text) VALUES (now() - interval '200 days', 'cowork', 'old-h', 'secret old text')`); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx,
		`INSERT INTO ops_prompts (ts, source, prompt_hash, prompt_text) VALUES (now() - interval '30 days', 'cowork', 'new-h', 'recent text')`); err != nil {
		t.Fatal(err)
	}

	res, err := retention.Run(ctx, pool)
	if err != nil {
		t.Fatal(err)
	}
	if res.PromptsCleared != 1 {
		t.Fatalf("expected 1 prompt cleared, got %d", res.PromptsCleared)
	}
	var text *string
	if err := pool.QueryRow(ctx, "SELECT prompt_text FROM ops_prompts WHERE prompt_hash='old-h'").Scan(&text); err != nil {
		t.Fatal(err)
	}
	if text != nil {
		t.Fatalf("old prompt_text must be nulled, got %q", *text)
	}
	var recent string
	if err := pool.QueryRow(ctx, "SELECT prompt_text FROM ops_prompts WHERE prompt_hash='new-h'").Scan(&recent); err != nil {
		t.Fatal(err)
	}
	if recent != "recent text" {
		t.Fatalf("recent prompt must survive: %q", recent)
	}
}

func TestDropCounterMonthlyRollup(t *testing.T) {
	pool := testutil.Fresh(t)
	ctx := context.Background()

	// Past-month buckets: eligible for rollup. Current-month: not yet.
	if _, err := pool.Exec(ctx,
		`INSERT INTO drop_counters (source, bucket_day, dropped)
		 VALUES ('gateway', date_trunc('month', now()) - interval '1 day', 10),
		        ('gateway', date_trunc('month', now()) - interval '2 days', 5),
		        ('identity', date_trunc('month', now()) - interval '1 day', 3),
		        ('gateway', current_date, 100)`); err != nil {
		t.Fatal(err)
	}

	res, err := retention.Run(ctx, pool)
	if err != nil {
		t.Fatal(err)
	}
	if res.DropsRolledUp != 18 {
		t.Fatalf("expected 18 rolled drops, got %d", res.DropsRolledUp)
	}

	var events int
	if err := pool.QueryRow(ctx,
		"SELECT COUNT(*) FROM ops_events WHERE event_type='emitter_drop'").Scan(&events); err != nil {
		t.Fatal(err)
	}
	if events != 2 {
		t.Fatalf("expected 2 emitter_drop events (gateway + identity months), got %d", events)
	}

	// Idempotency: rerun must not duplicate rollups.
	res2, err := retention.Run(ctx, pool)
	if err != nil {
		t.Fatal(err)
	}
	if res2.DropsRolledUp != 0 {
		t.Fatalf("rollup not idempotent: %d", res2.DropsRolledUp)
	}
	if err := pool.QueryRow(ctx,
		"SELECT COUNT(*) FROM ops_events WHERE event_type='emitter_drop'").Scan(&events); err != nil {
		t.Fatal(err)
	}
	if events != 2 {
		t.Fatalf("duplicate emitter_drop events after rerun: %d", events)
	}
}
