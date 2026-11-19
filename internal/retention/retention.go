package retention

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	// Policy (issue #2 req 1): raw ops_events 90d; ops_prompts samples 180d
	// (prompt_hash + counters persist, prompt_text only is dropped);
	// mined_issues / feedback unlimited.
	eventsRetention  = 90 * 24 * time.Hour
	promptsRetention = 180 * 24 * time.Hour
	batchSize        = 5000
)

// Result reports what one cleanup pass deleted/cleared.
type Result struct {
	EventsDeleted   int64    `json:"events_deleted"`
	PromptsCleared  int64    `json:"prompts_cleared"`
	DropsRolledUp   int64    `json:"drop_counters_rolled_up"`
	DropBucketsSeen []string `json:"-"`
}

// Run executes one full cleanup pass: batched deletes with logged row
// counts; any error propagates to the caller so supervisor health history
// surfaces the failure (issue #2 req 2).
func Run(ctx context.Context, pool *pgxpool.Pool) (Result, error) {
	res := Result{}

	for {
		tag, err := pool.Exec(ctx,
			`DELETE FROM ops_events WHERE id IN (
				SELECT id FROM ops_events WHERE ts < now() - $1::interval LIMIT $2
			)`, fmt.Sprintf("%.0f seconds", eventsRetention.Seconds()), batchSize)
		if err != nil {
			return res, fmt.Errorf("delete ops_events batch: %w", err)
		}
		if tag.RowsAffected() == 0 {
			break
		}
		res.EventsDeleted += tag.RowsAffected()
		slog.Info("retention: ops_events batch deleted", "rows", tag.RowsAffected())
	}

	for {
		tag, err := pool.Exec(ctx,
			`UPDATE ops_prompts SET prompt_text = NULL WHERE id IN (
				SELECT id FROM ops_prompts WHERE ts < now() - $1::interval AND prompt_text IS NOT NULL LIMIT $2
			)`, fmt.Sprintf("%.0f seconds", promptsRetention.Seconds()), batchSize)
		if err != nil {
			return res, fmt.Errorf("clear ops_prompts batch: %w", err)
		}
		if tag.RowsAffected() == 0 {
			break
		}
		res.PromptsCleared += tag.RowsAffected()
		slog.Info("retention: ops_prompts batch cleared", "rows", tag.RowsAffected())
	}

	rolled, buckets, err := rollupDropCounters(ctx, pool)
	if err != nil {
		return res, err
	}
	res.DropsRolledUp = rolled
	res.DropBucketsSeen = buckets

	slog.Info("retention pass complete", "events_deleted", res.EventsDeleted,
		"prompts_cleared", res.PromptsCleared, "drop_counters_rolled_up", res.DropsRolledUp)
	return res, nil
}

// rollupDropCounters aggregates non-aggregated drop counters from past
// months into ops_events (event_type=emitter_drop) so silent emitter loss
// stays visible (issue #2 req 3), then marks the buckets aggregated.
func rollupDropCounters(ctx context.Context, pool *pgxpool.Pool) (int64, []string, error) {
	rows, err := pool.Query(ctx, `
		SELECT source, date_trunc('month', bucket_day)::date::text AS month, SUM(dropped) AS dropped
		FROM drop_counters
		WHERE NOT aggregated AND bucket_day < date_trunc('month', now())
		GROUP BY source, month
		ORDER BY month, source`)
	if err != nil {
		return 0, nil, fmt.Errorf("query drop counters: %w", err)
	}
	type roll struct {
		source, month string
		dropped       int64
	}
	var rolls []roll
	for rows.Next() {
		var r roll
		if err := rows.Scan(&r.source, &r.month, &r.dropped); err != nil {
			rows.Close()
			return 0, nil, fmt.Errorf("scan drop counter roll: %w", err)
		}
		rolls = append(rolls, r)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return 0, nil, fmt.Errorf("iterate drop counter rolls: %w", err)
	}

	var total int64
	buckets := []string{}
	for _, r := range rolls {
		tx, err := pool.Begin(ctx)
		if err != nil {
			return total, buckets, fmt.Errorf("begin drop rollup tx: %w", err)
		}
		if _, err := tx.Exec(ctx, `
			INSERT INTO ops_events (ts, source, event_type, payload)
			VALUES (date_trunc('month', $1::date) + interval '1 month - 1 second',
			        $2, 'emitter_drop',
			        jsonb_build_object('month', $1::text, 'dropped', $3::bigint))`,
			r.month, r.source, r.dropped); err != nil {
			_ = tx.Rollback(ctx)
			return total, buckets, fmt.Errorf("insert emitter_drop event: %w", err)
		}
		if _, err := tx.Exec(ctx,
			`UPDATE drop_counters SET aggregated = TRUE WHERE source = $1 AND date_trunc('month', bucket_day)::date = $2::date`,
			r.source, r.month); err != nil {
			_ = tx.Rollback(ctx)
			return total, buckets, fmt.Errorf("mark drop buckets aggregated: %w", err)
		}
		if err := tx.Commit(ctx); err != nil {
			return total, buckets, fmt.Errorf("commit drop rollup: %w", err)
		}
		total += r.dropped
		buckets = append(buckets, fmt.Sprintf("%s/%s", r.source, r.month))
		slog.Info("retention: drop counters rolled up", "source", r.source, "month", r.month, "dropped", r.dropped)
	}
	return total, buckets, nil
}
