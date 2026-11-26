package cluster

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// Clusterable event types (flywheel plan §5 / issue #3). health_alert
// (issue #18) connects the link-daemon device-health path to the C4
// failure-clustering loop; health_probe stays raw telemetry.
var clusterableTypes = []string{"http_error", "node_failure", "autotest_defect", "feedback_bug", "health_alert"}

const sampleCap = 10

// Signature derives the stable error signature for an event row.
// payload->>'error_signature' wins when the emitter provides one; otherwise
// event_type + HTTP status is deterministic enough for clustering.
// health_alert events cluster per failing service (issue #18): the daemon
// payload identifies the service, so the signature falls back to
// health_alert:<service> instead of the status-less event_type default.
const (
	sigExprE = `COALESCE(
		NULLIF(e.payload->>'error_signature', ''),
		CASE WHEN e.event_type = 'health_alert' THEN
			e.event_type || ':' || COALESCE(
				NULLIF(e.payload->>'service', ''),
				NULLIF(e.payload->>'instance', ''),
				'unknown')
		ELSE
			e.event_type || ':' || COALESCE(e.status::text, '-')
		END
	)`
)

// Issue is the threshold-API projection of a mined_issue row.
type Issue struct {
	ClusterKey     string  `json:"cluster_key"`
	Source         string  `json:"source"`
	Signature      string  `json:"signature"`
	Severity       string  `json:"severity"`
	Count          int64   `json:"count"`
	FirstSeen      string  `json:"first_seen"`
	LastSeen       string  `json:"last_seen"`
	SampleEventIDs []int64 `json:"sample_event_ids"`
	IssueURL       string  `json:"issue_url,omitempty"`
	State          string  `json:"state"`
}

func clusterKey(source, sig string) string { return source + "|" + sig }

// Run processes ops_events above the global watermark into mined_issues.
// Watermark = MAX(last_event_id) across clusters (0 on first run). Late
// events below the watermark are not retro-clustered (documented behavior).
func Run(ctx context.Context, pool *pgxpool.Pool) (int, error) {
	var watermark int64
	if err := pool.QueryRow(ctx, `SELECT COALESCE(MAX(last_event_id), 0) FROM mined_issues`).Scan(&watermark); err != nil {
		return 0, fmt.Errorf("read watermark: %w", err)
	}

	query := fmt.Sprintf(`
		SELECT ev.source, ev.sig, ev.sev,
		       COUNT(*) AS n, MIN(ev.ts) AS first_seen, MAX(ev.ts) AS last_seen, MAX(ev.id) AS max_id,
		       COALESCE(array_agg(ev.id ORDER BY ev.id DESC) FILTER (WHERE ev.rn <= %d), '{}') AS samples
		FROM (
		    SELECT e.id, e.ts, e.source,
		           %s AS sig,
		           COALESCE(e.payload->>'severity', 'normal') AS sev,
		           ROW_NUMBER() OVER (PARTITION BY e.source, %s ORDER BY e.id DESC) AS rn
		    FROM ops_events e
		    WHERE e.event_type = ANY($2) AND e.id > $1
		) ev
		GROUP BY ev.source, ev.sig, ev.sev
		ORDER BY first_seen`, sampleCap, sigExprE, sigExprE)

	rows, err := pool.Query(ctx, query, watermark, clusterableTypes)
	if err != nil {
		return 0, fmt.Errorf("scan clusterable events: %w", err)
	}

	type agg struct {
		source, sig, sev    string
		n                   int64
		firstSeen, lastSeen time.Time
		maxID               int64
		samples             []int64
	}
	aggs := map[string]*agg{}
	order := []string{}
	for rows.Next() {
		var a agg
		if err := rows.Scan(&a.source, &a.sig, &a.sev, &a.n, &a.firstSeen, &a.lastSeen, &a.maxID, &a.samples); err != nil {
			rows.Close()
			return 0, fmt.Errorf("scan cluster row: %w", err)
		}
		key := clusterKey(a.source, a.sig)
		if existing, ok := aggs[key]; ok {
			existing.n += a.n
			if a.firstSeen.Before(existing.firstSeen) {
				existing.firstSeen = a.firstSeen
			}
			if a.lastSeen.After(existing.lastSeen) {
				existing.lastSeen = a.lastSeen
			}
			if a.maxID > existing.maxID {
				existing.maxID = a.maxID
			}
			existing.samples = a.samples
		} else {
			aggs[key] = &a
			order = append(order, key)
		}
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return 0, fmt.Errorf("iterate cluster rows: %w", err)
	}

	applied := 0
	for _, key := range order {
		a := aggs[key]
		if a.samples == nil {
			a.samples = []int64{}
		}
		_, err := pool.Exec(ctx, `
			INSERT INTO mined_issues (cluster_key, first_seen, last_seen, count, sample_event_ids, last_event_id, severity, state)
			VALUES ($1, $2, $3, $4, $5, $6, $7, 'new')
			ON CONFLICT (cluster_key) DO UPDATE SET
				last_seen = EXCLUDED.last_seen,
				count = mined_issues.count + EXCLUDED.count,
				severity = EXCLUDED.severity,
				last_event_id = GREATEST(mined_issues.last_event_id, EXCLUDED.last_event_id),
				sample_event_ids = (
					SELECT array_agg(x) FROM (
						SELECT unnest(mined_issues.sample_event_ids || EXCLUDED.sample_event_ids) AS x
						ORDER BY x DESC LIMIT $8
					) t
				)`,
			key, a.firstSeen, a.lastSeen, a.n, a.samples, a.maxID, a.sev, sampleCap)
		if err != nil {
			return applied, fmt.Errorf("upsert cluster %s: %w", key, err)
		}
		slog.Info("cluster upserted", "cluster_key", key, "new_events", a.n, "severity", a.sev)
		applied++
	}
	return applied, nil
}

// Threshold lists clusters ready for issue drafting: state=new and
// (count >= threshold within window OR severity=P0 singleton).
func Threshold(ctx context.Context, pool *pgxpool.Pool, threshold int64, window time.Duration) ([]Issue, error) {
	rows, err := pool.Query(ctx, `
		SELECT cluster_key, split_part(cluster_key, '|', 1), substr(cluster_key, position('|' in cluster_key)+1),
		       severity, count, first_seen::text, last_seen::text, sample_event_ids, issue_url, state
		FROM mined_issues
		WHERE state = 'new'
		  AND (count >= $1 OR severity = 'P0')
		  AND last_seen > now() - $2::interval
		ORDER BY count DESC, last_seen DESC`,
		threshold, fmt.Sprintf("%.0f seconds", window.Seconds()))
	if err != nil {
		return nil, fmt.Errorf("threshold query: %w", err)
	}
	return scanIssues(rows)
}

// Backfill records the drafted GitHub issue for a cluster and flips it to
// state=open, which stops further drafts (dedup, issue #3 req 3).
func Backfill(ctx context.Context, pool *pgxpool.Pool, key, issueURL string) error {
	tag, err := pool.Exec(ctx,
		`UPDATE mined_issues SET issue_url = $2, state = 'open' WHERE cluster_key = $1 AND state = 'new'`,
		key, issueURL)
	if err != nil {
		return fmt.Errorf("backfill %s: %w", key, err)
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("cluster %s not found or not in state new", key)
	}
	slog.Info("cluster issue backfilled", "cluster_key", key, "issue_url", issueURL)
	return nil
}

// Close marks a cluster's issue closed so it stops generating drafts.
func Close(ctx context.Context, pool *pgxpool.Pool, key string) error {
	tag, err := pool.Exec(ctx,
		`UPDATE mined_issues SET state = 'closed' WHERE cluster_key = $1 AND state IN ('new', 'open')`, key)
	if err != nil {
		return fmt.Errorf("close %s: %w", key, err)
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("cluster %s not found or already closed", key)
	}
	slog.Info("cluster closed", "cluster_key", key)
	return nil
}
