package cluster_test

import (
	"context"
	"encoding/json"
	"io"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/dahai80/fusion-ops/internal/api"
	"github.com/dahai80/fusion-ops/internal/cluster"
	"github.com/dahai80/fusion-ops/internal/testutil"
	"github.com/jackc/pgx/v5/pgxpool"
)

func seedEvents(t *testing.T, pool *pgxpool.Pool, n int, sig string, status int, sev string) {
	t.Helper()
	for i := 0; i < n; i++ {
		payload := `{"error_signature":"` + sig + `"}`
		if sev != "" {
			payload = `{"error_signature":"` + sig + `","severity":"` + sev + `"}`
		}
		if _, err := pool.Exec(context.Background(),
			`INSERT INTO ops_events (source, event_type, status, payload) VALUES ('gateway', 'http_error', $1, $2::jsonb)`,
			status, payload); err != nil {
			t.Fatalf("seed event: %v", err)
		}
	}
}

func clusterState(t *testing.T, pool *pgxpool.Pool, key string) (int64, string) {	t.Helper()
	var cnt int64
	var state string
	err := pool.QueryRow(context.Background(),
		"SELECT count, state FROM mined_issues WHERE cluster_key=$1", key).Scan(&cnt, &state)
	if err != nil {
		t.Fatalf("read cluster %s: %v", key, err)
	}
	return cnt, state
}

func TestRunClustersRepeated5xx(t *testing.T) {
	pool := testutil.Fresh(t)
	ctx := context.Background()

	seedEvents(t, pool, 6, "boom", 500, "")
	n, err := cluster.Run(ctx, pool)
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if n != 1 {
		t.Fatalf("expected 1 cluster, got %d", n)
	}
	cnt, state := clusterState(t, pool, "gateway|boom")
	if cnt != 6 || state != "new" {
		t.Fatalf("cluster mismatch: count=%d state=%s", cnt, state)
	}

	// Second run with no new events: watermark must prevent double counting.
	if _, err := cluster.Run(ctx, pool); err != nil {
		t.Fatalf("rerun: %v", err)
	}
	cnt, _ = clusterState(t, pool, "gateway|boom")
	if cnt != 6 {
		t.Fatalf("watermark leak: count=%d", cnt)
	}

	// One more event increments the same cluster row.
	seedEvents(t, pool, 1, "boom", 500, "")
	if _, err := cluster.Run(ctx, pool); err != nil {
		t.Fatalf("rerun2: %v", err)
	}
	cnt, _ = clusterState(t, pool, "gateway|boom")
	if cnt != 7 {
		t.Fatalf("increment failed: count=%d", cnt)
	}
}

func TestThresholdAndBackfillDedup(t *testing.T) {
	pool := testutil.Fresh(t)
	ctx := context.Background()

	seedEvents(t, pool, 6, "boom", 500, "")
	seedEvents(t, pool, 1, "p0-single", 503, "P0")
	if _, err := cluster.Run(ctx, pool); err != nil {
		t.Fatal(err)
	}

	bigWindow := 24 * 365 * 10 * time.Hour
	issues, err := cluster.Threshold(ctx, pool, 5, bigWindow)
	if err != nil {
		t.Fatal(err)
	}
	if len(issues) != 2 {
		t.Fatalf("expected 2 clusters (boom>=5 + P0 singleton), got %d", len(issues))
	}
	again, err := cluster.Threshold(ctx, pool, 5, bigWindow)
	if err != nil {
		t.Fatal(err)
	}
	if len(again) != 2 {
		t.Fatalf("threshold not stable: %d", len(again))
	}

	if err := cluster.Backfill(ctx, pool, "gateway|boom", "https://github.com/dahai80/fusion/issues/1"); err != nil {
		t.Fatalf("backfill: %v", err)
	}
	_, state := clusterState(t, pool, "gateway|boom")
	if state != "open" {
		t.Fatalf("backfill state: %s", state)
	}

	issues, err = cluster.Threshold(ctx, pool, 5, bigWindow)
	if err != nil {
		t.Fatal(err)
	}
	for _, i := range issues {
		if i.ClusterKey == "gateway|boom" {
			t.Fatal("backfilled cluster must stop appearing in threshold query")
		}
	}

	// Backfill on non-new cluster must fail.
	if err := cluster.Backfill(ctx, pool, "gateway|boom", "https://x/2"); err == nil {
		t.Fatal("backfill must reject cluster not in state new")
	}

	// Closed clusters are excluded from drafting even when P0.
	if err := cluster.Close(ctx, pool, "gateway|p0-single"); err != nil {
		t.Fatalf("close: %v", err)
	}
	issues, err = cluster.Threshold(ctx, pool, 5, bigWindow)
	if err != nil {
		t.Fatal(err)
	}
	if len(issues) != 0 {
		t.Fatalf("closed cluster must not draft, got %d", len(issues))
	}
}

func TestAPIOutputPIIFree(t *testing.T) {
	pool := testutil.Fresh(t)
	ctx := context.Background()

	// Signature carrying raw PII: served output must be scrubbed.
	seedEvents(t, pool, 6, "boom user alice@example.com", 500, "")
	if _, err := cluster.Run(ctx, pool); err != nil {
		t.Fatal(err)
	}

	srv := httptest.NewServer(api.NewServer(pool).Handler())
	defer srv.Close()

	resp, err := srv.Client().Get(srv.URL + "/api/clusters?threshold=5&hours=876000")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != 200 {
		t.Fatalf("status %d: %s", resp.StatusCode, body)
	}
	if strings.Contains(string(body), "alice@example.com") {
		t.Fatalf("raw PII leaked in API output: %s", body)
	}
	var out struct {
		Clusters []cluster.Issue `json:"clusters"`
		Count    int             `json:"count"`
	}
	if err := json.Unmarshal(body, &out); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if out.Count != 1 {
		t.Fatalf("expected 1 cluster, got %d: %s", out.Count, body)
	}
	if !strings.Contains(out.Clusters[0].Signature, "[EMAIL]") {
		t.Fatalf("signature not desensitized: %q", out.Clusters[0].Signature)
	}
}

// seedHealthAlerts plants link-daemon health_alert events (issue #18):
// no error_signature in the payload — the service field must drive the
// signature so each failing service clusters on its own.
func seedHealthAlerts(t *testing.T, pool *pgxpool.Pool, service string, n int, sev string) {
	t.Helper()
	for i := 0; i < n; i++ {
		payload := `{"service":"` + service + `"}`
		if sev != "" {
			payload = `{"service":"` + service + `","severity":"` + sev + `"}`
		}
		if _, err := pool.Exec(context.Background(),
			`INSERT INTO ops_events (source, event_type, payload) VALUES ('ops', 'health_alert', $1::jsonb)`,
			payload); err != nil {
			t.Fatalf("seed health_alert: %v", err)
		}
	}
}

func TestRunClustersHealthAlertsPerService(t *testing.T) {
	pool := testutil.Fresh(t)
	seedHealthAlerts(t, pool, "office-api", 5, "critical")
	seedHealthAlerts(t, pool, "fusion-mlx", 2, "critical")
	if _, err := cluster.Run(context.Background(), pool); err != nil {
		t.Fatalf("cluster.Run: %v", err)
	}

	// threshold 5: office-api reached it, fusion-mlx (2 events) did not.
	cnt, state := clusterState(t, pool, "ops|health_alert:office-api")
	if cnt != 5 || state != "new" {
		t.Fatalf("office-api cluster wrong: cnt=%d state=%s", cnt, state)
	}
	// Run upserts every aggregate; the threshold filter lives in the API
	// query, so fusion-mlx (2 events) gets a row but stays below threshold.
	cnt2, _ := clusterState(t, pool, "ops|health_alert:fusion-mlx")
	if cnt2 != 2 {
		t.Fatalf("fusion-mlx cluster count wrong: %d", cnt2)
	}
}

func TestRunHealthAlertWithoutServiceFallsBack(t *testing.T) {
	pool := testutil.Fresh(t)
	for i := 0; i < 5; i++ {
		if _, err := pool.Exec(context.Background(),
			`INSERT INTO ops_events (source, event_type, payload) VALUES ('ops', 'health_alert', '{"instance":"dev-1"}'::jsonb)`); err != nil {
			t.Fatalf("seed: %v", err)
		}
	}
	if _, err := cluster.Run(context.Background(), pool); err != nil {
		t.Fatalf("cluster.Run: %v", err)
	}
	cnt, _ := clusterState(t, pool, "ops|health_alert:dev-1")
	if cnt != 5 {
		t.Fatalf("instance fallback signature wrong: cnt=%d", cnt)
	}
}
