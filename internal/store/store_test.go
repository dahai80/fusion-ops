package store_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/dahai80/fusion-ops/internal/store"
	"github.com/dahai80/fusion-ops/internal/testutil"
	"github.com/jackc/pgx/v5"
)

func adminDSN(t *testing.T) string {
	t.Helper()
	admin := os.Getenv("FUSION_OPS_TEST_ADMIN_URL")
	if admin == "" {
		admin = "postgres://127.0.0.1:5432/postgres?sslmode=disable"
	}
	return admin
}

func TestMigrateFreshApplyTwice(t *testing.T) {
	pool := testutil.Fresh(t)
	ctx := context.Background()
	for round := 1; round <= 2; round++ {
		if err := store.Migrate(ctx, pool, "../../migrations"); err != nil {
			t.Fatalf("migrate round %d: %v", round, err)
		}
	}
	entries, err := os.ReadDir("../../migrations")
	if err != nil {
		t.Fatal(err)
	}
	expect := 0
	for _, e := range entries {
		if !e.IsDir() && filepath.Ext(e.Name()) == ".sql" {
			expect++
		}
	}
	var n int
	if err := pool.QueryRow(ctx, "SELECT COUNT(*) FROM schema_migrations").Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != expect {
		t.Fatalf("expected %d recorded migrations, got %d", expect, n)
	}
}

func TestSmokeInsertReadAllTables(t *testing.T) {
	pool := testutil.Fresh(t)
	ctx := context.Background()

	var eventID int64
	if err := pool.QueryRow(ctx,
		`INSERT INTO ops_events (source, tenant_hash, event_type, latency_ms, status, payload)
		 VALUES ('gateway', 't1', 'http_error', 120, 500, '{"error_signature":"boom"}') RETURNING id`).Scan(&eventID); err != nil {
		t.Fatalf("insert ops_events: %v", err)
	}
	var src, etype string
	if err := pool.QueryRow(ctx, "SELECT source, event_type FROM ops_events WHERE id=$1", eventID).Scan(&src, &etype); err != nil {
		t.Fatalf("read ops_events: %v", err)
	}
	if src != "gateway" || etype != "http_error" {
		t.Fatalf("ops_events roundtrip mismatch: %s %s", src, etype)
	}

	if _, err := pool.Exec(ctx,
		`INSERT INTO ops_prompts (source, prompt_hash, category, pii_classes, prompt_text, model, latency_ms, outcome)
		 VALUES ('cowork', 'h1', 'code', '{email}', 'hello', 'mlx-model', 55, 'ok')`); err != nil {
		t.Fatalf("insert ops_prompts: %v", err)
	}
	var pt string
	if err := pool.QueryRow(ctx, "SELECT prompt_text FROM ops_prompts WHERE prompt_hash='h1'").Scan(&pt); err != nil {
		t.Fatalf("read ops_prompts: %v", err)
	}
	if pt != "hello" {
		t.Fatalf("ops_prompts roundtrip mismatch: %q", pt)
	}

	if _, err := pool.Exec(ctx,
		`INSERT INTO mined_issues (cluster_key, first_seen, last_seen, count, sample_event_ids)
		 VALUES ('gateway|boom', now(), now(), 3, ARRAY[$1::bigint])`, eventID); err != nil {
		t.Fatalf("insert mined_issues: %v", err)
	}
	var cnt int64
	if err := pool.QueryRow(ctx, "SELECT count FROM mined_issues WHERE cluster_key='gateway|boom'").Scan(&cnt); err != nil {
		t.Fatalf("read mined_issues: %v", err)
	}
	if cnt != 3 {
		t.Fatalf("mined_issues count mismatch: %d", cnt)
	}

	if _, err := pool.Exec(ctx,
		`INSERT INTO feedback (message_id, class, sentiment, urgency, state, sla_deadline)
		 VALUES ('m-1', 'bug', 'negative', 'high', 'new', now() + interval '2 days')`); err != nil {
		t.Fatalf("insert feedback: %v", err)
	}
	var class string
	if err := pool.QueryRow(ctx, "SELECT class FROM feedback WHERE message_id='m-1'").Scan(&class); err != nil {
		t.Fatalf("read feedback: %v", err)
	}
	if class != "bug" {
		t.Fatalf("feedback class mismatch: %q", class)
	}

	if _, err := pool.Exec(ctx,
		`INSERT INTO drop_counters (source, bucket_day, dropped) VALUES ('gateway', current_date, 42)`); err != nil {
		t.Fatalf("insert drop_counters: %v", err)
	}
	var dropped int64
	if err := pool.QueryRow(ctx, "SELECT dropped FROM drop_counters WHERE source='gateway'").Scan(&dropped); err != nil {
		t.Fatalf("read drop_counters: %v", err)
	}
	if dropped != 42 {
		t.Fatalf("drop_counters mismatch: %d", dropped)
	}
}

func TestProbePassesForOwner(t *testing.T) {
	pool := testutil.Fresh(t)
	ctx := context.Background()
	conn, err := pool.Acquire(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Release()
	if err := store.ProbePrivileges(ctx, conn); err != nil {
		t.Fatalf("probe should pass for table owner: %v", err)
	}
	if err := store.ProbeCanary(ctx, conn); err != nil {
		t.Fatalf("canary should pass for table owner: %v", err)
	}
}

func TestProbeFailsClosedForLimitedRole(t *testing.T) {
	testutil.Fresh(t)
	ctx := context.Background()
	admin, err := pgx.Connect(ctx, adminDSN(t))
	if err != nil {
		t.Skipf("admin connection unavailable: %v", err)
	}
	defer admin.Close(ctx)
	if _, err := admin.Exec(ctx, `DROP ROLE IF EXISTS fusion_ops_probe_r`); err != nil {
		t.Skipf("cannot manage roles: %v", err)
	}
	if _, err := admin.Exec(ctx, `CREATE ROLE fusion_ops_probe_r LOGIN PASSWORD 'probe'`); err != nil {
		t.Skipf("cannot create role: %v", err)
	}
	t.Cleanup(func() {
		_, _ = admin.Exec(context.Background(), `DROP ROLE IF EXISTS fusion_ops_probe_r`)
	})
	// Read-only grant: probe must fail closed on missing DML rights.
	if _, err := admin.Exec(ctx, `GRANT USAGE ON SCHEMA public TO fusion_ops_probe_r`); err != nil {
		t.Fatal(err)
	}
	if _, err := admin.Exec(ctx, `GRANT SELECT ON ALL TABLES IN SCHEMA public TO fusion_ops_probe_r`); err != nil {
		t.Fatal(err)
	}

	limited, err := store.Connect(ctx, "postgres://fusion_ops_probe_r:probe@127.0.0.1:5432/"+testutil.TestDBName(t)+"?sslmode=disable")
	if err != nil {
		t.Fatalf("connect as limited role: %v", err)
	}
	defer limited.Close()
	conn, err := limited.Acquire(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Release()
	if err := store.ProbePrivileges(ctx, conn); err == nil {
		t.Fatal("probe must fail closed for read-only role")
	} else {
		t.Logf("probe failed closed as expected: %v", err)
	}
	if err := store.ProbeCanary(ctx, conn); err == nil {
		t.Fatal("canary must fail closed for read-only role")
	}
}
