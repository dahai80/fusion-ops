package probe_test

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/dahai80/fusion-ops/internal/probe"
)

func TestDebounceTransientFailureNoAlert(t *testing.T) {
	state := map[string]*probe.Verdict{}
	r1 := probe.Result{Name: "site:/", OK: false}
	r2 := probe.Result{Name: "site:/", OK: true}
	alerts, recoveries := probe.ApplyRound(state, []probe.Result{r1})
	if len(alerts) != 0 || len(recoveries) != 0 {
		t.Fatalf("single transient failure must not alert: %v %v", alerts, recoveries)
	}
	alerts, recoveries = probe.ApplyRound(state, []probe.Result{r2})
	if len(alerts) != 0 || len(recoveries) != 0 {
		t.Fatalf("recovery before alert must stay silent: %v %v", alerts, recoveries)
	}
}

func TestDebounceTwoConsecutiveFailsAlertOnce(t *testing.T) {
	state := map[string]*probe.Verdict{}
	r1 := probe.Result{Name: "site:/", OK: false}
	r2 := probe.Result{Name: "site:/", OK: false}
	r3 := probe.Result{Name: "site:/", OK: false}

	alerts, _ := probe.ApplyRound(state, []probe.Result{r1})
	if len(alerts) != 0 {
		t.Fatal("first failure must not alert")
	}
	alerts, _ = probe.ApplyRound(state, []probe.Result{r2})
	if len(alerts) != 1 {
		t.Fatal("second consecutive failure must alert")
	}
	alerts, _ = probe.ApplyRound(state, []probe.Result{r3})
	if len(alerts) != 0 {
		t.Fatal("already-alerted episode must not re-alert")
	}
}

func TestDebounceRecoveryAfterFailEpisode(t *testing.T) {
	state := map[string]*probe.Verdict{}
	_, _ = probe.ApplyRound(state, []probe.Result{{Name: "site:/", OK: false}})
	alerts, _ := probe.ApplyRound(state, []probe.Result{{Name: "site:/", OK: false}})
	if len(alerts) != 1 {
		t.Fatal("expected alert")
	}
	_, recoveries := probe.ApplyRound(state, []probe.Result{{Name: "site:/", OK: true}})
	if len(recoveries) != 1 {
		t.Fatal("first success after alerted FAIL must emit recovery notice")
	}
	_, recoveries = probe.ApplyRound(state, []probe.Result{{Name: "site:/", OK: true}})
	if len(recoveries) != 0 {
		t.Fatal("recovery must fire only once")
	}
}

func TestHTTPProbe(t *testing.T) {
	okSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Write([]byte("healthy body"))
	}))
	defer okSrv.Close()
	r := probe.CheckHTTP(context.Background(), http.DefaultClient, okSrv.URL, "site:/")
	if !r.OK {
		t.Fatalf("200 non-empty must pass: %+v", r)
	}

	emptySrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {}))
	defer emptySrv.Close()
	r = probe.CheckHTTP(context.Background(), http.DefaultClient, emptySrv.URL, "site:/")
	if r.OK {
		t.Fatal("empty body must fail")
	}

	failSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(500)
	}))
	defer failSrv.Close()
	r = probe.CheckHTTP(context.Background(), http.DefaultClient, failSrv.URL, "site:/")
	if r.OK {
		t.Fatal("500 must fail")
	}
}

func TestTLSDaysRemaining(t *testing.T) {
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {}))
	defer srv.Close()
	days, err := probe.TLSDaysRemaining(context.Background(), hostPortOf(srv.URL), true)
	if err != nil {
		t.Fatalf("tls probe: %v", err)
	}
	if days <= 0 {
		t.Fatalf("test cert should have positive remaining days: %d", days)
	}
}

func TestDNSProbeMatchesExpected(t *testing.T) {
	cfg := probe.Config{
		ExpectedIP: "10.1.2.3",
		Resolver: func(ctx context.Context, host string) ([]string, error) {
			return []string{"10.1.2.3", "10.0.0.9"}, nil
		},
	}
	r := probe.CheckDNS(context.Background(), cfg, "fusion-mlx.cn")
	if !r.OK {
		t.Fatalf("expected IP in answer set must pass: %+v", r)
	}

	cfg.ExpectedIP = "99.9.9.9"
	r = probe.CheckDNS(context.Background(), cfg, "fusion-mlx.cn")
	if r.OK {
		t.Fatalf("wrong IP must fail: %+v", r)
	}

	cfg.ExpectedIP = ""
	r = probe.CheckDNS(context.Background(), cfg, "fusion-mlx.cn")
	if !r.OK {
		t.Fatal("unconfigured expectation must skip (pass)")
	}
}

func TestRoundProberDownNotCounted(t *testing.T) {
	// Control lookup fails -> verdicts skipped regardless of probe outcomes.
	cfg := probe.Config{
		SiteBase:    "https://example.invalid",
		Paths:       []string{"/"},
		ExpectedIP:  "10.0.0.1",
		ControlHost: "control.invalid",
		Resolver: func(ctx context.Context, host string) ([]string, error) {
			return nil, context.DeadlineExceeded
		},
	}
	state := map[string]*probe.Verdict{}
	client := &http.Client{Timeout: time.Second}
	results, counted := probe.RunRound(cfg, client)
	if len(results) == 0 {
		t.Fatal("results still recorded")
	}
	if counted {
		t.Fatal("prober-down round must not be counted")
	}
	if len(state) != 0 {
		t.Fatalf("verdicts must not advance on prober-down round: %v", state)
	}
}

func TestDeliverAlertIngestShape(t *testing.T) {
	type recv struct {
		Source   string         `json:"source"`
		Kind     string         `json:"kind"`
		Severity string         `json:"severity"`
		Detail   string         `json:"detail"`
		DedupKey string         `json:"dedup_key"`
		Probe    string         `json:"probe"`
		Results  []probe.Result `json:"results"`
	}
	var got recv
	done := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		_ = json.Unmarshal(body, &got)
		close(done)
	}))
	defer srv.Close()

	cfg := probe.Config{AlertWebhook: srv.URL}
	probe.DeliverAlert(cfg, "fail", "site:/", []probe.Result{{Name: "site:/", OK: false, Detail: "status 500"}})
	<-done
	if got.Source != "fusion-ops" || got.Kind != "fail" || got.Severity != "error" {
		t.Fatalf("ingest contract violated: %+v", got)
	}
	if got.DedupKey != "site:/" || got.Probe != "site:/" {
		t.Fatalf("dedup/probe mismatch: %+v", got)
	}
	if len(got.Results) != 1 || got.Results[0].Name != "site:/" {
		t.Fatalf("results not carried: %+v", got.Results)
	}

	probe.DeliverAlert(cfg, "recover", "site:/", []probe.Result{{Name: "site:/", OK: true}})
	<-done
	if got.Severity != "info" || got.Kind != "recover" {
		t.Fatalf("recovery severity/kind wrong: %+v", got)
	}
	if got.DedupKey != "site:/:recover" {
		t.Fatalf("recovery dedup_key must carry direction: %+v", got)
	}
}

func TestDeliverAlertPushTarget(t *testing.T) {
	var pushBody, pushTitle, pushPriority string
	done := make(chan struct{}, 4) // buffered + non-blocking: handler fires once per DeliverAlert call
	pushSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		pushBody = string(b)
		pushTitle = r.Header.Get("X-Title")
		pushPriority = r.Header.Get("X-Priority")
		select {
		case done <- struct{}{}:
		default:
		}
	}))
	defer pushSrv.Close()

	cfg := probe.Config{AlertPushURL: pushSrv.URL}
	probe.DeliverAlert(cfg, "fail", "site:/", []probe.Result{{Name: "site:/", OK: false, Detail: "status 500"}})
	<-done
	if pushTitle != "fusion-ops fail: site:/" {
		t.Fatalf("push title wrong: %q", pushTitle)
	}
	if pushPriority != "high" {
		t.Fatalf("fail push priority wrong: %q", pushPriority)
	}
	if !strings.Contains(pushBody, "failed 2 consecutive rounds") {
		t.Fatalf("push body must be the human detail: %q", pushBody)
	}

	probe.DeliverAlert(cfg, "recover", "site:/", []probe.Result{{Name: "site:/", OK: true}})
	<-done
	if pushPriority != "default" {
		t.Fatalf("recover push priority wrong: %q", pushPriority)
	}
	if !strings.Contains(pushBody, "recovered") {
		t.Fatalf("recovery push body wrong: %q", pushBody)
	}
}

func hostPortOf(raw string) string {
	return strings.TrimPrefix(strings.TrimPrefix(raw, "https://"), "http://")
}
