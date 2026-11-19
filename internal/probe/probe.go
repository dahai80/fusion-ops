package probe

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"strings"
	"time"
)

// Blackbox site-health probe (issue #4). Deterministic verdicts, no LLM:
// FAIL only after 2 consecutive failed rounds (debounce); recovery notice on
// first success after an alerted FAIL episode; prober-side network outages
// are logged, not alerted (prober-down != site-down).

const (
	interval       = 5 * time.Minute
	tlsWarnDays    = 14
	controlDomain  = "cloudflare.com"
	requestTimeout = 15 * time.Second
)

// Config carries the probe targets. Zero-value fields fall back to defaults.
type Config struct {
	SiteBase     string   // e.g. https://fusion-mlx.cn
	Paths        []string // HTTP paths to probe (default "/", "/auth/")
	ExpectedIP   string   // DNS must resolve to this VPS IP; empty = DNS probe skips
	ControlHost  string   // prober connectivity sentinel (default cloudflare.com)
	AlertWebhook string   // optional POST target for alert payloads
	Resolver     func(ctx context.Context, host string) ([]string, error)
}

type Result struct {
	Name      string `json:"probe"`
	OK        bool   `json:"ok"`
	LatencyMS int64  `json:"latency_ms"`
	Detail    string `json:"detail"`
	TS        string `json:"ts"`
}

func (c Config) resolver() func(ctx context.Context, host string) ([]string, error) {
	if c.Resolver != nil {
		return c.Resolver
	}
	return func(ctx context.Context, host string) ([]string, error) {
		return net.LookupHost(host)
	}
}

func (c Config) control() string {
	if c.ControlHost != "" {
		return c.ControlHost
	}
	return controlDomain
}

func (c Config) paths() []string {
	if len(c.Paths) > 0 {
		return c.Paths
	}
	return []string{"/", "/auth/"}
}

// RequestTimeout exposes the per-request timeout for caller HTTP clients.
func RequestTimeout() time.Duration { return requestTimeout }

// hostPort extracts host:port scheme-less from a base URL.
func hostPort(base string) string {
	h := strings.TrimPrefix(strings.TrimPrefix(base, "https://"), "http://")
	if i := strings.IndexByte(h, '/'); i >= 0 {
		h = h[:i]
	}
	if !strings.Contains(h, ":") {
		h += ":443"
	}
	return h
}

func hostOf(base string) string {
	h := hostPort(base)
	if i := strings.IndexByte(h, ':'); i >= 0 {
		h = h[:i]
	}
	return h
}

func hostOnly(hp string) string {
	if i := strings.LastIndexByte(hp, ':'); i >= 0 {
		return hp[:i]
	}
	return hp
}

// RunRound executes one probe round: all checks, results returned. Verdicts
// are NOT advanced here — the caller applies ApplyRound exactly once per
// round, and only when counted=true. counted=false means the prober's own
// connectivity sentinel failed (results logged, never alerted). Events are
// appended by the caller regardless.
func RunRound(cfg Config, client *http.Client) (results []Result, counted bool) {
	ctx := context.Background()
	host := hostOf(cfg.SiteBase)
	results = []Result{}
	for _, p := range cfg.paths() {
		results = append(results, CheckHTTP(ctx, client, cfg.SiteBase+p, "site:"+p))
	}
	results = append(results, tlsResult(ctx, host))
	results = append(results, CheckDNS(ctx, cfg, host))

	if !proberAlive(ctx, cfg) {
		for i := range results {
			if !results[i].OK {
				results[i].Detail = "prober network suspect (control lookup failed); not counted: " + results[i].Detail
			}
		}
		slog.Warn("probe round inconclusive: control lookup failed, verdicts skipped")
		return results, false
	}
	return results, true
}

func proberAlive(ctx context.Context, cfg Config) bool {
	cctx, cancel := context.WithTimeout(ctx, requestTimeout)
	defer cancel()
	_, err := cfg.resolver()(cctx, cfg.control())
	return err == nil
}

// CheckHTTP: pass requires 2xx and non-empty body.
func CheckHTTP(ctx context.Context, client *http.Client, url, name string) Result {
	r := Result{Name: name, TS: now()}
	start := time.Now()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		r.Detail = err.Error()
		return r
	}
	resp, err := client.Do(req)
	r.LatencyMS = time.Since(start).Milliseconds()
	if err != nil {
		r.Detail = err.Error()
		return r
	}
	defer resp.Body.Close()
	buf := make([]byte, 1)
	n, _ := resp.Body.Read(buf)
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		r.Detail = fmt.Sprintf("status %d", resp.StatusCode)
		return r
	}
	if n == 0 {
		r.Detail = "empty body"
		return r
	}
	r.OK = true
	r.Detail = fmt.Sprintf("status %d", resp.StatusCode)
	return r
}

// TLSDaysRemaining dials the live cert chain and returns whole days until
// the leaf expires. skipVerify bypasses chain verification (test helper
// against self-signed certs); production passes false so bad chains fail.
func TLSDaysRemaining(ctx context.Context, hp string, skipVerify bool) (int, error) {
	dialer := &net.Dialer{Timeout: requestTimeout}
	conn, err := tls.DialWithDialer(dialer, "tcp", hp, &tls.Config{ServerName: hostOnly(hp), InsecureSkipVerify: skipVerify})
	if err != nil {
		return 0, err
	}
	defer conn.Close()
	certs := conn.ConnectionState().PeerCertificates
	if len(certs) == 0 {
		return 0, fmt.Errorf("no peer certificate")
	}
	return int(time.Until(certs[0].NotAfter).Hours() / 24), nil
}

// tlsResult checks remaining validity; FAIL when fewer than tlsWarnDays
// remain (debounce applies like any other probe).
func tlsResult(ctx context.Context, host string) Result {
	r := Result{Name: "tls", TS: now()}
	start := time.Now()
	days, err := TLSDaysRemaining(ctx, host+":443", false)
	r.LatencyMS = time.Since(start).Milliseconds()
	if err != nil {
		r.Detail = err.Error()
		return r
	}
	r.Detail = fmt.Sprintf("%d days remaining", days)
	if days < tlsWarnDays {
		return r
	}
	r.OK = true
	return r
}

// CheckDNS verifies the site host resolves to the expected VPS IP. Unset
// expectation = skip (recorded OK with detail, never false-alarms).
func CheckDNS(ctx context.Context, cfg Config, host string) Result {
	r := Result{Name: "dns", TS: now()}
	if cfg.ExpectedIP == "" {
		r.OK = true
		r.Detail = "expected IP not configured, skipped"
		return r
	}
	start := time.Now()
	addrs, err := cfg.resolver()(ctx, host)
	r.LatencyMS = time.Since(start).Milliseconds()
	if err != nil {
		r.Detail = err.Error()
		return r
	}
	for _, a := range addrs {
		if a == cfg.ExpectedIP {
			r.OK = true
			r.Detail = "resolves to " + a
			return r
		}
	}
	r.Detail = fmt.Sprintf("resolved %v, expected %s", addrs, cfg.ExpectedIP)
	return r
}

// Verdict is the debounce state machine for one probe name.
type Verdict struct {
	Name    string `json:"-"`
	Fails   int    `json:"fails"`
	Alerted bool   `json:"alerted"`
}

// Apply feeds one round result. Returns (alert, recovery).
func (v *Verdict) Apply(ok bool) (bool, bool) {
	if !ok {
		v.Fails++
		if v.Fails == 2 && !v.Alerted {
			v.Alerted = true
			return true, false
		}
		return false, false
	}
	wasAlerted := v.Alerted
	v.Fails = 0
	v.Alerted = false
	return false, wasAlerted
}

// ApplyRound feeds all results through the debouncer and returns alert and
// recovery results (alert carries the failing result; the caller pairs it
// with the previous round for the 2-consecutive payload, issue #4 req 4).
func ApplyRound(state map[string]*Verdict, results []Result) (alerts, recoveries []Result) {
	for _, r := range results {
		v := state[r.Name]
		if v == nil {
			v = &Verdict{Name: r.Name}
			state[r.Name] = v
		}
		alert, recovery := v.Apply(r.OK)
		if alert {
			alerts = append(alerts, r)
		}
		if recovery {
			recoveries = append(recoveries, r)
		}
	}
	return alerts, recoveries
}

// AlertPayload is the supervisor-channel body: probe name, 2 consecutive
// results, timestamp (issue #4 req 4).
type AlertPayload struct {
	Kind      string   `json:"kind"` // "fail" or "recover"
	Probe     string   `json:"probe"`
	Results   []Result `json:"results"`
	Timestamp string   `json:"timestamp"`
}

// DeliverAlert posts to the configured webhook (existing supervisor channel
// fronting) when set; always logs the alert locally. The webhook body follows
// the fusion-sv ingest contract (POST /api/v1/alerts/ingest, issue #139:
// source/kind/severity/detail required, extra keys ignored); structured
// probe fields ride along for other consumers.
func DeliverAlert(cfg Config, kind, probeName string, results []Result) {
	payload := AlertPayload{Kind: kind, Probe: probeName, Results: results, Timestamp: now()}
	line, _ := json.Marshal(payload)
	if kind == "fail" {
		slog.Error("health probe alert", "payload", string(line))
	} else {
		slog.Info("health probe recovery", "payload", string(line))
	}
	if cfg.AlertWebhook == "" {
		return
	}
	severity := "info"
	if kind == "fail" {
		severity = "error"
	}
	detail := alertDetail(kind, probeName, results)
	// dedup_key carries direction: a recovery arriving within the ingest
	// dedup window after its failure must be recorded, not deduped away
	// (issue #7). Failures keep the bare probe key so repeated fail alerts
	// stay suppressed inside the window.
	dedupKey := probeName
	if kind != "fail" {
		dedupKey = probeName + ":recover"
	}
	body, _ := json.Marshal(map[string]any{
		"source":    "fusion-ops",
		"kind":      kind,
		"severity":  severity,
		"detail":    detail,
		"dedup_key": dedupKey,
		"probe":     probeName,
		"results":   results,
		"timestamp": payload.Timestamp,
	})
	client := &http.Client{Timeout: requestTimeout}
	resp, err := client.Post(cfg.AlertWebhook, "application/json", strings.NewReader(string(body)))
	if err != nil {
		slog.Warn("alert webhook delivery failed", "error", err)
		return
	}
	resp.Body.Close()
	slog.Info("alert webhook delivered", "status", resp.StatusCode)
}

func alertDetail(kind, probeName string, results []Result) string {
	last := Result{}
	if len(results) > 0 {
		last = results[len(results)-1]
	}
	if kind == "fail" {
		return fmt.Sprintf("health probe %s failed 2 consecutive rounds; last detail: %s", probeName, last.Detail)
	}
	return fmt.Sprintf("health probe %s recovered; detail: %s", probeName, last.Detail)
}

func now() string { return time.Now().Format(time.RFC3339) }

// Store persists round results and alerts into fusion_ops.ops_events and
// reads back the last round for verdict seeding (stateless one-shot runs).
type Store struct {
	Exec         func(ctx context.Context, sql string, args ...any) error
	Query        func(ctx context.Context, sql string, args ...any) ([]Result, error)
	QueryStrings func(ctx context.Context, sql string, args ...any) ([]string, error)
}

func (s Store) queryStrings(ctx context.Context, sql string, args ...any) ([]string, error) {
	if s.QueryStrings != nil {
		return s.QueryStrings(ctx, sql, args...)
	}
	return nil, fmt.Errorf("query strings not configured")
}

// LoadLastResults returns each probe's most recent health_probe result.
// Used to seed the debounce state so one-shot invocations (supervisor T2
// runs the binary per round) continue an episode instead of resetting it.
func (s Store) LoadLastResults(ctx context.Context) ([]Result, error) {
	if s.Query == nil {
		return nil, nil
	}
	return s.Query(ctx, `
		SELECT DISTINCT ON (payload->>'probe')
		       payload->>'probe' AS name,
		       (payload->>'ok')::boolean AS ok,
		       COALESCE((payload->>'latency_ms')::bigint, 0) AS latency_ms,
		       COALESCE(payload->>'detail', '') AS detail,
		       COALESCE(payload->>'ts', '') AS ts
		FROM ops_events
		WHERE event_type = 'health_probe'
		ORDER BY payload->>'probe', id DESC`)
}

// LoadActiveAlerts returns probes whose latest health_alert is newer than
// their latest successful health_probe — i.e. an alerted FAIL episode still
// in progress (no recovery yet). Seeded as Alerted so the episode does not
// re-alert on every round.
func (s Store) LoadActiveAlerts(ctx context.Context) ([]string, error) {
	if s.Query == nil {
		return nil, nil
	}
	rows, err := s.queryStrings(ctx, `
		SELECT DISTINCT payload->>'probe'
		FROM ops_events a
		WHERE event_type = 'health_alert'
		  AND id > COALESCE((
		        SELECT MAX(id) FROM ops_events b
		        WHERE b.event_type = 'health_probe'
		          AND (b.payload->>'ok')::boolean
		          AND b.payload->>'probe' = a.payload->>'probe'), 0)`)
	if err != nil {
		return nil, err
	}
	return rows, nil
}

// SeedState builds a verdict map from last-round results: a failed last
// round counts as one already-accumulated failure; activeAlerts marks
// episodes that already fired their alert.
func SeedState(last []Result, activeAlerts []string) map[string]*Verdict {
	state := map[string]*Verdict{}
	for _, r := range last {
		v := &Verdict{Name: r.Name}
		if !r.OK {
			v.Fails = 1
		}
		state[r.Name] = v
	}
	for _, name := range activeAlerts {
		v := state[name]
		if v == nil {
			v = &Verdict{Name: name}
			state[name] = v
		}
		v.Alerted = true
	}
	return state
}

// AppendEvents writes health_probe / health_alert events. No-op without an
// Exec (in-memory runs).
func (s Store) AppendEvents(ctx context.Context, eventType string, results []Result) {
	if s.Exec == nil {
		return
	}
	for _, r := range results {
		payload, _ := json.Marshal(r)
		sql := `INSERT INTO ops_events (source, event_type, latency_ms, payload)
		        VALUES ('ops', $1, $2, $3::jsonb)`
		if err := s.Exec(ctx, sql, eventType, r.LatencyMS, string(payload)); err != nil {
			slog.Error("probe event append failed", "event_type", eventType, "error", err)
		}
	}
}

// LoadConfig from environment; defaults match the verified live probe set.
func LoadConfig() Config {
	cfg := Config{
		SiteBase:     os.Getenv("FUSION_OPS_PROBE_SITE"),
		ExpectedIP:   os.Getenv("FUSION_OPS_EXPECTED_SITE_IP"),
		ControlHost:  os.Getenv("FUSION_OPS_PROBE_CONTROL"),
		AlertWebhook: os.Getenv("FUSION_OPS_ALERT_WEBHOOK"),
	}
	if cfg.SiteBase == "" {
		cfg.SiteBase = "https://fusion-mlx.cn"
	}
	if p := os.Getenv("FUSION_OPS_PROBE_PATHS"); p != "" {
		cfg.Paths = strings.Split(p, ",")
	}
	return cfg
}
