package broker_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/dahai80/fusion-ops/internal/broker"
	"github.com/dahai80/fusion-ops/internal/testutil"
)

// brokerFixture wires a broker server over a fresh test DB.
type brokerFixture struct {
	t      *testing.T
	pool   *pgxpool.Pool
	server *httptest.Server
	hub    *broker.Hub
}

func newFixture(t *testing.T) *brokerFixture {
	t.Helper()
	pool := testutil.Fresh(t)
	hub := broker.NewHub(broker.NewStore(pool))
	if err := hub.Store().ResetAllOffline(context.Background()); err != nil {
		t.Fatalf("reset offline: %v", err)
	}
	srv := broker.NewServer(hub, time.Hour, t.Name()) // read key = test name
	ts := httptest.NewServer(srv.Handler())
	t.Cleanup(ts.Close)
	return &brokerFixture{t: t, pool: pool, server: ts, hub: hub}
}

// connect performs the full hello handshake and returns the handshake frames
// plus a live frame channel fed by a single resident reader goroutine
// (coder/websocket forbids a read timeout without closing, so all reads
// after the handshake go through the channel).
func (f *brokerFixture) connect(ctx context.Context, deviceID, token string) (*websocket.Conn, []broker.Envelope, <-chan broker.Envelope, error) {
	wsURL := "ws" + strings.TrimPrefix(f.server.URL, "http") + "/link/ws"
	c, _, err := websocket.Dial(ctx, wsURL, nil)
	if err != nil {
		return nil, nil, nil, err
	}
	hello, _ := json.Marshal(broker.Envelope{V: 1, Type: "hello", DeviceID: deviceID,
		Payload: mustJSON(map[string]any{"token": token, "version": "test-1",
			"hostname": deviceID + ".local", "platform": "test/darwin-arm64"})})
	if err := c.Write(ctx, websocket.MessageText, hello); err != nil {
		return nil, nil, nil, err
	}
	frames := make(chan broker.Envelope, 64)
	go func() {
		defer close(frames)
		for {
			_, data, err := c.Read(ctx)
			if err != nil {
				return
			}
			var env broker.Envelope
			if err := json.Unmarshal(data, &env); err != nil {
				return
			}
			frames <- env
		}
	}()
	var handshake []broker.Envelope
	for {
		env, ok := <-frames
		if !ok {
			return c, handshake, frames, fmt.Errorf("connection closed during handshake for %s", deviceID)
		}
		handshake = append(handshake, env)
		if env.Type == "hello_ack" || env.Type == "error" {
			return c, handshake, frames, nil
		}
	}
}

// readFrame waits for one broker frame with a timeout.
func readFrame(t *testing.T, ch <-chan broker.Envelope, timeout time.Duration) (broker.Envelope, bool) {
	t.Helper()
	select {
	case env, ok := <-ch:
		return env, ok
	case <-time.After(timeout):
		return broker.Envelope{}, false
	}
}

func (f *brokerFixture) provision(deviceID, token string) {
	f.provisionTagged(deviceID, token, nil)
}

func (f *brokerFixture) provisionTagged(deviceID, token string, tags []string) {
	f.t.Helper()
	if err := broker.NewStore(f.pool).Provision(context.Background(), deviceID, token, "test-1", []string{"exec"}, tags); err != nil {
		f.t.Fatalf("provision %s: %v", deviceID, err)
	}
}

// getJSON fetches a read-surface endpoint with the fixture's bearer key.
func (f *brokerFixture) getJSON(path string) (int, []byte) {
	f.t.Helper()
	req, err := http.NewRequest(http.MethodGet, f.server.URL+path, nil)
	if err != nil {
		f.t.Fatalf("build request: %v", err)
	}
	req.Header.Set("Authorization", "Bearer "+f.t.Name())
	resp, err := f.server.Client().Do(req)
	if err != nil {
		f.t.Fatalf("get %s: %v", path, err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		f.t.Fatalf("read body: %v", err)
	}
	return resp.StatusCode, body
}

func (f *brokerFixture) brokerEvents() []map[string]any {
	f.t.Helper()
	rows, err := f.pool.Query(context.Background(),
		`SELECT event_type, payload->>'device_id' AS device_id FROM ops_events WHERE source='broker' ORDER BY id`)
	if err != nil {
		f.t.Fatalf("query broker events: %v", err)
	}
	defer rows.Close()
	var out []map[string]any
	for rows.Next() {
		var typ, dev string
		if err := rows.Scan(&typ, &dev); err != nil {
			f.t.Fatalf("scan broker event: %v", err)
		}
		out = append(out, map[string]any{"type": typ, "device": dev})
	}
	return out
}

func mustJSON(v any) json.RawMessage {
	b, _ := json.Marshal(v)
	return b
}

func TestBrokerHelloAuthAndOfflineDrain(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()
	f.provision("dev-1", "tok-1")

	// Task enqueued while device offline: parked in mailbox.
	st := broker.NewStore(f.pool)
	created, err := st.EnqueueTask(ctx, "dev-1", "task-1", "summarize", map[string]any{"doc": "a"}, time.Hour)
	if err != nil || !created {
		t.Fatalf("enqueue: created=%v err=%v", created, err)
	}

	c, hs, ch, err := f.connect(ctx, "dev-1", "tok-1")
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer c.Close(websocket.StatusNormalClosure, "")
	var ack broker.Envelope
	var ok bool
	if len(hs) > 0 {
		ack, ok = hs[0], true
	} else {
		ack, ok = readFrame(t, ch, 2*time.Second)
	}
	if !ok || ack.Type != "hello_ack" {
		t.Fatalf("expected hello_ack, got %+v ok=%v", ack, ok)
	}
	disp, ok := readFrame(t, ch, 2*time.Second)
	if !ok || disp.Type != "task.dispatch" {
		t.Fatalf("expected drained dispatch, got %+v ok=%v", disp, ok)
	}
	var p broker.TaskPayload
	if err := json.Unmarshal(disp.Payload, &p); err != nil || p.TaskID != "task-1" {
		t.Fatalf("dispatch payload wrong: %v %v", p, err)
	}
	// Mailbox row closed out.
	var queued int
	if err := f.pool.QueryRow(ctx, `SELECT count(*) FROM task_mailbox WHERE state='queued'`).Scan(&queued); err != nil || queued != 0 {
		t.Fatalf("mailbox not drained: queued=%d err=%v", queued, err)
	}
}

func frameTypes(frames []broker.Envelope) []string {
	var out []string
	for _, f := range frames {
		out = append(out, f.Type)
	}
	return out
}

var _ = frameTypes // retained for ad-hoc debug output

func TestBrokerRejectsBadToken(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()
	f.provision("dev-2", "tok-2")
	c, hs, _, err := f.connect(ctx, "dev-2", "wrong")
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer c.Close(websocket.StatusNormalClosure, "")
	if len(hs) != 1 || hs[0].Type != "error" {
		t.Fatalf("expected single error frame, got %d frames", len(hs))
	}
	env := hs[0]
	var p broker.ErrorPayload
	if err := json.Unmarshal(env.Payload, &p); err != nil || p.Code != "E_AUTH" {
		t.Fatalf("expected E_AUTH, got %+v", p)
	}
	// Failed auth must not flip state online.
	var state string
	if err := f.pool.QueryRow(ctx, `SELECT state FROM device_registry WHERE device_id='dev-2'`).Scan(&state); err != nil || state != "offline" {
		t.Fatalf("state after bad auth: %s err=%v", state, err)
	}
}

func TestBrokerOnlineDispatchAndEvents(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()
	f.provision("dev-3", "tok-3")

	c, _, ch, err := f.connect(ctx, "dev-3", "tok-3")
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer c.Close(websocket.StatusNormalClosure, "")

	// Dispatch via API while online: straight through.
	body := mustJSON(map[string]any{"device_id": "dev-3", "task_id": "task-3", "type": "ping", "params": map[string]any{"n": 1}})
	resp, err := f.server.Client().Post(f.server.URL+"/api/tasks", "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("dispatch api: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("dispatch api status %d", resp.StatusCode)
	}
	env, ok := readFrame(t, ch, 5*time.Second)
	if !ok || env.Type != "task.dispatch" {
		t.Fatalf("expected dispatch, got %+v ok=%v", env, ok)
	}

	// Unknown device rejected 404.
	body = mustJSON(map[string]any{"device_id": "ghost", "task_id": "t", "type": "ping"})
	resp, err = f.server.Client().Post(f.server.URL+"/api/tasks", "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("ghost dispatch: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != 404 {
		t.Fatalf("unknown device must 404, got %d", resp.StatusCode)
	}

	// Heartbeats never enter the event stream; disconnect is a transition.
	for i := 0; i < 3; i++ {
		hb, _ := json.Marshal(broker.Envelope{V: 1, Type: "heartbeat", DeviceID: "dev-3", Payload: mustJSON(map[string]any{})})
		if err := c.Write(ctx, websocket.MessageText, hb); err != nil {
			t.Fatalf("heartbeat write: %v", err)
		}
	}
	time.Sleep(200 * time.Millisecond)
	c.Close(websocket.StatusNormalClosure, "")
	time.Sleep(200 * time.Millisecond)

	events := f.brokerEvents()
	kinds := map[string]int{}
	for _, e := range events {
		kinds[e["type"].(string)]++
	}
	if kinds["device_online"] != 1 || kinds["device_offline"] != 1 || kinds["heartbeat"] != 0 {
		t.Fatalf("event discipline violated: %v", kinds)
	}
}

func TestBrokerRestartPreservesRegistryAndMailbox(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()
	f.provision("dev-4", "tok-4")
	st := broker.NewStore(f.pool)
	if _, err := st.EnqueueTask(ctx, "dev-4", "task-4", "sync", nil, 24*time.Hour); err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	// "Restart": fresh hub+server over the same PG state.
	hub2 := broker.NewHub(st)
	if err := hub2.Store().ResetAllOffline(ctx); err != nil {
		t.Fatalf("restart reset: %v", err)
	}
	srv2 := broker.NewServer(hub2, time.Hour, f.t.Name())
	ts2 := httptest.NewServer(srv2.Handler())
	defer ts2.Close()
	f.server = ts2

	c, hs, ch, err := f.connect(ctx, "dev-4", "tok-4")
	if err != nil {
		t.Fatalf("reconnect after restart: %v", err)
	}
	defer c.Close(websocket.StatusNormalClosure, "")
	if len(hs) == 0 || hs[0].Type != "hello_ack" {
		t.Fatalf("expected hello_ack in handshake, got %d frames", len(hs))
	}
	disp, ok := readFrame(t, ch, 2*time.Second)
	if !ok || disp.Type != "task.dispatch" {
		t.Fatalf("mailbox lost across restart: %+v ok=%v", disp, ok)
	}
}

func TestBrokerReconnectStormDrainsMailbox(t *testing.T) {
	if testing.Short() {
		t.Skip("storm skipped in -short")
	}
	f := newFixture(t)
	ctx := context.Background()
	st := broker.NewStore(f.pool)

	const n = 1000
	for i := 0; i < n; i++ {
		id := fmt.Sprintf("storm-%04d", i)
		f.provision(id, "tok-"+id)
		if _, err := st.EnqueueTask(ctx, id, "task-"+id, "sync", nil, 24*time.Hour); err != nil {
			t.Fatalf("enqueue %s: %v", id, err)
		}
	}

	dialAll := func() []*websocket.Conn {
		conns := make([]*websocket.Conn, n)
		var wg sync.WaitGroup
		var mu sync.Mutex
		var firstErr error
		for i := 0; i < n; i++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				id := fmt.Sprintf("storm-%04d", i)
				c, _, _, err := f.connect(ctx, id, "tok-"+id)
				mu.Lock()
				if err != nil && firstErr == nil {
					firstErr = fmt.Errorf("device %s: %w", id, err)
				}
				mu.Unlock()
				conns[i] = c
			}(i)
		}
		wg.Wait()
		if firstErr != nil {
			t.Fatalf("storm dial: %v", firstErr)
		}
		return conns
	}

	// Wave 1: all connect (drains their queued task), then all drop.
	conns := dialAll()
	for _, c := range conns {
		_ = c.Close(websocket.StatusGoingAway, "")
	}
	time.Sleep(500 * time.Millisecond)

	// Re-queue while everyone is offline (storm aftermath).
	for i := 0; i < n; i++ {
		if _, err := st.EnqueueTask(ctx, fmt.Sprintf("storm-%04d", i), "post-storm", "sync", nil, 24*time.Hour); err != nil {
			t.Fatalf("requeue %d: %v", i, err)
		}
	}

	// Wave 2: everyone returns; every queued task must drain.
	conns = dialAll()
	defer func() {
		for _, c := range conns {
			_ = c.Close(websocket.StatusNormalClosure, "")
		}
	}()

	deadline := time.Now().Add(30 * time.Second)
	for {
		var queued int
		if err := f.pool.QueryRow(ctx, `SELECT count(*) FROM task_mailbox WHERE state='queued' AND task_id='post-storm'`).Scan(&queued); err != nil {
			t.Fatalf("count queued: %v", err)
		}
		if queued == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("mailbox drain incomplete after storm: %d still queued", queued)
		}
		time.Sleep(250 * time.Millisecond)
	}
}

func TestBrokerMailboxTTLPurgedByRetention(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()
	f.provision("dev-5", "tok-5")
	st := broker.NewStore(f.pool)

	if _, err := st.EnqueueTask(ctx, "dev-5", "expired", "sync", nil, -time.Hour); err != nil {
		t.Fatalf("enqueue expired: %v", err)
	}
	if _, err := st.EnqueueTask(ctx, "dev-5", "alive", "sync", nil, time.Hour); err != nil {
		t.Fatalf("enqueue alive: %v", err)
	}
	purged, err := st.PurgeExpiredMailbox(ctx, 100)
	if err != nil {
		t.Fatalf("purge: %v", err)
	}
	if purged != 1 {
		t.Fatalf("purged=%d want 1", purged)
	}
	var alive int
	if err := f.pool.QueryRow(ctx, `SELECT count(*) FROM task_mailbox WHERE task_id='alive'`).Scan(&alive); err != nil || alive != 1 {
		t.Fatalf("live task must survive: %d err=%v", alive, err)
	}
}

func TestBrokerTaskFailedEventOnly(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()
	f.provision("dev-6", "tok-6")
	c, _, _, err := f.connect(ctx, "dev-6", "tok-6")
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer c.Close(websocket.StatusNormalClosure, "")

	evt, _ := json.Marshal(broker.Envelope{V: 1, Type: "task.event", DeviceID: "dev-6", Payload: mustJSON(map[string]any{"task_id": "t9", "type": "sync", "event": "failed", "detail": "boom"})})
	if err := c.Write(ctx, websocket.MessageText, evt); err != nil {
		t.Fatalf("write task.event: %v", err)
	}
	time.Sleep(300 * time.Millisecond)

	var n int
	if err := f.pool.QueryRow(ctx, `SELECT count(*) FROM ops_events WHERE source='broker' AND event_type='task_failed'`).Scan(&n); err != nil || n != 1 {
		t.Fatalf("task_failed event missing: n=%d err=%v", n, err)
	}
}

func TestBrokerReadDevicesShape(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()
	f.provisionTagged("dev-r1", "tok-r1", []string{"default-execution"})

	// Offline first: snapshot shows online=false, no connected_since.
	code, body := f.getJSON("/api/devices")
	if code != 200 {
		t.Fatalf("devices status %d: %s", code, body)
	}
	var out struct {
		Devices []struct {
			DeviceID           string           `json:"device_id"`
			Hostname           string           `json:"hostname"`
			Platform           string           `json:"platform"`
			Online             bool             `json:"online"`
			ConnectedSince     *string          `json:"connected_since"`
			IsDefaultExecution bool             `json:"is_default_execution"`
			Services           []map[string]any `json:"services"`
			Tags               []string         `json:"tags"`
		} `json:"devices"`
	}
	if err := json.Unmarshal(body, &out); err != nil || len(out.Devices) != 1 {
		t.Fatalf("devices parse: %v body=%s", err, body)
	}
	d := out.Devices[0]
	if d.DeviceID != "dev-r1" || d.Online || d.ConnectedSince != nil || !d.IsDefaultExecution {
		t.Fatalf("offline device view wrong: %+v", d)
	}
	if d.Services == nil {
		t.Fatalf("services must be [] not null")
	}

	// Connect with hostname/platform + heartbeat carrying services.
	c, hs, _, err := f.connect(ctx, "dev-r1", "tok-r1")
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer c.Close(websocket.StatusNormalClosure, "")
	if len(hs) == 0 || hs[0].Type != "hello_ack" {
		t.Fatalf("handshake failed: %v", hs)
	}
	hbPayload := map[string]any{"services": []map[string]any{{"name": "fusion-mlx", "port": 11434, "status": "ok"}}}
	hb, _ := json.Marshal(broker.Envelope{V: 1, Type: "heartbeat", DeviceID: "dev-r1", Payload: mustJSON(hbPayload)})
	if err := c.Write(ctx, websocket.MessageText, hb); err != nil {
		t.Fatalf("heartbeat: %v", err)
	}
	time.Sleep(300 * time.Millisecond)

	code, body = f.getJSON("/api/devices")
	if code != 200 {
		t.Fatalf("devices status %d", code)
	}
	if err := json.Unmarshal(body, &out); err != nil || len(out.Devices) != 1 {
		t.Fatalf("devices reparse: %v", err)
	}
	d = out.Devices[0]
	if !d.Online || d.ConnectedSince == nil || d.Hostname == "" || d.Platform == "" {
		t.Fatalf("online device view wrong: %+v", d)
	}
	if len(d.Services) != 1 || d.Services[0]["name"] != "fusion-mlx" {
		t.Fatalf("services from heartbeat missing: %+v", d.Services)
	}
}

func TestBrokerReadTasksLifecycleAndEvents(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()
	f.provision("dev-r2", "tok-r2")
	st := broker.NewStore(f.pool)

	// Queued offline task shows in list.
	if _, err := st.EnqueueTask(ctx, "dev-r2", "task-r2", "sync", map[string]any{"k": "v"}, time.Hour); err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	code, body := f.getJSON("/api/tasks")
	if code != 200 {
		t.Fatalf("tasks status %d: %s", code, body)
	}
	var list struct {
		Tasks []map[string]any `json:"tasks"`
	}
	if err := json.Unmarshal(body, &list); err != nil || len(list.Tasks) != 1 {
		t.Fatalf("tasks parse: %v body=%s", err, body)
	}
	if list.Tasks[0]["status"] != "queued" {
		t.Fatalf("expected queued, got %v", list.Tasks[0]["status"])
	}

	// Expired derivation via read path.
	if _, err := st.EnqueueTask(ctx, "dev-r2", "task-exp", "sync", nil, -time.Minute); err != nil {
		t.Fatalf("enqueue expired: %v", err)
	}
	code, body = f.getJSON("/api/tasks")
	if code != 200 {
		t.Fatalf("tasks status %d", code)
	}
	if err := json.Unmarshal(body, &list); err != nil {
		t.Fatalf("tasks reparse: %v", err)
	}
	states := map[string]bool{}
	for _, tk := range list.Tasks {
		states[tk["status"].(string)] = true
	}
	if !states["expired"] {
		t.Fatalf("expired state missing: %v", list.Tasks)
	}

	// Detail + events: run the full uplink path through the live WSS.
	c, _, _, err := f.connect(ctx, "dev-r2", "tok-r2")
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer c.Close(websocket.StatusNormalClosure, "")

	up := func(event string, extra map[string]any) {
		payload := map[string]any{"task_id": "task-r2", "type": "sync", "event": event,
			"steps": []map[string]any{{"step": event, "ok": true}}}
		for k, v := range extra {
			payload[k] = v
		}
		raw, _ := json.Marshal(broker.Envelope{V: 1, Type: "task.event", DeviceID: "dev-r2",
			Payload: mustJSON(payload)})
		if err := c.Write(ctx, websocket.MessageText, raw); err != nil {
			t.Fatalf("write %s: %v", event, err)
		}
		time.Sleep(300 * time.Millisecond)
	}
	up("started", nil)
	up("failed", map[string]any{"code": "E_TEST", "message": "boom on device"})

	code, body = f.getJSON("/api/tasks/task-r2")
	if code != 200 {
		t.Fatalf("task detail status %d: %s", code, body)
	}
	var detail map[string]any
	if err := json.Unmarshal(body, &detail); err != nil {
		t.Fatalf("detail parse: %v", err)
	}
	if detail["status"] != "failed" {
		t.Fatalf("expected failed, got %v", detail["status"])
	}
	if errObj, ok := detail["error"].(map[string]any); !ok || errObj["code"] != "E_TEST" || errObj["message"] == "" {
		t.Fatalf("structured error missing: %v", detail)
	}

	code, body = f.getJSON("/api/tasks/task-r2/events")
	if code != 200 {
		t.Fatalf("task events status %d", code)
	}
	var ev struct {
		Events []map[string]any `json:"events"`
	}
	if err := json.Unmarshal(body, &ev); err != nil || len(ev.Events) != 2 {
		t.Fatalf("events parse: %v body=%s", err, body)
	}
	if ev.Events[0]["event"] != "started" || ev.Events[1]["event"] != "failed" {
		t.Fatalf("event order wrong: %+v", ev.Events)
	}

	// Unknown task 404.
	code, _ = f.getJSON("/api/tasks/nope")
	if code != 404 {
		t.Fatalf("unknown task must 404, got %d", code)
	}
}

func TestBrokerReadAuthGate(t *testing.T) {
	f := newFixture(t)
	resp, err := f.server.Client().Get(f.server.URL + "/api/devices")
	if err != nil {
		t.Fatalf("no-key get: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != 401 {
		t.Fatalf("missing key must 401, got %d", resp.StatusCode)
	}
	req, _ := http.NewRequest(http.MethodGet, f.server.URL+"/api/tasks", nil)
	req.Header.Set("Authorization", "Bearer wrong-key")
	resp, err = f.server.Client().Do(req)
	if err != nil {
		t.Fatalf("bad-key get: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != 401 {
		t.Fatalf("wrong key must 401, got %d", resp.StatusCode)
	}
	// Health + metrics stay unauthenticated (supervisor probes).
	resp, err = f.server.Client().Get(f.server.URL + "/healthz")
	if err != nil {
		t.Fatalf("health: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("health must stay open, got %d", resp.StatusCode)
	}
}
