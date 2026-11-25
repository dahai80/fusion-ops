package broker

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"strconv"
	"sync"
	"time"
)

// DefaultMailboxTTL is the offline-device task lifetime (issue #10).
const DefaultMailboxTTL = 24 * time.Hour

// Server exposes the broker surface: WSS endpoint, dispatch API (localhost
// only, front traffic via nginx TLS path), read surface for the site
// console (issue #11, bearer key), health + Prometheus metrics.
type Server struct {
	hub    *Hub
	mu     sync.Mutex
	ttl    time.Duration
	apiKey string
}

// NewServer wires the broker HTTP surface. ttl <= 0 falls back to
// DefaultMailboxTTL. apiKey (FUSION_BROKER_API_KEY) gates GET /api/* reads;
// empty = loopback callers only (the broker binds loopback anyway).
func NewServer(hub *Hub, ttl time.Duration, apiKey string) *Server {
	if ttl <= 0 {
		ttl = DefaultMailboxTTL
	}
	return &Server{hub: hub, ttl: ttl, apiKey: apiKey}
}

// Handler exposes the router for httptest and embedding.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", s.handleHealth)
	mux.HandleFunc("GET /metrics", s.handleMetrics)
	mux.HandleFunc("POST /api/tasks", s.handleDispatch)
	mux.Handle("GET /api/devices", s.readGate(s.handleListDevices))
	mux.Handle("GET /api/tasks", s.readGate(s.handleListTasks))
	mux.Handle("GET /api/tasks/{task_id}", s.readGate(s.handleGetTask))
	mux.Handle("GET /api/tasks/{task_id}/events", s.readGate(s.handleTaskEvents))
	mux.HandleFunc("/link/ws", func(w http.ResponseWriter, r *http.Request) {
		if err := s.hub.HandleWS(r.Context(), w, r); err != nil && ctxErr(err) == nil {
			slog.Warn("broker ws session ended with error", "error", err)
			s.hub.WSErrors.Add(1)
		}
	})
	return mux
}

// readGate wraps a read handler with the optional bearer-key check
// (issue #11 auth: same credential class as dispatch, provisioned via env).
func (s *Server) readGate(next http.HandlerFunc) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if s.apiKey != "" && r.Header.Get("Authorization") != "Bearer "+s.apiKey {
			writeError(w, http.StatusUnauthorized, "missing or invalid bearer key")
			return
		}
		next(w, r)
	})
}

// Run blocks serving addr until the context is cancelled.
func (s *Server) Run(ctx context.Context, addr string) error {
	srv := &http.Server{Addr: addr, Handler: s.Handler(), ReadHeaderTimeout: 5 * time.Second}
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutdownCtx)
	}()
	slog.Info("broker listening", "addr", addr, "mailbox_ttl", s.ttl.String())
	err := srv.ListenAndServe()
	if err == http.ErrServerClosed {
		return nil
	}
	return err
}

type dispatchRequest struct {
	DeviceID string         `json:"device_id"`
	TaskID   string         `json:"task_id"`
	Type     string         `json:"type"`
	Params   map[string]any `json:"params"`
}

func (s *Server) handleDispatch(w http.ResponseWriter, r *http.Request) {
	var req dispatchRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.DeviceID == "" || req.TaskID == "" || req.Type == "" {
		writeError(w, http.StatusBadRequest, "device_id, task_id, type required")
		return
	}
	if err := s.hub.Dispatch(r.Context(), req.DeviceID, req.TaskID, req.Type, req.Params, s.ttl); err != nil {
		if errorsIs(err, errUnknownDevice) {
			writeError(w, http.StatusNotFound, "unknown device")
			return
		}
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"queued": true, "ttl": s.ttl.String()})
}

func (s *Server) handleHealth(w http.ResponseWriter, _ *http.Request) {
	ctx, cancel := contextWithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	online, err := s.hub.store.CountOnline(ctx)
	if err != nil {
		writeError(w, http.StatusServiceUnavailable, err.Error())
		return
	}
	queued, err := s.hub.store.CountQueued(ctx)
	if err != nil {
		writeError(w, http.StatusServiceUnavailable, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"status": "ok", "devices_online": online, "tasks_queued": queued})
}

func (s *Server) handleMetrics(w http.ResponseWriter, _ *http.Request) {
	ctx, cancel := contextWithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	online, _ := s.hub.store.CountOnline(ctx)
	queued, _ := s.hub.store.CountQueued(ctx)
	fmt.Fprintf(w, "# HELP broker_devices_online Registered devices currently connected.\n")
	fmt.Fprintf(w, "# TYPE broker_devices_online gauge\nbroker_devices_online %d\n", online)
	fmt.Fprintf(w, "# HELP broker_tasks_queued Mailbox backlog (queued, unexpired).\n")
	fmt.Fprintf(w, "# TYPE broker_tasks_queued gauge\nbroker_tasks_queued %d\n", queued)
	fmt.Fprintf(w, "# HELP broker_tasks_dispatched_total Tasks delivered to devices since boot.\n")
	fmt.Fprintf(w, "# TYPE broker_tasks_dispatched_total counter\nbroker_tasks_dispatched_total %d\n", s.hub.Dispatched.Load())
	fmt.Fprintf(w, "# HELP broker_wss_errors_total WSS protocol/auth failures since boot.\n")
	fmt.Fprintf(w, "# TYPE broker_wss_errors_total counter\nbroker_wss_errors_total %d\n", s.hub.WSErrors.Load())
}

// deviceJSON is the console device-table row (issue #11 contract).
type deviceJSON struct {
	DeviceID       string     `json:"device_id"`
	Hostname       string     `json:"hostname"`
	Platform       string     `json:"platform"`
	Online         bool       `json:"online"`
	Version        string     `json:"version,omitempty"`
	LastHeartbeat  *time.Time `json:"last_heartbeat,omitempty"`
	ConnectedSince *time.Time `json:"connected_since,omitempty"`

	IsDefaultExecution bool             `json:"is_default_execution"`
	Services           []map[string]any `json:"services"`
	Tags               []string         `json:"tags"`
}

// handleListDevices serves the registry snapshot: live-session online flag,
// hello-reported hostname/platform, heartbeat-carried service health
// (empty list until devices report it — console renders the degraded state).
func (s *Server) handleListDevices(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := contextWithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	devices, err := s.hub.store.ListDevices(ctx)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	online := s.hub.OnlineSet()
	out := make([]deviceJSON, 0, len(devices))
	for _, d := range devices {
		isDefault := false
		for _, tag := range d.Tags {
			if tag == "default-execution" {
				isDefault = true
				break
			}
		}
		row := deviceJSON{
			DeviceID:           d.DeviceID,
			Hostname:           d.Hostname,
			Platform:           d.Platform,
			Online:             online[d.DeviceID] || d.State == "online",
			Version:            d.Version,
			ConnectedSince:     d.ConnectedSince,
			IsDefaultExecution: isDefault,
			Services:           servicesFromHealth(d.LastHealth),
			Tags:               d.Tags,
		}
		if !d.LastSeen.IsZero() {
			row.LastHeartbeat = &d.LastSeen
		}
		out = append(out, row)
	}
	writeJSON(w, http.StatusOK, map[string]any{"devices": out})
}

// servicesFromHealth extracts the local service list from the heartbeat
// health snapshot (`services: [{name,port,status,detail}]`); absent = [].
func servicesFromHealth(health []byte) []map[string]any {
	if len(health) == 0 {
		return []map[string]any{}
	}
	var snap struct {
		Services []map[string]any `json:"services"`
	}
	if err := json.Unmarshal(health, &snap); err != nil || snap.Services == nil {
		return []map[string]any{}
	}
	return snap.Services
}

func (s *Server) handleListTasks(w http.ResponseWriter, r *http.Request) {
	limit := 50
	if v := r.URL.Query().Get("limit"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			limit = n
		}
	}
	ctx, cancel := contextWithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	_ = s.hub.store.ExpireStaleQueued(ctx)
	tasks, err := s.hub.store.ListTasks(ctx, limit)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"tasks": tasks})
}

func (s *Server) handleGetTask(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := contextWithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	_ = s.hub.store.ExpireStaleQueued(ctx)
	task, ok, err := s.hub.store.GetTask(ctx, r.PathValue("task_id"))
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	if !ok {
		writeError(w, http.StatusNotFound, "unknown task")
		return
	}
	writeJSON(w, http.StatusOK, task)
}

// handleTaskEvents serves the polling progress feed (SSE deferred; the
// bridge page polls ~2s while a task is active, issue #11).
func (s *Server) handleTaskEvents(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := contextWithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	events, err := s.hub.store.LoadTaskEvents(ctx, r.PathValue("task_id"))
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	if events == nil {
		events = []TaskEvent{}
	}
	writeJSON(w, http.StatusOK, map[string]any{"events": events})
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}
