package broker

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"sync"
	"time"
)

// DefaultMailboxTTL is the offline-device task lifetime (issue #10).
const DefaultMailboxTTL = 24 * time.Hour

// Server exposes the broker surface: WSS endpoint, dispatch API (localhost
// only, front traffic via nginx TLS path), health + Prometheus metrics.
type Server struct {
	hub *Hub
	mu  sync.Mutex
	ttl time.Duration
}

// NewServer wires the broker HTTP surface. ttl <= 0 falls back to
// DefaultMailboxTTL.
func NewServer(hub *Hub, ttl time.Duration) *Server {
	if ttl <= 0 {
		ttl = DefaultMailboxTTL
	}
	return &Server{hub: hub, ttl: ttl}
}

// Handler exposes the router for httptest and embedding.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", s.handleHealth)
	mux.HandleFunc("GET /metrics", s.handleMetrics)
	mux.HandleFunc("POST /api/tasks", s.handleDispatch)
	mux.HandleFunc("/link/ws", func(w http.ResponseWriter, r *http.Request) {
		if err := s.hub.HandleWS(r.Context(), w, r); err != nil && ctxErr(err) == nil {
			slog.Warn("broker ws session ended with error", "error", err)
			s.hub.WSErrors.Add(1)
		}
	})
	return mux
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

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}
