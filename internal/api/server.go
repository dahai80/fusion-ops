package api

import (
	"encoding/json"
	"log/slog"
	"net/http"
	"strconv"
	"time"

	"github.com/dahai80/fusion-ops/internal/cluster"
	"github.com/dahai80/fusion-ops/internal/desensitize"
	"github.com/dahai80/fusion-ops/internal/retention"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Server exposes the Loop C threshold API consumed by fusion-autotest#55
// plus operational endpoints. Binds localhost only — front traffic goes
// through fusion-gateway (:11432), never exposed directly.
type Server struct {
	pool *pgxpool.Pool
}

func NewServer(pool *pgxpool.Pool) *Server { return &Server{pool: pool} }

// Handler exposes the router for httptest and embedding.
func (s *Server) Handler() http.Handler { return s.routes() }

func (s *Server) routes() *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", s.handleHealth)
	mux.HandleFunc("GET /api/clusters", s.handleClusters)
	mux.HandleFunc("POST /api/clusters/backfill", s.handleBackfill)
	mux.HandleFunc("POST /api/clusters/close", s.handleClose)
	mux.HandleFunc("POST /api/jobs/retention", s.handleRetention)
	return mux
}

// Run starts the HTTP server; blocks until ctx is cancelled or ListenAndServe
// fails.
func (s *Server) Run(addr string) error {
	srv := &http.Server{
		Addr:              addr,
		Handler:           s.routes(),
		ReadHeaderTimeout: 5 * time.Second,
	}
	slog.Info("api server listening", "addr", addr)
	err := srv.ListenAndServe()
	if err == http.ErrServerClosed {
		return nil
	}
	return err
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(v); err != nil {
		slog.Error("write response", "error", err)
	}
}

func writeErr(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	if err := s.pool.Ping(r.Context()); err != nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"status": "down", "error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

// handleClusters serves the threshold query: GET /api/clusters?threshold=5&hours=24.
// Output is audited PII-free: signature text passes desensitize v1 before it
// is served (issue #3 req 4).
func (s *Server) handleClusters(w http.ResponseWriter, r *http.Request) {
	threshold := int64(5)
	if v := r.URL.Query().Get("threshold"); v != "" {
		parsed, err := strconv.ParseInt(v, 10, 64)
		if err != nil || parsed < 1 {
			writeErr(w, http.StatusBadRequest, "threshold must be a positive integer")
			return
		}
		threshold = parsed
	}
	window := 24 * time.Hour
	if v := r.URL.Query().Get("hours"); v != "" {
		parsed, err := strconv.ParseFloat(v, 64)
		if err != nil || parsed <= 0 {
			writeErr(w, http.StatusBadRequest, "hours must be a positive number")
			return
		}
		window = time.Duration(parsed * float64(time.Hour))
	}
	issues, err := cluster.Threshold(r.Context(), s.pool, threshold, window)
	if err != nil {
		slog.Error("threshold query failed", "error", err)
		writeErr(w, http.StatusInternalServerError, "threshold query failed")
		return
	}
	for i := range issues {
		issues[i].Signature = desensitize.Text(issues[i].Signature)
		issues[i].ClusterKey = desensitize.Text(issues[i].ClusterKey)
	}
	writeJSON(w, http.StatusOK, map[string]any{"clusters": issues, "count": len(issues)})
}

func (s *Server) handleBackfill(w http.ResponseWriter, r *http.Request) {
	var req struct {
		ClusterKey string `json:"cluster_key"`
		IssueURL   string `json:"issue_url"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.ClusterKey == "" || req.IssueURL == "" {
		writeErr(w, http.StatusBadRequest, "cluster_key and issue_url are required")
		return
	}
	if err := cluster.Backfill(r.Context(), s.pool, req.ClusterKey, req.IssueURL); err != nil {
		writeErr(w, http.StatusConflict, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "backfilled", "cluster_key": req.ClusterKey})
}

func (s *Server) handleClose(w http.ResponseWriter, r *http.Request) {
	var req struct {
		ClusterKey string `json:"cluster_key"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.ClusterKey == "" {
		writeErr(w, http.StatusBadRequest, "cluster_key is required")
		return
	}
	if err := cluster.Close(r.Context(), s.pool, req.ClusterKey); err != nil {
		writeErr(w, http.StatusConflict, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "closed", "cluster_key": req.ClusterKey})
}

// handleRetention triggers one cleanup pass on demand (supervisor invokes the
// jobs binary nightly; this endpoint exists for drills and manual runs).
func (s *Server) handleRetention(w http.ResponseWriter, r *http.Request) {
	res, err := retention.Run(r.Context(), s.pool)
	if err != nil {
		slog.Error("retention job failed", "error", err)
		writeErr(w, http.StatusInternalServerError, "retention job failed: "+err.Error())
		return
	}
	writeJSON(w, http.StatusOK, res)
}
