package broker

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// pgxRows alias keeps the scan helpers readable.
type pgxRows = pgx.Rows

// Store is the PG-backed durable state for the broker: device registry +
// task mailbox live in fusion_ops so a broker restart preserves both
// (issue #10 acceptance).
type Store struct {
	pool *pgxpool.Pool
}

func NewStore(pool *pgxpool.Pool) Store { return Store{pool: pool} }

// Device is one registered fusion-link device.
type Device struct {
	DeviceID       string
	TokenHash      string
	Version        string
	Capabilities   []string
	State          string
	LastSeen       time.Time
	Hostname       string
	Platform       string
	ConnectedSince *time.Time
	LastHealth     []byte
	Tags           []string
}

// ResetAllOffline marks every device offline at broker boot: no live
// connections survive a restart, so stale online rows must not outlive the
// process. No ops_events rows here — a broker restart is not a device
// state transition.
func (s Store) ResetAllOffline(ctx context.Context) error {
	tag, err := s.pool.Exec(ctx, `UPDATE device_registry SET state='offline' WHERE state='online'`)
	if err != nil {
		return fmt.Errorf("reset registry offline: %w", err)
	}
	if tag.RowsAffected() > 0 {
		slog.Info("broker boot: stale online rows reset", "count", tag.RowsAffected())
	}
	return nil
}

// AuthDevice verifies bound credentials and returns the device row.
func (s Store) AuthDevice(ctx context.Context, deviceID, token string) (Device, error) {
	var d Device
	err := s.pool.QueryRow(ctx, `
		SELECT device_id, token_hash, version, capabilities, state, last_seen,
		       hostname, platform, connected_since, last_health, tags
		FROM device_registry WHERE device_id = $1`, deviceID).
		Scan(&d.DeviceID, &d.TokenHash, &d.Version, &d.Capabilities, &d.State, &d.LastSeen,
			&d.Hostname, &d.Platform, &d.ConnectedSince, &d.LastHealth, &d.Tags)
	if err != nil {
		return Device{}, fmt.Errorf("lookup device: %w", err)
	}
	if d.TokenHash != TokenHash(token) {
		return Device{}, fmt.Errorf("token mismatch for device %s", deviceID)
	}
	return d, nil
}

// SetDeviceState persists the online/offline transition and refreshes the
// fields reported at hello (version, capabilities, hostname, platform).
func (s Store) SetDeviceState(ctx context.Context, deviceID, state, version string, caps []string) error {
	return s.setDeviceState(ctx, deviceID, state, version, caps, "", "")
}

// SetDeviceStateDetailed is SetDeviceState plus hello-reported description
// fields and the connected_since marker (issue #11 console fields).
func (s Store) SetDeviceStateDetailed(ctx context.Context, deviceID, state, version string, caps []string, hostname, platform string) error {
	return s.setDeviceState(ctx, deviceID, state, version, caps, hostname, platform)
}

func (s Store) setDeviceState(ctx context.Context, deviceID, state, version string, caps []string, hostname, platform string) error {
	if state == "online" {
		_, err := s.pool.Exec(ctx, `
			UPDATE device_registry
			SET state=$2, last_seen=now(),
			    version=CASE WHEN $3='' THEN version ELSE $3 END,
			    capabilities=CASE WHEN $4::text[] IS NULL THEN capabilities ELSE $4::text[] END,
			    hostname=CASE WHEN $5='' THEN hostname ELSE $5 END,
			    platform=CASE WHEN $6='' THEN platform ELSE $6 END,
			    connected_since=CASE WHEN $2='online' THEN now() ELSE connected_since END
			WHERE device_id=$1`, deviceID, state, version, caps, hostname, platform)
		if err != nil {
			return fmt.Errorf("set device state: %w", err)
		}
		return nil
	}
	_, err := s.pool.Exec(ctx, `
		UPDATE device_registry
		SET state=$2, last_seen=now(), connected_since=NULL
		WHERE device_id=$1`, deviceID, state)
	if err != nil {
		return fmt.Errorf("set device state: %w", err)
	}
	return nil
}

// RecordHeartbeat stores the health snapshot and refreshes last_seen.
// Deliberately event-free (issue #10 discipline).
func (s Store) RecordHeartbeat(ctx context.Context, deviceID string, health []byte) error {
	_, err := s.pool.Exec(ctx, `
		UPDATE device_registry SET last_seen=now(),
		       last_health=CASE WHEN $2::jsonb IS NULL THEN last_health ELSE $2::jsonb END
		WHERE device_id=$1`, deviceID, string(health))
	if err != nil {
		return fmt.Errorf("record heartbeat: %w", err)
	}
	return nil
}

// QueuedTask is one mailbox row waiting for its device.
type QueuedTask struct {
	ID       int64
	TaskID   string
	TaskType string
	Params   map[string]any
}

// LoadQueued returns undelivered tasks for a device, FIFO.
func (s Store) LoadQueued(ctx context.Context, deviceID string) ([]QueuedTask, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT id, task_id, task_type, params FROM task_mailbox
		WHERE device_id=$1 AND state='queued' AND ttl > now()
		ORDER BY id`, deviceID)
	if err != nil {
		return nil, fmt.Errorf("load queued tasks: %w", err)
	}
	defer rows.Close()
	var out []QueuedTask
	for rows.Next() {
		var t QueuedTask
		if err := rows.Scan(&t.ID, &t.TaskID, &t.TaskType, &t.Params); err != nil {
			return nil, fmt.Errorf("scan queued task: %w", err)
		}
		out = append(out, t)
	}
	return out, rows.Err()
}

// EnqueueTask inserts a task for a device; idempotent per (device, task).
// Returns whether a new row was created.
func (s Store) EnqueueTask(ctx context.Context, deviceID, taskID, taskType string, params map[string]any, ttl time.Duration) (bool, error) {
	tag, err := s.pool.Exec(ctx, `
		INSERT INTO task_mailbox (device_id, task_id, task_type, params, ttl)
		VALUES ($1, $2, $3, $4::jsonb, now() + $5::interval)
		ON CONFLICT (device_id, task_id) DO NOTHING`,
		deviceID, taskID, taskType, paramsJSON(params), fmt.Sprintf("%.0f seconds", ttl.Seconds()))
	if err != nil {
		return false, fmt.Errorf("enqueue task: %w", err)
	}
	return tag.RowsAffected() > 0, nil
}

// MarkDispatched closes out a delivered mailbox row.
func (s Store) MarkDispatched(ctx context.Context, id int64) error {
	_, err := s.pool.Exec(ctx, `
		UPDATE task_mailbox SET state='dispatched', dispatched_at=now(), updated_at=now() WHERE id=$1`, id)
	if err != nil {
		return fmt.Errorf("mark dispatched: %w", err)
	}
	return nil
}

// ApplyTaskUplink folds a device task.event/task.result frame into the task
// row: state machine transition, best-effort steps, error fields. Returns
// the mailbox id (0 when the task is unknown — device-side tasks may not
// have passed through this broker).
func (s Store) ApplyTaskUplink(ctx context.Context, deviceID, taskID, event, detail string, steps []byte, errCode, errMsg string) (int64, error) {
	state := uplinkState(event)
	if state == "" {
		return 0, nil
	}
	tag, err := s.pool.Exec(ctx, `
		UPDATE task_mailbox SET state=$3, updated_at=now(),
		       steps=CASE WHEN $4::jsonb IS NULL THEN steps ELSE $4::jsonb END,
		       error_code=CASE WHEN $5='' THEN error_code ELSE $5 END,
		       error_message=CASE WHEN $6='' THEN error_message ELSE $6 END
		WHERE device_id=$1 AND task_id=$2`, deviceID, taskID, state, stepsJSON(steps), errCode, errMsg)
	if err != nil {
		return 0, fmt.Errorf("apply task uplink: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return 0, nil
	}
	var id int64
	if err := s.pool.QueryRow(ctx, `SELECT id FROM task_mailbox WHERE device_id=$1 AND task_id=$2`, deviceID, taskID).Scan(&id); err != nil {
		return 0, err
	}
	return id, nil
}

func uplinkState(event string) string {
	switch event {
	case "started", "running":
		return "running"
	case "awaiting_local_confirm":
		return "awaiting_local_confirm"
	case "completed", "succeeded":
		return "completed"
	case "failed":
		return "failed"
	}
	return ""
}

func stepsJSON(b []byte) any {
	if len(b) == 0 {
		return nil
	}
	return string(b)
}

// ExpireStaleQueued flips queued tasks past their TTL to expired so reads
// show the expired pill without waiting for the retention purge.
func (s Store) ExpireStaleQueued(ctx context.Context) error {
	_, err := s.pool.Exec(ctx, `UPDATE task_mailbox SET state='expired', updated_at=now() WHERE state='queued' AND ttl < now()`)
	if err != nil {
		return fmt.Errorf("expire stale queued: %w", err)
	}
	return nil
}

// AppendTaskEvent records one progress frame in the task history.
func (s Store) AppendTaskEvent(ctx context.Context, taskID, event, detail string, payload []byte) {
	_, err := s.pool.Exec(ctx, `
		INSERT INTO task_events (task_id, event, detail, payload)
		VALUES ($1, $2, $3, CASE WHEN $4::jsonb IS NULL THEN '{}'::jsonb ELSE $4::jsonb END)`,
		taskID, event, detail, string(payload))
	if err != nil {
		slog.Error("task event append failed", "task_id", taskID, "error", err)
	}
}

// TaskView is the read model for the bridge page (issue #11).
type TaskView struct {
	TaskID    string          `json:"task_id"`
	DeviceID  string          `json:"device_id"`
	Type      string          `json:"type"`
	State     string          `json:"status"`
	CreatedAt time.Time       `json:"created_at"`
	UpdatedAt time.Time       `json:"updated_at"`
	Steps     json.RawMessage `json:"steps"`
	Error     *TaskError      `json:"error,omitempty"`
	errorCode string
	errorMsg  string
}

// TaskError is the structured failure payload (issue #11 contract).
type TaskError struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

// finalizeError materializes the Error field after scanning (nil when the
// task carries no error).
func (t *TaskView) finalizeError() {
	if t.errorCode != "" || t.errorMsg != "" {
		t.Error = &TaskError{Code: t.errorCode, Message: t.errorMsg}
	}
}

// ListDevices returns the full registry snapshot (console device table).
func (s Store) ListDevices(ctx context.Context) ([]Device, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT device_id, token_hash, version, capabilities, state, last_seen,
		       hostname, platform, connected_since, last_health, tags
		FROM device_registry ORDER BY device_id`)
	if err != nil {
		return nil, fmt.Errorf("list devices: %w", err)
	}
	defer rows.Close()
	var out []Device
	for rows.Next() {
		var d Device
		if err := rows.Scan(&d.DeviceID, &d.TokenHash, &d.Version, &d.Capabilities, &d.State, &d.LastSeen,
			&d.Hostname, &d.Platform, &d.ConnectedSince, &d.LastHealth, &d.Tags); err != nil {
			return nil, fmt.Errorf("scan device: %w", err)
		}
		out = append(out, d)
	}
	return out, rows.Err()
}

// ListTasks returns the most recent tasks (bridge page list).
func (s Store) ListTasks(ctx context.Context, limit int) ([]TaskView, error) {
	if limit <= 0 || limit > 200 {
		limit = 50
	}
	rows, err := s.pool.Query(ctx, `
		SELECT task_id, device_id, task_type, state, created_at, updated_at, steps, error_code, error_message
		FROM task_mailbox ORDER BY updated_at DESC LIMIT $1`, limit)
	if err != nil {
		return nil, fmt.Errorf("list tasks: %w", err)
	}
	defer rows.Close()
	views, err := scanTaskViews(rows)
	if err != nil {
		return nil, err
	}
	if views == nil {
		views = []TaskView{}
	}
	return views, nil
}

// GetTask returns one task by id.
func (s Store) GetTask(ctx context.Context, taskID string) (TaskView, bool, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT task_id, device_id, task_type, state, created_at, updated_at, steps, error_code, error_message
		FROM task_mailbox WHERE task_id=$1`, taskID)
	if err != nil {
		return TaskView{}, false, fmt.Errorf("get task: %w", err)
	}
	defer rows.Close()
	views, err := scanTaskViews(rows)
	if err != nil || len(views) == 0 {
		return TaskView{}, false, err
	}
	return views[0], true, nil
}

func scanTaskViews(rows pgxRows) ([]TaskView, error) {
	var out []TaskView
	for rows.Next() {
		var t TaskView
		if err := rows.Scan(&t.TaskID, &t.DeviceID, &t.Type, &t.State, &t.CreatedAt, &t.UpdatedAt, &t.Steps, &t.errorCode, &t.errorMsg); err != nil {
			return nil, fmt.Errorf("scan task view: %w", err)
		}
		t.finalizeError()
		out = append(out, t)
	}
	return out, rows.Err()
}

// TaskEvent is one progress-history entry (polling feed, issue #11).
type TaskEvent struct {
	TS      time.Time       `json:"ts"`
	Event   string          `json:"event"`
	Detail  string          `json:"detail"`
	Payload json.RawMessage `json:"payload,omitempty"`
}

// LoadTaskEvents returns the progress history for one task, oldest first.
func (s Store) LoadTaskEvents(ctx context.Context, taskID string) ([]TaskEvent, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT ts, event, detail, payload FROM task_events WHERE task_id=$1 ORDER BY id`, taskID)
	if err != nil {
		return nil, fmt.Errorf("load task events: %w", err)
	}
	defer rows.Close()
	var out []TaskEvent
	for rows.Next() {
		var e TaskEvent
		if err := rows.Scan(&e.TS, &e.Event, &e.Detail, &e.Payload); err != nil {
			return nil, fmt.Errorf("scan task event: %w", err)
		}
		out = append(out, e)
	}
	return out, rows.Err()
}

// CountQueued reports the mailbox backlog (metrics + health).
func (s Store) CountQueued(ctx context.Context) (int64, error) {
	var n int64
	err := s.pool.QueryRow(ctx, `SELECT count(*) FROM task_mailbox WHERE state='queued' AND ttl > now()`).Scan(&n)
	return n, err
}

// DeviceExists reports whether a device_id is registered.
func (s Store) DeviceExists(ctx context.Context, deviceID string) (bool, error) {
	var exists bool
	err := s.pool.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM device_registry WHERE device_id=$1)`, deviceID).Scan(&exists)
	return exists, err
}

// CountOnline reports registered devices currently online.
func (s Store) CountOnline(ctx context.Context) (int64, error) {
	var n int64
	err := s.pool.QueryRow(ctx, `SELECT count(*) FROM device_registry WHERE state='online'`).Scan(&n)
	return n, err
}

// AppendEvent writes a broker emitter row into ops_events v1. Only
// failures and state transitions call this (issue #10).
func (s Store) AppendEvent(ctx context.Context, eventType, detail string, extra map[string]any) {
	payload := map[string]any{"detail": detail}
	for k, v := range extra {
		payload[k] = v
	}
	_, err := s.pool.Exec(ctx, `
		INSERT INTO ops_events (source, event_type, payload)
		VALUES ('broker', $1, $2::jsonb)`, eventType, paramsJSON(payload))
	if err != nil {
		slog.Error("broker event append failed", "event_type", eventType, "error", err)
	}
}

// Provision binds a device: token stored hashed only. Tags carry console
// routing metadata (e.g. default-execution, issue #11).
func (s Store) Provision(ctx context.Context, deviceID, token, version string, caps []string, tags []string) error {
	if tags == nil {
		tags = []string{}
	}
	_, err := s.pool.Exec(ctx, `
		INSERT INTO device_registry (device_id, token_hash, version, capabilities, tags)
		VALUES ($1, $2, $3, $4::text[], $5::text[])
		ON CONFLICT (device_id) DO UPDATE
		SET token_hash=EXCLUDED.token_hash, version=EXCLUDED.version, capabilities=EXCLUDED.capabilities, tags=EXCLUDED.tags`,
		deviceID, TokenHash(token), version, caps, tags)
	if err != nil {
		return fmt.Errorf("provision device: %w", err)
	}
	slog.Info("device provisioned", "device_id", deviceID)
	return nil
}

func paramsJSON(m map[string]any) string {
	if len(m) == 0 {
		return "{}"
	}
	b, err := json.Marshal(m)
	if err != nil {
		return "{}"
	}
	return string(b)
}

// PurgeExpiredMailbox deletes TTL-expired rows; called by the retention job
// (issue #10: mailbox TTL 24h enforced by existing retention).
func (s Store) PurgeExpiredMailbox(ctx context.Context, batchSize int) (int64, error) {
	var total int64
	for {
		tag, err := s.pool.Exec(ctx, `DELETE FROM task_mailbox WHERE id IN (
			SELECT id FROM task_mailbox WHERE ttl < now() LIMIT $1)`, batchSize)
		if err != nil {
			return total, fmt.Errorf("purge expired mailbox: %w", err)
		}
		total += tag.RowsAffected()
		if tag.RowsAffected() < int64(batchSize) {
			return total, nil
		}
	}
}

// sanitizeDeviceID guards log injection from untrusted hello frames.
func sanitizeDeviceID(id string) string {
	return strings.Map(func(r rune) rune {
		if r > 31 && r < 127 {
			return r
		}
		return '?'
	}, id)
}
