package broker

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// Store is the PG-backed durable state for the broker: device registry +
// task mailbox live in fusion_ops so a broker restart preserves both
// (issue #10 acceptance).
type Store struct {
	pool *pgxpool.Pool
}

func NewStore(pool *pgxpool.Pool) Store { return Store{pool: pool} }

// Device is one registered fusion-link device.
type Device struct {
	DeviceID     string
	TokenHash    string
	Version      string
	Capabilities []string
	State        string
	LastSeen     time.Time
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
		SELECT device_id, token_hash, version, capabilities, state, last_seen
		FROM device_registry WHERE device_id = $1`, deviceID).
		Scan(&d.DeviceID, &d.TokenHash, &d.Version, &d.Capabilities, &d.State, &d.LastSeen)
	if err != nil {
		return Device{}, fmt.Errorf("lookup device: %w", err)
	}
	if d.TokenHash != TokenHash(token) {
		return Device{}, fmt.Errorf("token mismatch for device %s", deviceID)
	}
	return d, nil
}

// SetDeviceState persists the online/offline transition and refreshes
// version/capabilities reported at hello.
func (s Store) SetDeviceState(ctx context.Context, deviceID, state, version string, caps []string) error {
	_, err := s.pool.Exec(ctx, `
		UPDATE device_registry
		SET state=$2, last_seen=now(), version=CASE WHEN $3='' THEN version ELSE $3 END,
		    capabilities=CASE WHEN $4::text[] IS NULL THEN capabilities ELSE $4::text[] END
		WHERE device_id=$1`, deviceID, state, version, caps)
	if err != nil {
		return fmt.Errorf("set device state: %w", err)
	}
	return nil
}

// TouchLastSeen records a heartbeat. Deliberately event-free.
func (s Store) TouchLastSeen(ctx context.Context, deviceID string) error {
	_, err := s.pool.Exec(ctx, `UPDATE device_registry SET last_seen=now() WHERE device_id=$1`, deviceID)
	if err != nil {
		return fmt.Errorf("touch last_seen: %w", err)
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
		UPDATE task_mailbox SET state='dispatched', dispatched_at=now() WHERE id=$1`, id)
	if err != nil {
		return fmt.Errorf("mark dispatched: %w", err)
	}
	return nil
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

// Provision binds a device: token stored hashed only.
func (s Store) Provision(ctx context.Context, deviceID, token, version string, caps []string) error {
	_, err := s.pool.Exec(ctx, `
		INSERT INTO device_registry (device_id, token_hash, version, capabilities)
		VALUES ($1, $2, $3, $4::text[])
		ON CONFLICT (device_id) DO UPDATE
		SET token_hash=EXCLUDED.token_hash, version=EXCLUDED.version, capabilities=EXCLUDED.capabilities`,
		deviceID, TokenHash(token), version, caps)
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
