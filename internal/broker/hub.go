package broker

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/coder/websocket"
)

// atomicInt64 keeps counter dependencies stdlib-only for the metrics hook.
type atomicInt64 = atomic.Int64

// readWait bounds a single WSS read; hello/auth must complete inside
// handshakeWindow or the connection is dropped.
const (
	readWait        = 30 * time.Second
	writeWait       = 10 * time.Second
	handshakeWindow = 15 * time.Second
)

// errUnknownDevice rejects dispatches to unregistered devices.
var errUnknownDevice = errors.New("unknown device")

// Hub owns live device connections and routes dispatches. Durable state
// lives in Store; the in-memory map is only the routing layer.
type Hub struct {
	store Store
	mu    sync.Mutex
	conns map[string]*websocket.Conn

	Online     func() int // metrics hook, set by tests; nil-safe
	WSErrors   atomicInt64
	Dispatched atomicInt64
	handshaked map[*websocket.Conn]bool
}

// NewHub creates the routing layer over a PG-backed store.
func NewHub(store Store) *Hub {
	return &Hub{store: store, conns: map[string]*websocket.Conn{}, handshaked: map[*websocket.Conn]bool{}}
}

// Store exposes the durable-state layer (boot reset, provisioning, tests).
func (h *Hub) Store() *Store { return &h.store }

func (h *Hub) setConn(deviceID string, c *websocket.Conn) {
	h.mu.Lock()
	h.conns[deviceID] = c
	h.handshaked[c] = true
	h.mu.Unlock()
}

func (h *Hub) dropConn(deviceID string, c *websocket.Conn) bool {
	h.mu.Lock()
	delete(h.handshaked, c)
	if cur, ok := h.conns[deviceID]; ok && cur == c {
		delete(h.conns, deviceID)
		h.mu.Unlock()
		return true
	}
	h.mu.Unlock()
	return false
}

func (h *Hub) getConn(deviceID string) *websocket.Conn {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.conns[deviceID]
}

// HandleWS upgrades and serves one device connection. Blocks until the
// peer disconnects or the context is cancelled.
func (h *Hub) HandleWS(ctx context.Context, w http.ResponseWriter, r *http.Request) error {
	c, err := websocket.Accept(w, r, nil)
	if err != nil {
		return err
	}
	defer c.Close(websocket.StatusInternalError, "server closing")

	helloDeadline := time.Now().Add(handshakeWindow)
	env, err := h.readEnvelope(ctx, c, helloDeadline)
	if err != nil {
		h.WSErrors.Add(1)
		return err
	}
	if env.Type != TypeHello || env.DeviceID == "" {
		h.WSErrors.Add(1)
		_ = h.send(ctx, c, TypeError, "", ErrorPayload{Code: ErrUnsupported, Detail: "first message must be hello"})
		return nil
	}
	var hello HelloPayload
	if err := json.Unmarshal(env.Payload, &hello); err != nil || hello.Token == "" {
		h.WSErrors.Add(1)
		_ = h.send(ctx, c, TypeError, env.DeviceID, ErrorPayload{Code: ErrAuth, Detail: "malformed hello"})
		return nil
	}
	deviceID := sanitizeDeviceID(env.DeviceID)
	if _, err := h.store.AuthDevice(ctx, deviceID, hello.Token); err != nil {
		h.WSErrors.Add(1)
		slog.Warn("broker auth failed", "device_id", deviceID)
		_ = h.send(ctx, c, TypeError, deviceID, ErrorPayload{Code: ErrAuth, Detail: "authentication failed"})
		return nil
	}
	if err := h.send(ctx, c, TypeHelloAck, deviceID, map[string]any{"ok": true, "queued": true}); err != nil {
		return nil
	}
	h.setConn(deviceID, c)
	_ = h.store.SetDeviceState(ctx, deviceID, "online", hello.Version, hello.Capabilities)
	h.store.AppendEvent(ctx, "device_online", "device connected", map[string]any{"device_id": deviceID, "version": hello.Version})
	slog.Info("device online", "device_id", deviceID, "version", hello.Version)

	// Reconnect drain: any task queued while the device was offline goes out
	// immediately after hello (reconnect storm acceptance).
	h.drainMailbox(ctx, deviceID, c)

	readErr := h.readLoop(ctx, deviceID, c)

	if h.dropConn(deviceID, c) {
		_ = h.store.SetDeviceState(ctx, deviceID, "offline", "", nil)
		h.store.AppendEvent(ctx, "device_offline", "device disconnected", map[string]any{"device_id": deviceID})
		slog.Info("device offline", "device_id", deviceID)
	}
	if ctx.Err() != nil || readErr == nil {
		return nil
	}
	return readErr
}

func (h *Hub) readLoop(ctx context.Context, deviceID string, c *websocket.Conn) error {
	for {
		env, err := h.readEnvelope(ctx, c, time.Now().Add(readWait))
		if err != nil {
			return err
		}
		switch env.Type {
		case TypeHeartbeat:
			_ = h.store.TouchLastSeen(ctx, deviceID) // normal traffic: no ops_events
		case TypeTaskEvent, TypeTaskResult:
			var p TaskPayload
			if err := json.Unmarshal(env.Payload, &p); err != nil {
				_ = h.send(ctx, c, TypeError, deviceID, ErrorPayload{Code: ErrUnsupported, Detail: "malformed task payload"})
				continue
			}
			if env.Type == TypeTaskEvent && p.Event == "failed" {
				h.store.AppendEvent(ctx, "task_failed", p.Detail, map[string]any{"device_id": deviceID, "task_id": p.TaskID, "task_type": p.Type})
			}
			slog.Info("task uplink", "device_id", deviceID, "frame", env.Type, "task_id", p.TaskID, "event", p.Event)
		case TypeHello:
			_ = h.send(ctx, c, TypeError, deviceID, ErrorPayload{Code: ErrUnsupported, Detail: "already authenticated"})
		default:
			_ = h.send(ctx, c, TypeError, deviceID, ErrorPayload{Code: ErrUnsupported, Detail: "unknown type " + env.Type})
		}
	}
}

// drainMailbox pushes queued tasks to a freshly connected device.
func (h *Hub) drainMailbox(ctx context.Context, deviceID string, c *websocket.Conn) {
	tasks, err := h.store.LoadQueued(ctx, deviceID)
	if err != nil {
		slog.Error("mailbox drain query failed", "device_id", deviceID, "error", err)
		return
	}
	for _, t := range tasks {
		payload := TaskPayload{TaskID: t.TaskID, Type: t.TaskType, Params: t.Params}
		if err := h.send(ctx, c, TypeDispatch, deviceID, payload); err != nil {
			slog.Warn("mailbox drain send failed", "device_id", deviceID, "task_id", t.TaskID, "error", err)
			return
		}
		if err := h.store.MarkDispatched(ctx, t.ID); err != nil {
			slog.Error("mark dispatched failed", "task_id", t.TaskID, "error", err)
			continue
		}
		h.Dispatched.Add(1)
		slog.Info("task dispatched", "device_id", deviceID, "task_id", t.TaskID)
	}
}

// Dispatch routes a task: straight to the device when online, otherwise
// parked in the PG mailbox until reconnect or TTL (issue #10).
func (h *Hub) Dispatch(ctx context.Context, deviceID, taskID, taskType string, params map[string]any, ttl time.Duration) error {
	exists, err := h.store.DeviceExists(ctx, deviceID)
	if err != nil {
		return err
	}
	if !exists {
		return errUnknownDevice
	}
	created, err := h.store.EnqueueTask(ctx, deviceID, taskID, taskType, params, ttl)
	if err != nil {
		return err
	}
	if !created {
		slog.Info("task already queued", "device_id", deviceID, "task_id", taskID)
		return nil
	}
	c := h.getConn(deviceID)
	if c == nil {
		slog.Info("task parked in mailbox", "device_id", deviceID, "task_id", taskID)
		return nil
	}
	tasks, err := h.store.LoadQueued(ctx, deviceID)
	if err != nil {
		return err
	}
	for _, t := range tasks {
		if t.TaskID != taskID {
			continue
		}
		if err := h.send(ctx, c, TypeDispatch, deviceID, TaskPayload{TaskID: t.TaskID, Type: t.TaskType, Params: t.Params}); err != nil {
			slog.Warn("online dispatch failed; stays queued", "device_id", deviceID, "task_id", taskID, "error", err)
			return nil
		}
		if err := h.store.MarkDispatched(ctx, t.ID); err != nil {
			return err
		}
		h.Dispatched.Add(1)
		slog.Info("task dispatched online", "device_id", deviceID, "task_id", taskID)
	}
	return nil
}

func (h *Hub) send(ctx context.Context, c *websocket.Conn, typ, deviceID string, payload any) error {
	env, err := newEnvelope(typ, deviceID, payload)
	if err != nil {
		return err
	}
	raw, err := json.Marshal(env)
	if err != nil {
		return err
	}
	wctx, cancel := context.WithTimeout(ctx, writeWait)
	defer cancel()
	return c.Write(wctx, websocket.MessageText, raw)
}

func (h *Hub) readEnvelope(ctx context.Context, c *websocket.Conn, deadline time.Time) (Envelope, error) {
	rctx, cancel := context.WithDeadline(ctx, deadline)
	defer cancel()
	_, data, err := c.Read(rctx)
	if err != nil {
		return Envelope{}, err
	}
	var env Envelope
	if err := json.Unmarshal(data, &env); err != nil {
		return Envelope{}, err
	}
	return env, nil
}
