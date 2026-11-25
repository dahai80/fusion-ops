package broker

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"time"
)

// Envelope is the fusion-link WSS message envelope (PRD §7): all messages
// are JSON, ack + seq guarantee reliable ordered delivery.
type Envelope struct {
	V        int             `json:"v"`
	Type     string          `json:"type"`
	TS       string          `json:"ts"`
	DeviceID string          `json:"device_id"`
	Seq      int64           `json:"seq"`
	Payload  json.RawMessage `json:"payload"`
}

// Message types (PRD §7).
const (
	TypeHello      = "hello"
	TypeHelloAck   = "hello_ack"
	TypeHeartbeat  = "heartbeat"
	TypeDispatch   = "task.dispatch"
	TypeTaskEvent  = "task.event"
	TypeTaskResult = "task.result"
	TypeError      = "error"
)

// Error codes (PRD §7).
const (
	ErrAuth        = "E_AUTH"
	ErrOffline     = "E_OFFLINE"
	ErrUnsupported = "E_UNSUPPORTED"
)

// HelloPayload carries bound credentials + device description (hello, up).
type HelloPayload struct {
	Token        string   `json:"token"`
	Version      string   `json:"version"`
	Capabilities []string `json:"capabilities"`
	Hostname     string   `json:"hostname,omitempty"` // console device table (issue #11)
	Platform     string   `json:"platform,omitempty"`
}

// HeartbeatPayload is the health snapshot (heartbeat, up). Contents opaque
// to the broker; heartbeat updates last_seen only and never enters
// ops_events (issue #10: heartbeat-level normal events must NOT enter the
// event stream).
type HeartbeatPayload struct {
	Health map[string]any `json:"health"`
}

// TaskPayload rides task.dispatch (down) and task.result / task.event (up).
type TaskPayload struct {
	TaskID       string          `json:"task_id"`
	Type         string          `json:"type"`
	Params       map[string]any  `json:"params,omitempty"`
	Event        string          `json:"event,omitempty"` // task.event state machine event
	Detail       string          `json:"detail,omitempty"`
	Steps        json.RawMessage `json:"steps,omitempty"` // best-effort progress (issue #11)
	ErrorCode    string          `json:"code,omitempty"`  // error frame fields
	ErrorMessage string          `json:"message,omitempty"`
}

// ErrorPayload is the error frame (both directions).
type ErrorPayload struct {
	Code   string `json:"code"`
	Detail string `json:"detail"`
}

// TokenHash derives the persisted credential digest (raw tokens never
// stored in device_registry, issue #10).
func TokenHash(token string) string {
	sum := sha256.Sum256([]byte(token))
	return hex.EncodeToString(sum[:])
}

func newEnvelope(typ, deviceID string, payload any) (Envelope, error) {
	raw, err := json.Marshal(payload)
	if err != nil {
		return Envelope{}, err
	}
	return Envelope{V: 1, Type: typ, TS: time.Now().UTC().Format(time.RFC3339), DeviceID: deviceID, Payload: raw}, nil
}
