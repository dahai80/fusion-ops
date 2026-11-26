package broker

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"strconv"
	"time"
)

// FlexString accepts either a JSON string or a JSON number and normalizes
// to a string. It exists because the fusion-link Rust daemon sends `ts` as
// numeric epoch millis while early Go drafts used RFC3339 strings — the
// broker (the hub serving every daemon) must accept both (issue #12).
type FlexString string

func (f *FlexString) UnmarshalJSON(b []byte) error {
	if len(b) > 0 && b[0] == '"' {
		var s string
		if err := json.Unmarshal(b, &s); err != nil {
			return err
		}
		*f = FlexString(s)
		return nil
	}
	var n json.Number
	if err := json.Unmarshal(b, &n); err != nil {
		return err
	}
	*f = FlexString(n.String())
	return nil
}

// MarshalJSON emits pure-digit values as bare JSON numbers (wire type the
// Rust client's `ts: u64` expects) and everything else as a string.
func (f FlexString) MarshalJSON() ([]byte, error) {
	v := string(f)
	if v == "" {
		return []byte(`0`), nil
	}
	for _, r := range v {
		if r < '0' || r > '9' {
			return json.Marshal(v)
		}
	}
	return []byte(v), nil
}

// Envelope is the fusion-link WSS message envelope (PRD §7): all messages
// are JSON, ack + seq guarantee reliable ordered delivery. ts is numeric
// epoch millis on the wire (PRD §7 pinned in fusion-ops#12); the Go side
// accepts both numbers and strings for compatibility with early drafts.
type Envelope struct {
	V        int             `json:"v"`
	Type     string          `json:"type"`
	TS       FlexString      `json:"ts"`
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
	TypeAck        = "ack"
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
// The fusion-link daemon emits the state-machine position as `state`
// (task.event/task.result) with `summary` + `artifact_refs` on task.result;
// early Go drafts used `event` / `detail` / `steps`. The broker accepts
// both spellings and normalizes to one read model (issue #20).
type TaskPayload struct {
	TaskID       string          `json:"task_id"`
	Type         string          `json:"type"`
	Params       map[string]any  `json:"params,omitempty"`
	Event        string          `json:"event,omitempty"` // task.event state machine event (Go draft)
	State        string          `json:"state,omitempty"` // daemon field name (fusion-link TaskState)
	Detail       string          `json:"detail,omitempty"`
	Summary      string          `json:"summary,omitempty"` // task.result summary (fusion-link)
	Steps        json.RawMessage `json:"steps,omitempty"`   // best-effort progress (issue #11)
	ArtifactRefs json.RawMessage `json:"artifact_refs,omitempty"`
	ErrorCode    string          `json:"code,omitempty"` // error frame fields
	ErrorMessage string          `json:"message,omitempty"`
}

// UplinkEvent returns the state-machine event regardless of which wire
// field the sender used.
func (p TaskPayload) UplinkEvent() string {
	if p.Event != "" {
		return p.Event
	}
	return p.State
}

// UplinkDetail returns the human-readable reason regardless of which wire
// field the sender used.
func (p TaskPayload) UplinkDetail() string {
	if p.Detail != "" {
		return p.Detail
	}
	return p.Summary
}

// UplinkSteps returns the best-effort steps blob: explicit steps when sent,
// otherwise artifact_refs from a task.result frame (issue #20).
func (p TaskPayload) UplinkSteps() json.RawMessage {
	if len(p.Steps) > 0 {
		return p.Steps
	}
	return p.ArtifactRefs
}

// ErrorPayload is the error frame (both directions).
type ErrorPayload struct {
	Code string `json:"code"`
	// Detail is the unified wire field; Message accepts the PRD-draft name.
	Detail  string `json:"detail,omitempty"`
	Message string `json:"message,omitempty"`
}

// EffectiveDetail returns the human-readable reason regardless of which
// field name the sender used.
func (p ErrorPayload) EffectiveDetail() string {
	if p.Detail != "" {
		return p.Detail
	}
	return p.Message
}

// AckPayload rides the ack frame (both directions).
type AckPayload struct {
	AckSeq int64 `json:"ack_seq"`
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
	// ts = epoch millis (numeric on the wire via FlexString.MarshalJSON).
	return Envelope{V: 1, Type: typ, TS: FlexString(strconv.FormatInt(time.Now().UnixMilli(), 10)), DeviceID: deviceID, Payload: raw}, nil
}
