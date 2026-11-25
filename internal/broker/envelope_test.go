package broker_test

import (
	"encoding/json"
	"os"
	"strings"
	"testing"

	"github.com/dahai80/fusion-ops/internal/broker"
)

// Golden contract fixture mirrors the fusion-link Rust protocol shape
// (protocol.rs: ts u64 epoch millis, seq u64 — issue #12). The broker must
// unmarshal it verbatim; CI in this repo guards the Go side.
func TestEnvelopeGoldenFusionLinkFixture(t *testing.T) {
	raw, err := os.ReadFile("testdata/golden_envelope.json")
	if err != nil {
		t.Fatalf("read fixture: %v", err)
	}
	var env broker.Envelope
	if err := json.Unmarshal(raw, &env); err != nil {
		t.Fatalf("numeric-ts envelope rejected: %v", err)
	}
	if env.Type != "hello" || env.DeviceID != "dev-golden" || env.Seq != 7 {
		t.Fatalf("fields wrong: %+v", env)
	}
	if env.TS != "1732512000123" {
		t.Fatalf("ts expected normalized digits, got %q", env.TS)
	}
	var hello broker.HelloPayload
	if err := json.Unmarshal(env.Payload, &hello); err != nil {
		t.Fatalf("payload: %v", err)
	}
	if hello.Hostname != "mac-local" || hello.Platform != "darwin/arm64" || hello.Token != "golden-token" {
		t.Fatalf("hello payload wrong: %+v", hello)
	}

	// Round-trip: pure digits marshal back as a bare JSON number.
	out, err := json.Marshal(env)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if !strings.Contains(string(out), `"ts":1732512000123`) {
		t.Fatalf("ts must stay numeric on the wire: %s", out)
	}
}

func TestEnvelopeTSFlexibility(t *testing.T) {
	for name, in := range map[string]string{
		"numeric": `{"v":1,"type":"heartbeat","ts":1732512000999,"device_id":"d","seq":0,"payload":{}}`,
		"string":  `{"v":1,"type":"heartbeat","ts":"2026-11-25T07:00:00Z","device_id":"d","seq":0,"payload":{}}`,
		"missing": `{"v":1,"type":"heartbeat","device_id":"d","seq":0,"payload":{}}`,
	} {
		var env broker.Envelope
		if err := json.Unmarshal([]byte(in), &env); err != nil {
			t.Fatalf("%s ts rejected: %v", name, err)
		}
	}

	// String ts round-trips as a string; empty ts marshals as 0.
	out, err := json.Marshal(broker.Envelope{V: 1, Type: "hello", TS: "2026-11-25T07:00:00Z"})
	if err != nil || !strings.Contains(string(out), `"ts":"2026-11-25T07:00:00Z"`) {
		t.Fatalf("string ts must stay quoted: %s err=%v", out, err)
	}
	out, _ = json.Marshal(broker.Envelope{V: 1, Type: "hello"})
	if !strings.Contains(string(out), `"ts":0`) {
		t.Fatalf("empty ts must marshal as 0: %s", out)
	}
}
