package agents

import (
	"encoding/json"
	"testing"
)

// The engine parses these envelopes field-by-field
// (mod_agenttransport.cc: op=="tool_call" / op=="usage"). If a key name drifts
// the report is silently dropped — no error, just missing analytics — so pin
// the exact wire shape here rather than trusting the call to "work".
func capture(t *testing.T) (*WebTransportSession, *[][]byte) {
	t.Helper()
	var sent [][]byte
	s := newSession(
		WebTransportConfig{CallID: 42, SampleRate: 8000, NumChannels: 1},
		func([]byte) {},
		func(b []byte) { sent = append(sent, append([]byte(nil), b...)) },
	)
	return s, &sent
}

func decodeOne(t *testing.T, sent [][]byte) CtrlMsg {
	t.Helper()
	if len(sent) != 1 {
		t.Fatalf("expected exactly 1 control envelope, got %d", len(sent))
	}
	msgs, rem := DecodeCtrlStream(sent[0])
	if len(msgs) != 1 || len(rem) != 0 {
		t.Fatalf("envelope did not decode cleanly: msgs=%d rem=%d", len(msgs), len(rem))
	}
	return msgs[0]
}

func TestSendToolCallWireShape(t *testing.T) {
	s, sent := capture(t)
	s.SendToolCall("lookup_order", map[string]any{"id": "A1"}, map[string]any{"status": "shipped"})
	m := decodeOne(t, *sent)
	if m.Op() != "tool_call" || m["tool"] != "lookup_order" || m["call_id"].(float64) != 42 {
		t.Fatalf("bad envelope: %v", m)
	}
	// args/result must be OBJECTS — the engine splices them in with jobj().
	for _, k := range []string{"args", "result"} {
		if _, ok := m[k].(map[string]any); !ok {
			t.Fatalf("%s must be a JSON object, got %T", k, m[k])
		}
	}
}

func TestSendUsageWireShape(t *testing.T) {
	s, sent := capture(t)
	s.SendUsage("llm", map[string]any{"prompt_tokens": 12, "completion_tokens": 30}, "openai", "gpt-4o-mini")
	m := decodeOne(t, *sent)
	if m.Op() != "usage" || m["stage"] != "llm" || m["provider"] != "openai" || m["model"] != "gpt-4o-mini" {
		t.Fatalf("bad envelope: %v", m)
	}
	f, ok := m["fields"].(map[string]any)
	if !ok || f["prompt_tokens"].(float64) != 12 {
		t.Fatalf("fields must be a JSON object with the numbers: %v", m["fields"])
	}
}

// An empty tool/stage is dropped engine-side, so don't spend a datagram on it.
func TestEmptyToolAndStageAreNotSent(t *testing.T) {
	s, sent := capture(t)
	s.SendToolCall("", nil, nil)
	s.SendUsage("", nil, "", "")
	if len(*sent) != 0 {
		t.Fatalf("expected nothing sent, got %d", len(*sent))
	}
}

// nil maps must still serialize as {} — the engine defaults empty to {}, but a
// JSON `null` is not an object and would be spliced in as a literal null.
func TestNilMapsSerializeAsObjects(t *testing.T) {
	s, sent := capture(t)
	s.SendToolCall("t", nil, nil)
	m := decodeOne(t, *sent)
	b, _ := json.Marshal(m["args"])
	if string(b) != "{}" {
		t.Fatalf("nil args must marshal to {}, got %s", b)
	}
}
