package agents

import (
	"bytes"
	"testing"
)

func TestDatagramRoundTrip(t *testing.T) {
	p := []byte{1, 2, 3, 4, 5}
	enc := EncodeDatagram(MakeDatagram(0x1234, 0xabcd, p))
	if len(enc) != 6+len(p) {
		t.Fatalf("len=%d", len(enc))
	}
	// header bytes are big-endian, matching the Python/TS encoders
	if !bytes.Equal(enc[:6], []byte{1, 1, 0x12, 0x34, 0xab, 0xcd}) {
		t.Fatalf("header=%x", enc[:6])
	}
	dg, err := DecodeDatagram(enc)
	if err != nil || dg.CallID != 0x1234 || dg.Seq != 0xabcd || !bytes.Equal(dg.Payload, p) {
		t.Fatalf("decode: %+v err=%v", dg, err)
	}
	if _, err := DecodeDatagram([]byte{1, 1, 0}); err == nil {
		t.Fatal("short datagram accepted")
	}
	if _, err := DecodeDatagram([]byte{9, 1, 0, 0, 0, 0}); err == nil {
		t.Fatal("bad version accepted")
	}
}

func TestCtrlEncodeOpFirstAndSplitStream(t *testing.T) {
	b, err := EncodeCtrl("job_assign", map[string]any{"call_id": 7, "caller": "+1"})
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.HasPrefix(b, []byte(`{"op":"job_assign",`)) || !bytes.HasSuffix(b, []byte("}\n")) {
		t.Fatalf("envelope=%q", b)
	}
	// bare op
	b2, _ := EncodeCtrl("ping", nil)
	if string(b2) != "{\"op\":\"ping\"}\n" {
		t.Fatalf("bare=%q", b2)
	}
	// split across two reads
	m1, rem := DecodeCtrlStream(b[:10])
	if len(m1) != 0 || len(rem) != 10 {
		t.Fatalf("partial: msgs=%d rem=%d", len(m1), len(rem))
	}
	m2, rem2 := DecodeCtrlStream(append(rem, b[10:]...))
	if len(m2) != 1 || m2[0].Op() != "job_assign" || m2[0]["call_id"].(float64) != 7 || len(rem2) != 0 {
		t.Fatalf("joined: %+v rem=%d", m2, len(rem2))
	}
}
