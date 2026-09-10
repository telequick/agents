// Package agents is the TeleQuick external-agent SDK for Go: run your own
// voice agent process against real phone calls over the TeleQuick QUIC media
// plane. One outbound QUIC/WebTransport session (no inbound ports), HMAC auth
// with your media app key/secret, one Call per routed phone call.
//
// It is the Go sibling of the Python `telequick-agents` and TypeScript
// `@telequick/agents` packages — same wire protocol, same handshake, same
// Serve(handler) shape.
package agents

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
)

// Wire protocol — byte-identical to the Python/TS packages and the engine.
//
//	MEDIA   — WebTransport datagrams: version(1) type(1) call_id(2,BE) seq(2,BE) | payload
//	CONTROL — one bidi stream of newline-delimited JSON envelopes {"op":...,...}
const (
	WireVersion byte = 1
	DgAudio     byte = 1
	headerLen        = 6
)

// Datagram is one media frame on the wire.
type Datagram struct {
	CallID  uint16
	Seq     uint16
	Payload []byte
	Type    byte
	Version byte
}

// MakeDatagram builds an audio datagram for callID/seq.
func MakeDatagram(callID, seq uint16, payload []byte) Datagram {
	return Datagram{CallID: callID, Seq: seq, Payload: payload, Type: DgAudio, Version: WireVersion}
}

// EncodeDatagram serializes the 6-byte header + payload.
func EncodeDatagram(dg Datagram) []byte {
	out := make([]byte, headerLen+len(dg.Payload))
	out[0] = dg.Version
	out[1] = dg.Type
	out[2] = byte(dg.CallID >> 8)
	out[3] = byte(dg.CallID)
	out[4] = byte(dg.Seq >> 8)
	out[5] = byte(dg.Seq)
	copy(out[headerLen:], dg.Payload)
	return out
}

// DecodeDatagram parses a datagram; the payload is a copy.
func DecodeDatagram(buf []byte) (Datagram, error) {
	if len(buf) < headerLen {
		return Datagram{}, fmt.Errorf("short datagram: %d bytes", len(buf))
	}
	if buf[0] != WireVersion {
		return Datagram{}, fmt.Errorf("unsupported wire version %d", buf[0])
	}
	payload := make([]byte, len(buf)-headerLen)
	copy(payload, buf[headerLen:])
	return Datagram{
		Version: buf[0],
		Type:    buf[1],
		CallID:  uint16(buf[2])<<8 | uint16(buf[3]),
		Seq:     uint16(buf[4])<<8 | uint16(buf[5]),
		Payload: payload,
	}, nil
}

// CtrlMsg is one decoded control envelope.
type CtrlMsg map[string]any

// Op returns the envelope's "op", or "" when absent.
func (m CtrlMsg) Op() string {
	s, _ := m["op"].(string)
	return s
}

// EncodeCtrl builds one newline-terminated control envelope. "op" is
// serialized first (matches the Python/TS encoders); the rest of the fields
// follow in encoding/json's key order.
func EncodeCtrl(op string, fields map[string]any) ([]byte, error) {
	var buf bytes.Buffer
	buf.WriteString(`{"op":`)
	opb, err := json.Marshal(op)
	if err != nil {
		return nil, err
	}
	buf.Write(opb)
	if len(fields) > 0 {
		fb, err := json.Marshal(fields)
		if err != nil {
			return nil, err
		}
		buf.WriteByte(',')
		buf.Write(fb[1:]) // drop the leading '{' — fb already ends with '}'
	} else {
		buf.WriteByte('}')
	}
	buf.WriteByte('\n')
	return buf.Bytes(), nil
}

// DecodeCtrlStream parses whole envelopes from a growing byte buffer and
// returns them plus the unconsumed remainder (a partial trailing line to be
// prepended to the next read). Robust to envelopes split across QUIC STREAM
// frames. A malformed line is skipped, not fatal.
func DecodeCtrlStream(buf []byte) ([]CtrlMsg, []byte) {
	var msgs []CtrlMsg
	for {
		nl := bytes.IndexByte(buf, '\n')
		if nl < 0 {
			break
		}
		line := bytes.TrimSpace(buf[:nl])
		buf = buf[nl+1:]
		if len(line) == 0 {
			continue
		}
		var m CtrlMsg
		if err := json.Unmarshal(line, &m); err == nil {
			msgs = append(msgs, m)
		}
	}
	rem := make([]byte, len(buf))
	copy(rem, buf)
	return msgs, rem
}

var errShort = errors.New("short read")
