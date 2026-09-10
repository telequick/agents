package agents

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"
)

// AuthError is a rejected or malformed handshake.
type AuthError struct {
	Code int // 4001 unknown key, 4009 bad signature, 0 = protocol/timeout
	Msg  string
}

func (e *AuthError) Error() string { return fmt.Sprintf("auth failed (code=%d): %s", e.Code, e.Msg) }

// JobAssign is engine → agent: a call has been routed to this worker.
type JobAssign struct {
	CallID       uint16
	RoomName     string
	AgentID      string
	CallerNumber string // ANI
	CalledNumber string // DNIS
	TrunkID      string
	Attributes   map[string]any
	// Operator-defined context KVs from the agent config.
	Metadata map[string]string
	// The agent's effective resource tags (key/value, inherited down the
	// platform's parent chain) — governed cost-allocation / ownership /
	// environment labels. Distinct from Metadata (free-form context).
	Tags map[string]string
}

func stringMap(v any) map[string]string {
	out := map[string]string{}
	if m, ok := v.(map[string]any); ok {
		for k, val := range m {
			out[k] = fmt.Sprint(val)
		}
	}
	return out
}

func str(v any) string {
	s, _ := v.(string)
	return s
}

type assignPair struct {
	assign JobAssign
	sess   *WebTransportSession
}

// MediaConnection is one authenticated WebTransport/QUIC session per agent.
// It owns the control stream + datagram plane, runs the auth handshake BEFORE
// any media flows, and demuxes concurrent calls by call_id.
type MediaConnection struct {
	cfg          WebTransportConfig
	sendDatagram func([]byte)
	sendStream   func([]byte)

	ctrlIn  chan CtrlMsg
	assigns chan assignPair

	mu       sync.RWMutex
	authed   bool
	socketID string
	calls    map[uint16]*WebTransportSession
}

func newMediaConnection(cfg WebTransportConfig, sendDatagram, sendStream func([]byte)) *MediaConnection {
	return &MediaConnection{
		cfg:          cfg,
		sendDatagram: sendDatagram,
		sendStream:   sendStream,
		ctrlIn:       make(chan CtrlMsg, 16),
		assigns:      make(chan assignPair, 16),
		calls:        map[uint16]*WebTransportSession{},
	}
}

// Authenticated reports whether the handshake completed.
func (m *MediaConnection) Authenticated() bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.authed
}

// -- fed by the connection reader ------------------------------------

func (m *MediaConnection) onControl(msg CtrlMsg) {
	if !m.Authenticated() {
		select {
		case m.ctrlIn <- msg: // welcome / ready / error → the handshake
		default:
		}
		return
	}
	if msg.Op() == "job_assign" {
		m.onJobAssign(msg)
		return
	}
	cid, ok := msg["call_id"].(float64)
	if !ok {
		return
	}
	m.mu.RLock()
	sess := m.calls[uint16(cid)]
	m.mu.RUnlock()
	if sess != nil {
		sess.onControl(msg)
	}
}

func (m *MediaConnection) onDatagram(buf []byte) {
	if !m.Authenticated() {
		return // never accept media before auth
	}
	dg, err := DecodeDatagram(buf)
	if err != nil {
		return
	}
	m.mu.RLock()
	sess := m.calls[dg.CallID]
	m.mu.RUnlock()
	if sess != nil {
		sess.onDatagram(buf)
	}
}

// -- handshake: hello -> welcome -> auth -> ready --------------------

func (m *MediaConnection) send(op string, fields map[string]any) error {
	b, err := EncodeCtrl(op, fields)
	if err != nil {
		return err
	}
	m.sendStream(b)
	return nil
}

func (m *MediaConnection) await1(timeout time.Duration) (CtrlMsg, error) {
	select {
	case msg := <-m.ctrlIn:
		return msg, nil
	case <-time.After(timeout):
		return nil, &AuthError{Msg: "handshake timeout"}
	}
}

// Authenticate runs the handshake; it must complete before NextCall.
func (m *MediaConnection) Authenticate(timeout time.Duration) error {
	if err := m.send("hello", map[string]any{"app_key": m.cfg.AppKey}); err != nil {
		return err
	}
	welcome, err := m.await1(timeout)
	if err != nil {
		return err
	}
	sid, ok := welcome["socket_id"].(string)
	if welcome.Op() != "welcome" || !ok {
		return &AuthError{Msg: fmt.Sprintf("expected welcome, got %v", welcome)}
	}
	m.mu.Lock()
	m.socketID = sid
	m.mu.Unlock()
	if err := m.send("auth", map[string]any{
		"app_key":      m.cfg.AppKey,
		"channel":      MediaChannel,
		"channel_data": m.cfg.AgentName,
		"auth":         MediaAuth(m.cfg.AppKey, m.cfg.AppSecret, sid, m.cfg.AgentName),
	}); err != nil {
		return err
	}
	resp, err := m.await1(timeout)
	if err != nil {
		return err
	}
	if resp.Op() != "ready" {
		code, _ := resp["code"].(float64)
		return &AuthError{Code: int(code), Msg: resp.Op()}
	}
	m.mu.Lock()
	m.authed = true
	m.mu.Unlock()
	return nil
}

// -- per-call sessions ----------------------------------------------

func (m *MediaConnection) onJobAssign(msg CtrlMsg) {
	cidf, _ := msg["call_id"].(float64)
	cid := uint16(cidf)
	cfg := m.cfg
	cfg.CallID = cid
	sess := newSession(cfg, m.sendDatagram, m.sendStream)
	m.mu.Lock()
	m.calls[cid] = sess
	m.mu.Unlock()
	attrs, _ := msg["attrs"].(map[string]any)
	if attrs == nil {
		attrs = map[string]any{}
	}
	assign := JobAssign{
		CallID:       cid,
		RoomName:     str(msg["room"]),
		AgentID:      str(msg["agent_id"]),
		CallerNumber: str(msg["caller"]),
		CalledNumber: str(msg["called"]),
		TrunkID:      str(msg["trunk_id"]),
		Attributes:   attrs,
		Metadata:     stringMap(msg["metadata"]),
		Tags:         stringMap(msg["tags"]),
	}
	select {
	case m.assigns <- assignPair{assign, sess}:
	default:
	}
}

// NextCall blocks until the engine routes a call to this worker.
func (m *MediaConnection) NextCall(ctx context.Context) (JobAssign, *WebTransportSession, error) {
	select {
	case p := <-m.assigns:
		return p.assign, p.sess, nil
	case <-ctx.Done():
		return JobAssign{}, nil, ctx.Err()
	}
}

// Ping is a keepalive over the control stream (the engine ignores the op).
func (m *MediaConnection) Ping() error { return m.send("ping", nil) }

var errClosed = errors.New("engine connection closed")
