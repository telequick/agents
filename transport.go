package agents

import (
	"sync"
	"time"
)

// WebTransportConfig is everything one media session needs.
type WebTransportConfig struct {
	EngineURL   string // e.g. "https://engine.telequick.dev:443"
	AppKey      string
	AppSecret   string
	AgentName   string
	CallID      uint16
	SampleRate  int // wire rate — 8000 for telephony
	NumChannels int
}

// PlayoutReport is the engine's per-segment playout feedback.
type PlayoutReport struct {
	PlaybackPosition float64
	Interrupted      bool
}

// WebTransportSession is one call's media over the shared QUIC session.
//
// Caller audio arrives inbound via datagrams as 20 ms pcm16 frames. Agent
// audio (your TTS) goes out through a REAL-TIME PACED sender: TTS arrives
// bursty in arbitrary chunk sizes, but the caller leg needs a steady
// 8 kHz / 20 ms stream, so SendFrame buffers and a 20 ms pacer emits fixed
// frames. The wire codec is raw pcm16 passthrough — resample to SampleRate
// before sending.
type WebTransportSession struct {
	cfg          WebTransportConfig
	sendDatagram func([]byte)
	sendStream   func([]byte)

	inbound chan []byte // closed on shutdown
	playout chan PlayoutReport
	closeIn sync.Once

	mu         sync.Mutex
	outBuf     []byte
	txSeq      uint16
	frameBytes int
	pacer      *time.Ticker
	pacerDone  chan struct{}
	closed     bool
}

func newSession(cfg WebTransportConfig, sendDatagram, sendStream func([]byte)) *WebTransportSession {
	ch := cfg.NumChannels
	if ch <= 0 {
		ch = 1
	}
	fb := cfg.SampleRate / 50 // 20 ms of samples
	if fb < 1 {
		fb = 1
	}
	return &WebTransportSession{
		cfg:          cfg,
		sendDatagram: sendDatagram,
		sendStream:   sendStream,
		inbound:      make(chan []byte, 256),
		playout:      make(chan PlayoutReport, 16),
		frameBytes:   fb * 2 * ch,
		pacerDone:    make(chan struct{}),
	}
}

// SampleRate is the wire sample rate for this call.
func (s *WebTransportSession) SampleRate() int { return s.cfg.SampleRate }

// Closed reports whether the session has been shut down.
func (s *WebTransportSession) Closed() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.closed
}

// -- fed by the connection readers -------------------------------------

func (s *WebTransportSession) onDatagram(buf []byte) {
	dg, err := DecodeDatagram(buf)
	if err != nil || dg.CallID != s.cfg.CallID || dg.Type != DgAudio {
		return
	}
	select {
	case s.inbound <- dg.Payload:
	default: // receiver is not keeping up — a dropped 20 ms is better than a stall
	}
}

func (s *WebTransportSession) onControl(msg CtrlMsg) {
	if cid, ok := msg["call_id"].(float64); ok && uint16(cid) != s.cfg.CallID {
		return
	}
	switch msg.Op() {
	case "playout":
		pos, _ := msg["position"].(float64)
		intr, _ := msg["interrupted"].(bool)
		select {
		case s.playout <- PlayoutReport{PlaybackPosition: pos, Interrupted: intr}:
		default:
		}
	case "shutdown":
		s.closeIn.Do(func() { close(s.inbound) })
	}
}

// -- media -------------------------------------------------------------

// RecvFrame returns the next caller frame (pcm16); ok=false once the call ends.
func (s *WebTransportSession) RecvFrame() (pcm []byte, ok bool) {
	pcm, ok = <-s.inbound
	return
}

// SendFrame queues agent audio (pcm16 @ SampleRate); it is paced onto the
// wire as 20 ms datagrams.
func (s *WebTransportSession) SendFrame(pcm []byte) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return
	}
	s.outBuf = append(s.outBuf, pcm...)
	if s.pacer == nil {
		s.pacer = time.NewTicker(20 * time.Millisecond)
		go s.drainLoop(s.pacer)
	}
}

func (s *WebTransportSession) drainLoop(t *time.Ticker) {
	for {
		select {
		case <-t.C:
			s.drainOne()
		case <-s.pacerDone:
			return
		}
	}
}

func (s *WebTransportSession) drainOne() {
	s.mu.Lock()
	if s.closed || len(s.outBuf) < s.frameBytes {
		s.mu.Unlock()
		return
	}
	chunk := make([]byte, s.frameBytes)
	copy(chunk, s.outBuf[:s.frameBytes])
	s.outBuf = s.outBuf[s.frameBytes:]
	dg := MakeDatagram(s.cfg.CallID, s.txSeq, chunk)
	s.txSeq++
	s.mu.Unlock()
	s.sendDatagram(EncodeDatagram(dg))
}

func (s *WebTransportSession) ctrl(op string, fields map[string]any) {
	if fields == nil {
		fields = map[string]any{}
	}
	fields["call_id"] = s.cfg.CallID
	if b, err := EncodeCtrl(op, fields); err == nil {
		s.sendStream(b)
	}
}

// SignalFlush marks the current outbound segment complete (enables playout reports).
func (s *WebTransportSession) SignalFlush() { s.ctrl("flush", nil) }

// SignalClear is barge-in: drop buffered agent audio so playout stops immediately.
func (s *WebTransportSession) SignalClear() {
	s.mu.Lock()
	s.outBuf = s.outBuf[:0]
	s.mu.Unlock()
	s.ctrl("clear", nil)
}

// SendTranscript forwards a turn ("user"/"assistant") to platform transcript
// storage, analytics and voice.transcript.ready webhooks.
func (s *WebTransportSession) SendTranscript(role, text string, isFinal bool) {
	s.ctrl("transcript", map[string]any{"role": role, "text": text, "is_final": isFinal})
}

// SendToolCall reports a tool this agent invoked, so the platform logs it
// exactly as it logs a native agent's (analytics timeline +
// voice.agent.tool_called webhooks). Your tool loop is invisible to the engine
// — nothing is recorded unless you call this.
//
// REDACT SENSITIVE VALUES YOURSELF: args/result are forwarded verbatim as
// opaque JSON. An empty tool is dropped engine-side.
func (s *WebTransportSession) SendToolCall(tool string, args, result map[string]any) {
	if tool == "" {
		return
	}
	if args == nil {
		args = map[string]any{}
	}
	if result == nil {
		result = map[string]any{}
	}
	s.ctrl("tool_call", map[string]any{"tool": tool, "args": args, "result": result})
}

// SendUsage reports per-stage usage for token analytics. You run STT/LLM/TTS on
// your OWN provider keys, so the platform only sees what you forward.
//
// stage is "stt"/"llm"/"tts"; fields carries the numbers (llm:
// {"prompt_tokens":…,"completion_tokens":…}, stt: {"audio_seconds":…}, tts:
// {"characters":…}). An empty stage is dropped engine-side.
func (s *WebTransportSession) SendUsage(stage string, fields map[string]any, provider, model string) {
	if stage == "" {
		return
	}
	if fields == nil {
		fields = map[string]any{}
	}
	s.ctrl("usage", map[string]any{"stage": stage, "provider": provider, "model": model, "fields": fields})
}

// NextPlayout blocks for the next playout report.
func (s *WebTransportSession) NextPlayout() PlayoutReport { return <-s.playout }

// Close ends the call from the agent side.
func (s *WebTransportSession) Close() {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return
	}
	s.closed = true
	if s.pacer != nil {
		s.pacer.Stop()
		close(s.pacerDone)
	}
	s.mu.Unlock()
	s.closeIn.Do(func() { close(s.inbound) })
	s.ctrl("shutdown", nil)
}
