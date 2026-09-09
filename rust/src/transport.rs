//! One call's media over the shared QUIC session.
//!
//! Caller audio arrives inbound via datagrams as 20 ms pcm16 frames. Agent
//! audio (your TTS) goes out through a REAL-TIME PACED sender: TTS arrives
//! bursty in arbitrary chunk sizes, but the caller leg needs a steady
//! 8 kHz / 20 ms stream, so `send_frame` buffers and a 20 ms pacer emits
//! fixed frames. The wire codec is raw pcm16 passthrough — resample to
//! `sample_rate` before sending.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use serde_json::{Map, Value};
use tokio::sync::mpsc;

use crate::wire::{self, CtrlMsg};

/// Everything one media session needs.
#[derive(Debug, Clone)]
pub struct WebTransportConfig {
    pub engine_url: String, // e.g. "https://engine.telequick.dev:443"
    pub app_key: String,
    pub app_secret: String,
    pub agent_name: String,
    pub call_id: u16,
    pub sample_rate: u32, // wire rate — 8000 for telephony
    pub num_channels: u16,
}

/// The engine's per-segment playout feedback.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct PlayoutReport {
    pub playback_position: f64,
    pub interrupted: bool,
}

/// A fire-and-forget byte sink (datagram or control stream) provided by the
/// QUIC layer.
pub type Sender = Arc<dyn Fn(Vec<u8>) + Send + Sync>;

struct OutState {
    out_buf: Vec<u8>,
    tx_seq: u16,
    pacer_started: bool,
    closed: bool,
}

pub struct WebTransportSession {
    cfg: WebTransportConfig,
    send_datagram: Sender,
    send_stream: Sender,
    inbound_tx: Mutex<Option<mpsc::Sender<Vec<u8>>>>,
    inbound_rx: tokio::sync::Mutex<mpsc::Receiver<Vec<u8>>>,
    playout_tx: mpsc::Sender<PlayoutReport>,
    playout_rx: tokio::sync::Mutex<mpsc::Receiver<PlayoutReport>>,
    out: Mutex<OutState>,
    frame_bytes: usize,
    ended: AtomicBool,
}

impl WebTransportSession {
    pub fn new(cfg: WebTransportConfig, send_datagram: Sender, send_stream: Sender) -> Arc<Self> {
        let ch = cfg.num_channels.max(1) as usize;
        let frame_bytes = (cfg.sample_rate as usize / 50).max(1) * 2 * ch; // 20 ms of pcm16
        let (itx, irx) = mpsc::channel(256);
        let (ptx, prx) = mpsc::channel(16);
        Arc::new(Self {
            cfg,
            send_datagram,
            send_stream,
            inbound_tx: Mutex::new(Some(itx)),
            inbound_rx: tokio::sync::Mutex::new(irx),
            playout_tx: ptx,
            playout_rx: tokio::sync::Mutex::new(prx),
            out: Mutex::new(OutState { out_buf: Vec::new(), tx_seq: 0, pacer_started: false, closed: false }),
            frame_bytes,
            ended: AtomicBool::new(false),
        })
    }

    pub fn sample_rate(&self) -> u32 {
        self.cfg.sample_rate
    }

    pub fn closed(&self) -> bool {
        self.out.lock().unwrap().closed
    }

    // -- fed by the connection readers -----------------------------------

    pub(crate) fn on_datagram(&self, buf: &[u8]) {
        let Ok(dg) = wire::decode_datagram(buf) else { return };
        if dg.call_id != self.cfg.call_id || dg.kind != wire::DG_AUDIO {
            return;
        }
        if let Some(tx) = self.inbound_tx.lock().unwrap().as_ref() {
            let _ = tx.try_send(dg.payload); // receiver lagging: drop 20 ms rather than stall
        }
    }

    pub(crate) fn on_control(&self, msg: &CtrlMsg) {
        if let Some(cid) = msg.get("call_id").and_then(Value::as_u64) {
            if cid as u16 != self.cfg.call_id {
                return;
            }
        }
        match wire::ctrl_op(msg) {
            "playout" => {
                let _ = self.playout_tx.try_send(PlayoutReport {
                    playback_position: msg.get("position").and_then(Value::as_f64).unwrap_or(0.0),
                    interrupted: msg.get("interrupted").and_then(Value::as_bool).unwrap_or(false),
                });
            }
            "shutdown" => self.end_inbound(),
            _ => {}
        }
    }

    fn end_inbound(&self) {
        self.ended.store(true, Ordering::SeqCst);
        self.inbound_tx.lock().unwrap().take(); // dropping the sender ends recv_frame with None
    }

    // -- media -----------------------------------------------------------

    /// Next caller frame (pcm16), or `None` once the call ends.
    pub async fn recv_frame(&self) -> Option<Vec<u8>> {
        self.inbound_rx.lock().await.recv().await
    }

    /// Queue agent audio (pcm16 @ sample_rate); paced onto the wire as 20 ms datagrams.
    pub fn send_frame(self: &Arc<Self>, pcm: &[u8]) {
        let mut st = self.out.lock().unwrap();
        if st.closed {
            return;
        }
        st.out_buf.extend_from_slice(pcm);
        if !st.pacer_started {
            st.pacer_started = true;
            let me = Arc::clone(self);
            tokio::spawn(async move {
                let mut tick = tokio::time::interval(Duration::from_millis(20));
                loop {
                    tick.tick().await;
                    if !me.drain_one() {
                        return;
                    }
                }
            });
        }
    }

    /// Emit one paced frame if available. Returns false once the session closed.
    fn drain_one(&self) -> bool {
        let frame = {
            let mut st = self.out.lock().unwrap();
            if st.closed {
                return false;
            }
            if st.out_buf.len() < self.frame_bytes {
                return true;
            }
            let chunk: Vec<u8> = st.out_buf.drain(..self.frame_bytes).collect();
            let seq = st.tx_seq;
            st.tx_seq = st.tx_seq.wrapping_add(1);
            wire::encode_datagram(&wire::make_datagram(self.cfg.call_id, seq, chunk))
        };
        (self.send_datagram)(frame);
        true
    }

    fn ctrl(&self, op: &str, mut fields: Map<String, Value>) {
        fields.insert("call_id".into(), self.cfg.call_id.into());
        (self.send_stream)(wire::encode_ctrl(op, fields));
    }

    /// Mark the current outbound segment complete (enables playout reports).
    pub fn signal_flush(&self) {
        self.ctrl("flush", Map::new());
    }

    /// Barge-in: drop buffered agent audio so playout stops immediately.
    pub fn signal_clear(&self) {
        self.out.lock().unwrap().out_buf.clear();
        self.ctrl("clear", Map::new());
    }

    /// Forward a turn ("user"/"assistant") to platform transcript storage,
    /// analytics and `voice.transcript.ready` webhooks.
    pub fn send_transcript(&self, role: &str, text: &str, is_final: bool) {
        let mut f = Map::new();
        f.insert("role".into(), role.into());
        f.insert("text".into(), text.into());
        f.insert("is_final".into(), is_final.into());
        self.ctrl("transcript", f);
    }

    /// Report a tool this agent invoked, so the platform logs it exactly as it
    /// logs a native agent's (analytics timeline + `voice.agent.tool_called`
    /// webhooks). Your tool loop is invisible to the engine — nothing is
    /// recorded unless you call this.
    ///
    /// REDACT SENSITIVE VALUES YOURSELF: `args`/`result` are forwarded verbatim
    /// as opaque JSON. An empty `tool` is dropped engine-side.
    pub fn send_tool_call(&self, tool: &str, args: Value, result: Value) {
        if tool.is_empty() {
            return;
        }
        let mut f = Map::new();
        f.insert("tool".into(), tool.into());
        f.insert("args".into(), args);
        f.insert("result".into(), result);
        self.ctrl("tool_call", f);
    }

    /// Report per-stage usage for token analytics. You run STT/LLM/TTS on your
    /// OWN provider keys, so the platform only sees what you forward.
    ///
    /// `stage` is `"stt"`/`"llm"`/`"tts"`; `fields` carries the numbers (llm:
    /// `{"prompt_tokens":..,"completion_tokens":..}`, stt: `{"audio_seconds":..}`,
    /// tts: `{"characters":..}`). An empty `stage` is dropped engine-side.
    pub fn send_usage(&self, stage: &str, fields: Value, provider: &str, model: &str) {
        if stage.is_empty() {
            return;
        }
        let mut f = Map::new();
        f.insert("stage".into(), stage.into());
        f.insert("provider".into(), provider.into());
        f.insert("model".into(), model.into());
        f.insert("fields".into(), fields);
        self.ctrl("usage", f);
    }

    /// Wait for the next playout report.
    pub async fn next_playout(&self) -> Option<PlayoutReport> {
        self.playout_rx.lock().await.recv().await
    }

    /// End the call from the agent side.
    pub fn close(&self) {
        {
            let mut st = self.out.lock().unwrap();
            if st.closed {
                return;
            }
            st.closed = true;
        }
        self.end_inbound();
        self.ctrl("shutdown", Map::new());
    }
}
