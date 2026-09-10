//! High-level external-agent API: register-and-wait, handle calls.
//!
//! Your process dials ONE outbound QUIC/WebTransport session to the engine,
//! authenticates with your media app key/secret, registers under
//! `agent_name`, and gets a [`Call`] for every phone call the platform routes
//! to it. Route a number to the agent by pointing a platform agent's
//! `VENDOR_BRIDGE` node (`vendor_room = agent_name`) at it — the console's
//! "External agent" create flow does this and mints the media key in one step.

use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use serde_json::{Map, Value};

use crate::connection::JobAssign;
use crate::quic::open_media_connection;
use crate::transport::{PlayoutReport, WebTransportConfig, WebTransportSession};

/// Everything the worker needs to reach the engine and identify itself.
#[derive(Debug, Clone)]
pub struct AgentConfig {
    pub host: String,
    pub app_key: String,
    pub app_secret: String,
    pub agent_name: String,
    pub port: u16,        // default 443
    pub sample_rate: u32, // default 8000 — PSTN legs are 8 kHz
    pub verify: bool,     // default true; false skips engine cert validation (dev only)
}

#[derive(Debug, thiserror::Error)]
#[error("AgentConfig missing {0:?} — set TELEQUICK_HOST / TELEQUICK_MEDIA_KEY / TELEQUICK_MEDIA_SECRET / TELEQUICK_AGENT or pass them explicitly")]
pub struct ConfigError(Vec<&'static str>);

impl AgentConfig {
    /// Build from `TELEQUICK_HOST` / `TELEQUICK_MEDIA_KEY` / `TELEQUICK_MEDIA_SECRET` /
    /// `TELEQUICK_AGENT` (and optional `TELEQUICK_PORT`).
    pub fn from_env() -> Result<Self, ConfigError> {
        let env = |k: &str| std::env::var(k).unwrap_or_default();
        let cfg = Self {
            host: env("TELEQUICK_HOST"),
            app_key: env("TELEQUICK_MEDIA_KEY"),
            app_secret: env("TELEQUICK_MEDIA_SECRET"),
            agent_name: env("TELEQUICK_AGENT"),
            port: env("TELEQUICK_PORT").parse().unwrap_or(443),
            sample_rate: 8000,
            verify: true,
        };
        cfg.validate()
    }

    pub fn with_agent_name(mut self, name: impl Into<String>) -> Self {
        self.agent_name = name.into();
        self
    }

    fn validate(self) -> Result<Self, ConfigError> {
        let mut missing = Vec::new();
        for (k, v) in [("host", &self.host), ("app_key", &self.app_key), ("app_secret", &self.app_secret), ("agent_name", &self.agent_name)] {
            if v.is_empty() {
                missing.push(k);
            }
        }
        if missing.is_empty() { Ok(self) } else { Err(ConfigError(missing)) }
    }

    pub(crate) fn wt(&self) -> WebTransportConfig {
        WebTransportConfig {
            engine_url: format!("https://{}:{}", self.host, self.port),
            app_key: self.app_key.clone(),
            app_secret: self.app_secret.clone(),
            agent_name: self.agent_name.clone(),
            call_id: 0,
            sample_rate: self.sample_rate,
            num_channels: 1,
        }
    }
}

/// One live phone call handed to your handler. Audio in both directions is
/// 16-bit PCM at `sample_rate()` (8 kHz for telephony). Inbound arrives as
/// 20 ms frames; outbound accepts any chunk size and is paced onto the wire
/// as steady 20 ms frames.
#[derive(Clone)]
pub struct Call {
    assign: JobAssign,
    sess: Arc<WebTransportSession>,
}

impl Call {
    pub fn call_id(&self) -> u16 { self.assign.call_id }
    pub fn room(&self) -> &str { &self.assign.room_name }
    /// ANI — who is calling.
    pub fn caller_number(&self) -> &str { &self.assign.caller_number }
    /// DNIS — the number they dialed.
    pub fn called_number(&self) -> &str { &self.assign.called_number }
    pub fn trunk_id(&self) -> &str { &self.assign.trunk_id }
    pub fn attributes(&self) -> &Map<String, Value> { &self.assign.attributes }
    /// Operator-defined context KVs from the agent config.
    pub fn metadata(&self) -> &HashMap<String, String> { &self.assign.metadata }
    /// The agent's effective resource tags (`{key: value}`) — the platform's
    /// governed cost-allocation / ownership / environment labels, inherited
    /// down the parent chain. Route, bill, or branch per tenant without a lookup.
    pub fn tags(&self) -> &HashMap<String, String> { &self.assign.tags }
    pub fn sample_rate(&self) -> u32 { self.sess.sample_rate() }
    pub fn ended(&self) -> bool { self.sess.closed() }

    /// Next caller frame (pcm16), or `None` once the call ends:
    /// `while let Some(pcm) = call.recv_audio().await { … }`.
    pub async fn recv_audio(&self) -> Option<Vec<u8>> { self.sess.recv_frame().await }
    /// Queue agent audio for the caller (pcm16 @ `sample_rate()`).
    pub fn send_audio(&self, pcm: &[u8]) { self.sess.send_frame(pcm) }
    /// Mark the current outbound segment complete (enables playout reports).
    pub fn flush(&self) { self.sess.signal_flush() }
    /// Barge-in: drop buffered agent audio so playout stops immediately.
    pub fn clear(&self) { self.sess.signal_clear() }
    pub async fn next_playout(&self) -> Option<PlayoutReport> { self.sess.next_playout().await }
    /// Forward a turn ("user"/"assistant") into platform transcript storage,
    /// analytics, and `voice.transcript.ready` webhooks.
    pub fn send_transcript(&self, role: &str, text: &str, is_final: bool) { self.sess.send_transcript(role, text, is_final) }
    /// Report a tool you invoked → analytics timeline + `voice.agent.tool_called`
    /// webhooks. REDACT sensitive values yourself: `args`/`result` ride verbatim.
    pub fn send_tool_call(&self, tool: &str, args: serde_json::Value, result: serde_json::Value) {
        self.sess.send_tool_call(tool, args, result)
    }
    /// Report per-stage usage (`"stt"`/`"llm"`/`"tts"`) → token analytics. You
    /// run the providers, so only what you forward is counted.
    pub fn send_usage(&self, stage: &str, fields: serde_json::Value, provider: &str, model: &str) {
        self.sess.send_usage(stage, fields, provider, model)
    }
    /// End the call from the agent side.
    pub fn close(&self) { self.sess.close() }
}

/// A boxed per-call handler future.
pub type Handler = Arc<dyn Fn(Call) -> Pin<Box<dyn Future<Output = ()> + Send>> + Send + Sync>;

/// Connection-lifecycle hooks.
#[derive(Clone, Default)]
pub struct ServeOptions {
    /// Fires once the worker is connected, authenticated and registered —
    /// present to the platform and awaiting calls — and again after every
    /// automatic reconnect. Drive a readiness probe / health check off it.
    pub on_ready: Option<Arc<dyn Fn() + Send + Sync>>,
    /// Fires when the connection drops (engine restart, network); `serve`
    /// then reconnects with backoff and `on_ready` fires again.
    pub on_disconnected: Option<Arc<dyn Fn(String) + Send + Sync>>,
}

/// Connect, authenticate, register, and run `handler` per assigned call.
/// Reconnects with exponential backoff on any drop so presence recovers
/// without operator action. Never returns; abort the task to stop.
pub async fn serve<F, Fut>(handler: F, cfg: AgentConfig, opts: ServeOptions)
where
    F: Fn(Call) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = ()> + Send + 'static,
{
    let handler: Handler = Arc::new(move |c| Box::pin(handler(c)));
    let mut backoff = Duration::from_secs(1);
    loop {
        let err = match open_media_connection(cfg.wt(), &cfg.host, cfg.port, cfg.verify).await {
            Err(e) => e.to_string(),
            Ok(opened) => {
                eprintln!("[telequick] worker ready (agent={}) — awaiting calls", cfg.agent_name);
                backoff = Duration::from_secs(1);
                if let Some(cb) = &opts.on_ready {
                    cb();
                }
                loop {
                    tokio::select! {
                        next = opened.mc.next_call() => match next {
                            Some((assign, sess)) => {
                                eprintln!("[telequick] call_id={} from={} to={} → handler", assign.call_id, assign.caller_number, assign.called_number);
                                let call = Call { assign, sess: Arc::clone(&sess) };
                                let h = Arc::clone(&handler);
                                tokio::spawn(async move {
                                    h(call).await;
                                    if !sess.closed() { sess.close(); }
                                });
                            }
                            None => break,
                        },
                        _ = opened.closed.notified() => break,
                    }
                    if opened.is_closed() { break; }
                }
                opened.close();
                "engine connection closed".to_owned()
            }
        };
        if let Some(cb) = &opts.on_disconnected {
            cb(err.clone());
        }
        eprintln!("[telequick] worker connection lost ({err}) — reconnecting in {:?}", backoff);
        tokio::time::sleep(backoff).await;
        backoff = (backoff * 2).min(Duration::from_secs(30));
    }
}

/// Convenience: `run(handler)` with env-based config.
pub async fn run<F, Fut>(handler: F) -> Result<(), ConfigError>
where
    F: Fn(Call) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = ()> + Send + 'static,
{
    let cfg = AgentConfig::from_env()?;
    serve(handler, cfg, ServeOptions::default()).await;
    Ok(())
}
