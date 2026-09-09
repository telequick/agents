//! One authenticated WebTransport/QUIC session per agent.
//!
//! Owns the control stream + datagram plane, runs the auth handshake BEFORE
//! any media flows, and demuxes concurrent calls by call_id. Tenant isolation
//! is enforced authoritatively engine-side; the pre-auth guards here are
//! defense in depth.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use serde_json::{Map, Value};
use tokio::sync::{mpsc, Notify};

use crate::auth;
use crate::transport::{Sender, WebTransportConfig, WebTransportSession};
use crate::wire::{self, CtrlMsg};

#[derive(Debug, thiserror::Error)]
#[error("auth failed (code={code:?}): {msg}")]
pub struct AuthError {
    pub code: Option<u32>,
    pub msg: String,
}

/// Engine → agent: a call has been routed to this worker.
#[derive(Debug, Clone, Default)]
pub struct JobAssign {
    pub call_id: u16,
    pub room_name: String,
    pub agent_id: String,
    pub caller_number: String, // ANI
    pub called_number: String, // DNIS
    pub trunk_id: String,
    pub attributes: Map<String, Value>,
    /// Operator-defined context KVs from the agent config.
    pub metadata: HashMap<String, String>,
    /// The agent's effective resource tags (key/value, inherited down the
    /// platform's parent chain) — governed cost-allocation / ownership /
    /// environment labels. Distinct from `metadata` (free-form context).
    pub tags: HashMap<String, String>,
}

fn string_map(v: Option<&Value>) -> HashMap<String, String> {
    v.and_then(Value::as_object)
        .map(|m| {
            m.iter()
                .map(|(k, v)| (k.clone(), v.as_str().map(str::to_owned).unwrap_or_else(|| v.to_string())))
                .collect()
        })
        .unwrap_or_default()
}

fn s(v: Option<&Value>) -> String {
    v.and_then(Value::as_str).unwrap_or("").to_owned()
}

pub struct MediaConnection {
    cfg: WebTransportConfig,
    send_datagram: Sender,
    send_stream: Sender,
    ctrl_tx: mpsc::Sender<CtrlMsg>,
    ctrl_rx: tokio::sync::Mutex<mpsc::Receiver<CtrlMsg>>,
    assign_tx: mpsc::Sender<(JobAssign, Arc<WebTransportSession>)>,
    assign_rx: tokio::sync::Mutex<mpsc::Receiver<(JobAssign, Arc<WebTransportSession>)>>,
    authed: AtomicBool,
    socket_id: Mutex<Option<String>>,
    calls: Mutex<HashMap<u16, Arc<WebTransportSession>>>,
    /// Signalled by the QUIC layer when the connection is dead.
    pub closed: Arc<Notify>,
    pub is_closed: AtomicBool,
}

impl MediaConnection {
    pub fn new(cfg: WebTransportConfig, send_datagram: Sender, send_stream: Sender) -> Arc<Self> {
        let (ctx, crx) = mpsc::channel(16);
        let (atx, arx) = mpsc::channel(16);
        Arc::new(Self {
            cfg,
            send_datagram,
            send_stream,
            ctrl_tx: ctx,
            ctrl_rx: tokio::sync::Mutex::new(crx),
            assign_tx: atx,
            assign_rx: tokio::sync::Mutex::new(arx),
            authed: AtomicBool::new(false),
            socket_id: Mutex::new(None),
            calls: Mutex::new(HashMap::new()),
            closed: Arc::new(Notify::new()),
            is_closed: AtomicBool::new(false),
        })
    }

    pub fn authenticated(&self) -> bool {
        self.authed.load(Ordering::SeqCst)
    }

    /// Mark the connection dead and wake anyone waiting on `closed`.
    pub fn mark_closed(&self) {
        self.is_closed.store(true, Ordering::SeqCst);
        self.closed.notify_waiters();
        self.closed.notify_one();
    }

    // -- fed by the connection reader ------------------------------------

    pub fn on_control(&self, msg: CtrlMsg) {
        if !self.authenticated() {
            let _ = self.ctrl_tx.try_send(msg); // welcome / ready / error → handshake
            return;
        }
        if wire::ctrl_op(&msg) == "job_assign" {
            self.on_job_assign(&msg);
            return;
        }
        let Some(cid) = msg.get("call_id").and_then(Value::as_u64) else { return };
        let sess = self.calls.lock().unwrap().get(&(cid as u16)).cloned();
        if let Some(sess) = sess {
            sess.on_control(&msg);
        }
    }

    pub fn on_datagram(&self, buf: &[u8]) {
        if !self.authenticated() {
            return; // never accept media before auth
        }
        let Ok(dg) = wire::decode_datagram(buf) else { return };
        let sess = self.calls.lock().unwrap().get(&dg.call_id).cloned();
        if let Some(sess) = sess {
            sess.on_datagram(buf);
        }
    }

    // -- handshake: hello -> welcome -> auth -> ready --------------------

    fn send(&self, op: &str, fields: Map<String, Value>) {
        (self.send_stream)(wire::encode_ctrl(op, fields));
    }

    async fn await1(&self, timeout: Duration) -> Result<CtrlMsg, AuthError> {
        let mut rx = self.ctrl_rx.lock().await;
        match tokio::time::timeout(timeout, rx.recv()).await {
            Ok(Some(m)) => Ok(m),
            Ok(None) => Err(AuthError { code: None, msg: "control channel closed".into() }),
            Err(_) => Err(AuthError { code: None, msg: "handshake timeout".into() }),
        }
    }

    /// Run the handshake; must complete before `next_call`.
    pub async fn authenticate(&self, timeout: Duration) -> Result<(), AuthError> {
        let mut hello = Map::new();
        hello.insert("app_key".into(), self.cfg.app_key.clone().into());
        self.send("hello", hello);

        let welcome = self.await1(timeout).await?;
        let sid = match (wire::ctrl_op(&welcome), welcome.get("socket_id").and_then(Value::as_str)) {
            ("welcome", Some(sid)) => sid.to_owned(),
            _ => return Err(AuthError { code: None, msg: format!("expected welcome, got {}", Value::Object(welcome)) }),
        };
        *self.socket_id.lock().unwrap() = Some(sid.clone());

        let mut a = Map::new();
        a.insert("app_key".into(), self.cfg.app_key.clone().into());
        a.insert("channel".into(), auth::MEDIA_CHANNEL.into());
        a.insert("channel_data".into(), self.cfg.agent_name.clone().into());
        a.insert("auth".into(), auth::media_auth(&self.cfg.app_key, &self.cfg.app_secret, &sid, &self.cfg.agent_name).into());
        self.send("auth", a);

        let resp = self.await1(timeout).await?;
        if wire::ctrl_op(&resp) != "ready" {
            return Err(AuthError {
                code: resp.get("code").and_then(Value::as_u64).map(|c| c as u32),
                msg: wire::ctrl_op(&resp).to_owned(),
            });
        }
        self.authed.store(true, Ordering::SeqCst);
        Ok(())
    }

    // -- per-call sessions ----------------------------------------------

    fn on_job_assign(&self, msg: &CtrlMsg) {
        let cid = msg.get("call_id").and_then(Value::as_u64).unwrap_or(0) as u16;
        let mut cfg = self.cfg.clone();
        cfg.call_id = cid;
        let sess = WebTransportSession::new(cfg, Arc::clone(&self.send_datagram), Arc::clone(&self.send_stream));
        self.calls.lock().unwrap().insert(cid, Arc::clone(&sess));
        let assign = JobAssign {
            call_id: cid,
            room_name: s(msg.get("room")),
            agent_id: s(msg.get("agent_id")),
            caller_number: s(msg.get("caller")),
            called_number: s(msg.get("called")),
            trunk_id: s(msg.get("trunk_id")),
            attributes: msg.get("attrs").and_then(Value::as_object).cloned().unwrap_or_default(),
            metadata: string_map(msg.get("metadata")),
            tags: string_map(msg.get("tags")),
        };
        let _ = self.assign_tx.try_send((assign, sess));
    }

    /// Wait for the next call the engine routes to this worker.
    pub async fn next_call(&self) -> Option<(JobAssign, Arc<WebTransportSession>)> {
        self.assign_rx.lock().await.recv().await
    }

    /// Keepalive over the control stream (the engine ignores the op).
    pub fn ping(&self) {
        self.send("ping", Map::new());
    }
}
