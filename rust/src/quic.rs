//! The QUIC socket under [`MediaConnection`], via `wtransport`:
//!   - extended CONNECT to `https://<host>:<port>/media/<app_key>`
//!   - control on ONE client-opened bidi WT stream (the engine replies on it)
//!   - media on QUIC datagrams
//!   - app-level ping every 20 s + an RX watchdog: no packet for > 45 s ⇒ peer
//!     gone (engine restart) ⇒ `closed` fires and [`serve`](crate::serve) reconnects.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::{mpsc, Notify};
use wtransport::{ClientConfig, Connection, Endpoint, VarInt};

use crate::connection::{AuthError, MediaConnection};
use crate::transport::{Sender, WebTransportConfig};
use crate::wire;

const KEEPALIVE_EVERY: Duration = Duration::from_secs(20);
const RX_DEADLINE: Duration = Duration::from_secs(45);

#[derive(Debug, thiserror::Error)]
pub enum OpenError {
    #[error("endpoint: {0}")]
    Endpoint(#[from] std::io::Error),
    #[error("webtransport connect {url}: {msg}")]
    Connect { url: String, msg: String },
    #[error("control stream: {0}")]
    Stream(String),
    #[error(transparent)]
    Auth(#[from] AuthError),
}

/// A live, authenticated media connection.
pub struct Opened {
    pub mc: Arc<MediaConnection>,
    /// Notified when the connection is dead (a permit is kept, so a late
    /// `notified().await` returns immediately).
    pub closed: Arc<Notify>,
    conn: Connection,
}

impl Opened {
    pub fn is_closed(&self) -> bool {
        self.mc.is_closed.load(Ordering::SeqCst)
    }
    pub fn close(&self) {
        self.mc.mark_closed();
        self.conn.close(VarInt::from_u32(0), b"");
    }
}

/// Dial, authenticate and return the connection. `verify = false` skips engine
/// certificate validation — dev only.
pub async fn open_media_connection(
    cfg: WebTransportConfig,
    host: &str,
    port: u16,
    verify: bool,
) -> Result<Opened, OpenError> {
    let url = format!("https://{host}:{port}/media/{}", cfg.app_key);

    let builder = ClientConfig::builder().with_bind_default();
    let client_cfg = if verify { builder.with_native_certs() } else { builder.with_no_cert_validation() }
        .keep_alive_interval(Some(Duration::from_secs(15)))
        .build();
    let endpoint = Endpoint::client(client_cfg)?;
    let conn = endpoint
        .connect(url.clone())
        .await
        .map_err(|e| OpenError::Connect { url: url.clone(), msg: e.to_string() })?;

    // Control: ONE client-opened bidi stream; the engine replies on it.
    let (mut send, mut recv) = conn
        .open_bi()
        .await
        .map_err(|e| OpenError::Stream(e.to_string()))?
        .await
        .map_err(|e| OpenError::Stream(e.to_string()))?;

    let base = Instant::now();
    let last_rx = Arc::new(AtomicU64::new(0));
    let touch = {
        let last_rx = Arc::clone(&last_rx);
        move || last_rx.store(base.elapsed().as_millis() as u64, Ordering::Relaxed)
    };

    // Writes to the control stream are serialized through one writer task so
    // the sync `send_stream` sink never blocks a caller.
    let (wtx, mut wrx) = mpsc::unbounded_channel::<Vec<u8>>();
    let send_stream: Sender = {
        let wtx = wtx.clone();
        Arc::new(move |b: Vec<u8>| {
            let _ = wtx.send(b);
        })
    };
    let send_datagram: Sender = {
        let c = conn.clone();
        Arc::new(move |b: Vec<u8>| {
            let _ = c.send_datagram(b); // a lost datagram is a lost 20 ms, never a stall
        })
    };

    let mc = MediaConnection::new(cfg, send_datagram, send_stream);

    // Control writer.
    {
        let mc = Arc::clone(&mc);
        tokio::spawn(async move {
            while let Some(b) = wrx.recv().await {
                if AsyncWriteExt::write_all(&mut send, &b).await.is_err() {
                    mc.mark_closed();
                    return;
                }
            }
        });
    }
    // Control reader.
    {
        let mc = Arc::clone(&mc);
        let touch = touch.clone();
        tokio::spawn(async move {
            let mut buf: Vec<u8> = Vec::new();
            let mut chunk = vec![0u8; 16 * 1024];
            loop {
                match AsyncReadExt::read(&mut recv, &mut chunk).await {
                    Ok(n) if n > 0 => {
                        touch();
                        buf.extend_from_slice(&chunk[..n]);
                        let (msgs, rem) = wire::decode_ctrl_stream(&buf);
                        buf = rem;
                        for m in msgs {
                            mc.on_control(m);
                        }
                    }
                    _ => {
                        mc.mark_closed();
                        return;
                    }
                }
            }
        });
    }
    // Datagram reader.
    {
        let mc = Arc::clone(&mc);
        let c = conn.clone();
        let touch = touch.clone();
        tokio::spawn(async move {
            loop {
                match c.receive_datagram().await {
                    Ok(d) => {
                        touch();
                        mc.on_datagram(&d);
                    }
                    Err(_) => {
                        mc.mark_closed();
                        return;
                    }
                }
            }
        });
    }

    mc.authenticate(Duration::from_secs(10)).await?;

    // Keepalive + RX watchdog + transport-closed watcher.
    {
        let mc = Arc::clone(&mc);
        let c = conn.clone();
        let last_rx = Arc::clone(&last_rx);
        tokio::spawn(async move {
            let mut ka = tokio::time::interval(KEEPALIVE_EVERY);
            let mut wd = tokio::time::interval(Duration::from_secs(10));
            ka.tick().await;
            wd.tick().await;
            let closed = c.closed();
            tokio::pin!(closed);
            loop {
                tokio::select! {
                    _ = ka.tick() => mc.ping(),
                    _ = wd.tick() => {
                        let since = base.elapsed().as_millis() as u64 - last_rx.load(Ordering::Relaxed);
                        if since > RX_DEADLINE.as_millis() as u64 { mc.mark_closed(); return; }
                    }
                    _ = &mut closed => { mc.mark_closed(); return; }
                    _ = mc.closed.notified() => return,
                }
            }
        });
    }

    let closed = Arc::clone(&mc.closed);
    Ok(Opened { mc, closed, conn })
}
