//! `telequick-agents` — TeleQuick external-agent SDK for Rust.
//!
//! Run your own voice agent process against real phone calls over the
//! TeleQuick QUIC media plane. One outbound QUIC/WebTransport session (no
//! inbound ports), HMAC auth with your media app key/secret, one [`Call`] per
//! routed phone call. The Rust sibling of the Python `telequick-agents`,
//! TypeScript `@telequick/agents` and Go `github.com/telequick/agents`
//! packages — same wire protocol, same handshake, same `serve(handler)` shape.
//!
//! ```no_run
//! use telequick_agents::{serve, AgentConfig, Call, ServeOptions};
//!
//! async fn handler(call: Call) {
//!     println!("call from {}", call.caller_number());
//!     while let Some(pcm) = call.recv_audio().await {   // 20 ms pcm16 @ 8 kHz
//!         call.send_audio(&pcm);                        // echo
//!     }
//! }
//!
//! # #[tokio::main] async fn main() {
//! let cfg = AgentConfig::from_env().expect("TELEQUICK_* env");
//! serve(|c| Box::pin(handler(c)), cfg, ServeOptions::default()).await;
//! # }
//! ```

pub mod agent;
pub mod auth;
pub mod connection;
pub mod quic;
pub mod transport;
pub mod wire;

pub use agent::{run, serve, AgentConfig, Call, Handler, ServeOptions};
pub use connection::{AuthError, JobAssign, MediaConnection};
pub use quic::{open_media_connection, Opened};
pub use transport::{PlayoutReport, WebTransportConfig, WebTransportSession};

pub const VERSION: &str = env!("CARGO_PKG_VERSION");
