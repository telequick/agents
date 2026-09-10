# telequick-agents (Rust)

TeleQuick external-agent SDK for **Rust** — run your own voice agent process
against real phone calls over the TeleQuick QUIC media plane. The Rust sibling
of the Python [`telequick-agents`](../python), TypeScript
[`@telequick/agents`](../typescript) and Go
[`github.com/telequick/agents`](../go) packages: same wire protocol, same
handshake, same `serve(handler)` shape.

```rust
use telequick_agents::{serve, AgentConfig, Call, ServeOptions};

async fn handler(call: Call) {
    println!("call from {}", call.caller_number());
    while let Some(pcm) = call.recv_audio().await {   // 20 ms pcm16 @ 8 kHz
        call.send_audio(&pcm);                        // echo
    }
}

let cfg = AgentConfig::from_env()?.with_agent_name("salesbot");
serve(handler, cfg, ServeOptions {
    on_ready: Some(Arc::new(|| println!("connected + registered — awaiting calls"))),
    on_disconnected: Some(Arc::new(|e| eprintln!("dropped, reconnecting: {e}"))),
}).await;
```

- **Media plane** — one outbound QUIC/WebTransport session (no inbound ports),
  HMAC auth with your media app key/secret, one `Call` per routed phone call:
  `recv_audio`, `send_audio`, `clear` (barge-in), `flush`, `send_transcript`, `send_usage` /
  `send_tool_call` (token analytics + tool timeline),
  ANI/DNIS/trunk via `caller_number`/`called_number`/`trunk_id`, and the
  agent's resource `tags()`.
- **Lifecycle** — `serve` reconnects with backoff on any drop; `on_ready` /
  `on_disconnected` tell you when the worker is present and awaiting calls.
- **Bring your own AI** — swap the echo body for your STT/LLM/TTS. Audio in and
  out is pcm16 at `call.sample_rate()` (8 kHz for telephony).

Built on [wtransport](https://crates.io/crates/wtransport) (QUIC via quinn,
rustls). Rust 1.88+, tokio.

## Install

The crate is served from our sparse registry, not crates.io (its deps still
resolve from crates.io):

```toml
# .cargo/config.toml
[registries.telequick]
index = "sparse+https://artifacts.clutchcall.dev/cargo/"

# Cargo.toml
[dependencies]
telequick-agents = { version = "0.3.0", registry = "telequick" }
```

## Configure

`AgentConfig::from_env()` reads `TELEQUICK_HOST`, `TELEQUICK_MEDIA_KEY`,
`TELEQUICK_MEDIA_SECRET`, `TELEQUICK_AGENT` (optional `TELEQUICK_PORT`). Get the
media key/secret + agent name from the console's **External agent** create
flow. Set `verify = false` only for a dev engine with a self-signed cert.

## Run the example

```bash
TELEQUICK_HOST=engine.telequick.dev TELEQUICK_MEDIA_KEY=ck_… \
TELEQUICK_MEDIA_SECRET=… TELEQUICK_AGENT=echo cargo run --example echo
```

## Test

`cargo test` — wire framing round-trips and the HMAC known vector
(byte-identical to the Python package and the engine).
