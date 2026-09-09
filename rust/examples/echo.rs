//! Minimal external agent: echo the caller's audio straight back.
//!
//! Proves the whole media path — caller → engine → QUIC → your process → back
//! onto the call — with no STT/LLM/TTS. Swap the handler body for your own
//! pipeline (see the sketch below).
//!
//! ```text
//! export TELEQUICK_HOST=engine.telequick.dev
//! export TELEQUICK_MEDIA_KEY=mk_...
//! export TELEQUICK_MEDIA_SECRET=...
//! export TELEQUICK_AGENT=salesbot
//! cargo run --example echo
//! ```

use std::sync::Arc;

use telequick_agents::{serve, AgentConfig, Call, ServeOptions};

async fn handler(call: Call) {
    println!(
        "call {} from {} → {} (trunk={} tags={:?})",
        call.call_id(), call.caller_number(), call.called_number(), call.trunk_id(), call.tags()
    );
    let mut frames = 0u32;
    while let Some(pcm) = call.recv_audio().await { // 20 ms pcm16 @ 8 kHz
        call.send_audio(&pcm); // echo caller audio back
        frames += 1;
        if frames % 250 == 0 {
            println!("  {}: {frames} frames", call.call_id());
        }
    }
    println!("call {} ended after {frames} frames", call.call_id());
}

// ── Your real agent goes here instead ──────────────────────────────────────
// async fn voice_bot(call: Call) {
//     while let Some(pcm) = call.recv_audio().await {
//         let Some(text) = your_stt(&pcm, call.sample_rate()).await else { continue };
//         call.send_transcript("user", &text, true);          // lands in TeleQuick history
//         let reply = your_llm(&text).await;
//         call.send_transcript("assistant", &reply, true);
//         for chunk in your_tts(&reply, call.sample_rate()).await {
//             call.send_audio(&chunk);                         // pcm16 @ call.sample_rate()
//         }
//         call.flush();
//     }
// }

#[tokio::main]
async fn main() {
    let cfg = AgentConfig::from_env().expect("TELEQUICK_HOST / TELEQUICK_MEDIA_KEY / TELEQUICK_MEDIA_SECRET / TELEQUICK_AGENT");
    serve(
        handler,
        cfg,
        ServeOptions {
            on_ready: Some(Arc::new(|| println!("[echo] connected + registered — awaiting calls"))),
            on_disconnected: Some(Arc::new(|e| eprintln!("[echo] dropped, reconnecting: {e}"))),
        },
    )
    .await;
}
