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
// You run STT/LLM/TTS on your OWN keys, so the platform only sees what you
// forward. send_transcript/send_usage/send_tool_call give an external agent the
// same history, token analytics and tool timeline a native agent gets.
//
// async fn voice_bot(call: Call) {
//     while let Some(pcm) = call.recv_audio().await {
//         let Some(stt) = your_stt(&pcm, call.sample_rate()).await else { continue };
//         call.send_transcript("user", &stt.text, true);       // lands in TeleQuick history
//         call.send_usage("stt", json!({"audio_seconds": stt.seconds}), "deepgram", "nova-2");
//
//         let llm = your_llm(&stt.text).await;
//         call.send_usage("llm",
//             json!({"prompt_tokens": llm.prompt_tokens, "completion_tokens": llm.completion_tokens}),
//             "openai", "gpt-4o-mini");
//         for t in &llm.tool_calls {
//             // REDACT sensitive values — args/result are forwarded verbatim.
//             call.send_tool_call(&t.name, t.args.clone(), t.result.clone());
//         }
//
//         call.send_transcript("assistant", &llm.text, true);
//         for chunk in your_tts(&llm.text, call.sample_rate()).await {
//             call.send_audio(&chunk);                          // pcm16 @ call.sample_rate()
//         }
//         call.send_usage("tts", json!({"characters": llm.text.len()}), "elevenlabs", "");
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
