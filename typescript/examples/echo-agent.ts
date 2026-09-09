/**
 * Minimal external agent: echo the caller's audio straight back.
 *
 * Proves the whole media path — caller → engine → QUIC → your process → back
 * onto the call — with no STT/LLM/TTS. Swap the body of `handler` for your own
 * pipeline (see the `voiceBot` sketch below).
 *
 *   export TELEQUICK_HOST=voice.telequick.dev
 *   export TELEQUICK_MEDIA_KEY=mk_...
 *   export TELEQUICK_MEDIA_SECRET=...
 *   export TELEQUICK_AGENT=salesbot
 *   node --import tsx examples/echo-agent.ts
 */
import { AgentConfig, Call, serve } from "../src/index.js";

async function handler(call: Call): Promise<void> {
  console.log(`call ${call.callId} from ${call.callerNumber} → ${call.calledNumber}`);
  let frames = 0;
  for await (const pcm of call.audio()) {
    await call.sendAudio(pcm); // echo caller audio back
    if (++frames % 250 === 0) console.log(`  ${call.callId}: ${frames} frames`);
  }
  console.log(`call ${call.callId} ended after ${frames} frames`);
}

// ── Your real agent goes here instead ──────────────────────────────────────
// async function voiceBot(call: Call): Promise<void> {
//   for await (const pcm of call.audio()) {
//     const text = await yourSTT(pcm);                 // pcm16 @ call.sampleRate
//     if (!text) continue;
//     call.sendTranscript("user", text);               // lands in TeleQuick history
//     const reply = await yourLLM(text);
//     call.sendTranscript("assistant", reply);
//     for await (const chunk of yourTTS(reply, call.sampleRate)) {
//       await call.sendAudio(chunk);                   // pcm16 @ call.sampleRate
//     }
//     await call.flush();
//   }
// }

await serve(handler, AgentConfig.fromEnv({ agentName: process.env.TELEQUICK_AGENT || "echo" }), {
  onReady: () => console.log("[echo] connected + registered — awaiting calls"),
  onDisconnected: (err) => console.warn("[echo] dropped, reconnecting:", err.message),
});
