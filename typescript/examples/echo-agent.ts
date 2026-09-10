/**
 * Minimal external agent: echo the caller's audio straight back.
 *
 * Proves the whole media path — caller → engine → QUIC → your process → back
 * onto the call — with no STT/LLM/TTS. Swap the body of `handler` for your own
 * pipeline (see the `voiceBot` sketch below).
 *
 *   export TELEQUICK_HOST=engine.telequick.dev
 *   export TELEQUICK_MEDIA_KEY=ck_...
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
// You run STT/LLM/TTS on your OWN keys, so the platform only sees what you
// forward. sendTranscript/sendUsage/sendToolCall give an external agent the
// same history, token analytics and tool timeline a native agent gets.
//
// async function voiceBot(call: Call): Promise<void> {
//   for await (const pcm of call.audio()) {
//     const stt = await yourSTT(pcm);                  // pcm16 @ call.sampleRate
//     if (!stt.text) continue;
//     call.sendTranscript("user", stt.text);           // lands in TeleQuick history
//     call.sendUsage("stt", { audioSeconds: stt.seconds }, "deepgram", "nova-2");
//
//     const llm = await yourLLM(stt.text);
//     call.sendUsage("llm",
//       { promptTokens: llm.promptTokens, completionTokens: llm.completionTokens },
//       "openai", "gpt-4o-mini");
//     for (const t of llm.toolCalls ?? []) {
//       // REDACT sensitive values — args/result are forwarded verbatim.
//       call.sendToolCall(t.name, t.args, t.result);
//     }
//
//     call.sendTranscript("assistant", llm.text);
//     for await (const chunk of yourTTS(llm.text, call.sampleRate)) {
//       await call.sendAudio(chunk);                   // pcm16 @ call.sampleRate
//     }
//     call.sendUsage("tts", { characters: llm.text.length }, "elevenlabs");
//     await call.flush();
//   }
// }

await serve(handler, AgentConfig.fromEnv({ agentName: process.env.TELEQUICK_AGENT || "echo" }), {
  onReady: () => console.log("[echo] connected + registered — awaiting calls"),
  onDisconnected: (err) => console.warn("[echo] dropped, reconnecting:", err.message),
});
