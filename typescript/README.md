# @telequick/agents

TeleQuick external-agent SDK for **TypeScript** — run your own voice agent
process against real phone calls over the TeleQuick QUIC media plane. The
TypeScript sibling of the Python [`telequick-agents`](../python)
package: same wire protocol, same handshake, same `serve(handler)` shape.

```ts
import { AgentConfig, serve, type Call } from "@telequick/agents";

async function handler(call: Call): Promise<void> {
  console.log("call from", call.callerNumber);
  for await (const pcm of call.audio()) {   // 20 ms pcm16 @ 8 kHz
    await call.sendAudio(pcm);              // echo
  }
}

await serve(handler, AgentConfig.fromEnv({ agentName: "salesbot" }), {
  onReady: () => console.log("connected + registered — awaiting calls"),
  onDisconnected: (e) => console.warn("dropped, reconnecting:", e.message),
});
```

- **Media plane** — one outbound QUIC/WebTransport session (no inbound ports),
  HMAC auth with your media app key/secret, one `Call` per routed phone call:
  `call.audio()`, `sendAudio`, `clear` (barge-in), `flush`, `sendTranscript`,
  ANI/DNIS via `callerNumber`/`calledNumber`.
- **Lifecycle** — `serve()` reconnects with backoff on any drop; `onReady` /
  `onDisconnected` tell you when the worker is present and awaiting calls.
- **Bring your own AI** — swap the echo body for your STT/LLM/TTS. Audio in and
  out is pcm16 at `call.sampleRate` (8 kHz for telephony).

## Install

```bash
npm install https://artifacts.clutchcall.dev/npm/tarballs/telequick-agents-0.2.0.tgz \
            @fails-components/webtransport
```

The SDK is served from our package origin (`artifacts.clutchcall.dev`), not
npmjs; its deps still resolve from npmjs as normal.

`@fails-components/webtransport` is the native QUIC/WebTransport client for Node
(Node has no built-in one); it is an optional peer. Node 20+.

## Configure

`AgentConfig.fromEnv()` reads `TELEQUICK_HOST`, `TELEQUICK_MEDIA_KEY`,
`TELEQUICK_MEDIA_SECRET`, `TELEQUICK_AGENT` (any overridable per option). Get the
media key/secret + agent name from the console's **External agent** create flow.

## Run the example

```bash
TELEQUICK_HOST=voice.telequick.dev TELEQUICK_MEDIA_KEY=mk_… \
TELEQUICK_MEDIA_SECRET=… TELEQUICK_AGENT=echo \
  npm run echo
```

## Docs

[Build a Voice Agent in TypeScript](https://docs.telequick.dev/modalities/voice/recipes/typescript-agent).
Already on LiveKit Agents? Use `@telequick/livekit-transport` instead and keep
your `AgentSession` verbatim.
