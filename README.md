# telequick-agents

The **telequick external-agent SDK** — run your own voice agent process against real
phone calls over the telequick QUIC media plane. One outbound QUIC/WebTransport
session (no inbound ports), HMAC auth with your media app key/secret, one
`Call` per routed phone call.

Four languages, one wire protocol, one `serve(handler)` shape:

| Language | Package | Source |
|---|---|---|
| Go | `github.com/telequick/agents` | this repo root |
| Python | `telequick-agents` (pip) | [`python/`](python) |
| TypeScript | `@telequick/agents` (npm) | [`typescript/`](typescript) |
| Rust | `telequick-agents` (crate) | [`rust/`](rust) |

```bash
export GOPROXY=https://artifacts.clutchcall.dev/go GOSUMDB=off   # Go
go get github.com/telequick/agents@v0.2.0

pip install 'telequick-agents[quic]==0.2.0' --extra-index-url https://artifacts.clutchcall.dev/pip/simple/
npm install https://artifacts.clutchcall.dev/npm/tarballs/telequick-agents-0.2.0.tgz
```

Each language ships a runnable `examples/echo` and its own README. Provider
starters (Twilio, Plivo, Telnyx, Vapi, Vobiz, Pipecat) live in
[telequick/examples](https://github.com/telequick/examples).

Docs: https://docs.telequick.dev/modalities/voice/recipes/typescript-agent

MIT licensed.
