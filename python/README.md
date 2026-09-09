# telequick-agents

TeleQuick external-agent SDK: run your own voice agent process against real
phone calls over the TeleQuick QUIC media plane, plus the management-API
client and webhook verification.

```python
import asyncio
from telequick_agents import AgentConfig, serve

async def handler(call):
    print("call from", call.caller_number)
    async for pcm in call.audio():        # 20 ms pcm16 @ 8 kHz
        await call.send_audio(pcm)        # echo

asyncio.run(serve(handler, AgentConfig.from_env(agent_name="salesbot")))
```

- **Media plane** — one outbound QUIC/WebTransport session (no inbound ports),
  HMAC auth with your media app key/secret, one `Call` per routed phone call:
  `call.audio()`, `send_audio`, `clear` (barge-in), `flush`, `send_transcript`,
  ANI/DNIS.
- **Control plane** — `TeleQuickAPI(base_url=…, api_key="mpk_…", org_id=…)`
  `.call("voice.calls.originate", {...})` for originate / transfer / webhooks /
  provisioning.
- **Webhooks** — `telequick_agents.webhooks.verify_signature` for signed
  deliveries.
- **G.711** — `telequick_agents.g711` μ-law/A-law ↔ pcm16 (pure-Python on 3.13+).

Install: `pip install 'telequick-agents[quic]==0.2.0' --extra-index-url
https://artifacts.clutchcall.dev/pip/simple/`

The `[quic]` extra pulls `aioquic` (the WebTransport/QUIC socket); the rest of
the package imports without it.

Provider starter ports (Twilio, Plivo, Telnyx, Vapi, Pipecat, Vobiz) live in
the `telequick-examples` repo this package ships from.
