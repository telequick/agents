"""Media transport for a single engine-assigned call.

One WebTransport/QUIC session per agent process, dialed OUTBOUND (the agent is
always the client — no inbound ports, NAT-friendly). Per-call media rides
WebTransport **datagrams** (raw pcm16 little-endian); per-call control rides a
bidi **stream** of newline-JSON envelopes (job-assign, flush, clear/barge-in,
playout, shutdown, transcript).

This module is provider-neutral: audio is plain ``bytes`` of 16-bit PCM at the
configured wire rate (8000 Hz for telephony). Framework adapters (Pipecat,
LiveKit, your own loop) sit on top.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass


@dataclass
class PlayoutReport:
    """Engine → agent: the caller finished hearing a flushed segment.

    ``interrupted`` is True when a barge-in (clear) cut the segment short.
    """

    playback_position: float
    interrupted: bool = False


@dataclass
class WebTransportConfig:
    engine_url: str  # e.g. "https://engine.example.com:443"
    app_key: str  # media app key (mint via mediaApps.provision / console)
    app_secret: str  # media app secret — server-side cred, signs the HMAC auth
    agent_name: str = ""  # advertised on presence + bound in media auth
    call_id: int = 0  # demux key for datagrams on a shared session
    sample_rate: int = 8000  # wire rate; 8000 for PSTN legs
    num_channels: int = 1
    codec: str = "pcm16"  # raw pcm16 passthrough today


class WebTransportSession:
    """Bidirectional audio channel for ONE call over the shared QUIC session.

    Inbound = the caller's audio (engine → agent), surfaced as 20 ms pcm16
    frames from ``recv_frame``. Outbound = the agent's audio (``send_frame``),
    buffered and paced onto the wire as steady 20 ms datagrams — TTS arrives
    bursty and the caller leg needs a steady stream, else it clicks.

    A ``send_datagram`` / ``send_stream`` callable pair is injected so the same
    session logic runs against a live aioquic connection OR an in-memory fake.
    """

    def __init__(self, cfg: WebTransportConfig, *, send_datagram, send_stream) -> None:
        from . import wire

        if cfg.codec not in ("pcm16",):
            raise NotImplementedError(f"codec {cfg.codec!r} not wired yet — pcm16 works today")
        self._cfg = cfg
        self._wire = wire
        self._send_datagram = send_datagram  # (bytes) -> None
        self._send_stream = send_stream  # (bytes) -> None  (control stream)
        self._inbound: asyncio.Queue[bytes | None] = asyncio.Queue()
        self._playout: asyncio.Queue[PlayoutReport] = asyncio.Queue()
        self._tx_seq = 0
        self._out_buf = bytearray()
        self._frame_bytes = max(1, int(cfg.sample_rate * 0.02)) * 2 * cfg.num_channels
        self._pacer_task: asyncio.Task | None = None
        self._closed = False

    # -- fed by the connection's datagram/stream readers ------------------
    def on_datagram(self, buf: bytes) -> None:
        dg = self._wire.decode_datagram(buf)
        if dg.call_id != self._cfg.call_id or dg.type != self._wire.DG_AUDIO:
            return
        self._inbound.put_nowait(dg.payload)

    def on_control(self, msg: dict) -> None:
        if msg.get("call_id") not in (self._cfg.call_id, None):
            return
        op = msg.get("op")
        if op == "playout":
            self._playout.put_nowait(
                PlayoutReport(
                    playback_position=float(msg.get("position", 0.0)),
                    interrupted=bool(msg.get("interrupted", False)),
                )
            )
        elif op == "shutdown":
            self._inbound.put_nowait(None)

    # -- media API ---------------------------------------------------------
    @property
    def sample_rate(self) -> int:
        """Wire rate (8 kHz for telephony). Resample wideband TTS DOWN to this
        before ``send_frame`` — the wire is a raw pcm16 passthrough, so
        un-resampled audio plays at the wrong speed on the caller leg."""
        return self._cfg.sample_rate

    @property
    def closed(self) -> bool:
        return self._closed

    async def recv_frame(self) -> bytes | None:
        """Next inbound caller frame (pcm16 @ wire rate), or None at call end."""
        return await self._inbound.get()

    async def send_frame(self, pcm: bytes) -> None:
        """Push outbound agent audio (pcm16 @ wire rate, any chunk size)."""
        self._out_buf += pcm
        if self._pacer_task is None and not self._closed:
            self._pacer_task = asyncio.ensure_future(self._pacer())

    async def _pacer(self) -> None:
        try:
            while not self._closed:
                if len(self._out_buf) >= self._frame_bytes:
                    chunk = bytes(self._out_buf[: self._frame_bytes])
                    del self._out_buf[: self._frame_bytes]
                    dg = self._wire.Datagram(
                        call_id=self._cfg.call_id, seq=self._tx_seq, payload=chunk
                    )
                    self._tx_seq = (self._tx_seq + 1) & 0xFFFF
                    self._send_datagram(self._wire.encode_datagram(dg))
                await asyncio.sleep(0.02)
        except asyncio.CancelledError:  # pragma: no cover
            pass

    def send_transcript(self, role: str, text: str, is_final: bool = True) -> None:
        """Forward a transcript turn so this call's history lands in the same
        store (and webhooks) as a platform-native agent's."""
        self._send_stream(
            self._wire.encode_ctrl(
                "transcript", call_id=self._cfg.call_id, role=role, text=text, is_final=is_final
            )
        )

    def send_tool_call(
        self, tool: str, args: dict | None = None, result: dict | None = None
    ) -> None:
        """Report a tool this agent invoked, so the platform logs it exactly as
        it logs a native agent's (analytics timeline + ``voice.agent.tool_called``
        webhooks). Your tool loop is invisible to the engine, so nothing is
        recorded unless you call this.

        REDACT SENSITIVE VALUES YOURSELF — ``args``/``result`` are forwarded
        verbatim as opaque JSON; the engine cannot see inside your tool to
        redact for you. An empty ``tool`` is dropped engine-side.
        """
        if not tool:
            return
        self._send_stream(
            self._wire.encode_ctrl(
                "tool_call",
                call_id=self._cfg.call_id,
                tool=tool,
                args=args or {},
                result=result or {},
            )
        )

    def send_usage(
        self,
        stage: str,
        fields: dict | None = None,
        provider: str = "",
        model: str = "",
    ) -> None:
        """Report per-stage usage for token analytics. You run STT/LLM/TTS on
        your OWN provider keys, so the platform only sees what you forward.

        ``stage`` is ``"stt"``/``"llm"``/``"tts"``; ``fields`` carries the
        numbers (llm: ``{"prompt_tokens": .., "completion_tokens": ..}``,
        stt: ``{"audio_seconds": ..}``, tts: ``{"characters": ..}``).
        An empty ``stage`` is dropped engine-side.
        """
        if not stage:
            return
        self._send_stream(
            self._wire.encode_ctrl(
                "usage",
                call_id=self._cfg.call_id,
                stage=stage,
                provider=provider,
                model=model,
                fields=fields or {},
            )
        )

    async def signal_flush(self) -> None:
        """Mark the current outbound segment complete (enables playout reports)."""
        self._send_stream(self._wire.encode_ctrl("flush", call_id=self._cfg.call_id))

    async def signal_clear(self) -> None:
        """Barge-in: drop everything buffered for the caller, stop playout now."""
        self._out_buf.clear()
        self._send_stream(self._wire.encode_ctrl("clear", call_id=self._cfg.call_id))

    async def next_playout(self) -> PlayoutReport:
        return await self._playout.get()

    async def aclose(self) -> None:
        self._closed = True
        if self._pacer_task is not None:
            self._pacer_task.cancel()
        self._send_stream(self._wire.encode_ctrl("shutdown", call_id=self._cfg.call_id))
