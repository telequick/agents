"""High-level external-agent API: register-and-wait, handle calls.

The drop-in seam for every provider starter in this repo. Your process dials
ONE outbound QUIC/WebTransport session to the engine, authenticates with your
media app key/secret, registers under ``agent_name``, and gets a ``Call`` for
every phone call the platform routes to it:

    import asyncio
    from telequick_agents import AgentConfig, serve

    async def handler(call):
        print("call from", call.caller_number)
        async for pcm in call.audio():          # 20 ms pcm16 @ 8 kHz
            await call.send_audio(pcm)          # echo

    asyncio.run(serve(handler, AgentConfig.from_env(agent_name="salesbot")))

Route a number to the agent by pointing a platform agent's ``VENDOR_BRIDGE``
node (``vendor_room = agent_name``) at it — the console's "External agent"
create flow does this and mints the media key in one step.
"""

from __future__ import annotations

import asyncio
import logging
import os
from dataclasses import dataclass
import inspect
from typing import AsyncIterator, Awaitable, Callable, Optional

from .transport import PlayoutReport, WebTransportConfig, WebTransportSession
from .types import JobAssign

logger = logging.getLogger("telequick")


@dataclass
class AgentConfig:
    """Everything the worker needs to reach the engine and identify itself."""

    host: str
    app_key: str
    app_secret: str
    agent_name: str
    port: int = 443
    sample_rate: int = 8000  # PSTN legs are 8 kHz
    verify: bool = True  # dev only: False skips engine cert validation

    @classmethod
    def from_env(cls, **overrides) -> "AgentConfig":
        """Build from TELEQUICK_HOST / TELEQUICK_MEDIA_KEY /
        TELEQUICK_MEDIA_SECRET / TELEQUICK_AGENT (overridable per kwarg)."""
        vals = dict(
            host=os.environ.get("TELEQUICK_HOST", ""),
            app_key=os.environ.get("TELEQUICK_MEDIA_KEY", ""),
            app_secret=os.environ.get("TELEQUICK_MEDIA_SECRET", ""),
            agent_name=os.environ.get("TELEQUICK_AGENT", ""),
        )
        vals.update(overrides)
        missing = [k for k in ("host", "app_key", "app_secret", "agent_name") if not vals.get(k)]
        if missing:
            raise ValueError(
                f"AgentConfig missing {missing} — set TELEQUICK_HOST/TELEQUICK_MEDIA_KEY/"
                "TELEQUICK_MEDIA_SECRET/TELEQUICK_AGENT or pass them explicitly"
            )
        return cls(**vals)

    def _wt(self, call_id: int = 0) -> WebTransportConfig:
        return WebTransportConfig(
            engine_url=f"https://{self.host}:{self.port}",
            app_key=self.app_key,
            app_secret=self.app_secret,
            agent_name=self.agent_name,
            call_id=call_id,
            sample_rate=self.sample_rate,
        )


class Call:
    """One live phone call handed to your worker.

    Audio in both directions is 16-bit PCM at ``sample_rate`` (8 kHz for
    telephony). Inbound arrives as 20 ms frames; outbound accepts any chunk
    size and is paced onto the wire as steady 20 ms frames.
    """

    def __init__(self, assign: JobAssign, session: WebTransportSession) -> None:
        self._assign = assign
        self._session = session
        self._ended = asyncio.Event()

    # -- identity ---------------------------------------------------------
    @property
    def call_id(self) -> int:
        return self._assign.call_id

    @property
    def room(self) -> str:
        return self._assign.room_name

    @property
    def caller_number(self) -> str:
        """ANI — who is calling."""
        return self._assign.caller_number

    @property
    def called_number(self) -> str:
        """DNIS — the number they dialed."""
        return self._assign.called_number

    @property
    def trunk_id(self) -> str:
        return self._assign.trunk_id

    @property
    def attributes(self) -> dict:
        return self._assign.attributes

    @property
    def tags(self) -> dict:
        """The agent's effective resource tags ({key: value}) — the platform's
        governed cost-allocation / ownership / environment labels, inherited
        down the parent chain. Use them to route, bill, or branch per tenant
        without a second lookup."""
        return self._assign.tags

    @property
    def sample_rate(self) -> int:
        return self._session.sample_rate

    @property
    def ended(self) -> bool:
        return self._ended.is_set()

    # -- audio ------------------------------------------------------------
    async def recv_audio(self) -> bytes | None:
        """Next caller frame (pcm16), or None once the call ends."""
        pcm = await self._session.recv_frame()
        if pcm is None:
            self._ended.set()
        return pcm

    async def audio(self) -> AsyncIterator[bytes]:
        """Iterate caller audio until hangup: ``async for pcm in call.audio()``."""
        while True:
            pcm = await self.recv_audio()
            if pcm is None:
                return
            yield pcm

    async def send_audio(self, pcm: bytes) -> None:
        """Queue agent audio for the caller (pcm16 @ ``sample_rate``)."""
        await self._session.send_frame(pcm)

    async def flush(self) -> None:
        """Mark the current outbound segment complete (enables playout reports)."""
        await self._session.signal_flush()

    async def clear(self) -> None:
        """Barge-in: drop buffered agent audio so playout stops immediately."""
        await self._session.signal_clear()

    async def next_playout(self) -> PlayoutReport:
        return await self._session.next_playout()

    # -- platform parity --------------------------------------------------
    def send_transcript(self, role: str, text: str, is_final: bool = True) -> None:
        """Forward a turn ('user'/'assistant') into platform transcript
        storage, analytics, and ``voice.transcript.ready`` webhooks."""
        self._session.send_transcript(role, text, is_final)

    async def aclose(self) -> None:
        self._ended.set()
        await self._session.aclose()


Handler = Callable[[Call], Awaitable[None]]
# Lifecycle hooks — sync or async both work.
ReadyHook = Callable[[], object]
DisconnectHook = Callable[[Exception], object]


async def _fire(cb: Optional[Callable], *args) -> None:
    if cb is None:
        return
    try:
        res = cb(*args)
        if inspect.isawaitable(res):
            await res
    except Exception:  # noqa: BLE001 — a bad hook must never take the worker down
        logger.exception("telequick: lifecycle hook raised")


async def serve(
    handler: Handler,
    cfg: AgentConfig,
    *,
    on_ready: Optional[ReadyHook] = None,
    on_disconnected: Optional[DisconnectHook] = None,
) -> None:
    """Connect, authenticate, register, and run ``handler`` per assigned call.

    Reconnects with exponential backoff on any drop (engine restart, network),
    so presence recovers without operator action. Runs until cancelled.

    To know the worker is live, pass ``on_ready`` (fires when connected +
    registered, and again after every reconnect) and ``on_disconnected`` (fires
    on a drop, before the retry). Both may be sync or async.
    """
    from .quic_client import media_connection

    async def _serve_one(assign: JobAssign, session: WebTransportSession) -> None:
        call = Call(assign, session)
        try:
            await handler(call)
        except Exception:  # noqa: BLE001
            logger.exception("telequick: handler failed for call_id=%s", assign.call_id)
        finally:
            if not session.closed:
                await call.aclose()

    backoff = 1.0
    while True:
        try:
            async with media_connection(
                cfg._wt(), host=cfg.host, port=cfg.port, verify=cfg.verify
            ) as mc:
                logger.info("telequick worker ready (agent=%s) — awaiting calls", cfg.agent_name)
                backoff = 1.0
                await _fire(on_ready)
                closed = getattr(mc, "closed_event", None)
                tasks: set[asyncio.Task] = set()
                try:
                    while True:
                        next_task = asyncio.ensure_future(mc.next_call())
                        waiters = [next_task]
                        closed_task = asyncio.ensure_future(closed.wait()) if closed else None
                        if closed_task is not None:
                            waiters.append(closed_task)
                        await asyncio.wait(waiters, return_when=asyncio.FIRST_COMPLETED)
                        if closed is not None and closed.is_set():
                            next_task.cancel()
                            if closed_task is not None:
                                closed_task.cancel()
                            raise ConnectionError("engine connection closed")
                        if closed_task is not None:
                            closed_task.cancel()
                        assign, session = next_task.result()
                        logger.info(
                            "telequick: call_id=%s from=%s to=%s → handler",
                            assign.call_id, assign.caller_number, assign.called_number,
                        )
                        t = asyncio.create_task(_serve_one(assign, session))
                        tasks.add(t)
                        t.add_done_callback(tasks.discard)
                finally:
                    for t in tasks:
                        t.cancel()
        except asyncio.CancelledError:
            raise
        except Exception as e:  # noqa: BLE001 — reconnect on any transport failure
            await _fire(on_disconnected, e)
            logger.warning(
                "telequick worker connection lost (%s: %s) — reconnecting in %.1fs",
                type(e).__name__, e, backoff,
            )
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 30.0)


def run(
    handler: Handler,
    cfg: AgentConfig | None = None,
    *,
    on_ready: Optional[ReadyHook] = None,
    on_disconnected: Optional[DisconnectHook] = None,
) -> None:
    """Blocking convenience wrapper: ``run(handler)`` with env-based config."""
    asyncio.run(
        serve(handler, cfg or AgentConfig.from_env(), on_ready=on_ready, on_disconnected=on_disconnected)
    )
