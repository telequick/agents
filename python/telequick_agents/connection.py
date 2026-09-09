"""MediaConnection — one authenticated WebTransport/QUIC session per agent.

Owns the control stream + datagram plane, runs the auth handshake BEFORE any
media, and multiplexes per-call `WebTransportSession`s over the one connection.
Enforces call isolation on the client side too (datagrams / control for a
call_id the engine never assigned to us are dropped) — the engine remains the
authoritative check, this is defense in depth.

The aioquic socket binding feeds `on_control` / `on_datagram` and provides
`send_stream` / `send_datagram`; the same logic runs against an in-memory fake
in tests, so the handshake + isolation are provable without a socket.
"""

from __future__ import annotations

import asyncio
import logging

from . import auth, wire
from .types import JobAssign
from .transport import WebTransportConfig, WebTransportSession

logger = logging.getLogger("telequick")


class AuthError(Exception):
    def __init__(self, code: int | None, msg: str = "") -> None:
        super().__init__(f"auth failed (code={code}): {msg}")
        self.code = code


class MediaConnection:
    def __init__(self, cfg: WebTransportConfig, *, send_datagram, send_stream) -> None:
        self._cfg = cfg
        self._send_datagram = send_datagram
        self._send_stream = send_stream
        self._ctrl_in: asyncio.Queue[dict] = asyncio.Queue()
        self._authed = False
        self._socket_id: str | None = None
        # call_id -> per-call media session (only for engine-assigned calls)
        self._calls: dict[int, WebTransportSession] = {}
        self._assign_waiters: asyncio.Queue[tuple[JobAssign, WebTransportSession]] = asyncio.Queue()

    # -- fed by the connection reader ------------------------------------
    def on_control(self, msg: dict) -> None:
        # pre-auth control (welcome / ready / error) goes to the handshake queue
        if not self._authed:
            self._ctrl_in.put_nowait(msg)
            return
        op = msg.get("op")
        if op == "job_assign":
            self._on_job_assign(msg)
            return
        cid = msg.get("call_id")
        sess = self._calls.get(cid)
        if sess is None:
            logger.warning("telequick: control for unassigned call_id=%s dropped", cid)
            return
        sess.on_control(msg)

    def on_datagram(self, buf: bytes) -> None:
        if not self._authed:
            return  # never accept media before auth
        try:
            dg = wire.decode_datagram(buf)
        except ValueError:
            return
        sess = self._calls.get(dg.call_id)
        if sess is None:
            logger.warning("telequick: datagram for unassigned call_id=%s dropped", dg.call_id)
            return
        sess.on_datagram(buf)

    # -- handshake --------------------------------------------------------
    async def authenticate(self, *, timeout: float = 10.0) -> None:
        """hello -> welcome -> auth -> ready. Raises AuthError on rejection.

        The control stream is client-initiated, so we speak first (`hello`) to
        let the engine bind the stream and issue its `welcome` nonce.
        """
        self._send_stream(wire.encode_ctrl("hello", app_key=self._cfg.app_key))
        welcome = await asyncio.wait_for(self._ctrl_in.get(), timeout)
        if welcome.get("op") != "welcome" or "socket_id" not in welcome:
            raise AuthError(None, f"expected welcome, got {welcome!r}")
        self._socket_id = welcome["socket_id"]

        token = auth.media_auth(
            self._cfg.app_key, self._cfg.app_secret, self._socket_id, self._cfg.agent_name
        )
        self._send_stream(
            wire.encode_ctrl(
                "auth",
                app_key=self._cfg.app_key,
                channel=auth.MEDIA_CHANNEL,
                channel_data=self._cfg.agent_name,
                auth=token,
            )
        )
        resp = await asyncio.wait_for(self._ctrl_in.get(), timeout)
        if resp.get("op") != "ready":
            raise AuthError(resp.get("code"), resp.get("op", ""))
        self._authed = True
        logger.info("telequick: media connection authenticated (agent=%s)", self._cfg.agent_name)

    # -- per-call sessions ------------------------------------------------
    def _on_job_assign(self, msg: dict) -> WebTransportSession:
        from dataclasses import replace

        cid = int(msg["call_id"])
        cfg = replace(self._cfg, call_id=cid)
        sess = WebTransportSession(cfg, send_datagram=self._send_datagram, send_stream=self._send_stream)
        self._calls[cid] = sess
        assign = JobAssign(
            call_id=cid,
            room_name=msg.get("room", ""),
            agent_id=msg.get("agent_id", ""),
            caller_number=msg.get("caller", ""),
            called_number=msg.get("called", ""),
            trunk_id=msg.get("trunk_id", ""),
            attributes=msg.get("attrs", {}) or {},
            tags=msg.get("tags", {}) or {},
        )
        self._assign_waiters.put_nowait((assign, sess))
        return sess

    async def next_call(
        self, *, timeout: float | None = None
    ) -> tuple[JobAssign, WebTransportSession]:
        """Await the next engine-assigned call: its identity + media transport."""
        return await asyncio.wait_for(self._assign_waiters.get(), timeout)

    def ping(self) -> None:
        """Keepalive over the control stream.

        A register-and-wait agent sends no media while idle, so without traffic
        the QUIC session idle-times-out (~60s) and the engine tears it down —
        which lets the presence key (TTL 90, refreshed only while the session is
        alive) lapse, so the next inbound call finds no agent to dispatch to.
        A periodic `ping` keeps the session (and thus presence) alive. The
        engine ignores the op (unknown pre/post-auth ops are no-ops)."""
        self._send_stream(wire.encode_ctrl("ping"))

    @property
    def authenticated(self) -> bool:
        return self._authed
