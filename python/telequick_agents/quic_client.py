"""aioquic binding — the WebTransport/QUIC socket under MediaConnection.

This is the thin, on-infra I/O layer: everything it drives (framing, auth
handshake, per-call demux, isolation) is already unit-tested via injected
callables. Here we just: open a QUIC connection, negotiate a WebTransport
session to `/media/<app_key>`, open the control bidi stream, and pump events
into `MediaConnection.on_datagram` / `on_control` while exposing
`send_datagram` / `send_stream`.

Needs the `quic` extra (`pip install 'telequick-agents[quic]'`) and a
live engine `/media/` handler (see ENGINE_MEDIA_HANDLER.md). aioquic is imported
lazily so the rest of the package imports without it.

Transport notes (repo standing rules): QUIC handshake is ECDSA, not RSA;
multi-brand SNI per cert; `verify` must validate the engine cert in prod.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
from typing import AsyncIterator

from . import wire
from .connection import MediaConnection
from .transport import WebTransportConfig

logger = logging.getLogger("telequick")


@contextlib.asynccontextmanager
async def media_connection(
    cfg: WebTransportConfig, *, host: str, port: int = 443, verify: bool = True
) -> AsyncIterator[MediaConnection]:
    """Open + authenticate a WebTransport/QUIC session to /media/<app_key>.

    Yields the authenticated `MediaConnection`; the QUIC connection stays open
    for the duration of the `async with` body.
    """
    try:
        from aioquic.asyncio import connect
        from aioquic.asyncio.protocol import QuicConnectionProtocol
        from aioquic.h3.connection import H3_ALPN, H3Connection
        from aioquic.h3.events import (
            DatagramReceived,
            HeadersReceived,
            WebTransportStreamDataReceived,
        )
        from aioquic.quic.configuration import QuicConfiguration
        from aioquic.quic.events import ProtocolNegotiated
    except ImportError as e:  # pragma: no cover
        raise RuntimeError(
            "install the 'quic' extra: pip install 'telequick-agents[quic]'"
        ) from e

    path = f"/media/{cfg.app_key}"

    class _Client(QuicConnectionProtocol):
        def __init__(self, *a, **k):
            super().__init__(*a, **k)
            self._h3: H3Connection | None = None
            self._session_id: int | None = None
            self._ctrl: int | None = None  # single client-opened bidi control stream
            self._ctrl_buf = b""
            self.ready = asyncio.Event()
            self.mc: MediaConnection | None = None
            # Set when the QUIC connection terminates (engine restart, idle,
            # network drop). run_agent_worker watches this to reconnect instead
            # of hanging forever on a dead next_call().
            self.closed = asyncio.Event()
            # Liveness: our keepalive pings are ack-eliciting, so a HEALTHY
            # connection receives ACK packets ~every ping. When the engine
            # restarts, packets stop arriving even though aioquic (kept "active"
            # by our own sends) never fires connection_lost. The watchdog treats
            # a long RX gap as a dead peer. import time locally to avoid a
            # module-level dep.
            import time as _t
            self._mono = _t.monotonic
            self.last_rx = self._mono()

        def datagram_received(self, data, addr):  # type: ignore[no-untyped-def]
            self.last_rx = self._mono()
            super().datagram_received(data, addr)

        def connection_lost(self, exc):  # type: ignore[no-untyped-def]
            self.closed.set()
            super().connection_lost(exc)

        def _send_datagram(self, data: bytes) -> None:
            self._h3.send_datagram(self._session_id, data)
            self.transmit()

        def _send_stream(self, data: bytes) -> None:
            # Control rides ONE client-opened BIDI WT stream; the engine replies on
            # it (matches the engine's wt_session: on_stream(bidi) + wt_stream.write,
            # with no server-initiated streams). aioquic doesn't mark the initiator's
            # OWN bidi WT stream for INBOUND parsing, so without this the engine's
            # replies never surface as WebTransportStreamDataReceived. Set it here.
            if self._ctrl is None:
                from aioquic.h3.connection import FrameType

                self._ctrl = self._h3.create_webtransport_stream(
                    self._session_id, is_unidirectional=False
                )
                with self._h3._get_or_create_stream(self._ctrl) as st:
                    st.frame_type = FrameType.WEBTRANSPORT_STREAM
                    st.session_id = self._session_id
            self._quic.send_stream_data(self._ctrl, data, end_stream=False)
            self.transmit()

        def open_session(self) -> None:
            self._h3 = H3Connection(self._quic, enable_webtransport=True)
            sid = self._quic.get_next_available_stream_id()
            self._session_id = sid
            self._h3.send_headers(
                sid,
                [
                    (b":method", b"CONNECT"),
                    (b":protocol", b"webtransport"),
                    (b":scheme", b"https"),
                    (b":authority", host.encode()),
                    (b":path", path.encode()),
                ],
            )
            self.mc = MediaConnection(
                cfg, send_datagram=self._send_datagram, send_stream=self._send_stream
            )
            self.transmit()

        def quic_event_received(self, event) -> None:
            if isinstance(event, ProtocolNegotiated):
                return
            if self._h3 is None:
                return
            for h3ev in self._h3.handle_event(event):
                self._on_h3(h3ev)

        def _on_h3(self, ev) -> None:
            if isinstance(ev, HeadersReceived) and ev.stream_id == self._session_id:
                status = dict(ev.headers).get(b":status", b"")
                if status.startswith(b"2"):
                    self.ready.set()  # session accepted; uni stream opened on first send
            elif isinstance(ev, DatagramReceived) and self.mc is not None:
                self.mc.on_datagram(ev.data)
            elif isinstance(ev, WebTransportStreamDataReceived) and self.mc is not None:
                # server->client control on the server's uni WT stream
                self._ctrl_buf += ev.data
                msgs, self._ctrl_buf = wire.decode_ctrl_stream(self._ctrl_buf)
                for m in msgs:
                    self.mc.on_control(m)

    # max_datagram_frame_size MUST be set or QUIC datagrams (our media plane) are
    # disabled and every frame is silently dropped.
    config = QuicConfiguration(
        alpn_protocols=H3_ALPN, is_client=True, max_datagram_frame_size=65536
    )
    if not verify:
        import ssl

        config.verify_mode = ssl.CERT_NONE  # dev only — prod must validate the engine cert

    async with connect(host, port, configuration=config, create_protocol=_Client) as proto:
        proto.open_session()
        await asyncio.wait_for(proto.ready.wait(), 10)
        await proto.mc.authenticate()

        # Keepalive: an idle register-and-wait agent sends no media, so without
        # this the QUIC session idle-times-out and the engine drops it — taking
        # the presence key with it, so inbound calls find no agent. Ping well
        # under the ~60s idle timeout.
        async def _keepalive() -> None:
            while True:
                await asyncio.sleep(20)
                try:
                    proto.mc.ping()
                except Exception:
                    proto.closed.set()  # can't even send → peer gone
                    return

        # RX watchdog: if no packet arrives for > RX_DEADLINE while we're still
        # pinging (which a live peer would ACK), the engine is gone — mark the
        # connection closed so the worker reconnects.
        RX_DEADLINE = 45.0
        async def _watchdog() -> None:
            import time as _t
            while True:
                await asyncio.sleep(10)
                if _t.monotonic() - proto.last_rx > RX_DEADLINE:
                    proto.closed.set()
                    return

        ka = asyncio.ensure_future(_keepalive())
        wd = asyncio.ensure_future(_watchdog())
        # Surface connection loss to the worker so it can reconnect.
        proto.mc.closed_event = proto.closed  # type: ignore[attr-defined]
        try:
            yield proto.mc
        finally:
            ka.cancel()
            wd.cancel()
