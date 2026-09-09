"""TeleQuick agent-transport wire protocol.

Two lanes over one WebTransport/QUIC session (frozen spec):

  * MEDIA — WebTransport **datagrams**. Fixed 6-byte header + payload:

        version(1) type(1) call_id(2, BE) seq(2, BE)  | payload

    `call_id` demuxes concurrent calls on a shared session; `seq` lets the
    receiver detect loss/reorder (a dropped datagram is a lost 20 ms, never a
    stall). The audio codec + sample rate are negotiated ONCE on the control
    stream (job_assign.audio), so datagrams stay lean and carry only payload.

  * CONTROL — a bidi **stream** of newline-delimited JSON envelopes, same shape
    style as the Pusher frames mod_realtime already speaks:

        {"op":"job_assign", "call_id":.., "room":.., "audio":{...}, ...}
        {"op":"flush","call_id":..}  {"op":"clear","call_id":..}
        {"op":"playout","call_id":..,"position":..,"interrupted":..}
        {"op":"shutdown","call_id":..}   {"op":"register",...} {"op":"ready"}

This module is pure (no I/O) so the framing is unit-tested without a socket;
`transport.WebTransportSession` binds it to an aioquic connection.
"""

from __future__ import annotations

import json
from dataclasses import dataclass

WIRE_VERSION = 1

# datagram types
DG_AUDIO = 1

_HEADER_LEN = 6


@dataclass
class Datagram:
    call_id: int
    seq: int
    payload: bytes
    type: int = DG_AUDIO
    version: int = WIRE_VERSION


def encode_datagram(dg: Datagram) -> bytes:
    if not (0 <= dg.call_id <= 0xFFFF):
        raise ValueError(f"call_id out of range: {dg.call_id}")
    if not (0 <= dg.seq <= 0xFFFF):
        raise ValueError(f"seq out of range: {dg.seq}")
    header = bytes(
        (
            dg.version & 0xFF,
            dg.type & 0xFF,
            (dg.call_id >> 8) & 0xFF,
            dg.call_id & 0xFF,
            (dg.seq >> 8) & 0xFF,
            dg.seq & 0xFF,
        )
    )
    return header + dg.payload


def decode_datagram(buf: bytes) -> Datagram:
    if len(buf) < _HEADER_LEN:
        raise ValueError(f"short datagram: {len(buf)} bytes")
    version, type_ = buf[0], buf[1]
    if version != WIRE_VERSION:
        raise ValueError(f"unsupported wire version {version}")
    call_id = (buf[2] << 8) | buf[3]
    seq = (buf[4] << 8) | buf[5]
    return Datagram(call_id=call_id, seq=seq, payload=buf[_HEADER_LEN:], type=type_, version=version)


def encode_ctrl(op: str, **fields) -> bytes:
    """One newline-terminated control envelope."""
    obj = {"op": op, **fields}
    return (json.dumps(obj, separators=(",", ":")) + "\n").encode()


def decode_ctrl_stream(buf: bytes) -> tuple[list[dict], bytes]:
    """Parse whole envelopes from a byte buffer.

    Returns (messages, remainder) — the remainder is a partial trailing line to
    be prepended to the next read. Robust to envelopes split across QUIC STREAM
    frames.
    """
    msgs: list[dict] = []
    while (nl := buf.find(b"\n")) != -1:
        line, buf = buf[:nl], buf[nl + 1 :]
        line = line.strip()
        if line:
            msgs.append(json.loads(line))
    return msgs, buf
