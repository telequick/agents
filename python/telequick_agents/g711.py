"""G.711 μ-law / A-law ↔ 16-bit linear PCM.

The wire between your agent and the engine is pcm16, but several upstream
APIs the starters bridge to (OpenAI Realtime in Twilio's sample, for one)
speak ``g711_ulaw`` at 8 kHz. stdlib ``audioop`` did this but was removed in
Python 3.13, so this module carries a pure-Python fallback and uses
``audioop`` when it exists.

Fallback design: decode is the CCITT G.711 expansion formula; encode picks the
code point whose decoded value is nearest the sample (binary search over the
256-entry decode table). That makes encode/decode self-consistent by
construction — no companding-convention (13/14/16-bit) drift — at ~8 k
bisects/second of audio, which is nothing.
"""

from __future__ import annotations

from bisect import bisect_left

try:
    import audioop  # type: ignore[import-not-found]  # removed in Python 3.13

    _HAS_AUDIOOP = True
except ImportError:
    _HAS_AUDIOOP = False

_BIAS = 0x84


def _ulaw_decode_sample(u: int) -> int:
    u = ~u & 0xFF
    sign = u & 0x80
    seg = (u >> 4) & 0x07
    s = ((((u & 0x0F) << 3) + _BIAS) << seg) - _BIAS
    return -s if sign else s


def _alaw_decode_sample(a: int) -> int:
    a ^= 0x55
    t = (a & 0x0F) << 4
    seg = (a & 0x70) >> 4
    if seg == 0:
        t += 8
    else:
        t = (t + 0x108) << (seg - 1)
    # In A-law the (pre-XOR) sign bit SET means positive.
    return t if a & 0x80 else -t
    # (t is at most 0x7F80, so the 16-bit range is never exceeded.)


def _make_tables(decode):
    dec = [decode(i) for i in range(256)]
    pairs = sorted((v, i) for i, v in enumerate(dec))
    values = [v for v, _ in pairs]
    codes = [c for _, c in pairs]
    return dec, values, codes


_ULAW_DEC, _ULAW_VALUES, _ULAW_CODES = _make_tables(_ulaw_decode_sample)
_ALAW_DEC, _ALAW_VALUES, _ALAW_CODES = _make_tables(_alaw_decode_sample)


def _nearest_code(values: list[int], codes: list[int], s: int) -> int:
    i = bisect_left(values, s)
    if i == 0:
        return codes[0]
    if i == len(values):
        return codes[-1]
    return codes[i] if values[i] - s < s - values[i - 1] else codes[i - 1]


def _encode(pcm: bytes, values: list[int], codes: list[int]) -> bytes:
    n = len(pcm) // 2
    out = bytearray(n)
    for i in range(n):
        s = int.from_bytes(pcm[2 * i : 2 * i + 2], "little", signed=True)
        out[i] = _nearest_code(values, codes, s)
    return bytes(out)


def _decode(data: bytes, table: list[int]) -> bytes:
    out = bytearray(2 * len(data))
    for i, b in enumerate(data):
        out[2 * i : 2 * i + 2] = table[b].to_bytes(2, "little", signed=True)
    return bytes(out)


def pcm_to_ulaw(pcm: bytes) -> bytes:
    """16-bit LE PCM → μ-law bytes."""
    if _HAS_AUDIOOP:
        return audioop.lin2ulaw(pcm, 2)
    return _encode(pcm, _ULAW_VALUES, _ULAW_CODES)


def ulaw_to_pcm(ulaw: bytes) -> bytes:
    """μ-law bytes → 16-bit LE PCM."""
    if _HAS_AUDIOOP:
        return audioop.ulaw2lin(ulaw, 2)
    return _decode(ulaw, _ULAW_DEC)


def pcm_to_alaw(pcm: bytes) -> bytes:
    """16-bit LE PCM → A-law bytes."""
    if _HAS_AUDIOOP:
        return audioop.lin2alaw(pcm, 2)
    return _encode(pcm, _ALAW_VALUES, _ALAW_CODES)


def alaw_to_pcm(alaw: bytes) -> bytes:
    """A-law bytes → 16-bit LE PCM."""
    if _HAS_AUDIOOP:
        return audioop.alaw2lin(alaw, 2)
    return _decode(alaw, _ALAW_DEC)
