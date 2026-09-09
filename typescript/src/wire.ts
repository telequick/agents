/**
 * TeleQuick agent-transport wire protocol (TypeScript).
 *
 * Two lanes over one WebTransport/QUIC session (frozen spec, byte-identical to
 * the Python `telequick_agents.wire` and the engine's media handler):
 *
 *   MEDIA — WebTransport datagrams. Fixed 6-byte header + payload:
 *       version(1) type(1) call_id(2, BE) seq(2, BE) | payload
 *     `call_id` demuxes concurrent calls on the shared session; `seq` lets the
 *     receiver detect loss/reorder (a dropped datagram is a lost 20 ms, never a
 *     stall). Codec + sample rate are negotiated once on the control stream, so
 *     datagrams stay lean.
 *
 *   CONTROL — a bidi stream of newline-delimited JSON envelopes:
 *       {"op":"job_assign","call_id":..,"room":..,"caller":..,"called":..}
 *       {"op":"flush","call_id":..}  {"op":"clear","call_id":..}
 *       {"op":"playout","call_id":..,"position":..,"interrupted":..}
 *       {"op":"shutdown","call_id":..}  {"op":"transcript",...}  {"op":"ready"}
 *
 * Pure (no I/O) so the framing is unit-tested without a socket.
 */

export const WIRE_VERSION = 1;
export const DG_AUDIO = 1;
const HEADER_LEN = 6;

export interface Datagram {
  callId: number;
  seq: number;
  payload: Uint8Array;
  type: number;
  version: number;
}

export function makeDatagram(
  callId: number,
  seq: number,
  payload: Uint8Array,
  type = DG_AUDIO,
): Datagram {
  return { callId, seq, payload, type, version: WIRE_VERSION };
}

export function encodeDatagram(dg: Datagram): Uint8Array {
  if (!(dg.callId >= 0 && dg.callId <= 0xffff)) {
    throw new RangeError(`call_id out of range: ${dg.callId}`);
  }
  if (!(dg.seq >= 0 && dg.seq <= 0xffff)) {
    throw new RangeError(`seq out of range: ${dg.seq}`);
  }
  const out = new Uint8Array(HEADER_LEN + dg.payload.length);
  out[0] = dg.version & 0xff;
  out[1] = dg.type & 0xff;
  out[2] = (dg.callId >> 8) & 0xff;
  out[3] = dg.callId & 0xff;
  out[4] = (dg.seq >> 8) & 0xff;
  out[5] = dg.seq & 0xff;
  out.set(dg.payload, HEADER_LEN);
  return out;
}

export function decodeDatagram(buf: Uint8Array): Datagram {
  if (buf.length < HEADER_LEN) {
    throw new RangeError(`short datagram: ${buf.length} bytes`);
  }
  const version = buf[0]!;
  const type = buf[1]!;
  if (version !== WIRE_VERSION) {
    throw new RangeError(`unsupported wire version ${version}`);
  }
  const callId = (buf[2]! << 8) | buf[3]!;
  const seq = (buf[4]! << 8) | buf[5]!;
  return { callId, seq, payload: buf.slice(HEADER_LEN), type, version };
}

const textEncoder = new TextEncoder();
const textDecoder = new TextDecoder();

/** One newline-terminated control envelope (`op` serialized first, matches Python). */
export function encodeCtrl(op: string, fields: Record<string, unknown> = {}): Uint8Array {
  return textEncoder.encode(JSON.stringify({ op, ...fields }) + "\n");
}

export interface CtrlMsg {
  op?: string;
  [k: string]: unknown;
}

/**
 * Parse whole envelopes from a growing byte buffer. Returns [messages, remainder]
 * — the remainder is a partial trailing line to prepend to the next read, so this
 * is robust to envelopes split across QUIC STREAM frames.
 */
export function decodeCtrlStream(buf: Uint8Array): [CtrlMsg[], Uint8Array] {
  const msgs: CtrlMsg[] = [];
  let start = 0;
  for (let i = 0; i < buf.length; i++) {
    if (buf[i] === 0x0a) {
      const line = textDecoder.decode(buf.subarray(start, i)).trim();
      if (line) msgs.push(JSON.parse(line) as CtrlMsg);
      start = i + 1;
    }
  }
  return [msgs, buf.slice(start)];
}
