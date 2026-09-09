/**
 * WebTransport client binding (TypeScript) — the QUIC socket under MediaConnection.
 *
 * Node has no built-in WebTransport client, so this dynamically loads
 * `@fails-components/webtransport` (native QUIC). One outbound session:
 *   - control on ONE client-opened bidi stream (hello/auth/job_assign/flush/…)
 *   - media on datagrams
 *   - keepalive ping every 20 s + an RX watchdog: if no packet arrives for
 *     > RX_DEADLINE the peer is gone (engine restart) -> `closed` resolves.
 * `serve()` owns the reconnect loop and races nextCall() against `closed`.
 */

import * as wire from "./wire.js";
import { MediaConnection } from "./connection.js";
import type { WebTransportConfig } from "./transport.js";

interface WTReader {
  read(): Promise<{ value?: Uint8Array; done: boolean }>;
}
interface WTWriter {
  write(b: Uint8Array): unknown;
}
interface WTReadable {
  getReader(): WTReader;
}
interface WTWritable {
  getWriter(): WTWriter;
}
interface WTStream {
  readable: WTReadable;
  writable: WTWritable;
}
interface WTLike {
  ready: Promise<void>;
  closed: Promise<unknown>;
  datagrams: { readable: WTReadable; writable: WTWritable };
  createBidirectionalStream(): Promise<WTStream>;
  close(info?: { closeCode?: number; reason?: string }): void;
}
type WTCtor = new (url: string, opts?: unknown) => WTLike;

const RX_DEADLINE_MS = 45_000;
const KEEPALIVE_MS = 20_000;

export interface OpenOpts {
  host: string;
  port?: number;
  verify?: boolean;
}

export interface OpenedConnection {
  mc: MediaConnection;
  closed: Promise<void>;
  close: () => Promise<void>;
}

export async function openMediaConnection(
  cfg: WebTransportConfig,
  opts: OpenOpts,
): Promise<OpenedConnection> {
  const port = opts.port ?? 443;
  const url = `https://${opts.host}:${port}/media/${cfg.appKey}`;

  // Variable specifier so tsc doesn't try to resolve the optional native dep.
  const spec = "@fails-components/webtransport";
  const mod: any = await import(spec);
  const WT: WTCtor = mod.WebTransport ?? mod.default?.WebTransport ?? mod.default;
  const wt: WTLike = new WT(url, opts.verify === false ? { serverCertificateHashes: [] } : undefined);
  await wt.ready;

  let lastRx = Date.now();
  let onClosed!: () => void;
  const closed = new Promise<void>((resolve) => (onClosed = resolve));

  const ctrl = await wt.createBidirectionalStream();
  const ctrlWriter = ctrl.writable.getWriter();
  const dgWriter = wt.datagrams.writable.getWriter();
  const sendStream = (b: Uint8Array): void => void ctrlWriter.write(b);
  const sendDatagram = (b: Uint8Array): void => void dgWriter.write(b);

  const mc = new MediaConnection(cfg, sendDatagram, sendStream);

  const readCtrl = async () => {
    const reader = ctrl.readable.getReader();
    let buf: Uint8Array = new Uint8Array(0);
    try {
      for (;;) {
        const { value, done } = await reader.read();
        if (done) break;
        lastRx = Date.now();
        if (value) {
          const merged = new Uint8Array(buf.length + value.length);
          merged.set(buf);
          merged.set(value, buf.length);
          const [msgs, rem] = wire.decodeCtrlStream(merged);
          buf = rem;
          for (const m of msgs) mc.onControl(m);
        }
      }
    } catch {
      /* stream error → watchdog/closed handles it */
    }
  };
  const readDatagrams = async () => {
    const reader = wt.datagrams.readable.getReader();
    try {
      for (;;) {
        const { value, done } = await reader.read();
        if (done) break;
        lastRx = Date.now();
        if (value) mc.onDatagram(value);
      }
    } catch {
      /* ignore */
    }
  };
  void readCtrl();
  void readDatagrams();

  await mc.authenticate();

  const keepalive = setInterval(() => {
    try {
      mc.ping();
    } catch {
      onClosed();
    }
  }, KEEPALIVE_MS);
  const watchdog = setInterval(() => {
    if (Date.now() - lastRx > RX_DEADLINE_MS) onClosed();
  }, 10_000);
  void wt.closed.then(onClosed, onClosed);

  mc.closedEvent = closed;

  const close = async (): Promise<void> => {
    clearInterval(keepalive);
    clearInterval(watchdog);
    try {
      wt.close();
    } catch {
      /* already closing */
    }
  };
  void closed.then(close);

  return { mc, closed, close };
}
