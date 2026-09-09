/**
 * MediaConnection — one authenticated WebTransport/QUIC session per agent.
 *
 * Owns the control stream + datagram plane and runs the auth handshake BEFORE
 * any media flows. Demuxes concurrent calls by call_id: each `job_assign` mints
 * a WebTransportSession the worker hands to your call handler. Tenant isolation
 * is enforced authoritatively engine-side; the pre-auth guards here are defense
 * in depth.
 */

import * as wire from "./wire.js";
import * as auth from "./auth.js";
import { WebTransportSession, AsyncQueue, type WebTransportConfig } from "./transport.js";

export class AuthError extends Error {
  constructor(public code: number | null, msg = "") {
    super(msg || `auth failed (code=${code})`);
  }
}

export interface JobAssign {
  callId: number;
  roomName: string;
  agentId: string;
  callerNumber: string; // ANI
  calledNumber: string; // DNIS
  trunkId: string;
  attributes: Record<string, unknown>;
  /** Operator-defined context KVs from the agent config, forwarded on dispatch. */
  metadata: Record<string, string>;
  /** The agent's effective resource tags (key/value, inherited down the
   *  platform's parent chain) — governed cost-allocation / ownership /
   *  environment labels. Distinct from `metadata` (free-form context). */
  tags: Record<string, string>;
}

function parseMetadata(v: unknown): Record<string, string> {
  if (v == null || typeof v !== "object" || Array.isArray(v)) return {};
  return Object.fromEntries(
    Object.entries(v as Record<string, unknown>).map(([k, val]) => [k, String(val)]),
  );
}

export class MediaConnection {
  private ctrlIn = new AsyncQueue<wire.CtrlMsg>();
  private authed = false;
  private socketId: string | null = null;
  private calls = new Map<number, WebTransportSession>();
  private assignWaiters = new AsyncQueue<[JobAssign, WebTransportSession]>();
  /** Resolves when the underlying QUIC connection terminates (set by the client). */
  closedEvent: Promise<void> | null = null;

  constructor(
    private cfg: WebTransportConfig,
    private sendDatagram: (b: Uint8Array) => void,
    private sendStream: (b: Uint8Array) => void,
  ) {}

  // -- fed by the connection reader ------------------------------------
  onControl(msg: wire.CtrlMsg): void {
    if (!this.authed) {
      this.ctrlIn.put(msg); // welcome / ready / error → the handshake
      return;
    }
    if (msg.op === "job_assign") {
      this.onJobAssign(msg);
      return;
    }
    const cid = msg["call_id"] as number | undefined;
    if (cid !== undefined) this.calls.get(cid)?.onControl(msg);
  }

  onDatagram(buf: Uint8Array): void {
    if (!this.authed) return; // never accept media before auth
    let dg: wire.Datagram;
    try {
      dg = wire.decodeDatagram(buf);
    } catch {
      return;
    }
    this.calls.get(dg.callId)?.onDatagram(buf);
  }

  // -- handshake: hello -> welcome -> auth -> ready --------------------
  async authenticate(timeoutMs = 10_000): Promise<void> {
    this.sendStream(wire.encodeCtrl("hello", { app_key: this.cfg.appKey }));
    const welcome = await this.await1(timeoutMs);
    if (welcome.op !== "welcome" || typeof welcome["socket_id"] !== "string") {
      throw new AuthError(null, `expected welcome, got ${JSON.stringify(welcome)}`);
    }
    this.socketId = welcome["socket_id"] as string;
    const token = auth.mediaAuth(this.cfg.appKey, this.cfg.appSecret, this.socketId, this.cfg.agentName);
    this.sendStream(
      wire.encodeCtrl("auth", {
        app_key: this.cfg.appKey,
        channel: auth.MEDIA_CHANNEL,
        channel_data: this.cfg.agentName,
        auth: token,
      }),
    );
    const resp = await this.await1(timeoutMs);
    if (resp.op !== "ready") {
      throw new AuthError((resp["code"] as number) ?? null, String(resp.op ?? ""));
    }
    this.authed = true;
  }

  private await1(timeoutMs: number): Promise<wire.CtrlMsg> {
    return Promise.race([
      this.ctrlIn.get(),
      new Promise<wire.CtrlMsg>((_, rej) =>
        setTimeout(() => rej(new AuthError(null, "handshake timeout")), timeoutMs),
      ),
    ]);
  }

  // -- per-call sessions ----------------------------------------------
  private onJobAssign(msg: wire.CtrlMsg): void {
    const cid = Number(msg["call_id"]);
    const sess = new WebTransportSession(
      { ...this.cfg, callId: cid },
      this.sendDatagram,
      this.sendStream,
    );
    this.calls.set(cid, sess);
    const assign: JobAssign = {
      callId: cid,
      roomName: String(msg["room"] ?? ""),
      agentId: String(msg["agent_id"] ?? ""),
      callerNumber: String(msg["caller"] ?? ""),
      calledNumber: String(msg["called"] ?? ""),
      trunkId: String(msg["trunk_id"] ?? ""),
      attributes: (msg["attrs"] as Record<string, unknown>) ?? {},
      metadata: parseMetadata(msg["metadata"]),
      tags: parseMetadata(msg["tags"]),
    };
    this.assignWaiters.put([assign, sess]);
  }

  /** Await the next call routed to this worker. */
  nextCall(): Promise<[JobAssign, WebTransportSession]> {
    return this.assignWaiters.get();
  }

  get authenticated(): boolean {
    return this.authed;
  }

  /** Keepalive over the control stream (the engine ignores the op). */
  ping(): void {
    this.sendStream(wire.encodeCtrl("ping", {}));
  }
}
