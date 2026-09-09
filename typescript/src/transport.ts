/**
 * Transport primitives (TypeScript) — one call's media over QUIC.
 *
 * Caller audio arrives inbound via datagrams; agent audio (your TTS) goes out
 * via a REAL-TIME PACED sender: TTS arrives bursty in arbitrary chunk sizes, but
 * the caller leg needs a steady 8 kHz / 20 ms stream, so `sendFrame` buffers and
 * a 20 ms pacer emits fixed frames. The wire codec is raw pcm16 passthrough, so
 * whatever you send must already be pcm16 at `sampleRate` (8000 for telephony).
 */

import * as wire from "./wire.js";

export interface PlayoutReport {
  playbackPosition: number;
  interrupted: boolean;
}

export interface WebTransportConfig {
  engineUrl: string; // e.g. "https://voice.telequick.dev"
  appKey: string;
  appSecret: string;
  agentName: string;
  callId: number;
  sampleRate: number; // wire rate — 8000 for telephony
  numChannels: number;
  codec: "pcm16";
}

export function defaultConfig(
  p: Partial<WebTransportConfig> & { engineUrl: string; appKey: string; appSecret: string },
): WebTransportConfig {
  return { agentName: "", callId: 0, sampleRate: 8000, numChannels: 1, codec: "pcm16", ...p };
}

/** Simple async queue: backpressure-free put + awaitable get. */
export class AsyncQueue<T> {
  private items: T[] = [];
  private waiters: ((v: T) => void)[] = [];
  put(v: T): void {
    const w = this.waiters.shift();
    if (w) w(v);
    else this.items.push(v);
  }
  get(): Promise<T> {
    const v = this.items.shift();
    if (v !== undefined) return Promise.resolve(v);
    return new Promise<T>((resolve) => this.waiters.push(resolve));
  }
}

export class WebTransportSession {
  private inbound = new AsyncQueue<Uint8Array | null>();
  private playout = new AsyncQueue<PlayoutReport>();
  private txSeq = 0;
  private outBuf: number[] = []; // queued pcm16 bytes
  private frameBytes: number;
  private pacer: ReturnType<typeof setInterval> | null = null;
  private _closed = false;

  constructor(
    private cfg: WebTransportConfig,
    private sendDatagram: (b: Uint8Array) => void,
    private sendStream: (b: Uint8Array) => void,
  ) {
    this.frameBytes = Math.max(1, Math.floor(cfg.sampleRate * 0.02)) * 2 * cfg.numChannels;
  }

  get sampleRate(): number {
    return this.cfg.sampleRate;
  }
  get closed(): boolean {
    return this._closed;
  }

  // -- fed by the connection readers -----------------------------------
  onDatagram(buf: Uint8Array): void {
    const dg = wire.decodeDatagram(buf);
    if (dg.callId !== this.cfg.callId || dg.type !== wire.DG_AUDIO) return;
    this.inbound.put(dg.payload);
  }

  onControl(msg: wire.CtrlMsg): void {
    const cid = msg["call_id"] as number | undefined;
    if (cid !== undefined && cid !== this.cfg.callId) return;
    if (msg.op === "playout") {
      this.playout.put({
        playbackPosition: Number(msg["position"] ?? 0),
        interrupted: Boolean(msg["interrupted"] ?? false),
      });
    } else if (msg.op === "shutdown") {
      this.inbound.put(null);
    }
  }

  // -- media -----------------------------------------------------------
  /** Next caller frame (pcm16), or null once the call ends. */
  recvFrame(): Promise<Uint8Array | null> {
    return this.inbound.get();
  }

  /** Queue agent audio (pcm16 @ sampleRate); paced onto the wire as 20 ms frames. */
  async sendFrame(pcm: Uint8Array): Promise<void> {
    for (let i = 0; i < pcm.length; i++) this.outBuf.push(pcm[i]!);
    if (this.pacer === null && !this._closed) {
      this.pacer = setInterval(() => this.drain(), 20);
    }
  }

  private drain(): void {
    if (this._closed) return;
    if (this.outBuf.length >= this.frameBytes) {
      const chunk = Uint8Array.from(this.outBuf.splice(0, this.frameBytes));
      const dg = wire.makeDatagram(this.cfg.callId, this.txSeq, chunk);
      this.txSeq = (this.txSeq + 1) & 0xffff;
      this.sendDatagram(wire.encodeDatagram(dg));
    }
  }

  async signalFlush(): Promise<void> {
    this.sendStream(wire.encodeCtrl("flush", { call_id: this.cfg.callId }));
  }

  /** Barge-in: drop queued agent audio so playout stops immediately. */
  async signalClear(): Promise<void> {
    this.outBuf.length = 0;
    this.sendStream(wire.encodeCtrl("clear", { call_id: this.cfg.callId }));
  }

  /** Forward a transcript turn to the platform (storage + analytics + webhooks). */
  sendTranscript(role: string, text: string, isFinal = true): void {
    this.sendStream(
      wire.encodeCtrl("transcript", { call_id: this.cfg.callId, role, text, is_final: isFinal }),
    );
  }

  /**
   * Report a tool this agent invoked, so the platform logs it exactly as it
   * logs a native agent's (analytics timeline + `voice.agent.tool_called`
   * webhooks). Your tool loop is invisible to the engine — nothing is recorded
   * unless you call this.
   *
   * REDACT SENSITIVE VALUES YOURSELF: `args`/`result` are forwarded verbatim as
   * opaque JSON. An empty `tool` is dropped engine-side.
   */
  sendToolCall(
    tool: string,
    args: Record<string, unknown> = {},
    result: Record<string, unknown> = {},
  ): void {
    if (!tool) return;
    this.sendStream(wire.encodeCtrl("tool_call", { call_id: this.cfg.callId, tool, args, result }));
  }

  /**
   * Report per-stage usage for token analytics. You run STT/LLM/TTS on your OWN
   * provider keys, so the platform only sees what you forward.
   *
   * `stage` is "stt" | "llm" | "tts"; `fields` carries the numbers (llm:
   * `{promptTokens, completionTokens}`, stt: `{audioSeconds}`, tts:
   * `{characters}`). An empty `stage` is dropped engine-side.
   */
  sendUsage(
    stage: "stt" | "llm" | "tts",
    fields: Record<string, unknown> = {},
    provider = "",
    model = "",
  ): void {
    if (!stage) return;
    this.sendStream(
      wire.encodeCtrl("usage", { call_id: this.cfg.callId, stage, provider, model, fields }),
    );
  }

  nextPlayout(): Promise<PlayoutReport> {
    return this.playout.get();
  }

  async aclose(): Promise<void> {
    this._closed = true;
    if (this.pacer !== null) {
      clearInterval(this.pacer);
      this.pacer = null;
    }
    this.sendStream(wire.encodeCtrl("shutdown", { call_id: this.cfg.callId }));
  }
}
