/**
 * High-level external-agent API: register-and-wait, handle calls.
 *
 * Your process dials ONE outbound QUIC/WebTransport session to the engine,
 * authenticates with your media app key/secret, registers under `agentName`, and
 * gets a `Call` for every phone call the platform routes to it:
 *
 *     import { AgentConfig, serve } from "@telequick/agents";
 *
 *     await serve(async (call) => {
 *       console.log("call from", call.callerNumber);
 *       for await (const pcm of call.audio()) {   // 20 ms pcm16 @ 8 kHz
 *         await call.sendAudio(pcm);              // echo
 *       }
 *     }, AgentConfig.fromEnv({ agentName: "salesbot" }));
 *
 * Route a number to the agent by pointing a platform agent's VENDOR_BRIDGE node
 * (vendor_room = agentName) at it — the console's "External agent" create flow
 * does this and mints the media key in one step.
 */

import { openMediaConnection } from "./quic-client.js";
import type { JobAssign } from "./connection.js";
import {
  WebTransportSession,
  type PlayoutReport,
  type WebTransportConfig,
} from "./transport.js";

/** Everything the worker needs to reach the engine and identify itself. */
export interface AgentConfigInit {
  host: string;
  appKey: string;
  appSecret: string;
  agentName: string;
  port?: number;
  sampleRate?: number; // PSTN legs are 8 kHz
  verify?: boolean; // dev only: false skips engine cert validation
}

export class AgentConfig {
  host: string;
  appKey: string;
  appSecret: string;
  agentName: string;
  port: number;
  sampleRate: number;
  verify: boolean;

  constructor(init: AgentConfigInit) {
    this.host = init.host;
    this.appKey = init.appKey;
    this.appSecret = init.appSecret;
    this.agentName = init.agentName;
    this.port = init.port ?? 443;
    this.sampleRate = init.sampleRate ?? 8000;
    this.verify = init.verify ?? true;
  }

  /**
   * Build from TELEQUICK_HOST / TELEQUICK_MEDIA_KEY / TELEQUICK_MEDIA_SECRET /
   * TELEQUICK_AGENT (any field overridable per option).
   */
  static fromEnv(overrides: Partial<AgentConfigInit> = {}): AgentConfig {
    const init: AgentConfigInit = {
      host: process.env.TELEQUICK_HOST ?? "",
      appKey: process.env.TELEQUICK_MEDIA_KEY ?? "",
      appSecret: process.env.TELEQUICK_MEDIA_SECRET ?? "",
      agentName: process.env.TELEQUICK_AGENT ?? "",
      ...overrides,
    };
    const missing = (["host", "appKey", "appSecret", "agentName"] as const).filter((k) => !init[k]);
    if (missing.length) {
      throw new Error(
        `AgentConfig missing ${missing.join(", ")} — set TELEQUICK_HOST / ` +
          "TELEQUICK_MEDIA_KEY / TELEQUICK_MEDIA_SECRET / TELEQUICK_AGENT or pass them explicitly",
      );
    }
    return new AgentConfig(init);
  }

  /** @internal */
  wt(callId = 0): WebTransportConfig {
    return {
      engineUrl: `https://${this.host}:${this.port}`,
      appKey: this.appKey,
      appSecret: this.appSecret,
      agentName: this.agentName,
      callId,
      sampleRate: this.sampleRate,
      numChannels: 1,
      codec: "pcm16",
    };
  }
}

/**
 * One live phone call handed to your worker. Audio in both directions is 16-bit
 * PCM at `sampleRate` (8 kHz for telephony). Inbound arrives as 20 ms frames;
 * outbound accepts any chunk size and is paced onto the wire as steady 20 ms
 * frames.
 */
export class Call {
  private endedFlag = false;

  constructor(
    private assign: JobAssign,
    private session: WebTransportSession,
  ) {}

  // -- identity ---------------------------------------------------------
  get callId(): number {
    return this.assign.callId;
  }
  get room(): string {
    return this.assign.roomName;
  }
  /** ANI — who is calling. */
  get callerNumber(): string {
    return this.assign.callerNumber;
  }
  /** DNIS — the number they dialed. */
  get calledNumber(): string {
    return this.assign.calledNumber;
  }
  get trunkId(): string {
    return this.assign.trunkId;
  }
  get attributes(): Record<string, unknown> {
    return this.assign.attributes;
  }
  /** Operator-defined context KVs from the agent config. */
  get metadata(): Record<string, string> {
    return this.assign.metadata;
  }
  /** The agent's effective resource tags ({key: value}) — the platform's
   *  governed cost-allocation / ownership / environment labels, inherited down
   *  the parent chain. Route, bill, or branch per tenant without a lookup. */
  get tags(): Record<string, string> {
    return this.assign.tags;
  }
  get sampleRate(): number {
    return this.session.sampleRate;
  }
  get ended(): boolean {
    return this.endedFlag;
  }

  // -- audio ------------------------------------------------------------
  /** Next caller frame (pcm16), or null once the call ends. */
  async recvAudio(): Promise<Uint8Array | null> {
    const pcm = await this.session.recvFrame();
    if (pcm === null) this.endedFlag = true;
    return pcm;
  }

  /** Iterate caller audio until hangup: `for await (const pcm of call.audio())`. */
  async *audio(): AsyncIterableIterator<Uint8Array> {
    for (;;) {
      const pcm = await this.recvAudio();
      if (pcm === null) return;
      yield pcm;
    }
  }

  /** Queue agent audio for the caller (pcm16 @ sampleRate). */
  async sendAudio(pcm: Uint8Array): Promise<void> {
    await this.session.sendFrame(pcm);
  }

  /** Mark the current outbound segment complete (enables playout reports). */
  async flush(): Promise<void> {
    await this.session.signalFlush();
  }

  /** Barge-in: drop buffered agent audio so playout stops immediately. */
  async clear(): Promise<void> {
    await this.session.signalClear();
  }

  async nextPlayout(): Promise<PlayoutReport> {
    return this.session.nextPlayout();
  }

  /** Forward a turn ('user'/'assistant') into platform transcript storage,
   * analytics, and voice.transcript.ready webhooks. */
  sendTranscript(role: "user" | "assistant", text: string, isFinal = true): void {
    this.session.sendTranscript(role, text, isFinal);
  }

  /** Report a tool you invoked → analytics timeline + `voice.agent.tool_called`
   *  webhooks. REDACT sensitive values yourself: args/result ride verbatim. */
  sendToolCall(
    tool: string,
    args: Record<string, unknown> = {},
    result: Record<string, unknown> = {},
  ): void {
    this.session.sendToolCall(tool, args, result);
  }

  /** Report per-stage usage ("stt"/"llm"/"tts") → token analytics. You run the
   *  providers, so only what you forward is counted. */
  sendUsage(
    stage: "stt" | "llm" | "tts",
    fields: Record<string, unknown> = {},
    provider = "",
    model = "",
  ): void {
    this.session.sendUsage(stage, fields, provider, model);
  }

  async aclose(): Promise<void> {
    this.endedFlag = true;
    await this.session.aclose();
  }
}

export type Handler = (call: Call) => Promise<void>;

export interface ServeOptions {
  /**
   * Fired once the worker is connected, authenticated, and registered — i.e.
   * present to the platform and awaiting calls. Fires again after every
   * automatic reconnect, so use it to drive a readiness probe / health check.
   */
  onReady?: () => void;
  /**
   * Fired when the connection drops (engine restart, network). `serve` then
   * reconnects with backoff; expect a matching `onReady` once it recovers.
   */
  onDisconnected?: (err: Error) => void;
}

const log = (msg: string) => console.error(`[telequick] ${msg}`);

/**
 * Connect, authenticate, register, and run `handler` per assigned call.
 * Reconnects with exponential backoff on any drop (engine restart, network) so
 * presence recovers without operator action. Runs until the process is killed.
 *
 * To know the worker is live, pass `onReady` (fires when connected + registered)
 * and `onDisconnected` (fires on drop, before reconnect).
 */
export async function serve(handler: Handler, cfg: AgentConfig, opts: ServeOptions = {}): Promise<void> {
  const serveOne = async (assign: JobAssign, session: WebTransportSession): Promise<void> => {
    const call = new Call(assign, session);
    try {
      await handler(call);
    } catch (e) {
      log(`handler failed for call_id=${assign.callId}: ${(e as Error)?.stack ?? e}`);
    } finally {
      if (!session.closed) await call.aclose();
    }
  };

  let backoff = 1.0;
  for (;;) {
    let conn: Awaited<ReturnType<typeof openMediaConnection>> | null = null;
    try {
      conn = await openMediaConnection(cfg.wt(), {
        host: cfg.host,
        port: cfg.port,
        verify: cfg.verify,
      });
      log(`worker ready (agent=${cfg.agentName}) — awaiting calls`);
      backoff = 1.0;
      opts.onReady?.();
      const { mc, closed } = conn;
      let dead = false;
      void closed.then(() => (dead = true));

      const CLOSED = Symbol("closed");
      for (;;) {
        const next = await Promise.race([mc.nextCall(), closed.then(() => CLOSED)]);
        if (next === CLOSED || dead) throw new Error("engine connection closed");
        const [assign, session] = next as [JobAssign, WebTransportSession];
        log(`call_id=${assign.callId} from=${assign.callerNumber} to=${assign.calledNumber} → handler`);
        void serveOne(assign, session);
      }
    } catch (e) {
      if (conn) await conn.close().catch(() => {});
      const err = e instanceof Error ? e : new Error(String(e));
      opts.onDisconnected?.(err);
      log(`worker connection lost (${err.message}) — reconnecting in ${backoff.toFixed(1)}s`);
      await new Promise((r) => setTimeout(r, backoff * 1000));
      backoff = Math.min(backoff * 2, 30.0);
    }
  }
}

/** Blocking convenience wrapper: `run(handler)` with env-based config. */
export async function run(handler: Handler, cfg?: AgentConfig, opts?: ServeOptions): Promise<void> {
  await serve(handler, cfg ?? AgentConfig.fromEnv(), opts);
}
