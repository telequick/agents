/**
 * @telequick/agents — TeleQuick external-agent SDK for TypeScript.
 *
 * Run your own voice agent process against real phone calls over the TeleQuick
 * QUIC media plane. One outbound QUIC/WebTransport session (no inbound ports),
 * HMAC auth with your media app key/secret, one `Call` per routed phone call.
 *
 * The TypeScript sibling of the Python `telequick-agents` package — same wire
 * protocol, same handshake, same `serve(handler)` shape.
 */

export {
  AgentConfig,
  Call,
  serve,
  run,
  type Handler,
  type AgentConfigInit,
  type ServeOptions,
} from "./agent.js";
export { MediaConnection, AuthError, type JobAssign } from "./connection.js";
export {
  WebTransportSession,
  defaultConfig,
  AsyncQueue,
  type WebTransportConfig,
  type PlayoutReport,
} from "./transport.js";
export { openMediaConnection, type OpenOpts, type OpenedConnection } from "./quic-client.js";
export * as wire from "./wire.js";
export * as auth from "./auth.js";

export const VERSION = "0.3.0";
