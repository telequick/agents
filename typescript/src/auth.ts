/**
 * Agent-transport auth (TypeScript) — wire-identical to the engine's HMAC scheme.
 *
 *   sign string = "<socket_id>:<channel>[:<channel_data>]"
 *   auth token  = "<app_key>:<hmac_sha256_hex(secret, sign_string)>"
 *
 * The connection auth is bound to a server-issued nonce (`socket_id`) so a
 * captured signature can't be replayed on a new connection. Media channel =
 * "media" (channel_data = agent name). Your media app key/secret are server-side
 * credentials — never ship the secret to a browser. Tenant isolation is enforced
 * authoritatively engine-side; the mirror here is for tests and defensive use.
 */

import { createHmac, timingSafeEqual } from "node:crypto";

export const MEDIA_CHANNEL = "media";
export const ERR_APP_KEY_NOT_FOUND = 4001;
export const ERR_AUTH_FAILED = 4009;

/** HMAC-SHA256 -> lowercase hex. Byte-identical to the engine's AWS-LC impl. */
export function hmacSha256Hex(key: string, msg: string): string {
  return createHmac("sha256", Buffer.from(key, "utf8"))
    .update(Buffer.from(msg, "utf8"))
    .digest("hex");
}

export function signString(socketId: string, channel: string, channelData = ""): string {
  let s = `${socketId}:${channel}`;
  if (channelData) s += `:${channelData}`;
  return s;
}

/** Constant-time string compare (matches engine ct_equal). */
export function ctEqual(a: string, b: string): boolean {
  const ba = Buffer.from(a, "utf8");
  const bb = Buffer.from(b, "utf8");
  if (ba.length !== bb.length) return false;
  return timingSafeEqual(ba, bb);
}

/** Auth token "<app_key>:<hmac>" — what the agent presents on connect. */
export function makeAuth(
  appKey: string,
  appSecret: string,
  socketId: string,
  channel: string,
  channelData = "",
): string {
  const sig = hmacSha256Hex(appSecret, signString(socketId, channel, channelData));
  return `${appKey}:${sig}`;
}

export function mediaAuth(appKey: string, appSecret: string, socketId: string, agentName = ""): string {
  return makeAuth(appKey, appSecret, socketId, MEDIA_CHANNEL, agentName);
}

/** Engine-side check, mirrored for tests and defensive client use. */
export function verifyAuth(
  appKey: string,
  appSecret: string,
  socketId: string,
  channel: string,
  channelData: string,
  presented: string,
): boolean {
  const idx = presented.indexOf(":");
  const key = idx === -1 ? presented : presented.slice(0, idx);
  const sig = idx === -1 ? "" : presented.slice(idx + 1);
  if (!ctEqual(key, appKey)) return false;
  const expect = hmacSha256Hex(appSecret, signString(socketId, channel, channelData));
  return ctEqual(sig, expect);
}
