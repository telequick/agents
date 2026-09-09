"""Agent-transport auth — wire-identical to the engine's Pusher/HMAC scheme.

The `/media/` QUIC endpoint takes connections from customer infra over the
public internet, so it authenticates BEFORE any media flows. We do NOT invent a
scheme: this mirrors mod_realtime's `verify_subscribe_auth` byte-for-byte, so
the engine validates our envelopes with its existing AWS-LC path.

Model (from mod_realtime.cc / pusher_protocol.cc):
  * App registry `telequick:realtime:apps`: app_key -> {app_id (= TENANT), secret}.
    The customer's agent holds its own tenant app_key + secret (server-side creds,
    like a Pusher server SDK — the browser never sees the secret).
  * Connection auth is bound to a **server-issued nonce** (`socket_id`) so a
    captured signature can't be replayed on a new connection.
  * sign string = "<socket_id>:<channel>[:<channel_data>]"
    auth token  = "<app_key>:<hmac_sha256_hex(secret, sign_string)>"
  * Media channel: "media" (channel_data = agent name). Presence:
    "presence-agents-<tenant>".

Handshake over the control stream:
    engine -> {"op":"welcome","socket_id":"<server nonce>"}
    agent  -> {"op":"auth","app_key":..,"channel":"media","channel_data":agent,
               "auth":"<app_key>:<hmac>"}
    engine -> {"op":"ready"}   |   {"op":"error","code":4001|4009}

Tenant isolation is enforced ENGINE-SIDE and authoritatively: the connection is
bound to app_id, dispatch only assigns that tenant's calls, and datagrams whose
call_id was not assigned to the connection are dropped (defense in depth on both
ends — see transport.WebTransportSession demux).
"""

from __future__ import annotations

import hashlib
import hmac

MEDIA_CHANNEL = "media"
ERR_APP_KEY_NOT_FOUND = 4001
ERR_AUTH_FAILED = 4009


def hmac_sha256_hex(key: str, msg: str) -> str:
    """HMAC-SHA256 -> lowercase hex. Byte-identical to the engine's AWS-LC impl
    (proven against its `hmac_known_vector` test)."""
    return hmac.new(key.encode(), msg.encode(), hashlib.sha256).hexdigest()


def sign_string(socket_id: str, channel: str, channel_data: str = "") -> str:
    s = f"{socket_id}:{channel}"
    if channel_data:
        s += f":{channel_data}"
    return s


def ct_equal(a: str, b: str) -> bool:
    """Constant-time compare (matches engine `ct_equal`)."""
    return hmac.compare_digest(a, b)


def make_auth(
    app_key: str, app_secret: str, socket_id: str, channel: str, channel_data: str = ""
) -> str:
    """Auth token "<app_key>:<hmac>" — what the agent presents on connect."""
    sig = hmac_sha256_hex(app_secret, sign_string(socket_id, channel, channel_data))
    return f"{app_key}:{sig}"


def media_auth(app_key: str, app_secret: str, socket_id: str, agent_name: str = "") -> str:
    return make_auth(app_key, app_secret, socket_id, MEDIA_CHANNEL, agent_name)


def presence_agents_auth(
    app_key: str, app_secret: str, socket_id: str, tenant: str, channel_data: str = ""
) -> str:
    return make_auth(app_key, app_secret, socket_id, f"presence-agents-{tenant}", channel_data)


def verify_auth(
    app_key: str,
    app_secret: str,
    socket_id: str,
    channel: str,
    channel_data: str,
    presented: str,
) -> bool:
    """Engine-side check, mirrored here for tests and defensive client use.

    Exactly `verify_subscribe_auth`: presented key must match, then constant-time
    compare the HMAC over the server-nonce-bound sign string.
    """
    key, _, sig = presented.partition(":")
    if not ct_equal(key, app_key):
        return False
    expect = hmac_sha256_hex(app_secret, sign_string(socket_id, channel, channel_data))
    return ct_equal(sig, expect)
