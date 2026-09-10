"""Verify platform webhook deliveries (Stripe-style signature scheme).

Deliveries carry:

    X-Clutchcall-Event:     voice.call.answered
    X-Clutchcall-Delivery:  <delivery id>
    X-Clutchcall-Signature: t=<unix epoch>,v1=<hmac_sha256_hex(secret, "t.body")>

``verify_signature`` checks the HMAC and an optional freshness window; use it
at the top of your webhook route before trusting the payload. The signing
secret (``whsec_…``) is returned once when the endpoint is created
(``webhooks.create``) or rotated.
"""

from __future__ import annotations

import hashlib
import hmac
import time

SIGNATURE_HEADER = "X-Clutchcall-Signature"
EVENT_HEADER = "X-Clutchcall-Event"
DELIVERY_HEADER = "X-Clutchcall-Delivery"


class WebhookVerificationError(ValueError):
    pass


def verify_signature(secret: str, signature_header: str, body: bytes,
                     *, tolerance_secs: int | None = 300) -> None:
    """Raise ``WebhookVerificationError`` unless the delivery is authentic.

    ``signature_header`` is the raw ``X-Clutchcall-Signature`` value;
    ``body`` is the EXACT raw request body (no re-serialization — a reformatted
    JSON body will not verify).
    """
    parts = dict(
        kv.split("=", 1) for kv in signature_header.split(",") if "=" in kv
    )
    ts, sig = parts.get("t"), parts.get("v1")
    if not ts or not sig:
        raise WebhookVerificationError("malformed signature header")
    if tolerance_secs is not None:
        try:
            age = abs(time.time() - int(ts))
        except ValueError as e:
            raise WebhookVerificationError("bad timestamp") from e
        if age > tolerance_secs:
            raise WebhookVerificationError(f"stale delivery ({age:.0f}s old)")
    expect = hmac.new(secret.encode(), f"{ts}.".encode() + body, hashlib.sha256).hexdigest()
    if not hmac.compare_digest(expect, sig):
        raise WebhookVerificationError("signature mismatch")
