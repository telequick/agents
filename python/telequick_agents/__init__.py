"""TeleQuick external-agent SDK.

Two halves, mirroring what a Twilio/Plivo/Telnyx/Vapi SDK gives you:

* **Media plane** — ``serve``/``run`` register your process as an external
  agent over one outbound QUIC/WebTransport session; each routed phone call
  arrives as a :class:`Call` with pcm16 audio in both directions.
* **Control plane** — :class:`TeleQuickAPI` invokes management operations
  (originate calls, provision media credentials, register webhooks) with an
  ``mpk_`` API key; :mod:`telequick_agents.webhooks` verifies deliveries.

See the provider starters in ``telequick-examples`` for drop-in ports of each
vendor's official sample app.
"""

from . import auth, g711, webhooks, wire
from .agent import AgentConfig, Call, run, serve
from .api import TeleQuickAPI, TeleQuickAPIError
from .connection import AuthError, MediaConnection
from .transport import PlayoutReport, WebTransportConfig, WebTransportSession
from .types import JobAssign

__all__ = [
    "AgentConfig",
    "AuthError",
    "Call",
    "JobAssign",
    "MediaConnection",
    "PlayoutReport",
    "TeleQuickAPI",
    "TeleQuickAPIError",
    "WebTransportConfig",
    "WebTransportSession",
    "auth",
    "g711",
    "run",
    "serve",
    "webhooks",
    "wire",
]

__version__ = "0.2.0"
