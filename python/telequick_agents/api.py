"""Control-plane client — the piece Twilio/Plivo/Telnyx/Vapi REST SDKs map to.

The platform's key-authenticated API is the MCP endpoint (``POST /mcp`` with a
``Bearer mpk_…`` management key). Every tenant operation — originate a call,
provision an external agent's media credential, register a webhook — is an
operation path invoked through the ``call_operation`` tool. This client wraps
that in one method, so starter code reads like the provider SDK it replaces:

    api = TeleQuickAPI(base_url="https://app.telequick.dev",
                       api_key=os.environ["TELEQUICK_API_KEY"],
                       org_id="org_abc")

    call = api.call("voice.calls.originate",
                    {"to": "+15551234567", "trunkId": "trunk_main",
                     "agent": "support-agent"})

Discovery: ``api.call`` with paths from ``list_operations`` /
``describe_operation`` (see ``api.operations()``). Keys are minted in the
console under Settings → API keys (scopes: ``read``, ``write``).
"""

from __future__ import annotations

import json
import urllib.request


class TeleQuickAPIError(RuntimeError):
    def __init__(self, message: str, *, status: int | None = None, data=None) -> None:
        super().__init__(message)
        self.status = status
        self.data = data


class TeleQuickAPI:
    """Minimal synchronous client for the ``/mcp`` management surface.

    Stdlib-only on purpose: the starters stay dependency-light, and anything
    fancier (retries, async) belongs in your own stack.
    """

    def __init__(self, *, base_url: str, api_key: str, org_id: str | None = None,
                 timeout: float = 30.0) -> None:
        self._endpoint = base_url.rstrip("/") + "/mcp"
        self._api_key = api_key
        self._org_id = org_id
        self._timeout = timeout
        self._next_id = 1

    def _rpc(self, method: str, params: dict) -> dict:
        req_id, self._next_id = self._next_id, self._next_id + 1
        body = json.dumps(
            {"jsonrpc": "2.0", "id": req_id, "method": method, "params": params}
        ).encode()
        req = urllib.request.Request(
            self._endpoint,
            data=body,
            headers={
                "Content-Type": "application/json",
                "Accept": "application/json",
                "Authorization": f"Bearer {self._api_key}",
            },
            method="POST",
        )
        try:
            with urllib.request.urlopen(req, timeout=self._timeout) as resp:
                payload = json.loads(resp.read().decode())
        except urllib.error.HTTPError as e:  # type: ignore[attr-defined]
            raise TeleQuickAPIError(
                f"HTTP {e.code} from {self._endpoint}: {e.read().decode(errors='replace')[:500]}",
                status=e.code,
            ) from e
        if "error" in payload:
            err = payload["error"]
            raise TeleQuickAPIError(
                f"{method}: {err.get('message', 'error')}", data=err
            )
        return payload.get("result", {})

    @staticmethod
    def _tool_result(result: dict):
        # MCP tool results carry JSON in a text content block.
        if result.get("isError"):
            texts = [c.get("text", "") for c in result.get("content", [])]
            raise TeleQuickAPIError("; ".join(t for t in texts if t) or "tool error",
                                    data=result)
        if "structuredContent" in result:
            return result["structuredContent"]
        for block in result.get("content", []):
            if block.get("type") == "text":
                try:
                    return json.loads(block["text"])
                except (ValueError, KeyError):
                    return block.get("text")
        return result

    def call(self, path: str, input: dict | None = None):
        """Invoke one operation (e.g. ``voice.calls.originate``).

        ``orgId`` is filled in from the client when the operation wants it —
        and is pinned server-side to the API key's org regardless.
        """
        args: dict = {"path": path}
        inp = dict(input or {})
        if self._org_id is not None:
            inp.setdefault("orgId", self._org_id)
        if inp:
            args["input"] = inp
        return self._tool_result(
            self._rpc("tools/call", {"name": "call_operation", "arguments": args})
        )

    def operations(self, group: str | None = None, filter: str | None = None):
        """List callable operation paths (maps to the ``list_operations`` tool)."""
        args = {k: v for k, v in (("group", group), ("filter", filter)) if v}
        return self._tool_result(
            self._rpc("tools/call", {"name": "list_operations", "arguments": args})
        )

    def describe(self, path: str):
        """Input schema for one operation (``describe_operation`` tool)."""
        return self._tool_result(
            self._rpc("tools/call", {"name": "describe_operation",
                                     "arguments": {"path": path}})
        )

    def whoami(self):
        return self._tool_result(self._rpc("tools/call", {"name": "whoami", "arguments": {}}))
