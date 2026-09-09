"""Wire shape of the reporting ops (tool_call / usage).

The engine parses these envelopes field-by-field (mod_agenttransport.cc:
op=="tool_call" / op=="usage"). If a key name drifts the report is SILENTLY
dropped — no error, just missing analytics — so pin the exact shape here rather
than trusting the call to "work".
"""

import json
import unittest

from telequick_agents.transport import WebTransportConfig, WebTransportSession


def _session():
    sent: list[bytes] = []
    cfg = WebTransportConfig(
        engine_url="https://x:443", app_key="k", app_secret="s", agent_name="a", call_id=42
    )
    sess = WebTransportSession(cfg, send_datagram=lambda b: None, send_stream=sent.append)
    return sess, sent


def _one(sent):
    assert len(sent) == 1, f"expected 1 envelope, got {len(sent)}"
    line = sent[0].decode().strip()
    assert line.endswith("}"), "envelope must be one newline-terminated JSON object"
    return json.loads(line)


class ToolCallWire(unittest.TestCase):
    def test_shape(self):
        sess, sent = _session()
        sess.send_tool_call("lookup_order", {"id": "A1"}, {"status": "shipped"})
        m = _one(sent)
        self.assertEqual(m["op"], "tool_call")
        self.assertEqual(m["call_id"], 42)
        self.assertEqual(m["tool"], "lookup_order")
        # args/result must be OBJECTS — the engine splices them with jobj().
        self.assertIsInstance(m["args"], dict)
        self.assertIsInstance(m["result"], dict)

    def test_none_becomes_empty_object(self):
        sess, sent = _session()
        sess.send_tool_call("t")
        m = _one(sent)
        self.assertEqual(m["args"], {})
        self.assertEqual(m["result"], {})

    def test_empty_tool_not_sent(self):
        sess, sent = _session()
        sess.send_tool_call("")
        self.assertEqual(sent, [])


class UsageWire(unittest.TestCase):
    def test_shape(self):
        sess, sent = _session()
        sess.send_usage("llm", {"prompt_tokens": 12, "completion_tokens": 30}, "openai", "gpt-4o-mini")
        m = _one(sent)
        self.assertEqual(m["op"], "usage")
        self.assertEqual(m["stage"], "llm")
        self.assertEqual(m["provider"], "openai")
        self.assertEqual(m["model"], "gpt-4o-mini")
        self.assertIsInstance(m["fields"], dict)
        self.assertEqual(m["fields"]["prompt_tokens"], 12)

    def test_empty_stage_not_sent(self):
        sess, sent = _session()
        sess.send_usage("")
        self.assertEqual(sent, [])


if __name__ == "__main__":
    unittest.main()
