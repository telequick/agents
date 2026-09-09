"""Core tests — stdlib unittest so the package tests run with zero deps.

    python3 -m unittest discover -s tests -v
"""

import asyncio
import sys
import time
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from telequick_agents import auth, g711, webhooks, wire  # noqa: E402
from telequick_agents.agent import Call  # noqa: E402
from telequick_agents.connection import MediaConnection  # noqa: E402
from telequick_agents.transport import WebTransportConfig, WebTransportSession  # noqa: E402


def cfg(call_id=0, **kw):
    return WebTransportConfig(
        engine_url="https://engine.example.com",
        app_key="ck_test",
        app_secret="secret",
        agent_name="salesbot",
        call_id=call_id,
        sample_rate=8000,
        **kw,
    )


class TestWire(unittest.TestCase):
    def test_datagram_roundtrip(self):
        dg = wire.Datagram(call_id=7, seq=0x1234, payload=b"\x01\x02" * 80)
        out = wire.decode_datagram(wire.encode_datagram(dg))
        self.assertEqual((out.call_id, out.seq, out.payload), (7, 0x1234, dg.payload))

    def test_ctrl_split_frames(self):
        buf = wire.encode_ctrl("flush", call_id=1) + b'{"op":"pl'
        msgs, rest = wire.decode_ctrl_stream(buf)
        self.assertEqual(msgs, [{"op": "flush", "call_id": 1}])
        msgs2, rest2 = wire.decode_ctrl_stream(rest + b'ayout","call_id":1}\n')
        self.assertEqual(msgs2, [{"op": "playout", "call_id": 1}])
        self.assertEqual(rest2, b"")


class TestAuth(unittest.TestCase):
    def test_engine_hmac_known_vector(self):
        # Wire-compat proof: same vector the engine's AWS-LC test uses.
        self.assertEqual(
            auth.hmac_sha256_hex("key", "The quick brown fox jumps over the lazy dog"),
            "f7bc83f430538424b13298e6aa6fb143ef4d59a14946175997479dbc2d1a3cd8",
        )

    def test_auth_verifies(self):
        token = auth.media_auth("ck_a", "s3", "123.456", "salesbot")
        self.assertTrue(auth.verify_auth("ck_a", "s3", "123.456", "media", "salesbot", token))
        self.assertFalse(auth.verify_auth("ck_a", "WRONG", "123.456", "media", "salesbot", token))


class TestG711(unittest.TestCase):
    def test_codepoint_roundtrip(self):
        # G.711 roundtrip is value-exact: decode(encode(decode(c))) == decode(c).
        # (Not byte-exact — μ-law has +0/−0 codes that decode identically.)
        for law_dec, law_enc in (
            (g711.ulaw_to_pcm, g711.pcm_to_ulaw),
            (g711.alaw_to_pcm, g711.pcm_to_alaw),
        ):
            codes = bytes(range(256))
            pcm = law_dec(codes)
            self.assertEqual(law_dec(law_enc(pcm)), pcm)

    def test_pure_python_matches_tables(self):
        # Exercise the pure path even when audioop exists.
        codes = bytes(range(256))
        for dec_tab, values, ncodes in (
            (g711._ULAW_DEC, g711._ULAW_VALUES, g711._ULAW_CODES),
            (g711._ALAW_DEC, g711._ALAW_VALUES, g711._ALAW_CODES),
        ):
            pcm = g711._decode(codes, dec_tab)
            self.assertEqual(g711._decode(g711._encode(pcm, values, ncodes), dec_tab), pcm)

    def test_silence_and_sign(self):
        # Encoding silence then decoding must stay (near) silent, both laws.
        silence = (0).to_bytes(2, "little", signed=True) * 160
        for enc, dec in ((g711.pcm_to_ulaw, g711.ulaw_to_pcm),
                         (g711.pcm_to_alaw, g711.alaw_to_pcm)):
            back = dec(enc(silence))
            for i in range(0, len(back), 2):
                self.assertLessEqual(abs(int.from_bytes(back[i:i + 2], "little", signed=True)), 8)


class TestSessionSeam(unittest.IsolatedAsyncioTestCase):
    async def test_call_audio_roundtrip_and_clear(self):
        sent_dgs, sent_ctrl = [], []
        sess = WebTransportSession(
            cfg(call_id=7), send_datagram=sent_dgs.append, send_stream=sent_ctrl.append
        )
        from telequick_agents.types import JobAssign

        call = Call(JobAssign(call_id=7, caller_number="+15550001111"), sess)

        # inbound: engine datagram → call.audio()
        frame = bytes(320)  # 20 ms @ 8 kHz pcm16
        sess.on_datagram(wire.encode_datagram(wire.Datagram(call_id=7, seq=0, payload=frame)))
        sess.on_control({"op": "shutdown", "call_id": 7})
        got = [pcm async for pcm in call.audio()]
        self.assertEqual(got, [frame])
        self.assertTrue(call.ended)

        # outbound: bursty send is paced into 20 ms datagrams
        await call.send_audio(bytes(320 * 3))
        for _ in range(50):
            if len(sent_dgs) >= 3:
                break
            await asyncio.sleep(0.02)
        self.assertGreaterEqual(len(sent_dgs), 3)
        self.assertEqual(len(wire.decode_datagram(sent_dgs[0]).payload), 320)

        # barge-in drops the buffer and emits a clear envelope
        await call.send_audio(bytes(320 * 100))
        await call.clear()
        ops = [m["op"] for m in
               wire.decode_ctrl_stream(b"".join(sent_ctrl))[0]]
        self.assertIn("clear", ops)
        await call.aclose()

    async def test_connection_isolation(self):
        sent = []
        mc = MediaConnection(cfg(), send_datagram=sent.append, send_stream=sent.append)
        mc._authed = True  # skip handshake for the demux test
        mc._on_job_assign({"op": "job_assign", "call_id": 3, "room": "salesbot",
                           "caller": "+1555", "called": "+1444"})
        assign, sess = await mc.next_call(timeout=1)
        self.assertEqual((assign.call_id, assign.caller_number), (3, "+1555"))
        # datagram for an unassigned call is dropped, assigned one delivered
        mc.on_datagram(wire.encode_datagram(wire.Datagram(call_id=9, seq=0, payload=b"x" * 8)))
        mc.on_datagram(wire.encode_datagram(wire.Datagram(call_id=3, seq=0, payload=b"y" * 8)))
        self.assertEqual(await asyncio.wait_for(sess.recv_frame(), 1), b"y" * 8)
        self.assertTrue(sess._inbound.empty())


class TestWebhooks(unittest.TestCase):
    def test_verify(self):
        import hashlib
        import hmac as _hmac

        secret, body = "whsec_abc", b'{"type":"voice.call.answered"}'
        ts = str(int(time.time()))
        sig = _hmac.new(secret.encode(), f"{ts}.".encode() + body, hashlib.sha256).hexdigest()
        webhooks.verify_signature(secret, f"t={ts},v1={sig}", body)
        with self.assertRaises(webhooks.WebhookVerificationError):
            webhooks.verify_signature(secret, f"t={ts},v1={sig}", body + b" ")
        with self.assertRaises(webhooks.WebhookVerificationError):
            webhooks.verify_signature(secret, f"t=1,v1={sig}", body)


if __name__ == "__main__":
    unittest.main()
