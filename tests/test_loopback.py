"""In-process WebRTC loopback: ProbePeer (caller) <-> EchoPeer (callee).

No relays, no Delta Chat - host candidates on localhost only. Verifies that
the SDP exchange connects, the echo returns the beeps, RTT is measured, and
the echo delay is accounted for. Takes ~15 s.

Run: python3 -m unittest tests.test_loopback -v
"""

import asyncio
import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from cmcall import rtc  # noqa: E402


async def _call(duration=3.0, delay=0.0, greeting=True, interval=0.5):
    caller = rtc.ProbePeer([], interval=interval, echo_delay=delay)
    callee = rtc.EchoPeer([], delay=delay, greeting=greeting)
    try:
        offer = await caller.offer()
        answer = await callee.accept(offer)
        await caller.accept_answer(answer)
        assert await caller.wait_connected(15), "caller did not connect"
        assert await callee.wait_connected(5), "callee did not connect"
        await asyncio.sleep(0.6)  # let the greeting pass
        seen = []
        await caller.run_probe(duration, progress=lambda s, r: seen.append((s, r)))
        return await caller.summary(), await callee.summary(), seen
    finally:
        await caller.close()
        await callee.close()


class LoopbackTest(unittest.TestCase):
    def test_echo_roundtrip(self):
        out, echo, seen = asyncio.run(_call())
        e = out["echo"]
        self.assertTrue(out["connected"])
        self.assertEqual(out["path"]["kind"], "direct")
        self.assertGreaterEqual(e["sent"], 5)
        self.assertEqual(e["received"], e["sent"], e)
        self.assertEqual(e["unexpected"], 0, e)
        # codec + jitter buffers on both sides, but on localhost well under 1 s
        self.assertLess(e["rtt_max_ms"], 400)
        self.assertGreater(e["rtt_min_ms"], 0)
        self.assertEqual(len(seen), e["sent"])
        self.assertTrue(out["rtp"]["packets_received"] > 0)
        self.assertGreater(echo["voice_s"], 0)
        self.assertEqual(out["codec"].lower().split("/")[0], "opus")

    def test_echo_delay_is_subtracted(self):
        out, _echo, _ = asyncio.run(_call(duration=3.0, delay=0.7, interval=1.0))
        e = out["echo"]
        self.assertGreaterEqual(e["received"], e["sent"] - 1, e)
        self.assertLess(e["rtt_avg_ms"], 600, e)


class HelperTest(unittest.TestCase):
    def test_parse_ice_servers_drops_ipv6(self):
        servers = rtc.parse_ice_servers(
            '[{"urls":["turn:1.2.3.4:3478","turn:[2a01::1]:3478"],'
            '"username":"u","credential":"p"},{"urls":["turn:[::1]:3478"]}]'
        )
        self.assertEqual(len(servers), 1)
        self.assertEqual(servers[0].urls, ["turn:1.2.3.4:3478"])
        self.assertEqual(servers[0].username, "u")

    def test_classify_path(self):
        self.assertEqual(rtc.classify_path("relay", "relay"), "relay")
        self.assertEqual(rtc.classify_path("relay", "host"), "relay/p2p")
        self.assertEqual(rtc.classify_path("srflx", "host"), "stun")
        self.assertEqual(rtc.classify_path("host", "host"), "direct")

    def test_sdp_helpers(self):
        sdp = (
            "m=audio 9 UDP/TLS/RTP/SAVPF 111 0\r\n"
            "a=rtpmap:111 opus/48000/2\r\n"
            "a=rtpmap:0 PCMU/8000\r\n"
            "a=candidate:1 1 udp 1 10.0.0.1 5000 typ host\r\n"
            "a=candidate:2 1 udp 1 1.2.3.4 6000 typ relay raddr 0.0.0.0 rport 0\r\n"
        )
        self.assertEqual(rtc.sdp_codec(sdp), "opus/48000/2")
        self.assertEqual(rtc.sdp_candidates(sdp), {"host": 1, "relay": 1})

    def test_relay_only_without_turn_fails(self):
        async def go():
            peer = rtc.ProbePeer([], relay_only=True)
            try:
                await peer.offer()
            finally:
                await peer.close()

        with self.assertRaises(rtc.CallError) as cm:
            asyncio.run(go())
        self.assertEqual(cm.exception.stage, "setup")


if __name__ == "__main__":
    unittest.main()
