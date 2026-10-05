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
import unittest.mock

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


async def _dc_app_like_call():
    """Offer shaped like Delta Chat's calls-webapp: audio + video + the two
    negotiated data channels, with one candidate trickled over iceTrickling."""
    import json

    from aiortc import RTCPeerConnection, RTCSessionDescription
    from aiortc.mediastreams import AudioStreamTrack, VideoStreamTrack

    app = RTCPeerConnection()
    trickle = app.createDataChannel("iceTrickling", negotiated=True, id=1)
    app.createDataChannel("mutedState", negotiated=True, id=3)
    app.addTrack(AudioStreamTrack())
    app.addTrack(VideoStreamTrack())
    echo = rtc.EchoPeer([], greeting=False)
    try:
        await app.setLocalDescription(await app.createOffer())
        answer = await echo.accept(app.localDescription.sdp)
        await app.setRemoteDescription(RTCSessionDescription(sdp=answer, type="answer"))
        assert await echo.wait_connected(15), "echo did not connect"
        for _ in range(100):
            if trickle.readyState == "open":
                break
            await asyncio.sleep(0.05)
        cand = app.localDescription.sdp.split("a=candidate:")[1].split("\r\n")[0]
        trickle.send(json.dumps({"candidate": "candidate:" + cand, "sdpMid": "0", "sdpMLineIndex": 0}))
        await asyncio.sleep(2.0)
        video_tracks = [t.receiver.track for t in echo.pc.getTransceivers() if t.kind == "video"]
        backlog = [t._queue.qsize() for t in video_tracks]
        return await echo.summary(), trickle.readyState, backlog
    finally:
        await echo.close()
        await app.close()


class DeltaChatAppOfferTest(unittest.TestCase):
    def test_audio_video_datachannel_offer(self):
        summary, trickle_state, video_backlog = asyncio.run(_dc_app_like_call())
        self.assertTrue(summary["connected"])
        self.assertEqual(trickle_state, "open")
        self.assertEqual(summary["trickled_candidates"], 1)
        self.assertGreater(summary["rtp"]["packets_received"], 50)
        self.assertGreater(summary["echo_frames"], 50)
        # video frames are consumed, not piling up in aiortc's queue
        self.assertTrue(video_backlog and max(video_backlog) < 5, video_backlog)


class PacketLossTest(unittest.TestCase):
    """5 % RTP loss must cost frames, not latency (aiortc's stock audio jitter
    buffer stalls at holes and stays 320 ms deep afterwards)."""

    def test_loss_does_not_inflate_rtt(self):
        import random

        from aiortc import rtcrtpreceiver

        orig = rtcrtpreceiver.RTCRtpReceiver._handle_rtp_packet
        rnd = random.Random(1)

        async def lossy(self, packet, arrival_time_ms):
            if rnd.random() < 0.05:
                return
            return await orig(self, packet, arrival_time_ms)

        with unittest.mock.patch.object(rtcrtpreceiver.RTCRtpReceiver, "_handle_rtp_packet", lossy):
            out, _echo, _ = asyncio.run(_call(duration=5.0, interval=0.5))
        e = out["echo"]
        self.assertGreater(out["rtp"]["loss_pct"], 1)
        self.assertGreaterEqual(e["received"], e["sent"] - 1, e)
        self.assertLess(e["rtt_avg_ms"], 300, e)

    def test_jitter_buffer_steps_over_holes(self):
        from aiortc.rtp import RtpPacket

        jb = rtc.AudioJitterBuffer(prefetch=2)
        out = []
        for seq in (1, 2, 4, 5, 6, 7):  # 3 is lost
            p = RtpPacket(sequence_number=seq, timestamp=seq * 960)
            p._data = bytes([seq])
            _pli, frame = jb.add(p)
            if frame:
                out.append(frame.data[0])
        self.assertEqual(out, [1, 2, 4, 5])  # 6, 7 stay buffered (prefetch)
        self.assertEqual(jb.skipped, 1)


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

    def test_rtp_stats_before_first_rtcp(self):
        from types import SimpleNamespace as NS

        class FakePc:
            async def getStats(self):
                return {
                    "o": NS(type="outbound-rtp", kind="audio", packetsSent=10, bytesSent=900),
                    "i": NS(type="inbound-rtp", kind="audio", packetsReceived=9, packetsLost=1, jitter=96),
                    "r": NS(type="remote-inbound-rtp", kind="audio", roundTripTime=None,
                            packetsLost=0, fractionLost=None, jitter=0),
                }

        out = asyncio.run(rtc.collect_rtp_stats(FakePc()))
        self.assertNotIn("rtcp_rtt_ms", out)
        self.assertEqual(out["packets_sent"], 10)
        self.assertEqual(out["jitter_ms"], 2.0)
        self.assertEqual(out["loss_pct"], 10.0)

    def test_relay_only_drops_stun(self):
        from aioice.ice import TransportPolicy
        from aiortc import RTCPeerConnection
        from aiortc.mediastreams import AudioStreamTrack

        ice = rtc.parse_ice_servers([
            {"urls": ["stun:192.0.2.1:3478"]},
            {"urls": ["turn:192.0.2.1:3478"], "username": "u", "credential": "p"},
        ])

        async def go():
            from aiortc import RTCConfiguration
            pc = RTCPeerConnection(RTCConfiguration(iceServers=ice))
            pc.addTrack(AudioStreamTrack())
            try:
                rtc.force_relay_only(pc)
                return [(c._transport_policy, c.stun_server, c.turn_server) for c in rtc._ice_connections(pc)]
            finally:
                await pc.close()

        conns = asyncio.run(go())
        self.assertTrue(conns)
        for policy, stun, turn in conns:
            self.assertEqual(policy, TransportPolicy.RELAY)
            self.assertIsNone(stun)
            self.assertEqual(turn, ("192.0.2.1", 3478))

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
