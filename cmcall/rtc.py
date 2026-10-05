"""WebRTC side of cmcall: RPC-agnostic building blocks for Delta Chat calls.

Delta Chat core only does *signalling* for calls: the caller's SDP offer
travels inside the call message (``place_call_info``), the callee's SDP
answer inside the hidden "call accepted" message (``accept_call_info``).
Media (ICE/DTLS/RTP) is entirely up to the client. This module provides that
client side with aiortc, independent of which RPC library moves the SDP:

* :class:`CallLoop`   - a dedicated asyncio loop in its own thread. aiortc runs
  ICE checks and RTP send/recv as tasks on its loop; if that loop is blocked
  (bot work, sync RPC calls) media stalls. Keep every aiortc object on it.
* :class:`EchoPeer`   - the *answerer*: accepts an offer and loops the caller's
  audio straight back (optionally delayed). Used by ``cmcall`` for the callee
  account and by the bouncer bot for its "call me to test" echo service.
* :class:`ProbePeer`  - the *offerer*: sends short tone beeps (frequency coded
  per sequence number), detects them in the echoed audio and measures the
  audio round-trip time and beep loss.
* :func:`collect_rtp_stats` / :func:`selected_path` - RTCP and ICE details.

Lessons taken from the hermes-deltachat-platform call handler:
  - aiortc cannot parse IPv6 TURN URLs -> drop them, keep the IPv4 ones.
  - don't use ``bundlePolicy=max-bundle`` (breaks gathering / checks).
  - as answerer, mirror the negotiated ``iceTrickling`` (id=1) and
    ``mutedState`` (id=3) data channels that Delta Chat's calls-webapp opens,
    and add the candidates it trickles over ``iceTrickling``.
  - as offerer towards a real Delta Chat client, offer audio only (the data
    channels' SCTP transport wedges ICE against a max-bundle answerer).
"""

from __future__ import annotations

import asyncio
import collections
import concurrent.futures
import contextlib
import fractions
import json
import logging
import math
import re
import threading
import time
from collections import Counter
from statistics import mean, stdev
from typing import Any, Optional

import av
import numpy as np
from aiortc import (
    RTCConfiguration,
    RTCIceServer,
    RTCPeerConnection,
    RTCSessionDescription,
)
from aiortc.mediastreams import MediaStreamError, MediaStreamTrack

logger = logging.getLogger("cmcall")

SAMPLE_RATE = 48000
FRAME_SAMPLES = 960  # 20 ms
FRAME_TIME = FRAME_SAMPLES / SAMPLE_RATE

#: Beep tone per sequence number (seq % 4). Exact FFT bins for a 20 ms frame
#: (50 Hz resolution), well apart so Opus doesn't blur them together.
PROBE_FREQS = (600.0, 900.0, 1200.0, 1500.0)
BEEP_FRAMES = 5  # 100 ms beep
BEEP_AMPLITUDE = 0.35
#: RMS (int16 scale) above which a received frame counts as "sound".
DETECT_RMS = 1200
#: RMS above which a frame counts as voiced for the human-caller audio stats.
VOICE_RMS = 400
#: Greeting played by the echo peer once the call connects. Both tones are
#: >120 Hz away from every probe frequency, so the detector ignores them.
GREETING_TONES = ((330.0, 6), (None, 4), (440.0, 6))
#: Keep the echo backlog short: input and output run at the same rate, so any
#: backlog (jitter burst, clock drift) would otherwise add latency for good.
ECHO_MAX_BACKLOG_FRAMES = 5

ICE_GATHER_TIMEOUT_S = 10.0


class CallError(Exception):
    """A call test failed; ``stage`` tells where (setup/signaling/ice/media)."""

    def __init__(self, stage: str, message: str):
        super().__init__(message)
        self.stage = stage


# --------------------------------------------------------------------------
# Dedicated event loop
# --------------------------------------------------------------------------


class CallLoop:
    """Run aiortc on its own asyncio loop in a daemon thread.

    ``run(coro)`` blocks the calling (sync) thread until the coroutine is done;
    ``submit(coro)`` returns a :class:`concurrent.futures.Future`.
    """

    def __init__(self, name: str = "cmcall-rtc") -> None:
        self.loop = asyncio.new_event_loop()
        self._thread = threading.Thread(target=self._run, name=name, daemon=True)
        self._thread.start()

    def _run(self) -> None:
        asyncio.set_event_loop(self.loop)
        self.loop.run_forever()

    def submit(self, coro) -> concurrent.futures.Future:
        return asyncio.run_coroutine_threadsafe(coro, self.loop)

    def run(self, coro, timeout: Optional[float] = None):
        fut = self.submit(coro)
        try:
            return fut.result(timeout)
        except concurrent.futures.TimeoutError:
            fut.cancel()
            raise

    def stop(self) -> None:
        if self.loop.is_running():
            self.loop.call_soon_threadsafe(self.loop.stop)
        self._thread.join(timeout=5)
        if not self._thread.is_alive() and not self.loop.is_closed():
            self.loop.close()


# --------------------------------------------------------------------------
# ICE helpers
# --------------------------------------------------------------------------


def parse_ice_servers(ice: Any) -> list[RTCIceServer]:
    """Convert Delta Chat's ``ice_servers`` JSON (string or list) for aiortc.

    Core resolves STUN/TURN hostnames to IPs (``turn:1.2.3.4:3478``, also IPv6
    ``turn:[2a01::1]:3478``). aiortc can't parse bracketed IPv6 URLs, and only
    ever uses the first STUN and the first TURN URL anyway.
    """
    data = json.loads(ice) if isinstance(ice, str) else ice
    servers = []
    for s in data or []:
        urls = s.get("urls", [])
        if isinstance(urls, str):
            urls = [urls]
        urls = [u for u in urls if "[" not in u]
        if not urls:
            continue
        servers.append(
            RTCIceServer(
                urls=urls,
                username=s.get("username"),
                credential=s.get("credential"),
            )
        )
    return servers


def describe_ice_servers(servers: list[RTCIceServer]) -> list[str]:
    """Human readable list like ``['turn:1.2.3.4:3478']`` (no credentials)."""
    out = []
    for s in servers:
        urls = s.urls if isinstance(s.urls, list) else [s.urls]
        out.extend(urls)
    return out


def _ice_connections(pc: RTCPeerConnection) -> list:
    """All aioice ``Connection`` objects behind a peer connection."""
    conns, seen = [], set()
    transports = []
    for t in pc.getTransceivers():
        transports.append(t.sender.transport or t.receiver.transport)
    if pc.sctp is not None:
        transports.append(pc.sctp.transport)
    for dtls in transports:
        if dtls is None:
            continue
        conn = dtls.transport.iceGatherer._connection
        if id(conn) not in seen:
            seen.add(id(conn))
            conns.append(conn)
    return conns


def force_relay_only(pc: RTCPeerConnection) -> None:
    """Emulate ``iceTransportPolicy: "relay"`` (not exposed by aiortc).

    Must be called after the transports exist (addTrack / setRemoteDescription)
    and before ``setLocalDescription`` (which gathers candidates).
    """
    from aioice.ice import TransportPolicy

    conns = _ice_connections(pc)
    if not conns:
        raise CallError("setup", "no ICE transport to configure")
    for conn in conns:
        if conn.turn_server is None:
            raise CallError("setup", "relay-only ICE requested, but no TURN server is known")
        conn._transport_policy = TransportPolicy.RELAY


def selected_path(pc: Optional[RTCPeerConnection]) -> dict:
    """Return the nominated ICE candidate pair (types + addresses)."""
    if pc is None:
        return {}
    for conn in _ice_connections(pc):
        pair = conn._nominated.get(1)
        if pair is None:
            continue
        local, remote = pair.local_candidate, pair.remote_candidate
        info = {
            "local_type": local.type,
            "local_addr": f"{local.host}:{local.port}",
            "remote_type": remote.type,
            "remote_addr": f"{remote.host}:{remote.port}",
            "protocol": local.transport,
            "turn_server": (
                f"{conn.turn_server[0]}:{conn.turn_server[1]}" if conn.turn_server else None
            ),
        }
        info["kind"] = classify_path(local.type, remote.type)
        return info
    return {}


def classify_path(local_type: str, remote_type: str) -> str:
    types = {local_type, remote_type}
    if "relay" in types:
        return "relay" if types == {"relay"} else "relay/p2p"
    if types & {"srflx", "prflx"}:
        return "stun"
    return "direct"


def sdp_candidates(sdp: str) -> dict:
    """Count candidate types in an SDP, e.g. ``{'host': 2, 'relay': 1}``."""
    return dict(Counter(re.findall(r"a=candidate:.*? typ (\w+)", sdp or "")))


def sdp_codec(sdp: str, kind: str = "audio") -> Optional[str]:
    """First codec of the first ``kind`` m-line, e.g. ``opus/48000/2``."""
    cur_kind, pts = None, []
    rtpmap = {}
    for raw in (sdp or "").splitlines():
        line = raw.strip()
        if line.startswith("m="):
            parts = line[2:].split()
            cur_kind = parts[0]
            if cur_kind == kind and not pts:
                pts = parts[3:]
        elif cur_kind == kind and line.startswith("a=rtpmap:"):
            pt, _, codec = line[len("a=rtpmap:"):].partition(" ")
            rtpmap.setdefault(pt, codec)
    for pt in pts:
        if pt in rtpmap:
            return rtpmap[pt]
    return None


# --------------------------------------------------------------------------
# Audio helpers
# --------------------------------------------------------------------------


def _frame_from_pcm(pcm: np.ndarray) -> av.AudioFrame:
    frame = av.AudioFrame.from_ndarray(
        pcm.astype(np.int16).reshape(1, -1), format="s16", layout="mono"
    )
    frame.sample_rate = SAMPLE_RATE
    return frame


def silence_frame() -> av.AudioFrame:
    return _frame_from_pcm(np.zeros(FRAME_SAMPLES, dtype=np.int16))


def tone_frames(freq: Optional[float], nframes: int, amplitude: float = BEEP_AMPLITUDE) -> list:
    """``nframes`` 20 ms mono frames of a sine (``freq=None`` -> silence)."""
    if freq is None:
        return [silence_frame() for _ in range(nframes)]
    n = nframes * FRAME_SAMPLES
    t = np.arange(n) / SAMPLE_RATE
    wave = np.sin(2 * math.pi * freq * t)
    # 5 ms fade in/out against clicks
    ramp = int(0.005 * SAMPLE_RATE)
    env = np.ones(n)
    env[:ramp] = np.linspace(0, 1, ramp)
    env[-ramp:] = np.linspace(1, 0, ramp)
    pcm = wave * env * amplitude * 32767
    return [_frame_from_pcm(pcm[i * FRAME_SAMPLES:(i + 1) * FRAME_SAMPLES]) for i in range(nframes)]


def frame_mono(frame: av.AudioFrame) -> np.ndarray:
    """Float mono samples of an AudioFrame (no plane padding)."""
    arr = frame.to_ndarray().astype(np.float32)
    channels = len(frame.layout.channels)
    if frame.format.is_planar:
        return arr.mean(axis=0)
    return arr.reshape(-1, channels).mean(axis=1) if channels > 1 else arr.reshape(-1)


def rms(samples: np.ndarray) -> float:
    return float(np.sqrt(np.mean(samples * samples))) if samples.size else 0.0


def dominant_freq(samples: np.ndarray, rate: int) -> float:
    if samples.size < 32:
        return 0.0
    spec = np.abs(np.fft.rfft(samples * np.hanning(samples.size)))
    spec[0] = 0
    return float(np.argmax(spec) * rate / samples.size)


class _PacedAudioTrack(MediaStreamTrack):
    """Audio track emitting one 20 ms frame per 20 ms of wall time."""

    kind = "audio"

    def __init__(self) -> None:
        super().__init__()
        self._start: Optional[float] = None
        self._ts = 0

    async def _pace(self) -> None:
        if self.readyState != "live":
            raise MediaStreamError
        if self._start is None:
            self._start = time.time()
            self._ts = 0
        else:
            self._ts += FRAME_SAMPLES
            wait = self._start + self._ts / SAMPLE_RATE - time.time()
            if wait > 0:
                await asyncio.sleep(wait)

    def _stamp(self, frame: av.AudioFrame) -> av.AudioFrame:
        frame.pts = self._ts
        frame.time_base = fractions.Fraction(1, SAMPLE_RATE)
        return frame


class EchoTrack(_PacedAudioTrack):
    """Plays back whatever was pushed into it, ``delay`` seconds later."""

    def __init__(self, delay: float = 0.0) -> None:
        super().__init__()
        self.delay = max(0.0, float(delay))
        self._queue: collections.deque = collections.deque()
        self._prio: collections.deque = collections.deque()
        self._resampler = av.AudioResampler(
            format="s16", layout="mono", rate=SAMPLE_RATE, frame_size=FRAME_SAMPLES
        )
        self._max_backlog = int(self.delay / FRAME_TIME) + ECHO_MAX_BACKLOG_FRAMES
        self.frames_echoed = 0
        self.frames_dropped = 0

    def push(self, frame: av.AudioFrame, now: Optional[float] = None) -> None:
        now = time.time() if now is None else now
        outs = self._resampler.resample(frame)
        if self._prio:
            # A greeting is playing: don't queue up audio behind it, that
            # backlog would delay the echo for the rest of the call.
            return
        for out in outs:
            self._queue.append((now, out))
        while len(self._queue) > self._max_backlog:
            self._queue.popleft()
            self.frames_dropped += 1

    def play(self, frames: list) -> None:
        """Queue frames (e.g. a greeting) that take precedence over the echo."""
        self._prio.extend(frames)

    async def recv(self) -> av.AudioFrame:
        await self._pace()
        if self._prio:
            frame = self._prio.popleft()
        elif self._queue and self._queue[0][0] + self.delay <= time.time():
            frame = self._queue.popleft()[1]
            self.frames_echoed += 1
        else:
            frame = silence_frame()
        return self._stamp(frame)


class ProbeTrack(_PacedAudioTrack):
    """Silence, plus a short beep every ``interval`` s while active.

    Beep ``seq`` uses ``PROBE_FREQS[seq % 4]``; ``sent`` records
    ``(seq, freq, wall_time_of_first_beep_frame)``.
    """

    def __init__(self, interval: float = 1.0) -> None:
        super().__init__()
        self.interval_frames = max(BEEP_FRAMES + 5, round(interval / FRAME_TIME))
        self.interval = self.interval_frames * FRAME_TIME
        self._beeps = [tone_frames(f, BEEP_FRAMES) for f in PROBE_FREQS]
        self._active = False
        self._max_beeps: Optional[int] = None
        self._pos = 0
        self.sent: list[tuple[int, float, float]] = []

    def start(self, max_beeps: Optional[int] = None) -> None:
        self._pos = 0
        self._max_beeps = max_beeps
        self._active = True

    def stop(self) -> None:
        self._active = False

    async def recv(self) -> av.AudioFrame:
        await self._pace()
        frame = None
        if self._active:
            seq, idx = divmod(self._pos, self.interval_frames)
            if self._max_beeps is not None and seq >= self._max_beeps:
                self._active = False
            elif idx < BEEP_FRAMES:
                freq_i = seq % len(PROBE_FREQS)
                if idx == 0:
                    self.sent.append((seq, PROBE_FREQS[freq_i], time.time()))
                # fresh frame object each time - aiortc may still hold the last one
                src = self._beeps[freq_i][idx]
                frame = _frame_from_pcm(src.to_ndarray().reshape(-1))
            self._pos += 1
        return self._stamp(frame or silence_frame())


class EchoDetector:
    """Find our beeps in the received (echoed) audio and time them."""

    def __init__(self, probe: ProbeTrack, echo_delay: float = 0.0) -> None:
        self.probe = probe
        self.echo_delay = echo_delay
        self._quiet = 99
        self.matched: dict[int, float] = {}  # seq -> rtt seconds
        self.unexpected = 0  # beep-like sound that matches no sent beep
        self.other_sounds = 0  # sound outside the probe band (voice, greeting)

    def feed(self, frame: av.AudioFrame, now: Optional[float] = None) -> None:
        now = time.time() if now is None else now
        mono = frame_mono(frame)
        level = rms(mono)
        if level < DETECT_RMS:
            self._quiet += 1
            return
        if self._quiet < 3:  # still inside the same beep
            self._quiet = 0
            return
        self._quiet = 0
        freq = dominant_freq(mono, frame.sample_rate or SAMPLE_RATE)
        target = min(PROBE_FREQS, key=lambda f: abs(f - freq))
        if abs(target - freq) > 120:
            self.other_sounds += 1
            return
        window = self.probe.interval * len(PROBE_FREQS)
        best = None
        for seq, f, sent_at in reversed(self.probe.sent):
            if f != target or seq in self.matched:
                continue
            rtt = now - sent_at - self.echo_delay
            if 0 < rtt < window:
                best = (seq, rtt)
            break
        if best is None:
            self.unexpected += 1
            return
        self.matched[best[0]] = best[1]


class AudioMeter:
    """Received-audio statistics, to tell a human caller whether we heard them."""

    def __init__(self) -> None:
        self.frames = 0
        self.voiced_frames = 0
        self.peak_rms = 0.0
        self.first_at: Optional[float] = None

    def feed(self, frame: av.AudioFrame) -> None:
        if self.first_at is None:
            self.first_at = time.time()
        self.frames += 1
        level = rms(frame_mono(frame))
        self.peak_rms = max(self.peak_rms, level)
        if level >= VOICE_RMS:
            self.voiced_frames += 1

    def summary(self) -> dict:
        frame_s = FRAME_TIME
        peak_dbfs = 20 * math.log10(self.peak_rms / 32768) if self.peak_rms > 0 else None
        return {
            "audio_received_s": round(self.frames * frame_s, 1),
            "voice_s": round(self.voiced_frames * frame_s, 1),
            "peak_dbfs": round(peak_dbfs, 1) if peak_dbfs is not None else None,
        }


# --------------------------------------------------------------------------
# Stats
# --------------------------------------------------------------------------


async def collect_rtp_stats(pc: Optional[RTCPeerConnection]) -> dict:
    """Audio RTP/RTCP counters of a peer connection (one direction each)."""
    if pc is None:
        return {}
    try:
        report = await pc.getStats()
    except Exception as e:  # closed pc etc.
        logger.debug("getStats failed: %s", e)
        return {}
    out: dict[str, Any] = {}
    for s in report.values():
        t = getattr(s, "type", "")
        if getattr(s, "kind", "audio") != "audio":
            continue
        if t == "outbound-rtp":
            out["packets_sent"] = s.packetsSent
            out["bytes_sent"] = s.bytesSent
        elif t == "inbound-rtp":
            out["packets_received"] = s.packetsReceived
            out["packets_lost"] = max(0, s.packetsLost)
            out["jitter_ms"] = round(s.jitter / (SAMPLE_RATE / 1000), 1)
        elif t == "remote-inbound-rtp":
            # what the *peer* reported about our stream (RTCP receiver report)
            out["rtcp_rtt_ms"] = round(s.roundTripTime * 1000, 1)
            out["remote_packets_lost"] = max(0, s.packetsLost)
            out["remote_fraction_lost"] = round(s.fractionLost / 256 * 100, 2)
            out["remote_jitter_ms"] = round(s.jitter / (SAMPLE_RATE / 1000), 1)
    recv, lost = out.get("packets_received"), out.get("packets_lost")
    if recv is not None and lost is not None and recv + lost > 0:
        out["loss_pct"] = round(lost / (recv + lost) * 100, 2)
    return out


# --------------------------------------------------------------------------
# Peers
# --------------------------------------------------------------------------


class _Peer:
    def __init__(self, ice_servers: list[RTCIceServer], relay_only: bool = False) -> None:
        self.ice_servers = ice_servers
        self.relay_only = relay_only
        self.pc: Optional[RTCPeerConnection] = None
        self.t_start = time.time()
        self.gather_s: Optional[float] = None
        self.connected_at: Optional[float] = None
        self.ice_start_at: Optional[float] = None
        self.failed_state: Optional[str] = None
        self.local_sdp: Optional[str] = None
        self.remote_sdp: Optional[str] = None
        self.path: dict = {}
        self._connected: Optional[asyncio.Event] = None
        self._tasks: list[asyncio.Task] = []

    def _new_pc(self) -> RTCPeerConnection:
        self._connected = asyncio.Event()
        pc = RTCPeerConnection(RTCConfiguration(iceServers=list(self.ice_servers)))

        @pc.on("connectionstatechange")
        def _on_state():
            logger.debug("pc connectionState=%s", pc.connectionState)
            if pc.connectionState == "connected" and self.connected_at is None:
                self.connected_at = time.time()
                self.path = selected_path(pc)
                self._connected.set()
                self._on_connected()
            elif pc.connectionState in ("failed", "closed"):
                if self.connected_at is None:
                    self.failed_state = pc.connectionState
                self._connected.set()

        self.pc = pc
        return pc

    def _on_connected(self) -> None:
        pass

    async def _set_local(self, desc) -> None:
        if self.relay_only:
            force_relay_only(self.pc)
        t0 = time.time()
        await self.pc.setLocalDescription(desc)
        self.gather_s = time.time() - t0
        self.local_sdp = self.pc.localDescription.sdp
        if self.relay_only and not sdp_candidates(self.local_sdp).get("relay"):
            raise CallError("ice", "no relay candidate gathered (TURN allocation failed)")

    async def wait_connected(self, timeout: float = 20.0) -> bool:
        if self._connected is None:
            return False
        with contextlib.suppress(asyncio.TimeoutError):
            await asyncio.wait_for(self._connected.wait(), timeout)
        return self.connected_at is not None

    @property
    def connect_s(self) -> Optional[float]:
        if self.connected_at is None or self.ice_start_at is None:
            return None
        return self.connected_at - self.ice_start_at

    def _spawn(self, coro) -> None:
        self._tasks.append(asyncio.ensure_future(coro))

    async def close(self) -> None:
        for task in self._tasks:
            task.cancel()
        if self.pc is not None:
            with contextlib.suppress(Exception):
                await self.pc.close()

    def base_summary(self) -> dict:
        return {
            "ice_servers": describe_ice_servers(self.ice_servers),
            "relay_only": self.relay_only,
            "gather_ms": round(self.gather_s * 1000) if self.gather_s is not None else None,
            "local_candidates": sdp_candidates(self.local_sdp),
            "remote_candidates": sdp_candidates(self.remote_sdp),
            "connected": self.connected_at is not None,
            "connect_ms": round(self.connect_s * 1000) if self.connect_s is not None else None,
            "path": self.path,
            "codec": sdp_codec(self.local_sdp),
        }


class EchoPeer(_Peer):
    """Answer an incoming call and echo its audio back.

    ``trickle_channels`` mirrors the negotiated data channels of Delta Chat's
    calls-webapp so trickled ICE candidates from real clients are used.
    """

    def __init__(
        self,
        ice_servers: list[RTCIceServer],
        *,
        relay_only: bool = False,
        delay: float = 0.0,
        greeting: bool = True,
        trickle_channels: bool = True,
    ) -> None:
        super().__init__(ice_servers, relay_only)
        self.echo = EchoTrack(delay)
        self.meter = AudioMeter()
        self.greeting = greeting
        self.trickle_channels = trickle_channels
        self.trickled = 0

    async def accept(self, offer_sdp: str) -> str:
        """Process the caller's offer; return our answer SDP (all candidates)."""
        pc = self._new_pc()
        self.remote_sdp = offer_sdp
        # Only real Delta Chat clients offer the data channels; creating them
        # for an audio-only offer would just gather an unused transport.
        if self.trickle_channels and "m=application" in offer_sdp:
            self._setup_trickle(pc)

        @pc.on("track")
        def _on_track(track):
            if track.kind == "audio":
                self._spawn(self._pump(track))

        await pc.setRemoteDescription(RTCSessionDescription(sdp=offer_sdp, type="offer"))
        pc.addTrack(self.echo)
        await self._set_local(await pc.createAnswer())
        self.ice_start_at = time.time()
        return self.local_sdp

    def _setup_trickle(self, pc: RTCPeerConnection) -> None:
        channel = pc.createDataChannel("iceTrickling", negotiated=True, id=1)
        pc.createDataChannel("mutedState", negotiated=True, id=3)

        @channel.on("message")
        async def _on_message(message):
            try:
                data = json.loads(message)
            except Exception:
                return
            if not data or not data.get("candidate"):
                with contextlib.suppress(Exception):
                    await pc.addIceCandidate(None)
                return
            try:
                from aiortc.sdp import candidate_from_sdp

                cand_str = data["candidate"]
                if cand_str.startswith("candidate:"):
                    cand_str = cand_str[len("candidate:"):]
                cand = candidate_from_sdp(cand_str)
                cand.sdpMid = data.get("sdpMid")
                cand.sdpMLineIndex = data.get("sdpMLineIndex")
                await pc.addIceCandidate(cand)
                self.trickled += 1
            except Exception as e:
                logger.debug("trickled candidate %r rejected: %s", data, e)

    def _on_connected(self) -> None:
        if self.greeting:
            frames = []
            for freq, n in GREETING_TONES:
                frames.extend(tone_frames(freq, n, amplitude=0.2))
            self.echo.play(frames)

    async def _pump(self, track) -> None:
        while True:
            try:
                frame = await track.recv()
            except MediaStreamError:
                return
            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.debug("echo pump stopped: %s", e)
                return
            self.meter.feed(frame)
            self.echo.push(frame)

    async def summary(self) -> dict:
        out = self.base_summary()
        out.update(self.meter.summary())
        out["rtp"] = await collect_rtp_stats(self.pc)
        out["trickled_candidates"] = self.trickled
        out["echo_frames"] = self.echo.frames_echoed
        out["echo_dropped_frames"] = self.echo.frames_dropped
        return out


class ProbePeer(_Peer):
    """Place a call (offer side), beep into it and time the echoes."""

    def __init__(
        self,
        ice_servers: list[RTCIceServer],
        *,
        relay_only: bool = False,
        interval: float = 1.0,
        echo_delay: float = 0.0,
    ) -> None:
        super().__init__(ice_servers, relay_only)
        self.probe = ProbeTrack(interval)
        self.detector = EchoDetector(self.probe, echo_delay)
        self.meter = AudioMeter()
        self.probe_s: Optional[float] = None

    async def offer(self) -> str:
        pc = self._new_pc()

        @pc.on("track")
        def _on_track(track):
            if track.kind == "audio":
                self._spawn(self._pump(track))

        pc.addTrack(self.probe)
        await self._set_local(await pc.createOffer())
        return self.local_sdp

    async def accept_answer(self, answer_sdp: str) -> None:
        self.remote_sdp = answer_sdp
        self.ice_start_at = time.time()
        await self.pc.setRemoteDescription(RTCSessionDescription(sdp=answer_sdp, type="answer"))

    async def _pump(self, track) -> None:
        while True:
            try:
                frame = await track.recv()
            except MediaStreamError:
                return
            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.debug("probe pump stopped: %s", e)
                return
            now = time.time()
            self.meter.feed(frame)
            self.detector.feed(frame, now)

    async def run_probe(self, duration: float, progress=None) -> None:
        """Beep for ``duration`` s, then wait for late echoes.

        ``progress(seq, rtt_or_None)`` is called (on the call loop) as beeps
        get matched or time out.
        """
        count = max(1, int(duration / self.probe.interval))
        t0 = time.time()
        self.probe.start(max_beeps=count)
        reported = set()
        grace = min(len(PROBE_FREQS) * self.probe.interval, 3.0)
        deadline = t0 + count * self.probe.interval + grace
        while time.time() < deadline:
            await asyncio.sleep(0.05)
            if self.pc is None or self.pc.connectionState in ("failed", "closed"):
                break
            if progress is not None:
                now = time.time()
                for seq, _f, sent_at in list(self.probe.sent):
                    if seq in reported:
                        continue
                    if seq in self.detector.matched:
                        reported.add(seq)
                        progress(seq, self.detector.matched[seq])
                    elif now - sent_at > grace:
                        reported.add(seq)
                        progress(seq, None)
            if len(self.detector.matched) >= count:
                break
        self.probe.stop()
        self.probe_s = time.time() - t0

    async def summary(self) -> dict:
        out = self.base_summary()
        out.update(self.meter.summary())
        sent = len(self.probe.sent)
        rtts = [r * 1000 for r in self.detector.matched.values()]
        out["echo"] = {
            "sent": sent,
            "received": len(rtts),
            "loss_pct": round((1 - len(rtts) / sent) * 100, 2) if sent else None,
            "rtt_min_ms": round(min(rtts), 1) if rtts else None,
            "rtt_avg_ms": round(mean(rtts), 1) if rtts else None,
            "rtt_max_ms": round(max(rtts), 1) if rtts else None,
            "rtt_mdev_ms": round(stdev(rtts), 1) if len(rtts) >= 2 else None,
            "unexpected": self.detector.unexpected,
            "other_sounds": self.detector.other_sounds,
        }
        out["rtp"] = await collect_rtp_stats(self.pc)
        return out
