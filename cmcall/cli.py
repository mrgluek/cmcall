"""
chatmail call test aka "cmcall": place a Delta Chat call and measure it.

Relay mode (``cmcall relay1 [relay2]``)
=======================================
1. SETUP: a caller profile on relay1 and a callee profile on relay2 (both
   local, reused across runs like cmping). Both go online (IMAP IDLE), which
   also fetches each relay's TURN credentials (IMAP METADATA).
2. SIGNALING: the caller creates a WebRTC offer and places the call; the offer
   travels as a Delta Chat message relay1 -> relay2. The callee answers; the
   acceptance travels back relay2 -> relay1. Both legs are timed.
3. ICE: by default relay-only (``--ice relay``), so media is forced through
   relay1's TURN and relay2's TURN - otherwise two profiles on the same host
   would just connect directly and the TURN servers would go untested.
4. MEDIA: the caller beeps every interval, the callee echoes the audio back,
   the caller times each echoed beep (audio round trip). RTP/RTCP counters
   (loss, jitter, RTCP RTT) are reported for both sides.

Echo-bot mode (``cmcall relay --to <invite link>``)
===================================================
A profile on ``relay`` securejoins the invite link (e.g. of the bouncer bot),
calls it and probes the echo it returns - an end-to-end test of a deployed
echo service, as seen from that relay.
"""

from __future__ import annotations

import argparse
import contextlib
import json
import logging
import queue
import re
import shutil
import socket
import sys
import time
import urllib.parse
import urllib.request
from typing import Optional

from deltachat_rpc_client import AttrDict, DeltaChat, Rpc
from xdg_base_dirs import xdg_cache_home

from . import __version__, rtc

ROLE_KEY = "ui.cmcall_role"
#: Echo bots (bouncer) start their post-call statistics message with this.
REPORT_PREFIX = "📞"


#: Cached test profiles only need messages for the few minutes of a test;
#: let core delete them so reused profiles don't grow their databases.
DELETE_DEVICE_AFTER = "3600"


class LoginRejected(Exception):
    """The relay refused a cached profile's login (typically: deleted after
    inactivity). ``remaining`` are the accounts not yet waited for."""

    def __init__(self, account, message: str, remaining: list):
        super().__init__(message)
        self.account = account
        self.remaining = remaining


def _login_rejected(ev) -> bool:
    msg = (ev.get("msg") or "").lower()
    return ev.kind in ("Warning", "Error") and "failed to login" in msg and "authentic" in msg


class Failure(Exception):
    def __init__(self, stage: str, message: str):
        super().__init__(message)
        self.stage = stage


# --------------------------------------------------------------------------
# output
# --------------------------------------------------------------------------


class Out:
    def __init__(self, quiet: bool, verbose: int):
        self.quiet = quiet
        self.verbose = verbose

    def __call__(self, line: str = "", level: int = 0) -> None:
        if not self.quiet and self.verbose >= level:
            print(line, flush=True)


# --------------------------------------------------------------------------
# accounts
# --------------------------------------------------------------------------


def _is_ip(host: str) -> bool:
    try:
        socket.inet_pton(socket.AF_INET, host)
        return True
    except OSError:
        pass
    try:
        socket.inet_pton(socket.AF_INET6, host)
        return True
    except OSError:
        return False


def relay_host(relay: str) -> str:
    if relay.startswith("https://"):
        return urllib.parse.urlparse(relay).hostname or relay
    return relay


def account_qr(relay: str) -> str:
    if relay.startswith("https://"):
        return f"dcaccount:{relay}"
    if _is_ip(relay):
        import random
        import string

        chars = string.ascii_lowercase + string.digits
        user = "".join(random.choices(chars, k=12))
        password = "".join(random.choices(chars, k=20))
        return f"dclogin:{user}@{relay}/?p={password}&v=1&ip=993&sp=465&ic=3&ss=default"
    return f"dcaccount:{relay}"


def https_endpoint_credentials(relay: str) -> Optional[tuple[str, str]]:
    """mailadm-style fallback: POST https://relay/new -> {email, password}."""
    if relay.startswith("https://"):
        endpoints = [relay]
    else:
        endpoints = [f"https://{relay}/new", f"https://{relay}/new_email"]
    for endpoint in endpoints:
        for method in ("POST", "GET"):
            try:
                req = urllib.request.Request(endpoint, method=method)
                req.add_header("User-Agent", f"cmcall/{__version__}")
                with urllib.request.urlopen(req, timeout=10) as resp:
                    data = json.loads(resp.read().decode())
                if "email" in data and "password" in data:
                    return data["email"], data["password"]
            except Exception:
                continue
    return None


def _add_transport(account, qr: str) -> None:
    if hasattr(account, "add_transport_from_qr"):
        account.add_transport_from_qr(qr)
    else:  # older deltachat-rpc-client
        account.set_config_from_qr(qr)


class Relay:
    """One deltachat-rpc-server (accounts dir) per relay, like cmping."""

    def __init__(self, relay: str, base_dir, stack: contextlib.ExitStack, out: Out):
        self.relay = relay
        self.out = out
        safe = re.sub(r"[^A-Za-z0-9._-]", "_", relay)
        self.dir = base_dir.joinpath(safe)
        if self.dir.exists() and not self.dir.joinpath("accounts.toml").exists():
            shutil.rmtree(self.dir, ignore_errors=True)
        out(f"# using accounts_dir for {relay} at: {self.dir}", 1)
        self.rpc = stack.enter_context(Rpc(accounts_dir=self.dir))
        self.dc = DeltaChat(self.rpc)

    def account(self, role: str):
        domain = relay_host(self.relay)
        for ac in self.dc.get_all_accounts():
            addr = ac.get_config("configured_addr")
            if addr and ac.get_config(ROLE_KEY) == role:
                if self.relay.startswith("https://") or addr.split("@")[-1] == domain:
                    return ac
        return self._create(role)

    def _create(self, role: str):
        ac = self.dc.add_account()
        try:
            _add_transport(ac, account_qr(self.relay))
        except Exception as e:
            self.out(f"# QR setup on {self.relay} failed ({e}), trying HTTPS endpoint", 2)
            creds = None if _is_ip(self.relay) else https_endpoint_credentials(self.relay)
            if not creds:
                with contextlib.suppress(Exception):
                    ac.remove()
                raise Failure("setup", f"cannot create a profile on {self.relay}: {e}") from e
            email, password = creds
            user, host = email.split("@", 1)
            qr = (
                f"dclogin:{user}@{host}/?p={urllib.parse.quote(password, safe='')}"
                f"&v=1&ih={host}&sh={host}&ip=993&sp=465&ic=3&ss=default"
            )
            try:
                _add_transport(ac, qr)
            except Exception as e2:
                with contextlib.suppress(Exception):
                    ac.remove()
                raise Failure("setup", f"cannot configure profile on {self.relay}: {e2}") from e2
        ac.set_config(ROLE_KEY, role)
        return ac

    def recreate(self, old, role: str):
        """Drop a profile the relay no longer accepts and make a fresh one."""
        with contextlib.suppress(Exception):
            old.stop_io()
        with contextlib.suppress(Exception):
            old.remove()
        return self._create(role)


class Events:
    """Read one account's core events with timeouts."""

    def __init__(self, account, out: Out, name: str):
        self.ac = account
        self.q = account._rpc.get_queue(account.id)
        self.out = out
        self.name = name

    def wait(self, pred, timeout: float, what: str, stage: str) -> AttrDict:
        deadline = time.time() + timeout
        while True:
            remaining = deadline - time.time()
            if remaining <= 0:
                raise Failure(stage, f"timeout ({timeout:.0f}s) waiting for {what}")
            try:
                ev = AttrDict(self.q.get(timeout=min(remaining, 1.0)))
            except queue.Empty:
                continue
            if ev.kind in ("Error", "Warning"):
                self.out(f"  [{self.name}] {ev.kind}: {ev.get('msg', '')}", 2)
            else:
                self.out(f"  [{self.name}] {ev.kind}", 3)
            if pred(ev):
                return ev

    def drain(self) -> None:
        with contextlib.suppress(queue.Empty):
            while True:
                self.q.get_nowait()


def bring_online(accounts, out: Out, timeout: float) -> None:
    """Start I/O and wait for IMAP IDLE; raise LoginRejected on a refused login."""
    for ac in accounts:
        ac.set_config("bot", "1")
        ac.set_config("delete_device_after", DELETE_DEVICE_AFTER)
        ac.start_io()
    for i, ac in enumerate(accounts):
        addr = ac.get_config("configured_addr")
        ev = Events(ac, out, addr).wait(
            lambda e: e.kind == "ImapInboxIdle" or _login_rejected(e),
            timeout,
            f"{addr} to go online (IMAP IDLE)",
            "setup",
        )
        if ev.kind != "ImapInboxIdle":
            raise LoginRejected(ac, ev.get("msg") or "login failed", accounts[i + 1:])


def bring_online_or_recreate(profiles: dict, ctx: dict, out: Out, timeout: float, result: dict) -> None:
    """``profiles`` maps role -> (relay, account); rejected cached profiles are
    replaced (once each) by fresh ones, updating ``profiles`` in place."""
    pending = [ac for _relay, ac in profiles.values()]
    recreated = set()
    while pending:
        try:
            bring_online(pending, out, timeout)
            return
        except LoginRejected as e:
            role, (relay, _old) = next(
                (r, v) for r, v in profiles.items() if v[1] is e.account
            )
            addr = e.account.get_config("configured_addr")
            if role in recreated:
                raise Failure("setup", f"{relay} rejected the login of a brand-new profile {addr}: {e}") from e
            out(f"# {addr} was rejected by {relay} (deleted after inactivity?), creating a new profile")
            try:
                fresh = ctx[relay].recreate(e.account, role)
            except Failure:
                raise
            except Exception as e2:
                raise Failure("setup", f"cannot recreate profile on {relay}: {e2}") from e2
            recreated.add(role)
            result.setdefault("recreated", []).append(addr)
            profiles[role] = (relay, fresh)
            pending = [fresh] + list(e.remaining)


def turn_label(servers, relay: str) -> str:
    urls = rtc.describe_ice_servers(servers)
    turn = [u for u in urls if u.startswith("turn")]
    if not turn:
        return "none" + (f" (stun: {', '.join(urls)})" if urls else "")
    url = turn[0]
    host = url.split(":")[1]
    try:
        relay_ips = {a[4][0] for a in socket.getaddrinfo(relay_host(relay), None)}
    except OSError:
        relay_ips = set()
    suffix = " (same host as relay)" if host in relay_ips else ""
    return url + suffix


# --------------------------------------------------------------------------
# the test
# --------------------------------------------------------------------------


def run(args, out: Out) -> dict:
    result: dict = {
        "version": __version__,
        "mode": "echo-bot" if args.to else "relay",
        "relay1": args.relay1,
        "relay2": None if args.to else args.relay2,
        "ice_policy": args.ice,
        "duration_s": args.duration,
        "ok": False,
        "stage": None,
        "error": None,
    }
    loop = rtc.CallLoop()
    peers: dict = {}
    try:
        with contextlib.ExitStack() as stack:
            try:
                _run(args, out, result, stack, loop, peers)
            finally:
                for peer in peers.values():
                    with contextlib.suppress(Exception):
                        loop.run(peer.close(), timeout=10)
    except (Failure, rtc.CallError) as e:
        result["stage"] = e.stage
        result["error"] = str(e)
    except KeyboardInterrupt:
        result["stage"] = result["stage"] or "interrupted"
        result["error"] = "interrupted"
    finally:
        loop.stop()
    return result


def _run(args, out: Out, result: dict, stack, loop: rtc.CallLoop, peers: dict) -> None:
    base_dir = xdg_cache_home().joinpath("cmcall")
    relays = [args.relay1] if args.to else list(dict.fromkeys([args.relay1, args.relay2]))
    if args.reset:
        for relay in relays:
            d = base_dir.joinpath(re.sub(r"[^A-Za-z0-9._-]", "_", relay))
            if d.exists():
                out(f"# removing account directory for {relay}: {d}")
                shutil.rmtree(d, ignore_errors=True)

    # ---- setup -------------------------------------------------------------
    t_setup = time.time()
    ctx = {r: Relay(r, base_dir, stack, out) for r in relays}
    out("# Setting up profiles...", 0)
    caller_role = "prober" if args.to else "caller"
    try:
        profiles = {caller_role: (args.relay1, ctx[args.relay1].account(caller_role))}
        if not args.to:
            profiles["callee"] = (args.relay2, ctx[args.relay2].account("callee"))
    except Failure:
        raise
    except Exception as e:
        raise Failure("setup", f"profile setup failed: {e}") from e
    bring_online_or_recreate(profiles, ctx, out, args.timeout, result)
    caller = profiles[caller_role][1]
    callee = profiles["callee"][1] if "callee" in profiles else None
    caller_addr = caller.get_config("configured_addr")
    result["caller"] = caller_addr
    caller_ev = Events(caller, out, caller_addr)
    callee_ev = None
    if callee is not None:
        callee.set_config("who_can_call_me", "0")  # everybody
        result["callee"] = callee.get_config("configured_addr")
        callee_ev = Events(callee, out, result["callee"])

    caller_ice = rtc.parse_ice_servers(caller.ice_servers())
    result["caller_turn"] = turn_label(caller_ice, args.relay1)
    if callee is not None:
        callee_ice = rtc.parse_ice_servers(callee.ice_servers())
        result["callee_turn"] = turn_label(callee_ice, args.relay2)

    # chat between the two
    if args.to:
        try:
            chat = caller.secure_join(args.to)
        except Exception as e:
            raise Failure("setup", f"invalid invite link: {e}") from e
        ev = caller_ev.wait(
            lambda e: e.kind == "SecurejoinJoinerProgress" and e.progress in (0, 1000),
            args.timeout,
            "securejoin with the echo bot",
            "setup",
        )
        if ev.progress == 0:
            raise Failure("setup", "securejoin with the echo bot failed")
        contacts = chat.get_contacts()
        result["callee"] = contacts[0].get_snapshot().address if contacts else "?"
    else:
        callee.create_contact(caller)
        chat = caller.create_contact(callee).create_chat()
    result["setup_s"] = round(time.time() - t_setup, 2)
    out("# Setting up profiles... Done!", 0)

    target_desc = result["callee"] if args.to else f"{args.relay2}({result['callee']})"
    out(
        f"CMCALL {args.relay1}({caller_addr}) -> {target_desc} "
        f"ice={args.ice} duration={args.duration:g}s"
    )
    if args.to:
        out(f"turn  caller: {result['caller_turn']}")
    else:
        out(f"turn  caller: {result['caller_turn']}   callee: {result['callee_turn']}")

    relay_only = args.ice == "relay"
    caller_peer = rtc.ProbePeer(
        caller_ice, relay_only=relay_only, interval=args.interval, echo_delay=args.echo_delay
    )
    peers["caller"] = caller_peer
    caller_ev.drain()
    if callee_ev is not None:
        callee_ev.drain()

    # ---- signaling -----------------------------------------------------------
    t_call = time.time()
    offer = loop.run(caller_peer.offer(), timeout=rtc.ICE_GATHER_TIMEOUT_S + 10)
    out(
        f"call  offer ready in {caller_peer.gather_s * 1000:.0f} ms "
        f"({_cands(caller_peer.local_sdp)})"
    )
    call_msg = chat.place_outgoing_call(offer, False)
    t_placed = time.time()
    signaling: dict = {"offer_ms": round((t_placed - t_call) * 1000)}
    result["signaling"] = signaling

    callee_msg = None
    if callee is not None:
        # match on our ICE ufrag: profiles are reused, ignore stray older calls
        ufrag = _ice_ufrag(offer)
        ev = callee_ev.wait(
            lambda e: e.kind == "IncomingCall" and (not ufrag or ufrag in e.place_call_info),
            args.timeout,
            "the call to arrive at the callee",
            "signaling",
        )
        t_ring = time.time()
        signaling["ring_ms"] = round((t_ring - t_placed) * 1000)
        out(f"call  ringing at callee after {signaling['ring_ms']} ms ({args.relay1} -> {args.relay2})")
        callee_peer = rtc.EchoPeer(callee_ice, relay_only=relay_only, greeting=False)
        peers["callee"] = callee_peer
        answer = loop.run(callee_peer.accept(ev.place_call_info), timeout=rtc.ICE_GATHER_TIMEOUT_S + 10)
        callee_msg = callee.get_message_by_id(ev.msg_id)
        callee_msg.accept_incoming_call(answer)
        t_answered = time.time()
        signaling["answer_ms"] = round((t_answered - t_ring) * 1000)

    ev = caller_ev.wait(
        lambda e: e.kind == "OutgoingCallAccepted" and e.msg_id == call_msg.id
        or (e.kind == "CallEnded" and e.msg_id == call_msg.id),
        args.timeout,
        "the call to be accepted",
        "signaling",
    )
    if ev.kind == "CallEnded":
        raise Failure("signaling", "call was declined/ended by the callee")
    t_accepted = time.time()
    if callee is not None:
        signaling["accept_ms"] = round((t_accepted - t_answered) * 1000)
        out(f"call  accepted, back at caller after {signaling['accept_ms']} ms ({args.relay2} -> {args.relay1})")
    signaling["total_ms"] = round((t_accepted - t_placed) * 1000)
    if args.to:
        out(f"call  answered by echo bot after {signaling['total_ms']} ms")

    # ---- ICE -------------------------------------------------------------------
    loop.run(caller_peer.accept_answer(ev.accept_call_info), timeout=10)
    connected = loop.run(caller_peer.wait_connected(args.ice_timeout), timeout=args.ice_timeout + 5)
    if not connected:
        state = caller_peer.failed_state or "timeout"
        raise Failure("ice", f"ICE/DTLS did not connect ({state}); remote candidates: "
                             f"{_cands(caller_peer.remote_sdp)}")
    path = caller_peer.path
    out(
        f"ice   connected in {caller_peer.connect_s * 1000:.0f} ms via {path.get('kind')} "
        f"({path.get('local_type')} {path.get('local_addr')} <-> "
        f"{path.get('remote_type')} {path.get('remote_addr')})"
    )

    # ---- media -----------------------------------------------------------------
    time.sleep(args.settle)

    def progress(seq, rtt):
        if rtt is None:
            out(f"beep seq={seq} lost")
        else:
            out(f"beep seq={seq} time={rtt * 1000:.2f}ms")

    loop.run(
        caller_peer.run_probe(args.duration, progress=progress),
        timeout=args.duration + 30,
    )
    result["caller_stats"] = loop.run(caller_peer.summary(), timeout=10)
    if peers.get("callee") is not None:
        result["callee_stats"] = loop.run(peers["callee"].summary(), timeout=10)

    # ---- hangup ----------------------------------------------------------------
    with contextlib.suppress(Exception):
        call_msg.end_call()
    if callee_ev is not None:
        with contextlib.suppress(Failure):
            callee_ev.wait(lambda e: e.kind == "CallEnded", 15, "hangup", "hangup")
            signaling["hangup_ms"] = round((time.time() - t_accepted) * 1000)
    elif args.to and args.peer_report:
        # the bouncer echo bot sends its call statistics ("📞 ...") after hangup
        report = []

        def is_report(e):
            if e.kind != "IncomingMsg" or e.chat_id != chat.id:
                return False
            text = caller.get_message_by_id(e.msg_id).get_snapshot().text or ""
            if text.startswith(REPORT_PREFIX):
                report.append(text)
                return True
            return False

        with contextlib.suppress(Failure):
            caller_ev.wait(is_report, args.peer_report, "echo bot report", "hangup")
        if report:
            result["peer_report"] = report[0]

    echo = result["caller_stats"]["echo"]
    result["ok"] = bool(echo["received"])
    if not result["ok"]:
        result["stage"] = "media"
        result["error"] = "connected, but no echoed audio came back"


def _ice_ufrag(sdp: str) -> Optional[str]:
    m = re.search(r"a=ice-ufrag:(\S+)", sdp or "")
    return m.group(1) if m else None


def _cands(sdp: Optional[str]) -> str:
    c = rtc.sdp_candidates(sdp or "")
    return " ".join(f"{k}:{v}" for k, v in sorted(c.items())) or "no candidates"


def _rtp_line(name: str, rtp: dict) -> str:
    if not rtp:
        return f"{name}: no RTP stats"
    parts = [
        f"sent {rtp.get('packets_sent', '?')}",
        f"recv {rtp.get('packets_received', '?')}",
        f"lost {rtp.get('packets_lost', '?')} ({rtp.get('loss_pct', 0):.2f}%)",
        f"jitter {rtp.get('jitter_ms', '?')} ms",
    ]
    if "rtcp_rtt_ms" in rtp:
        parts.append(f"rtcp-rtt {rtp['rtcp_rtt_ms']} ms")
    return f"{name}: " + ", ".join(parts)


def print_report(result: dict, out: Out) -> None:
    stats = result.get("caller_stats")
    if stats:
        e = stats["echo"]
        out("--- audio echo statistics ---")
        loss = f"{e['loss_pct']:.2f}%" if e["loss_pct"] is not None else "n/a"
        out(f"{e['sent']} beeps sent, {e['received']} echoed, {loss} loss")
        if e["rtt_avg_ms"] is not None:
            mdev = e["rtt_mdev_ms"] if e["rtt_mdev_ms"] is not None else e["rtt_max_ms"]
            out(
                f"rtt min/avg/max/mdev = {e['rtt_min_ms']:.3f}/{e['rtt_avg_ms']:.3f}/"
                f"{e['rtt_max_ms']:.3f}/{mdev:.3f} ms"
            )
        out("--- rtp statistics ---")
        out(_rtp_line("caller", stats.get("rtp")))
        if result.get("callee_stats"):
            out(_rtp_line("callee", result["callee_stats"].get("rtp")))
    sig = result.get("signaling")
    if sig or result.get("setup_s") is not None:
        out("--- timing statistics ---")
        if result.get("setup_s") is not None:
            out(f"account setup: {result['setup_s']:.2f}s")
        if sig and "total_ms" in sig:
            out(f"call signaling: {sig['total_ms'] / 1000:.2f}s")
        if stats and stats.get("connect_ms") is not None:
            out(f"ice connect: {stats['connect_ms']} ms")
    if result.get("peer_report"):
        out("--- echo bot report ---")
        out(result["peer_report"])
    if not result["ok"]:
        out(f"✗ FAILED at {result.get('stage')}: {result.get('error')}")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="cmcall",
        description="Test Delta Chat calls (signaling + WebRTC media) between chatmail relays.",
    )
    parser.add_argument("relay1", help="chatmail relay of the caller (domain, IP or https:// endpoint)")
    parser.add_argument(
        "relay2", nargs="?",
        help="chatmail relay of the echoing callee (defaults to relay1)",
    )
    parser.add_argument(
        "--to", metavar="INVITE",
        help="call an echo bot via its invite link instead of a local callee profile",
    )
    parser.add_argument("-d", "--duration", type=float, default=10.0,
                        help="seconds of audio probing (default 10)")
    parser.add_argument("-i", "--interval", type=float, default=1.0,
                        help="seconds between beeps (default 1.0)")
    parser.add_argument(
        "--ice", choices=("relay", "all"), default="relay",
        help="relay: force media through the relays' TURN servers (default); "
             "all: allow direct/STUN paths too",
    )
    parser.add_argument("--echo-delay", type=float, default=0.0,
                        help="known playback delay of the remote echo, subtracted from RTT")
    parser.add_argument("--settle", type=float, default=1.0,
                        help="seconds to wait after connecting before probing (default 1)")
    parser.add_argument("--timeout", type=float, default=60.0,
                        help="timeout for going online / ringing / answering (default 60)")
    parser.add_argument("--ice-timeout", type=float, default=20.0,
                        help="timeout for ICE/DTLS to connect (default 20)")
    parser.add_argument("--peer-report", type=float, default=15.0, metavar="SECONDS",
                        help="with --to: wait this long for the echo bot's statistics message "
                             "(0 = don't wait, default 15)")
    parser.add_argument("--reset", action="store_true",
                        help="remove the profiles of the tested relays to force fresh ones")
    parser.add_argument("--json", action="store_true", help="print a single JSON result object")
    parser.add_argument("-v", dest="verbose", action="count", default=0, help="increase verbosity")
    parser.add_argument("--version", action="version", version=f"%(prog)s {__version__}")
    return parser


def main(argv=None) -> None:
    """Place a Delta Chat call between chatmail relays and report its quality."""
    args = build_parser().parse_args(argv)
    if args.to and args.relay2:
        build_parser().error("--to takes a single relay (the caller's)")
    if not args.relay2:
        args.relay2 = args.relay1
    if args.verbose >= 3:
        logging.basicConfig(level=logging.DEBUG)
    elif args.verbose >= 2:
        logging.basicConfig(level=logging.INFO)
    out = Out(quiet=args.json, verbose=args.verbose)
    result = run(args, out)
    if args.json:
        print(json.dumps(result, indent=2, default=str))
    else:
        print_report(result, out)
    raise SystemExit(0 if result["ok"] else 1)


if __name__ == "__main__":
    main(sys.argv[1:])
