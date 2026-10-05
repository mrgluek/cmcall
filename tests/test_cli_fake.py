"""CLI flow with a fake Delta Chat layer and real WebRTC.

The fake accounts deliver the call signaling (IncomingCall /
OutgoingCallAccepted / CallEnded) through their event queues exactly like
deltachat-rpc-client does, so `cli.run()` executes end to end without any
relay. The relay-only variant needs a local coturn:

    turnserver -n --listening-ip=127.0.0.1 --relay-ip=127.0.0.1 \
      --user=u:p --realm=test --lt-cred-mech --no-tls --no-dtls \
      --allow-loopback-peers --no-cli

Run: python3 -m unittest tests.test_cli_fake -v
"""

import contextlib
import itertools
import os
import queue
import socket
import sys
import unittest
from unittest import mock

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from cmcall import cli  # noqa: E402

_ids = itertools.count(100)
LOCAL_TURN = [{"urls": ["turn:127.0.0.1:3478"], "username": "u", "credential": "p"}]


class FakeRpc:
    def __init__(self):
        self.queues = {}

    def get_queue(self, accid):
        return self.queues.setdefault(accid, queue.Queue())


class FakeMsg:
    def __init__(self, account, msg_id, peer=None, peer_msg=None):
        self.account, self.id, self.peer, self.peer_msg = account, msg_id, peer, peer_msg

    def accept_incoming_call(self, answer):
        if self.peer is not None and self.account.behaviour != "decline":
            self.peer.push(kind="OutgoingCallAccepted", msgId=self.peer_msg, chatId=1,
                           acceptCallInfo=answer)

    def end_call(self):
        if self.peer is not None:
            self.peer.push(kind="CallEnded", msgId=self.peer_msg, chatId=1)


class FakeChat:
    def __init__(self, owner, peer):
        self.owner, self.peer, self.id = owner, peer, 1

    def place_outgoing_call(self, sdp, has_video):
        mine, theirs = next(_ids), next(_ids)
        caller_msg = FakeMsg(self.owner, mine, self.peer, theirs)
        self.owner.msgs[mine] = caller_msg
        self.peer.msgs[theirs] = FakeMsg(self.peer, theirs, self.owner, mine)
        if self.peer.behaviour == "decline":
            self.owner.push(kind="CallEnded", msgId=mine, chatId=1)
        else:
            self.peer.push(kind="IncomingCall", msgId=theirs, chatId=1,
                           placeCallInfo=sdp, hasVideo=False)
        return caller_msg


class FakeContact:
    def __init__(self, owner, other):
        self.owner, self.other = owner, other

    def create_chat(self):
        return FakeChat(self.owner, self.other)


class FakeAccount:
    def __init__(self, rpc, addr, ice, behaviour="answer", reject_login=False):
        self._rpc, self.id = rpc, next(_ids)
        self.config = {"configured_addr": addr}
        self.ice, self.msgs, self.behaviour = ice, {}, behaviour
        self.reject_login = reject_login
        self.removed = False

    def push(self, **ev):
        self._rpc.get_queue(self.id).put(ev)

    def get_config(self, key):
        return self.config.get(key)

    def set_config(self, key, value):
        self.config[key] = value

    def start_io(self):
        if self.reject_login:
            # what core emits when the relay refuses the credentials
            self.push(kind="Warning", msg="IMAP failed to login: Authentication failed: "
                                          "AUTHENTICATIONFAILED Authentication failed.")
        else:
            self.push(kind="ImapInboxIdle")

    def stop_io(self):
        pass

    def remove(self):
        self.removed = True

    def ice_servers(self):
        return self.ice

    def create_contact(self, other):
        return FakeContact(self, other)

    def get_message_by_id(self, msg_id):
        return self.msgs[msg_id]


def fake_relays(ice, callee_behaviour="answer", rejected=(), always_rejected=()):
    """``rejected``: roles whose cached profile the relay refuses (a fresh one
    works); ``always_rejected``: roles refused even with a fresh profile."""
    rpc = FakeRpc()
    accounts = {}
    created = itertools.count(1)

    class FakeRelay:
        def __init__(self, relay, base_dir, stack, out):
            self.relay = relay

        def _new(self, role, reject):
            behaviour = callee_behaviour if role == "callee" else "answer"
            addr = f"{role}{next(created)}@{self.relay}"
            return FakeAccount(rpc, addr, ice, behaviour, reject_login=reject)

        def account(self, role):
            key = (self.relay, role)
            if key not in accounts:
                reject = role in rejected or role in always_rejected
                accounts[key] = self._new(role, reject)
            return accounts[key]

        def recreate(self, old, role):
            old.remove()
            accounts[(self.relay, role)] = self._new(role, role in always_rejected)
            return accounts[(self.relay, role)]

    FakeRelay.accounts = accounts
    return FakeRelay


def run_cli(argv, ice, callee_behaviour="answer", **relay_kw):
    args = cli.build_parser().parse_args(argv)
    args.relay2 = args.relay2 or args.relay1
    lines = []
    out = cli.Out(quiet=False, verbose=0)
    relay_cls = fake_relays(ice, callee_behaviour, **relay_kw)
    run_cli.accounts = relay_cls.accounts
    with mock.patch.object(cli, "Relay", relay_cls), \
            mock.patch("builtins.print", lambda *a, **k: lines.append(" ".join(map(str, a)))):
        result = cli.run(args, out)
        cli.print_report(result, out)
    return result, "\n".join(lines)


def _turn_running():
    with contextlib.suppress(OSError):
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.settimeout(1)
        # STUN binding request; coturn answers it
        s.sendto(bytes.fromhex("000100002112a442") + os.urandom(12), ("127.0.0.1", 3478))
        s.recvfrom(1024)
        return True
    return False


class CliFlowTest(unittest.TestCase):
    def test_direct_call_between_two_relays(self):
        result, text = run_cli(["a.example", "b.example", "--ice", "all", "-d", "3",
                                "-i", "0.5", "--settle", "0.3"], ice=[])
        self.assertTrue(result["ok"], result)
        sig = result["signaling"]
        for key in ("offer_ms", "ring_ms", "answer_ms", "accept_ms", "total_ms", "hangup_ms"):
            self.assertIn(key, sig)
        echo = result["caller_stats"]["echo"]
        self.assertEqual(echo["received"], echo["sent"])
        self.assertIn("CMCALL a.example(caller1@a.example) -> b.example(callee2@b.example)", text)
        self.assertIn("beeps sent", text)
        self.assertIn("rtt min/avg/max/mdev", text)
        self.assertIn("callee:", text)
        self.assertEqual(result["callee_stats"]["path"]["kind"], "direct")

    def test_relay_only_without_turn_fails_in_setup(self):
        result, text = run_cli(["a.example", "-d", "1"], ice=[])
        self.assertFalse(result["ok"])
        self.assertEqual(result["stage"], "setup")
        self.assertIn("FAILED at setup", text)

    def test_declined_call(self):
        result, _ = run_cli(["a.example", "b.example", "--ice", "all", "-d", "1",
                             "--timeout", "5"], ice=[], callee_behaviour="decline")
        self.assertFalse(result["ok"])
        self.assertEqual(result["stage"], "signaling")

    def test_rejected_cached_profile_is_recreated(self):
        result, text = run_cli(["a.example", "b.example", "--ice", "all", "-d", "1", "-i", "0.5",
                                "--settle", "0.2", "--timeout", "5"], ice=[], rejected=("callee",))
        self.assertTrue(result["ok"], result)
        self.assertEqual(result["recreated"], ["callee2@b.example"])
        self.assertEqual(result["callee"], "callee3@b.example")
        self.assertIn("callee2@b.example was rejected by b.example", text)
        for ac in run_cli.accounts.values():
            self.assertEqual(ac.config["delete_device_after"], cli.DELETE_DEVICE_AFTER)

    def test_rejected_fresh_profile_fails_setup(self):
        result, text = run_cli(["a.example", "--ice", "all", "-d", "1", "--timeout", "5"],
                               ice=[], always_rejected=("caller",))
        self.assertFalse(result["ok"])
        self.assertEqual(result["stage"], "setup")
        self.assertIn("brand-new profile", result["error"])

    @unittest.skipUnless(_turn_running(), "no local coturn on 127.0.0.1:3478")
    def test_relay_only_through_turn(self):
        result, text = run_cli(["a.example", "b.example", "-d", "3", "-i", "0.5",
                                "--settle", "0.3"], ice=LOCAL_TURN)
        self.assertTrue(result["ok"], result)
        self.assertEqual(result["caller_stats"]["path"]["kind"], "relay")
        self.assertEqual(result["caller_stats"]["local_candidates"], {"relay": 1})
        self.assertIn("via relay", text)


if __name__ == "__main__":
    unittest.main()
