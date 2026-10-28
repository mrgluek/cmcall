"""Relay: leftover profiles of killed runs are removed when the dir is opened.

Run: python3 -m unittest tests.test_relay_profiles -v
"""

import contextlib
import os
import pathlib
import sys
import tempfile
import unittest
from types import SimpleNamespace
from unittest import mock

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from cmcall import cli  # noqa: E402


class FakeAccount:
    def __init__(self, accid, addr=None, fail_remove=False):
        self.id = accid
        self.addr = addr
        self.config = {}
        self.fail_remove = fail_remove
        self.removed = False

    def get_config(self, key):
        return self.config.get(key)

    def is_configured(self):
        return bool(self.addr)

    @property
    def self_contact(self):
        # Like deltachat_rpc_client: Account.self_contact.get_snapshot().address
        return SimpleNamespace(get_snapshot=lambda: SimpleNamespace(address=self.addr))

    def remove(self):
        if self.fail_remove:
            raise RuntimeError("cannot remove")
        self.removed = True


class FakeRpc:
    def __init__(self, accounts_dir):
        self.accounts_dir = accounts_dir

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False


def open_relay(accounts):
    with tempfile.TemporaryDirectory() as tmp, contextlib.ExitStack() as stack, \
            mock.patch.object(cli, "Rpc", FakeRpc), \
            mock.patch.object(cli, "DeltaChat") as dc_cls:
        dc_cls.return_value.get_all_accounts.return_value = accounts
        cli.Relay("relay.example", pathlib.Path(tmp), stack, cli.Out(quiet=True, verbose=0))


class DropUnconfiguredTest(unittest.TestCase):
    def test_removes_only_unconfigured_profiles(self):
        good = FakeAccount(1, "caller@relay.example")
        orphans = [FakeAccount(i) for i in range(2, 6)]
        open_relay([good, *orphans])
        self.assertFalse(good.removed)
        self.assertTrue(all(ac.removed for ac in orphans))

    def test_failed_remove_does_not_abort(self):
        stuck, orphan = FakeAccount(1, fail_remove=True), FakeAccount(2)
        open_relay([stuck, orphan])
        self.assertTrue(orphan.removed)


if __name__ == "__main__":
    unittest.main()
