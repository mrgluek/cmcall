"""The closed-socket guard must not break sends over a TURN relay."""
import unittest
from unittest.mock import MagicMock

from aioice.ice import StunProtocol

import cmcall.rtc  # noqa: F401  (installs the guard)


class TurnLikeTransport:
    """Like aioice's TurnTransport: has sendto() but no is_closing()."""

    def __init__(self):
        self.sent = []

    def sendto(self, data, addr):
        self.sent.append((data, addr))


class StunGuardTest(unittest.IsolatedAsyncioTestCase):
    def _protocol(self, transport):
        proto = StunProtocol(MagicMock())
        proto.transport = transport
        return proto

    async def test_send_through_turn_transport(self):
        transport = TurnLikeTransport()
        message = MagicMock()
        message.__bytes__ = lambda self: b"stun"
        self._protocol(transport).send_stun(message, ("192.0.2.1", 3478))
        self.assertEqual(len(transport.sent), 1)

    async def test_skip_when_transport_closing_or_gone(self):
        closing = MagicMock()
        closing.is_closing.return_value = True
        self._protocol(closing).send_stun(MagicMock(), ("192.0.2.1", 3478))
        closing.sendto.assert_not_called()
        self._protocol(None).send_stun(MagicMock(), ("192.0.2.1", 3478))


if __name__ == "__main__":
    unittest.main()
