# cmcall changelog

## 0.1.0

### Added

- `cmcall relay1 [relay2]`: place a Delta Chat call from a profile on relay1 to
  a profile on relay2 that echoes the audio back. Reports signaling delivery
  in both directions, ICE/DTLS connect time and the selected candidate pair,
  echoed-beep audio round trip (min/avg/max/mdev, loss) and RTP/RTCP loss,
  jitter and round-trip time for both sides.
- Relay-only ICE by default (`--ice relay`), so media goes through the
  relays' own TURN servers; `--ice all` to allow direct/STUN paths.
- `--to INVITE`: securejoin an echo bot (e.g. bouncer) and probe its echo,
  optionally printing the statistics message the bot sends after the call.
- `--json` machine-readable output, failing `stage` + `error` on failure.
- `cmcall.rtc`: RPC-agnostic building blocks (`CallLoop`, `EchoPeer`,
  `ProbePeer`, stats helpers) shared with the bouncer bot's echo service.
- Unit tests: in-process WebRTC loopback, full CLI run against fake Delta Chat
  accounts, relay-only run through a local coturn.
