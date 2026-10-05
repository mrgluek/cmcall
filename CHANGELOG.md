# cmcall changelog

## 0.1.1

### Fixed

- `--ice relay` was not strictly relay-only when the relay also announces a
  STUN server: aioice's RELAY policy drops host candidates but still gathers
  server-reflexive ones, which could let ICE pick a STUN path and leave TURN
  untested. The STUN server is now dropped as well, and a local SDP with any
  non-relay candidate fails the run at `setup`.

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
- `EchoPeer` answers Delta Chat app offers (audio + video + negotiated
  `iceTrickling`/`mutedState` data channels), uses trickled candidates and
  drains received video so aiortc's decoded-frame queue cannot grow.
- `AudioJitterBuffer`: replaces aiortc's audio jitter buffer, which stalls
  at a lost packet until it is 320 ms deep and stays there - 2 % loss used to
  turn a 180 ms echo round trip into 550 ms and drop extra frames. Lost
  packets now cost only themselves (also for the bouncer echo service).
- Cached test profiles the relay no longer accepts (deleted after
  inactivity) are replaced by fresh ones automatically (`recreated` in the
  JSON result); profiles delete their messages after an hour.
- RTP stats no longer fail on calls shorter than the first RTCP report
  (aiortc leaves `roundTripTime` unset until then).
- Unit tests: in-process WebRTC loopback, a Delta Chat app-shaped offer,
  full CLI run against fake Delta Chat accounts, relay-only run through a
  local coturn.
