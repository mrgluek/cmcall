# cmcall — chatmail relay call tester

`cmcall` is to Delta Chat **calls** what [cmping](https://github.com/mrgluek/cmping)
is to messages: it places a real Delta Chat call between chatmail relays and
reports whether, how fast and how well it works.

A Delta Chat call has two halves, and `cmcall` tests both:

1. **Signaling** goes through the relays as ordinary (encrypted) Delta Chat
   messages: the caller's WebRTC offer travels relay1 → relay2, the callee's
   answer relay2 → relay1. `cmcall` times both legs.
2. **Media** is WebRTC (ICE/DTLS/RTP/Opus). Chatmail relays hand out TURN
   credentials over IMAP METADATA; `cmcall` forces the call through those TURN
   servers (`--ice relay`, the default), beeps into the call, has the other
   side echo the audio back and times every echoed beep. RTP loss, jitter and
   RTCP round-trip time are reported for both directions.

The WebRTC side runs on [aiortc](https://github.com/aiortc/aiortc); the
approach follows the call handler of
[hermes-deltachat-platform](https://github.com/Simon-Laux/hermes-deltachat-platform).

## Installation

```bash
python3 -m venv cmcall-venv
source cmcall-venv/bin/activate
pip install git+https://github.com/mrgluek/cmcall.git
```

Requires `deltachat-rpc-client`/`deltachat-rpc-server` **2.56.0** or newer
(installed as dependencies) and Python 3.10+.

## Quick start

Call from one relay to itself (caller and callee are both on `chatmail.uk`,
media through its TURN server):

    cmcall chatmail.uk

Call from `chatmail.uk` to `chat.gluek.info` (media through both TURN servers):

    cmcall chatmail.uk chat.gluek.info

Call a deployed echo bot (e.g. the bouncer bot) through its invite link and
also print the report the bot sends back after the call:

    cmcall chatmail.uk --to 'https://i.delta.chat/#...'

Example output:

    # Setting up profiles... Done!
    CMCALL chatmail.uk(x7k2m9p3w@chatmail.uk) -> chat.gluek.info(n8v5c1x6z@chat.gluek.info) ice=relay duration=10s
    turn  caller: turn:203.0.113.10:3478 (same host as relay)   callee: turn:198.51.100.7:3478 (same host as relay)
    call  offer ready in 143 ms (relay:1)
    call  ringing at callee after 812 ms (chatmail.uk -> chat.gluek.info)
    call  accepted, back at caller after 774 ms (chat.gluek.info -> chatmail.uk)
    ice   connected in 96 ms via relay (relay 203.0.113.10:51234 <-> relay 198.51.100.7:60123)
    beep seq=0 time=241.20ms
    beep seq=1 time=239.87ms
    ...
    --- audio echo statistics ---
    10 beeps sent, 10 echoed, 0.00% loss
    rtt min/avg/max/mdev = 238.410/240.512/244.903/1.902 ms
    --- rtp statistics ---
    caller: sent 552, recv 550, lost 0 (0.00%), jitter 1.1 ms, rtcp-rtt 58.3 ms
    callee: sent 552, recv 551, lost 0 (0.00%), jitter 0.9 ms, rtcp-rtt 57.9 ms
    --- timing statistics ---
    account setup: 3.42s
    call signaling: 1.59s
    ice connect: 96 ms

(Addresses above are documentation examples.)

### Reading the numbers

| Line | Meaning |
|---|---|
| `ringing at callee after` | offer delivery relay1 → relay2: SMTP submit, relay-to-relay, IMAP IDLE push |
| `accepted, back at caller after` | answer delivery relay2 → relay1 |
| `ice connected` | ICE + DTLS handshake after the answer arrived |
| `via relay` | path type: `relay` (TURN ↔ TURN), `relay/p2p`, `stun`, `direct` |
| beep `time` / `rtt` | audio round trip caller → echo → caller, **including** Opus encode/decode and the jitter buffers on both ends. aiortc adds a floor of ~180 ms even on localhost; the network share is roughly `rtt − 180 ms`, or see `rtcp-rtt` |
| `rtcp-rtt` | network round trip from RTCP receiver reports |
| `lost`, `jitter` | RTP counters of the audio stream received by that side |

Exit code is `0` when the call connected and echoed audio came back, `1`
otherwise. On failure the last line says at which stage: `setup` (profiles,
TURN credentials), `signaling` (call not delivered / not answered), `ice`
(no media path) or `media` (connected, but no audio came back).

## Options

    cmcall [-d DURATION] [-i INTERVAL] [--ice {relay,all}] [--to INVITE]
           [--echo-delay S] [--timeout S] [--ice-timeout S] [--json] [-v]
           [--reset] relay1 [relay2]

- `-d` seconds of audio probing (default 10), `-i` seconds between beeps (default 1).
- `--ice relay` (default) uses only TURN relay candidates on both sides. With
  two local profiles `--ice all` would connect them directly on the host and
  not test the relays' TURN at all — use it only with `--to`, to see which
  path a remote echo bot ends up on.
- `--to INVITE` calls an echo bot through its invite link; `--echo-delay`
  subtracts a known playback delay of that bot from the RTT;
  `--peer-report S` waits up to S seconds for the bot's `📞` report message.
- `--json` prints one JSON object (all of the above, machine-readable) — this
  is what the bouncer bot's `/cmcall` command consumes.
- `--reset` drops the cached profiles of the tested relays
  (they live in `~/.cache/cmcall/<relay>/`, separate from cmping's).
- `-v` / `-vv` / `-vvv` for core warnings, aiortc info and every core event.

### Test profiles

Like cmping, cmcall reuses its profiles between runs: one deltachat-rpc-server
per relay in `~/.cache/cmcall/<relay>/`, and per relay one profile per role
(`caller`, `callee`, `prober` for `--to`), so a relay can hold up to three.

- If a relay refuses the login of a cached profile (chatmail deletes
  accounts after a period of inactivity), cmcall drops it and creates a new
  one automatically; the JSON result lists the dropped addresses under
  `recreated`. A brand-new profile that is refused fails the run at `setup`.
- Profiles delete their messages after an hour (`delete_device_after`), so
  their databases don't grow with every run.
- Stray calls from earlier runs are ignored: the callee only answers the
  call carrying this run's ICE credentials.

Relays given as an IP address get a random `dclogin:` account; relays with a
mailadm-style endpoint can be given as `https://relay/new_email?t=TOKEN`, and
`https://relay/new` is tried as a fallback, as in cmping.

## Echo service

`cmcall.rtc.EchoPeer` is the answering side used for the local callee. The
[bouncer bot](https://github.com/mrgluek/deltachat_bouncer) uses the same class
to answer real calls: call the bot from any Delta Chat app, hear yourself
back, and get the call statistics as a message when you hang up.

## Development

```bash
pip install -e .
python3 -m unittest discover -s tests -v
```

`tests/test_loopback.py` runs a real WebRTC call between a probe and an echo
peer on localhost; `tests/test_cli_fake.py` runs the whole CLI with fake
Delta Chat accounts. The relay-only test needs a local coturn (command in the
test's docstring) and is skipped without one.
