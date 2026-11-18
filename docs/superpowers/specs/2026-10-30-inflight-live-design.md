# In-flight live match: design

_Status: draft for review, 2026-10-30_

## Intent

The owner wants to watch a live match on a plane with no internet of their own.
cosmosd should get the match onto the plane over the satellite link, keep playing through
the link's drops, and spread it peer to peer to the other devices in the cabin. Viewers
watch a few minutes behind live, without a spinner.

This is the scenario the project was built for ("On plane with Starlink: ~60s behind
live" in `docs/DESIGN.md`). The code has the pieces (bundles, store, epidemic sync, mDNS,
HLS playback) but has only been used for a pre-cut file on one LAN. This design covers
what's missing between that and a real flight.

## The picture

```
 [Ground node]                 [Gateway: on the plane]          [Cabin peers]
 home server / VPS             passenger laptop on the         other laptops: cosmosd
 cosmosd ingest --live  ──►    plane's satellite Wi-Fi   ──►   phones: browser player
 match stream → LIVE           cosmosd serve --peer ground     served by any cosmosd node
 bundles, relay port open      pulls when link is up,
                               keeps what it has when down
             satellite (drops, handovers, ~600 ms+ RTT)       cabin LAN (see below)
```

Three roles, one binary:

1. **Ground node.** Runs where the internet is. Turns a live stream into 10-second LIVE
   bundles as they're produced. Its relay port is reachable from the internet.
2. **Gateway.** One device on the plane that has the satellite connection (the owner's
   laptop with paid in-flight Wi-Fi). It peers with the ground node by address and pulls
   new bundles whenever the link is up.
3. **Cabin peers.** Other devices on a cabin LAN. Laptops run cosmosd and sync from the
   gateway (and each other) over mDNS. Phones don't run cosmosd in v1: they open a page
   served by a cosmosd node and play the HLS stream in the browser.

## MVP scope

There's no flight booked; this is a for-fun project, and the bar is an MVP that
demonstrably works. Since it can't be tried in the air, **"works" means the G6 demo passes
on one machine**:

1. A live source (ffmpeg's test pattern with a burned-in clock, or a real stream URL) runs
   into a ground node.
2. A gateway node pulls from it over a simulated satellite link (about 3 Mbps, 600 ms
   RTT) that the demo cuts for 30 seconds, then for 5 minutes, then restores.
3. A cabin node syncs from the gateway by mDNS.
4. A player on the cabin node plays the whole time about 2 minutes behind the burned-in
   clock. It doesn't stall through the 30-second cut. After the 5-minute cut it stalls,
   then catches back up to about 2 minutes behind.

Build order: G1 live ingest, G2 sync and encoding, G4 live playlist, then G6 demo
harness, which together make the MVP. After that come G3 checks, the per-direction tokens
and read-only API, and G5 phone page. Prebuilt binaries / release CI are after the MVP.

## Goals

- **G1. Live ingest.** `cosmosd ingest --live <url>` reads a live source (HLS/RTMP/
  SRT URL, or a local capture device), cuts 10-second segments with ffmpeg and pushes
  each one as soon as ffmpeg closes it, with the LIVE priority. It runs until stopped and
  survives the source dropping (restart ffmpeg, keep sequence numbers increasing).
- **G2. Satellite-friendly sync.** The gateway gets each new segment over a link that
  drops for seconds to minutes, then picks up where it left off:
  - backoff after failures is capped short (seconds, not the current 32 minutes);
  - a sync runs again soon after it finishes while the peer still has more (no fixed
    30-second wait between segments);
  - for LIVE content, missing bundles are pulled newest-relevant first (see G4), so after a
    long outage the gateway catches up to near-live instead of crawling through the backlog;
  - bundles on the wire are not inflated: today `payload: Vec<u8>` goes out as a JSON
    array of numbers, about 3-4x the segment's size. Fix the encoding.
- **G3. Cabin spread.** Cabin laptops find the gateway by mDNS and sync from it, and from
  each other, with the existing epidemic routing.
- **G4. Live playback that actually plays.** `/play/{content_id}` serves a correct live
  HLS playlist:
  - real segment durations (stored on the bundle, from ffmpeg), not a hardcoded `10.0`;
  - `EXT-X-MEDIA-SEQUENCE` matches the first listed segment, so the list can slide as old
    LIVE bundles expire;
  - a hole in the sequence is handled (skip ahead with `EXT-X-DISCONTINUITY`, or stop the
    list at the hole while it might still arrive: builder's call, see below);
  - the viewer starts a chosen delay behind the newest segment (default: 2 minutes), which
    is the "time-shifted, not buffered" promise.
- **G5. Phone viewing with no app.** `GET /` serves a small page listing the catalog with
  a player. Safari plays HLS natively; other browsers need hls.js, which is embedded in the
  binary (no internet on the plane, so no CDN). The API can bind to the cabin LAN.
- **G6. Testable on the ground.** A repeatable local setup (three nodes on one machine or
  in containers) with a way to cut and restore the ground-to-gateway link and add
  satellite-like latency, so every goal above is checked without flying.

## Non-goals (v1)

- Native phone apps, and phones running cosmosd themselves.
- New transports: BLE, Wi-Fi Direct, AWDL, LoRa. The cabin is a plain IP LAN in v1.
- Full BPv7 / RFC 9171 compliance. Moving toward its CBOR encoding is fine if it falls out
  of G2's encoding fix, but not required.
- Multiple bitrates, transcoding on the plane, DRM, airline system integration.
- Getting the match from any particular provider. The ground node takes a stream URL;
  where that comes from is the owner's business.

## Anti-goals: it fails even if it technically works when

- Playback stalls or loops every time the satellite link blinks.
- After an outage the viewer ends up far behind live and can't get closer.
- It needs the internet anywhere on the plane side beyond the gateway's own link (CDN
  scripts, NTP, update checks).
- The ground node's relay port is open to anyone on the internet without a token.

## Constraints and facts the builder should know

- **Codebase.** About 1,800 lines of Rust: `bundle` (format), `store` (sled), `router`
  (epidemic sync protocol over raw TCP), `peer` (sync loop, backoff), `discovery` (mDNS),
  `api` (axum), `auth` (shared token), `transport` (a `ConvergenceLayer` trait with a TCP
  implementation that the router doesn't actually use yet). There are unit tests for
  bundle, store and auth.
- **Wire compatibility.** No deployed fleet; breaking the sync wire format is acceptable.
  Bump a protocol version in the manifest so mismatched nodes fail clearly, not silently.
- **Toolchain.** The owner's Mac has no Rust toolchain installed yet (ffmpeg is installed).
  The Dockerfile pins `rust:1.77`.
- **Satellite link.** Expect hundreds of ms of round trip, a few Mbps shared, drops from
  seconds (handovers) to many minutes (oceans, weather). Plane Wi-Fi often blocks inbound
  connections, so the gateway always dials out to the ground node, never the reverse.
- **Cabin network.** Plane Wi-Fi usually isolates clients from each other, so cabin peers
  can't rely on it. v1 assumes a cabin LAN the owner brings: a travel router joined to
  the plane Wi-Fi as a client, with its own LAN for the gateway and peers (the plane sees
  one device). Any other shared LAN works the same way.
- **Security.** The ground node must refuse to start its relay on a non-local address
  without `--relay-token`. The token goes in plaintext over TCP today; v1 accepts that, and
  TLS or a tunnel is later work. The cabin side is a LAN the owner controls.
- **Rights and terms.** Sharing one paid Wi-Fi connection and redistributing a broadcast
  may break airline terms and broadcast rights. That's the owner's call when using it, but
  the docs shouldn't advertise it as a way around either.

## Decisions made so far

The owner's decisions:

- cosmosd itself, not the anvil homelab, is where this work lives.
- Satellite is the link onto the plane. Peer-to-peer in the cabin carries it on from there.

Calls made on the owner's behalf, still open to change:

- The gateway is a passenger laptop on paid plane Wi-Fi (the owner's), not the airline's
  onboard system.
- The cabin LAN is a travel router in v1 (no new radio transports).
- Phones watch in a browser rather than through an app.
- Default viewer delay: 2 minutes behind the newest segment.
- **Outages longer than the delay:** playback stalls, then jumps to newest-minus-delay
  when the link comes back, skipping what was missed. The pull order after an outage
  fetches that window first. A bigger delay is just a flag (`--delay`); the 30-minute LIVE
  TTL caps it.
- Segment length stays 10 seconds.
- **The ground node transcodes.** A live source can be any codec or bitrate, and the plane
  link gives one device a few Mbps at best, so the ground node always re-encodes to
  H.264/AAC (plays in Safari and hls.js) at a capped bitrate: default 720p at about
  1.2 Mbps total, set by `--max-bitrate`. File ingest keeps `-codec copy`. The G6 harness
  simulates a link of about 3 Mbps.
- **The relay port is configurable, and the docs suggest 443 for the ground node.** Plain
  TCP on 443 gets through many in-flight networks that block odd ports. Networks that
  proxy or inspect HTTPS still block it. Relay over TLS/WebSocket stays later work, and
  becomes v1 work if a real flight shows it's needed.
- **Tokens are per direction.** `RelayAuth` today uses one token both to dial out and to
  accept connections, so a gateway holding the ground node's token would demand it from
  every cabin laptop. Split it: a token per outgoing peer (`--peer host:port@token`, or a
  separate flag) and an optional token for the listener. Cabin peers don't need the
  ground node's secret.
- **The API is read-only beyond localhost.** When the API binds to the cabin LAN, only
  the player page, catalog, playlist and segment routes are served to other devices.
  `POST /ingest` and `POST /peers` answer only to localhost.
- **Cabin laptops run prebuilt binaries (after the MVP).** The owner and companions install a released
  build (Homebrew tap, or release downloads for Linux and Windows), not a Rust toolchain.
  CI producing those builds is in scope.

## Left to the builder

- The wire encoding (CBOR with `serde_bytes`, bincode, or length-prefixed binary payload
  after a JSON header) as long as payloads aren't inflated and the format is versioned.
- How "newest-relevant first" is decided: for example, pull everything within the viewer
  delay window of the newest sequence first, then fill backward.
- Hole handling in the playlist (G4).
- Whether the router moves onto the `ConvergenceLayer` trait now or later.
- The shape of the ground test harness (G6): shell script, docker compose, or a Rust
  integration test. On macOS, latency and cuts can use `pfctl`/`dnctl`. In Linux containers,
  `tc netem` and `iptables`.
- Store performance fixes the work runs into (several store calls scan and deserialize
  every bundle, including the payloads).
