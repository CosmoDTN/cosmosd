> **Disclaimer:** This project was largely written with AI assistance. Most of the code has not been formally reviewed, tested in production, or audited for correctness. Use at your own risk. Contributions, reviews, and bug reports are very welcome.

# cosmosd

**Delay-tolerant streaming daemon — stream across the void.**

`cosmosd` is the core relay node for the CosmoDTN framework. It stores HLS video segments as BPv7-inspired bundles, forwards them hop-by-hop to peers over any available link, and serves them to local players via a standard HTTP/HLS API.

Designed for networks where internet connectivity is absent, intermittent, or extremely high-latency: space stations, aircraft, submarines, remote villages, disaster zones.

---

## How it works

Content is chunked into 10-second HLS segments by `cosmosd ingest`. Each segment is wrapped in a **bundle**. A bundle names its publisher, content ID, sequence number and priority. It also carries a lifetime and an age. The node that ingests signs a manifest that lists each segment's BLAKE3 hash. Other nodes accept only content from publishers they have paired. See [Pairing and signed content](#pairing-and-signed-content).

`cosmosd` uses **epidemic routing**: when two nodes connect, they exchange bundle manifests and transfer what the other is missing. Bundles propagate through the network hop-by-hop until they reach every reachable node.

Viewers watch via any HLS player pointed at `http://localhost:7777/play/{content_id}`, or `/play/{publisher}/{content_id}` when two publishers use the same content ID. Playback is **time-shifted, not buffered**. cosmosd pre-positions chunks ahead of the playhead so there is no spinner.

```
[Ingest node]          [Relay node]          [Viewer node]
cosmoscast-ingest  →   cosmosd stores    →   cosmosd serves
POST /ingest           epidemic sync         GET /play/...
                        TCP / BLE / LoRa      HLS player
```

---

## Install

### macOS — Homebrew (recommended)

```bash
brew tap CosmoDTN/tap
brew install cosmosd
```

ffmpeg (required for `cosmosd ingest`):
```bash
brew install ffmpeg
```

---

### Ubuntu / Debian

Install system dependencies first:
```bash
sudo apt update
sudo apt install pkg-config libssl-dev ffmpeg
```

Install Rust (if not already installed):
```bash
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
source $HOME/.cargo/env
```

Install cosmosd:
```bash
cargo install cosmosd
```

---

### Other Linux (Fedora, Arch, Alpine)

```bash
# Fedora / RHEL
sudo dnf install pkg-config openssl-devel ffmpeg

# Arch
sudo pacman -S pkg-config openssl ffmpeg

# Alpine
sudo apk add pkgconfig openssl-dev ffmpeg
```

Then:
```bash
cargo install cosmosd
```

---

### Windows

Install Rust from [rustup.rs](https://rustup.rs), then:
```bash
cargo install cosmosd
```

For `cosmosd ingest`, install [ffmpeg for Windows](https://ffmpeg.org/download.html) and add it to your PATH.

---

### From source

```bash
git clone https://github.com/CosmoDTN/cosmosd
cd cosmosd
cargo build --release
# Binary at: target/release/cosmosd
```

---

## Usage

```bash
# Create this node's publisher key (once, before the first ingest)
cosmosd keys init

# Start the daemon (defaults: HTTP on 7777, relay on 7778)
cosmosd serve

# Ingest a local video file (requires ffmpeg + daemon running)
# Ingest signs under the node's key, then publishes the signed manifest.
cosmosd ingest /path/to/movie.mkv
cosmosd ingest /path/to/movie.mkv --title "Interstellar"
cosmosd ingest /path/to/movie.mkv --daemon http://127.0.0.1:8888  # a daemon on another port
# Ingest runs on the daemon's own host: POST /ingest answers only to localhost,
# so a remote --daemon gets 403 Forbidden.

# Start daemon

# Custom ports
cosmosd serve --port 8888 --relay-port 8889

# Connect to known peers at startup
cosmosd serve --peer 192.168.1.5:7778 --peer 192.168.1.6:7778

# Custom store path and node ID
cosmosd serve --store /data/cosmosd --node-id iss-node-1

# Require a shared secret from peers that dial us
cosmosd serve --relay-token my-secret

# Present a peer's token when dialing it (token follows the first @)
cosmosd serve --peer ground.example.com:443@their-secret

# Disable mDNS auto-discovery (for controlled environments; needs a token)
cosmosd serve --no-discovery --relay-token my-secret
```

All options:

| Flag | Default | Description |
|------|---------|-------------|
| `--port` | `7777` | HTTP API port (SDK and ingest connect here) |
| `--relay-port` | `7778` | TCP relay port (peer-to-peer bundle sync) |
| `--peer` | — | Known peer relay address, `host:port` or `host:port@token` (token after the first `@`, presented only to that peer); pass multiple times |
| `--store` | `.cosmosd/store` | Bundle store directory |
| `--node-id` | hostname | Human-readable node identifier |
| `--relay-token` | — | Shared secret guarding our relay port; peers that dial us must present it. It is not sent to peers we dial |
| `--open-relay` | false | Allow a relay with no `--relay-token` with `--no-discovery` or on a machine that has a public IPv4 address (otherwise `serve` refuses to start) |
| `--no-discovery` | false | Disable discovery: no mDNS, and peers that connect in are not dialed back. This is the ground role, so it needs `--relay-token` (or `--open-relay`) |
| `--bind` | `127.0.0.1` | HTTP API bind address (`0.0.0.0` to expose on all interfaces) |
| `--delay` | `120` | Seconds the viewer plays behind live; also the window of newest segments a gateway pulls first. Use the same value on the gateway and cabin nodes |
| `--sync-interval` | `5` | Seconds between syncs with a healthy peer |
| `--key-dir` | `$HOME/.config/cosmosd` | Publisher key and paired keys. Bundles from peers are accepted only from this node's key and the keys paired here |

---

## Pairing and signed content

Every piece of content has a publisher. A publisher is a node with an Ed25519
key. Create the key on the node that ingests:

```bash
cosmosd keys init    # create this node's publisher key
cosmosd keys show    # print its pairing string and key ID
```

`keys show` prints a pairing string that starts with `cosmos-key:`. Copy it to
every node that should accept this publisher's content and pair it there:

```bash
cosmosd keys pair cosmos-key:...
cosmosd keys list              # the paired key IDs
cosmosd keys unpair <key id>   # stop trusting a publisher
```

Keys live in `$HOME/.config/cosmosd`. Pass `--key-dir` to `keys` and `serve`
to use another directory. The key file is the node's identity. Do not copy it
to other machines and do not commit it.

Ingest signs content under the node's key. For each content item the node
publishes a signed manifest. The manifest lists every segment with its BLAKE3
hash. A live stream gets a new manifest version after each segment, listing
the last 180 segments. A file gets its manifest when `cosmosd ingest` finishes.
Without a key, `POST /ingest` answers 409 with `run cosmosd keys init first`.

A node stores a bundle from a peer only when it checks out:

- The publisher is the node's own key or a paired key.
- A manifest carries a valid signature. It replaces the stored one only when
  its version is higher.
- A segment matches the hash, sequence and duration its manifest lists. A
  segment that arrives before its manifest waits up to 10 minutes for it.
- The priority is `live` exactly when the manifest is live. The lifetime is
  the one the priority sets. A stored segment takes its title from the
  manifest.

Everything else is dropped. So the payload bytes, sequence and duration of a
segment are checked against a signed manifest. The age and the creation time
are not signed. A carrier can change them, which moves when a bundle expires
or where the live edge sits, but not the video itself.

A node refuses a manifest only when it holds a newer one. Once its manifest
expires it accepts an older signed version again, so a peer can replay an old
version of the content for a while.

A new live manifest is the publisher's whole window. When one is stored, the
node drops the segments of that stream that it does not list. A publisher
numbers manifest versions with a counter that outlives expiry, so a restarted
live stream always gets a higher version than before.

`serve` reads the keys once, at startup. After `keys pair` or `keys unpair`,
restart a running node. Until the restart it keeps its old trust. So a new
pairing, and a revocation by `keys unpair`, takes effect after a restart.
At startup a node deletes the stored content of every publisher it no longer
trusts, so it stops offering it to peers.

---

## Live over a dropping link

cosmosd can carry a live stream onto a plane over satellite Wi-Fi, where the
link blinks for seconds and drops for minutes, and spread it around the cabin.
There are three roles:

- **Ground node**, on a machine with good internet. It transcodes a live source
  and holds the segments. The gateway dials in to it, so it needs a reachable
  relay port and a token:

  ```bash
  cosmosd keys init      # once, then pair its key on the other nodes
  cosmosd serve --relay-port 443 --relay-token "$SECRET" --no-discovery
  cosmosd ingest --live <stream URL> --content-id match --title "The match"
  ```

  Always run the ground relay with a token. `serve` refuses an open relay on a
  public IPv4 address, but NAT, port forwarding and containers can expose a
  relay that has no public address of its own, so a `--no-discovery` node
  refuses to start without `--relay-token` (or `--open-relay`).

  `ingest --live` reads any URL ffmpeg can (HLS, RTMP, SRT, UDP) and re-encodes
  it to 720p H.264/AAC capped at `--max-bitrate` (default 1200 kbps total), in
  10-second segments pushed as soon as each one closes. It restarts ffmpeg if
  the source drops, and runs until Ctrl-C or SIGTERM.

  Run the relay on **port 443**: plain TCP on 443 gets through many in-flight
  networks that block odd ports. Networks that proxy or inspect HTTPS still
  block it. On Linux, binding 443 needs root or
  `setcap cap_net_bind_service=+ep`; or forward 443 to the default 7778.

- **Gateway**, a laptop on the plane's paid Wi-Fi. It always dials out to the
  ground node, never the reverse, and announces itself on the cabin LAN by mDNS.
  The ground's token rides on the `--peer` argument, so the gateway holds it
  without demanding it from the cabin nodes that dial in:

  ```bash
  cosmosd keys pair cosmos-key:...   # the ground node's `keys show` output
  cosmosd serve --peer "ground.example.com:443@$SECRET"
  ```

- **Cabin nodes**, other laptops on the same cabin LAN (a travel router you
  bring, since plane Wi-Fi isolates clients). They find the gateway by mDNS and
  serve the player to phones:

  ```bash
  cosmosd keys pair cosmos-key:...   # the ground node's `keys show` output
  cosmosd serve --bind 0.0.0.0
  # watch: http://<laptop>:7777/play/match in Safari, VLC or ffplay
  ```

  A phone on the cabin LAN opens `http://<laptop-ip>:7777/` (the cabin node's
  `--port`) in its browser and picks a title: the player page and hls.js are
  built into cosmosd, so it needs no app and no internet.

Playback is time-shifted, not buffered: `/play/<id>` lists only segments at
least `--delay` seconds old (default 120), so everything newer is buffer that
rides out link drops. A drop shorter than the delay is invisible to viewers.
After a longer outage playback stalls, then jumps to the live edge minus the
delay when the link returns; the gateway pulls that window first. A bigger
`--delay` survives longer outages (up to the 30-minute live TTL). Set the same
`--delay` on the gateway and the cabin nodes: the gateway's sets how much it
pulls first, the cabin's how far behind live phones play.

Sharing a paid in-flight Wi-Fi connection and redistributing a broadcast may
break the airline's terms of service and the broadcaster's rights: check both
before you use this.

### Try it on one machine

```bash
scripts/demo-inflight.sh
```

Needs ffmpeg and a Rust toolchain, and takes about 17 minutes. It runs an ffmpeg
test pattern into a ground node, a gateway that pulls it through `linksim` (a
3 Mbps, 600 ms round-trip proxy, `examples/linksim.rs`), and a cabin node that
finds the gateway by mDNS. A headless player (`examples/playcheck.rs`) watches
the cabin while the script cuts the link for 30 seconds, then for 5 minutes.
It checks that the short cut causes no stall, the long one does, and playback
ends about 2 minutes behind live, then prints `DEMO PASS` or `DEMO FAIL: <why>`.
The ground node gets a fresh key and the other two pair it. All three key dirs
are temp dirs, so the run never touches your own keys.
Watch along with `ffplay http://127.0.0.1:37777/play/demo`; logs are kept in the
temp directory it prints. If mDNS can't work on your machine,
`DEMO_CABIN_PEER=1` gives the cabin an explicit `--peer` to the gateway instead.

---

## HTTP API

The API binds to `127.0.0.1` by default. `--bind 0.0.0.0` exposes the read
routes (`GET`) to the network, so phones can play. The write routes
(`POST /ingest`, `POST /ingest/finish`, `POST /peers`) answer only to
localhost, whatever `--bind` says. They return 403 to anyone else.

The short forms `/play/:content_id` and `/segment/:content_id/:sequence` work
while exactly one publisher has that content ID. When several do, they answer
300 with a JSON list of the publisher IDs. Playlists always list the full
`/segment/:publisher/...` form.

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/` | Phone player page: lists titles and plays them (hls.js built in, no internet needed) |
| `GET` | `/hls.min.js` | The embedded hls.js used by the player page |
| `GET` | `/health` | Node status, version, publisher key ID (null without a key) |
| `GET` | `/catalog` | All stored content, one entry per publisher and content ID |
| `GET` | `/play/:publisher/:content_id` | HLS M3U8 playlist |
| `GET` | `/play/:content_id` | Same, when one publisher has this content ID |
| `GET` | `/segment/:publisher/:content_id/:sequence` | Raw `.ts` segment bytes |
| `GET` | `/segment/:content_id/:sequence` | Same, when one publisher has this content ID |
| `POST` | `/ingest` | Add a segment, signed under the node's key. A `live` segment also publishes the next manifest version (localhost only) |
| `POST` | `/ingest/finish` | Publish the signed manifest of a finished item, body `{"content_id": "..."}` (localhost only) |
| `GET` | `/peers` | Connected peers and sync status |
| `POST` | `/peers` | Add a peer at runtime (localhost only) |

### Example: ingest a segment

```bash
PAYLOAD_B64=$(base64 -i segment_00001.ts)

curl -s -X POST http://localhost:7777/ingest \
  -H "Content-Type: application/json" \
  -d "{
    \"content_id\": \"my_show_ep1\",
    \"content_title\": \"My Show — Episode 1\",
    \"sequence\": 1,
    \"mime_type\": \"video/mp2t\",
    \"priority\": \"vod\",
    \"duration_ms\": 10000,
    \"payload_b64\": \"$PAYLOAD_B64\"
  }"

# After the last segment of a non-live item, publish its manifest
curl -s -X POST http://localhost:7777/ingest/finish \
  -H "Content-Type: application/json" \
  -d '{"content_id": "my_show_ep1"}'
```

### Example: play content

Point any HLS player at:
```
http://localhost:7777/play/my_show_ep1
```

The short URL is a convenience. It answers 300 if two publishers share the
content ID. For a live stream, or any player that stays open, use the
publisher-qualified URL instead. `/catalog` and `cosmosd ingest` show the
publisher ID.
```
http://localhost:7777/play/<publisher>/my_show_ep1
```

Or with `ffplay`:
```bash
ffplay http://localhost:7777/play/my_show_ep1
```

---

## Bundle priorities

| Priority | TTL | Routing |
|----------|-----|---------|
| `live` | 30 minutes | Aggressive — any available link |
| `near_live` | 6 hours | Best-effort |
| `vod` | 30 days | Carry indefinitely |

A bundle expires by age, not by wall clock. Its lifetime comes from its
priority. Its age counts how long it has existed. Each node adds the time it
held the bundle before passing it on. The bundle expires when its age reaches
its lifetime. A node with a wrong clock still expires bundles on time.

Each node measures held time on a clock that only moves forward. It saves that
clock every 5 minutes and on a clean stop. Time while a node is off is not
counted. Expired bundles are reaped automatically every 5 minutes.

---

## Architecture

```
cosmosd
├── src/
│   ├── main.rs          — CLI, startup, background task wiring
│   ├── bundle/          — Bundle and content info, keyed by publisher
│   ├── wire/            — RFC 9171 encoding of bundles
│   ├── trust/           — Publisher keys, pairing and signed manifests
│   ├── store/           — Bundle store (sled), age-based expiry, admission checks
│   ├── router/          — Epidemic routing logic
│   ├── peer/            — Peer manager: outgoing sync + relay listener
│   ├── transport/       — Convergence layer trait + TCP implementation
│   ├── discovery/       — mDNS peer auto-discovery
│   ├── auth/            — Optional relay token authentication
│   └── api/             — Local HTTP API (axum)
```

---

## Transport plugins (convergence layers)

`cosmosd` ships with TCP. Any transport medium can be added by implementing three traits: `ConvergenceLayer`, `BundleListener`, and `BundleStream`.

See [`TRANSPORT.md`](TRANSPORT.md) for the interface, bundle framing spec, and bandwidth guidance per medium (TCP, Bluetooth, LoRa, ham radio, USB sneakernet).

Community transports:
- [`cosmodtn/transport-reticulum`] _(planned)_
- [`cosmodtn/transport-lora`] _(planned)_

---


## Protocol

Bundles travel as standard [IETF BPv7 (RFC 9171)](https://www.rfc-editor.org/rfc/rfc9171) bundles. The streaming fields (publisher, content ID, sequence number, priority, MIME type) ride in a private-use block, type 192. The age block carries the bundle's age.

---

## License

MIT
