> **Disclaimer:** This project was largely written with AI assistance. Most of the code has not been formally reviewed, tested in production, or audited for correctness. Use at your own risk. Contributions, reviews, and bug reports are very welcome.

# cosmosd

**Delay-tolerant streaming daemon — stream across the void.**

`cosmosd` is the core relay node for the CosmoDTN framework. It stores HLS video segments as BPv7-inspired bundles, forwards them hop-by-hop to peers over any available link, and serves them to local players via a standard HTTP/HLS API.

Designed for networks where internet connectivity is absent, intermittent, or extremely high-latency: space stations, aircraft, submarines, remote villages, disaster zones.

---

## How it works

Content is chunked into 10-second HLS segments by [`cosmoscast-ingest`](https://github.com/CosmoDTN/cosmocast-ingest). Each segment is wrapped in a **bundle** — a self-describing unit with a content ID, sequence number, priority, TTL, and a BLAKE3 integrity hash.

`cosmosd` uses **epidemic routing**: when two nodes connect, they exchange bundle manifests and transfer what the other is missing. Bundles propagate through the network hop-by-hop until they reach every reachable node.

Viewers watch via any HLS player pointed at `http://localhost:7777/play/{content_id}`. Playback is **time-shifted, not buffered** — cosmosd pre-positions chunks ahead of the playhead so there is no spinner.

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
# Start the daemon (defaults: HTTP on 7777, relay on 7778)
cosmosd serve

# Ingest a local video file (requires ffmpeg + daemon running)
cosmosd ingest /path/to/movie.mkv
cosmosd ingest /path/to/movie.mkv --title "Interstellar"
cosmosd ingest /path/to/movie.mkv --daemon http://10.2.0.119:7777

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
| `--delay` | `120` | Seconds live playlists sit behind the newest segment |
| `--sync-interval` | `5` | Seconds between syncs with a healthy peer |

---

## Live over a dropping link

cosmosd can carry a live stream onto a plane over satellite Wi-Fi, where the
link blinks for seconds and drops for minutes, and spread it around the cabin.
There are three roles:

- **Ground node**, on a machine with good internet. It transcodes a live source
  and holds the segments. The gateway dials in to it, so it needs a reachable
  relay port and a token:

  ```bash
  cosmosd serve --relay-port 443 --relay-token "$SECRET" --no-discovery
  cosmosd ingest --live <stream URL> --content-id match --title "The match"
  ```

  `ingest --live` reads any URL ffmpeg can (HLS, RTMP, SRT, UDP) and re-encodes
  it to 720p H.264/AAC capped at `--max-bitrate` (default 1200 kbps total), in
  10-second segments pushed as soon as each one closes. It restarts ffmpeg if
  the source drops, and runs until Ctrl-C.

  Run the relay on **port 443**: plain TCP on 443 gets through many in-flight
  networks that block odd ports. Networks that proxy or inspect HTTPS still
  block it. On Linux, binding 443 needs root or
  `setcap cap_net_bind_service=+ep`; or forward 443 to the default 7778.

- **Gateway**, a laptop on the plane's paid Wi-Fi. It always dials out to the
  ground node, never the reverse, and announces itself on the cabin LAN by mDNS.
  The ground's token rides on the `--peer` argument, so the gateway holds it
  without demanding it from the cabin nodes that dial in:

  ```bash
  cosmosd serve --peer "ground.example.com:443@$SECRET"
  ```

- **Cabin nodes**, other laptops on the same cabin LAN (a travel router you
  bring, since plane Wi-Fi isolates clients). They find the gateway by mDNS and
  serve the player to phones:

  ```bash
  cosmosd serve --bind 0.0.0.0
  # watch: http://<laptop>:7777/play/match in Safari, VLC or ffplay
  ```

Playback is time-shifted, not buffered: `/play/<id>` lists only segments at
least `--delay` seconds old (default 120), so everything newer is buffer that
rides out link drops. A drop shorter than the delay is invisible to viewers.
After a longer outage playback stalls, then jumps to the live edge minus the
delay when the link returns; the gateway pulls that window first. A bigger
`--delay` survives longer outages (up to the 30-minute live TTL).

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
Watch along with `ffplay http://127.0.0.1:37777/play/demo`; logs are kept in the
temp directory it prints. If mDNS can't work on your machine,
`DEMO_CABIN_PEER=1` gives the cabin an explicit `--peer` to the gateway instead.

---

## HTTP API

The API binds to `127.0.0.1` only — it is local-only by design.

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/health` | Node status, version |
| `GET` | `/catalog` | List all stored content |
| `GET` | `/play/:content_id` | HLS M3U8 playlist |
| `GET` | `/segment/:content_id/:sequence` | Raw `.ts` segment bytes |
| `POST` | `/ingest` | Add a bundle (used by cosmoscast-ingest) |
| `GET` | `/peers` | Connected peers and sync status |
| `POST` | `/peers` | Add a peer at runtime |

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
    \"payload_b64\": \"$PAYLOAD_B64\"
  }"
```

### Example: play content

Point any HLS player at:
```
http://localhost:7777/play/my_show_ep1
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

Expired bundles are reaped automatically every 5 minutes.

---

## Architecture

```
cosmosd
├── src/
│   ├── main.rs          — CLI, startup, background task wiring
│   ├── bundle/          — BPv7-inspired bundle format, BLAKE3 integrity
│   ├── store/           — Content-addressed bundle store (sled)
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

The bundle format is a working subset of [IETF BPv7 (RFC 9171)](https://www.rfc-editor.org/rfc/rfc9171) extended with streaming-specific fields (sequence number, priority, HLS MIME type). Full BPv7 wire compatibility is a future goal.

---

## License

MIT
