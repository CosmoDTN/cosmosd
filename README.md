# cosmosd

**Delay-tolerant streaming daemon — stream across the void.**

`cosmosd` is the core relay node for the CosmoDTN framework. It stores HLS video segments as BPv7-inspired bundles, forwards them hop-by-hop to peers over any available link, and serves them to local players via a standard HTTP/HLS API.

Designed for networks where internet connectivity is absent, intermittent, or extremely high-latency: space stations, aircraft, submarines, remote villages, disaster zones.

---

## How it works

Content is chunked into 10-second HLS segments by [`cosmoscast-ingest`](https://github.com/cosmodtn/cosmocast-ingest). Each segment is wrapped in a **bundle** — a self-describing unit with a content ID, sequence number, priority, TTL, and a BLAKE3 integrity hash.

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

**Prerequisites:** Rust 1.78+

```bash
git clone https://github.com/cosmodtn/cosmosd
cd cosmosd
cargo build --release
# Binary at: target/release/cosmosd
```

---

## Usage

```bash
# Start the daemon (defaults: HTTP on 7777, relay on 7778)
cosmosd serve

# Custom ports
cosmosd serve --port 8888 --relay-port 8889

# Connect to known peers at startup
cosmosd serve --peer 192.168.1.5:7778 --peer 192.168.1.6:7778

# Custom store path and node ID
cosmosd serve --store /data/cosmosd --node-id iss-node-1

# Require a shared secret for relay connections
cosmosd serve --relay-token my-secret

# Disable mDNS auto-discovery (for controlled environments)
cosmosd serve --no-discovery
```

All options:

| Flag | Default | Description |
|------|---------|-------------|
| `--port` | `7777` | HTTP API port (SDK and ingest connect here) |
| `--relay-port` | `7778` | TCP relay port (peer-to-peer bundle sync) |
| `--peer` | — | Known peer relay address; pass multiple times |
| `--store` | `.cosmosd/store` | Bundle store directory |
| `--node-id` | hostname | Human-readable node identifier |
| `--relay-token` | — | Shared secret; peers must supply the same token |
| `--no-discovery` | false | Disable mDNS auto-discovery |

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

Versioned protocol specs live in `docs/spec/`:
- `spec-001-bundle-format.md` — bundle structure and fields
- `spec-002-convergence-layer.md` — transport plugin interface
- `spec-003-routing.md` — epidemic routing and contact scheduling
- `spec-004-hls-mapping.md` — HLS segment to bundle mapping

---

## License

MIT
