# CosmosD — Design Document
_Delay-Tolerant Streaming Protocol for Disconnected Networks_

## Problem

Streaming video (YouTube, live sports) requires continuous internet connectivity. In space,
on aircraft, submarines, remote villages, and disaster zones — connectivity is absent, intermittent,
or extremely high-latency. No existing open framework solves streaming across these environments.

## Solution

CosmosD is an open-source framework for time-shifted video streaming over Delay-Tolerant Networks (DTN).
It implements a subset of IETF Bundle Protocol v7 (RFC 9171) extended with streaming-specific semantics
(chunk priority, TTL-based expiry, HLS-compatible delivery).

Content is chunked into 10-second HLS segments, bundled with metadata, and forwarded hop-by-hop through
relay nodes using a store-carry-forward model. Viewers watch time-shifted content with no buffering —
the node pre-positions chunks ahead of the playhead.

**Tagline:** _Stream across the void._

---

## Repositories

| Repo | Language | Purpose |
|------|----------|---------|
| `cosmoscast/cosmosd` | Rust | Core daemon + BPv7 + TCP + BLE convergence layers |
| `cosmoscast/cosmoscast-sdk` | Dart / Python / JS | App SDK over cosmosd local HTTP API |
| `cosmoscast/cosmoscast-ingest` | Python | yt-dlp + ffmpeg HLS pipeline |
| `cosmoscast/transport-reticulum` | Python/Rust | Reticulum convergence layer (community) |
| `cosmoscast/transport-lora` | Rust | LoRa convergence layer (community) |

---

## Architecture — Three Layers

### Layer 1: Convergence Layer (Transport)

A single Rust trait. Implement it to add any transport medium:

```rust
pub trait ConvergenceLayer: Send + Sync {
    fn name(&self) -> &str;
    async fn connect(&self, peer: &PeerAddr) -> Result<Box<dyn BundleStream>>;
    async fn listen(&self) -> Result<Box<dyn BundleListener>>;
}
```

Ships with: **TCP** (LAN/WiFi demo), **BLE** (phone-to-phone).
Community builds: LoRa, Reticulum, ham radio, USB sneakernet, laser comms.

### Layer 2: `cosmosd` Daemon

Single Rust binary. Runs on Raspberry Pi, Android, satellite hardware, ISS.

**Internal components:**

- **Bundle store** — content-addressed chunk storage (BPv7 format)
- **Priority queue** — `LIVE > NEAR_LIVE > VOD` scheduling
- **Router** — epidemic routing with contact-window scheduling
- **Convergence manager** — pluggable transport registry
- **Local HTTP API** — used by SDK and ingest pipeline

**HTTP API (local only, port 7777):**

```
GET  /catalog                    — list available content
GET  /play/{bundle_id}/index.m3u8 — HLS playlist for a bundle
POST /ingest                     — add new bundle from pipeline
GET  /peers                      — connected peers + signal strength
GET  /health                     — node status
```

Peer discovery: mDNS on LAN, manual address for radio/LoRa peers.

### Layer 3: SDK + Ingest Pipeline

**`cosmoscast-sdk`** — thin HTTP client over cosmosd's local API.

```dart
// Flutter usage
final sdk = CosmoscastSDK(daemonUrl: 'http://localhost:7777');
await sdk.play(bundleId: 'ipl_match_2026_03_07');
final peers = await sdk.peers();
```

Bindings: Flutter (Dart), Python, JavaScript (browser via WASM daemon).

**`cosmoscast-ingest`** — Python pipeline:

```
YouTube URL / Live RTMP stream
  → yt-dlp / ffmpeg
  → HLS segments (10s chunks)
  → BPv7 bundle (priority + TTL metadata)
  → POST /ingest to local cosmosd
```

---

## Bundle Priority Model

| Priority | Use case | TTL | Routing |
|----------|----------|-----|---------|
| `LIVE` | Live sports, breaking news | 30 min | Aggressive — any link |
| `NEAR_LIVE` | Sports with 5–30 min lag | 6 hours | Best-effort |
| `VOD` | YouTube, movies | 30 days | Carry indefinitely |

---

## Data Flow

### VOD (YouTube)

```
[Homelab]                     [Relay node / plane]      [Viewer phone]
yt-dlp download               cosmosd stores bundles    Flutter SDK
ffmpeg → HLS chunks    →      carry on WiFi sync    →   plays HLS
cosmosd ingest                forward via BLE/TCP        smooth, no buffer
```

### Live Sports

```
[Broadcast server]                                       [Space / plane node]
ffmpeg grabs RTMP stream                                 priority queue
→ 10s HLS chunks         →   cosmosd relay chain   →   plays 5–22 min behind live
→ LIVE priority bundle        any available link         no buffering, time-shifted
```

---

## Time-Shifted Viewing (Not Buffering)

CosmosD does NOT stream live and buffer. Instead:

- Chunks arrive and are stored locally ahead of the playhead
- Viewer starts playback only when N minutes of content is pre-positioned
- Result: smooth playback, X minutes behind live — no spinner

On ISS with TDRS relay: ~2–5 min behind live.
On plane with Starlink: ~60s behind live.
No relay at all: VOD only (pre-loaded content).

---


## License

MIT — maximum adoption, no friction for commercial use on top.

---

## Protocol Spec Location

`cosmosd/docs/spec/` — versioned, IETF-style RFCs for:
- `spec-001-bundle-format.md` — BPv7 subset + streaming extensions
- `spec-002-convergence-layer.md` — transport plugin interface
- `spec-003-routing.md` — epidemic routing + contact scheduling
- `spec-004-hls-mapping.md` — HLS chunk to bundle mapping
