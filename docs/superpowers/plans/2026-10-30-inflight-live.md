# In-flight Live Match Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A live stream ingested on a ground node reaches a cabin node through a gateway over a link that drops, and plays about 2 minutes behind live without stalling through short drops.

**Architecture:** One `cosmosd` binary in three roles. The ground node gains `ingest --live`: ffmpeg transcodes to capped H.264/AAC and pushes LIVE bundles. Sync moves to a versioned CBOR wire format, gains timeouts and short backoff, and pulls the segments a viewer needs soonest first. Live playlists use a wall-clock edge (now minus delay), so the segments between the edge and the newest one act as the buffer that rides out link drops. A userspace link simulator and a headless player check prove the whole thing on one machine.

**Tech Stack:** Rust 2021, tokio, axum 0.7, sled, ciborium (CBOR), serde_bytes, ffmpeg (external).

**Spec:** `docs/superpowers/specs/2026-10-30-inflight-live-design.md`

## Global Constraints

- Segment length stays 10 seconds (`-hls_time 10`).
- Default viewer delay: 120 seconds (`serve --delay`, in seconds).
- LIVE TTL stays 1800 seconds (unchanged `Priority::default_ttl_secs`).
- Ground transcode: H.264 (libx264) + AAC 128 kbps stereo, 720p (`scale=-2:720`), default total `--max-bitrate 1200` (kbps); video bitrate = total − 128.
- File ingest keeps `-codec copy`.
- Sync: default `--sync-interval 5` seconds; retry backoff 1, 2, 4, 8, then 15 s cap; per-frame read timeout 30 s; connect timeout 10 s.
- Every manifest carries `protocol: 2`; a mismatch fails the session with an error naming both versions.
- Simulated satellite link in the demo: 3000 kbps, 300 ms each way (600 ms RTT).
- Nothing on the plane side may fetch from the internet at runtime (no CDN, no update checks). hls.js is embedded.
- New dependencies only: `ciborium = "0.2"`, `serde_bytes = "0.11"`; dev: tokio `test-util`; Task 10 adds `if-addrs`; Task 11 adds dev `tower` (`util`).
- Match the codebase style: `//!` module docs, `// === Section ===` separators, tests in an in-file `#[cfg(test)] mod tests`.

## Review Focus

1. **A cut link that blackholes TCP** (no reset, bytes just stop): a sync mid-transfer must time out within 30 s, keep every bundle it already received, and retry. Test: `initiate_times_out_on_stalled_peer` (Task 4).
2. **A sync slower than the sync interval** (an 18 MB backlog over 3 Mbps takes about a minute): a second session to the same peer must not start while one runs. Test: `peer_not_due_while_syncing` (Task 5).
3. **Incoming connections registering the wrong address:** a peer that dials in must be registered at its own relay port, not a hardcoded 7778. In the demo the hardcoded port would let the cabin bypass the link simulator. Test: `sync_reports_peer_relay_port` (Task 4).
4. **Live ingest restarted mid-match:** a new run must continue after the last stored sequence, not restart at 0 and have its segments silently deduplicated away. Test: `next_sequence_continues_after_catalog` (Task 7).
5. **Segments slightly longer than 10 s** (ffmpeg live output is 10.01 s, and so on): `EXT-X-TARGETDURATION` must be at least the longest `EXTINF` rounded up, or players reject the playlist. Test: `target_duration_rounds_up` (Task 6).

---

### Task 1: Toolchain, bundle fields, CBOR codec

**Files:**
- Modify: `Cargo.toml`
- Modify: `src/bundle/mod.rs`
- Create: `src/codec.rs`
- Modify: `src/main.rs` (add `mod codec;`)

**Interfaces:**
- Produces:
  - `Bundle` gains `pub created_at: DateTime<Utc>` (set to `Utc::now()` in `Bundle::new`) and `pub duration_ms: u32` (default `DEFAULT_SEGMENT_MS`). `payload` gets `#[serde(with = "serde_bytes")]`.
  - `pub const DEFAULT_SEGMENT_MS: u32 = 10_000;` in `bundle`.
  - `impl Bundle { pub fn with_duration_ms(self, ms: u32) -> Self; pub fn content_id(&self) -> &str; pub fn meta(&self) -> BundleMeta }`. `content_id` is the id before the last `/`.
  - `#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)] pub struct BundleMeta { pub id: String, pub content_id: String, pub content_title: String, pub sequence: u32, pub priority: Priority, pub created_at: DateTime<Utc>, pub expires_at: DateTime<Utc>, pub duration_ms: u32 }` in `bundle`, with `pub fn is_valid(&self) -> bool`.
  - `codec::PROTOCOL_VERSION: u16 = 2`, `codec::encode<T: Serialize>(v: &T) -> anyhow::Result<Vec<u8>>`, `codec::decode<T: DeserializeOwned>(bytes: &[u8]) -> anyhow::Result<T>` (ciborium).

- [ ] **Step 1: Install Rust and confirm the baseline**

Run: `brew install rustup && rustup-init -y --no-modify-path && source "$HOME/.cargo/env" && cargo test`
Expected: the existing bundle, store and auth tests pass.

- [ ] **Step 2: Branch and commit the spec and plan**

```bash
git checkout -b feat/inflight-live
git add docs/superpowers
git commit -m "docs: in-flight live match design and plan"
```

- [ ] **Step 3: Add dependencies**

`Cargo.toml`: `ciborium = "0.2"`, `serde_bytes = "0.11"` under `[dependencies]`; `tokio = { version = "1", features = ["full", "test-util"] }` under `[dev-dependencies]`.

- [ ] **Step 4: Write failing tests** in `src/codec.rs` and `src/bundle/mod.rs`

```rust
// src/codec.rs
#[test]
fn bundle_roundtrips_through_cbor() {
    let b = Bundle::new("m", "Match", 3, "video/mp2t", Priority::Live, vec![7u8; 1000], "ground")
        .with_duration_ms(10_010);
    let back: Bundle = decode(&encode(&b).unwrap()).unwrap();
    assert_eq!(back.id, b.id);
    assert_eq!(back.payload, b.payload);
    assert_eq!(back.duration_ms, 10_010);
    assert_eq!(back.created_at, b.created_at);
}

#[test]
fn cbor_payload_is_not_inflated() {
    let b = Bundle::new("m", "Match", 0, "video/mp2t", Priority::Live, vec![0xAB; 1_000_000], "ground");
    let n = encode(&b).unwrap().len();
    assert!(n < 1_000_000 + 1024, "encoded {} bytes", n);
}

// src/bundle/mod.rs
#[test]
fn meta_carries_routing_fields() {
    let b = make_bundle(Priority::Live).with_duration_ms(9_500);
    let m = b.meta();
    assert_eq!(m.id, "test_content/00000");
    assert_eq!(m.content_id, "test_content");
    assert_eq!(m.duration_ms, 9_500);
    assert_eq!(m.priority, Priority::Live);
}
```

- [ ] **Step 5: Run to verify failure**

Run: `cargo test codec bundle`
Expected: compile errors for `encode`, `decode`, `with_duration_ms` and `meta`.

- [ ] **Step 6: Implement the fields, `BundleMeta` and `codec`** (signatures above)

- [ ] **Step 7: Run the tests**

Run: `cargo test`
Expected: all pass, including the existing JSON roundtrip test.

- [ ] **Step 8: Commit**

```bash
git add Cargo.toml Cargo.lock src
git commit -m "feat(bundle): CBOR codec, segment duration and creation time"
```

---

### Task 2: Store on CBOR with a metadata index

The store today decodes every bundle, payload included, for `all_ids`, `catalog` and `reap_expired`. Manifests every 5 s make that cost per sync, so metadata moves to its own tree.

**Files:**
- Modify: `src/store/mod.rs`

**Interfaces:**
- Consumes: `codec::{encode, decode}`, `Bundle::meta`, `BundleMeta` (Task 1).
- Produces:
  - `BundleStore` holds `db: sled::Db` (main tree: id → CBOR `Bundle`) and `meta: sled::Tree` named `"meta"` (id → CBOR `BundleMeta`).
  - `insert(&self, &Bundle) -> Result<bool>` (unchanged contract, writes both), `get(&self, id: &str) -> Result<Option<Bundle>>`.
  - `list_meta(&self, content_id: &str) -> Result<Vec<BundleMeta>>`: valid entries only, ascending by `sequence`.
  - `all_meta(&self) -> Result<Vec<BundleMeta>>`: valid entries only.
  - `all_ids(&self) -> Result<HashSet<String>>` reads `meta` only.
  - `catalog()` reads `meta` only; `ContentSummary` gains `pub last_sequence: u32`.
  - `reap_expired()` walks `meta` and removes from both trees.
  - `list_content` is removed (Task 6 moves its only caller to `list_meta`).

- [ ] **Step 1: Update the existing tests and add new ones**

Rewrite `list_content_returns_sorted_segments` as `list_meta_returns_sorted_segments`, asserting on `.sequence`. Expired-bundle tests insert through a helper `insert_raw(store, &bundle)` that writes both trees with `encode`. Add:

```rust
#[test]
fn catalog_reports_last_sequence() {
    let store = temp_store();
    for s in [0, 1, 7] { store.insert(&make_bundle("show_a", s)).unwrap(); }
    let a = store.catalog().unwrap().into_iter().find(|c| c.content_id == "show_a").unwrap();
    assert_eq!(a.last_sequence, 7);
    assert_eq!(a.segment_count, 3);
}

#[test]
fn reap_removes_payload_and_meta() {
    let store = temp_store();
    let mut b = make_bundle("show_a", 0);
    b.expires_at = Utc::now() - chrono::Duration::seconds(1);
    insert_raw(&store, &b);
    assert_eq!(store.reap_expired().unwrap(), 1);
    assert!(store.db.get(b.id.as_str()).unwrap().is_none());
    assert!(store.meta.get(b.id.as_str()).unwrap().is_none());
}
```

- [ ] **Step 2: Run to verify failure**

Run: `cargo test store`
Expected: compile errors (`list_meta`, `meta`, `last_sequence`).

- [ ] **Step 3: Implement** (interfaces above; `serve_playlist` in `src/api/mod.rs` switches to `list_meta` with the same output, so the crate compiles)

- [ ] **Step 4: Run the tests**

Run: `cargo test`
Expected: all pass.

- [ ] **Step 5: Commit**

```bash
git add src/store src/api
git commit -m "feat(store): CBOR values and a metadata index"
```

---

### Task 3: Pull order: what a viewer needs soonest comes first

**Files:**
- Create: `src/router/order.rs`
- Modify: `src/router/mod.rs` (add `mod order; pub use order::*;`)

**Interfaces:**
- Consumes: `Priority` (bundle).
- Produces:
  - `#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)] pub struct ManifestEntry { pub id: String, pub priority: Priority }`
  - `pub fn pull_order(their: &[ManifestEntry], ours: &HashSet<String>, delay: Duration) -> Vec<String>`

Algorithm, which the tests pin:
- Missing = `their` ids not in `ours`. An id is `<content_id>/<seq>`, split at the last `/`. An unparsable id goes to the end.
- For each Live or NearLive content: `newest` = the max seq of that content in `their` (all entries, not just missing). `urgent_from = newest.saturating_sub(delay.as_secs() as u32 / 10 + 6)`. The urgent set is missing seqs ≥ `urgent_from`, ascending. The backfill set is the remaining missing seqs, descending.
- Output: every urgent set (Live contents before NearLive, then by content_id), then every backfill set (same content order), then Vod missing ascending by (content_id, seq).

- [ ] **Step 1: Write failing tests**

```rust
fn live(id: &str) -> ManifestEntry { ManifestEntry { id: id.into(), priority: Priority::Live } }
fn vod(id: &str) -> ManifestEntry { ManifestEntry { id: id.into(), priority: Priority::Vod } }

#[test]
fn live_window_first_then_backfill_then_vod() {
    let mut their: Vec<_> = (0..100).map(|s| live(&format!("m/{:05}", s))).collect();
    their.push(vod("f/00001"));
    their.push(vod("f/00000"));
    let ours: HashSet<String> = (0..50).map(|s| format!("m/{:05}", s)).collect();
    let order = pull_order(&their, &ours, Duration::from_secs(120));
    // newest 99, delay 12 segments + 6 margin -> urgent from 81
    let expect_urgent: Vec<String> = (81..100).map(|s| format!("m/{:05}", s)).collect();
    assert_eq!(&order[..19], &expect_urgent[..]);
    assert_eq!(order[19], "m/00080");
    assert_eq!(order[49], "m/00050");
    assert_eq!(&order[50..], &["f/00000".to_string(), "f/00001".to_string()]);
}

#[test]
fn nothing_missing_means_nothing_to_pull() {
    let their = vec![live("m/00000")];
    let ours: HashSet<String> = ["m/00000".to_string()].into();
    assert!(pull_order(&their, &ours, Duration::from_secs(120)).is_empty());
}
```

- [ ] **Step 2: Run to verify failure**

Run: `cargo test order`
Expected: compile error, `pull_order` not found.

- [ ] **Step 3: Implement `pull_order`**

- [ ] **Step 4: Run the tests**

Run: `cargo test order`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/router
git commit -m "feat(router): pull the live window first, then backfill"
```

---

### Task 4: Sync protocol v2 with timeouts

**Files:**
- Modify: `src/router/mod.rs`
- Modify: `src/auth/mod.rs` (generic streams)

**Interfaces:**
- Consumes: `codec`, `ManifestEntry`, `pull_order` (Task 3), `BundleStore::all_meta` (Task 2).
- Produces:
  - Wire `Manifest { protocol: u16, node_id: String, relay_port: u16, entries: Vec<ManifestEntry> }` and `PullRequest { bundle_ids: Vec<String> }`. All frames are CBOR. Bundles are sent as CBOR `Bundle`, and an empty frame still means "don't have it".
  - `pub const CONNECT_TIMEOUT: Duration = Duration::from_secs(10); pub const FRAME_TIMEOUT: Duration = Duration::from_secs(30);` Every `recv_frame` is wrapped in `tokio::time::timeout(FRAME_TIMEOUT, …)`.
  - `send_frame` / `recv_frame` are generic over `S: AsyncRead + AsyncWrite + Unpin`, and so are `RelayAuth::{verify_incoming, authenticate_outgoing}`.
  - `EpidemicRouter::new(store: Arc<BundleStore>, node_id: String, relay_port: u16, delay: Duration) -> Self`
  - `async fn sync_with_peer(&self, addr: &str, auth: Arc<RelayAuth>) -> Result<SyncStats>`: TCP connect under `CONNECT_TIMEOUT`, auth, then `initiate`.
  - `pub async fn initiate<S: AsyncRead + AsyncWrite + Unpin>(&self, stream: S) -> Result<SyncStats>`: pulls in `pull_order(their, ours, self.delay)` order, inserting each verified, valid bundle as it arrives.
  - `pub async fn handle_incoming<S: AsyncRead + AsyncWrite + Unpin>(&self, stream: S) -> Result<SyncStats>`
  - `pub struct SyncStats { pub peer_node_id: String, pub peer_relay_port: u16, pub received: usize, pub sent: usize }`
  - A protocol mismatch is an error containing `"protocol"` and both numbers.

- [ ] **Step 1: Write failing tests** (in `src/router/mod.rs`; `router(name, port)` builds an `EpidemicRouter` on a temp store with a 120 s delay)

```rust
#[tokio::test]
async fn sync_pulls_missing_bundles() {
    let ground = router("ground", 17778);
    let gateway = router("gateway", 27778);
    for s in 0..3 { ground.store.insert(&live_bundle("m", s)).unwrap(); }
    let (a, b) = tokio::io::duplex(1 << 20);
    let (got, served) = tokio::join!(gateway.initiate(a), ground.handle_incoming(b));
    assert_eq!(got.unwrap().received, 3);
    assert_eq!(served.unwrap().sent, 3);
    assert!(gateway.store.get("m/00002").unwrap().is_some());
}

#[tokio::test]
async fn sync_reports_peer_relay_port() {
    let ground = router("ground", 17778);
    let gateway = router("gateway", 27778);
    let (a, b) = tokio::io::duplex(1 << 16);
    let (out, inc) = tokio::join!(gateway.initiate(a), ground.handle_incoming(b));
    assert_eq!(out.unwrap().peer_relay_port, 17778);
    assert_eq!(inc.unwrap().peer_relay_port, 27778);
}

#[tokio::test]
async fn protocol_mismatch_is_reported() {
    let gateway = router("gateway", 27778);
    let (a, mut b) = tokio::io::duplex(1 << 16);
    let peer = tokio::spawn(async move {
        let _ = recv_frame(&mut b).await;
        let m = Manifest { protocol: 1, node_id: "old".into(), relay_port: 7778, entries: vec![] };
        send_frame(&mut b, &codec::encode(&m).unwrap()).await.unwrap();
        b
    });
    let err = gateway.initiate(a).await.unwrap_err().to_string();
    assert!(err.contains("protocol") && err.contains('1') && err.contains('2'), "{}", err);
    drop(peer.await);
}

#[tokio::test(start_paused = true)]
async fn initiate_times_out_on_stalled_peer() {
    let gateway = router("gateway", 27778);
    let (a, _b) = tokio::io::duplex(1 << 16); // peer never answers, never closes
    let res = gateway.initiate(a).await;
    assert!(res.is_err());
}
```

- [ ] **Step 2: Run to verify failure**

Run: `cargo test router`
Expected: compile errors (`initiate`, `peer_relay_port`, new `EpidemicRouter::new`).

- [ ] **Step 3: Implement protocol v2** (interfaces above). Update `PeerManager` and `main.rs` call sites to the new `EpidemicRouter::new`, passing `relay_port` and a fixed `Duration::from_secs(120)` until Task 5 adds the flag.

- [ ] **Step 4: Run the tests**

Run: `cargo test`
Expected: all pass.

- [ ] **Step 5: Commit**

```bash
git add src
git commit -m "feat(router): sync protocol v2: CBOR frames, timeouts, live-first pulls"
```

---

### Task 5: Peer scheduling for a link that drops

**Files:**
- Modify: `src/peer/mod.rs`
- Modify: `src/main.rs` (serve flags)

**Interfaces:**
- Consumes: `EpidemicRouter::new`, `SyncStats::peer_relay_port` (Task 4).
- Produces:
  - `pub fn retry_delay(failed_attempts: u32, sync_interval: Duration) -> Duration`: 0 → `sync_interval`, then 1 → 1 s, 2 → 2 s, 3 → 4 s, 4 → 8 s, 5 or more → 15 s.
  - `PeerInfo` gains `pub syncing: bool` and `pub next_attempt: DateTime<Utc>` (new peers: now). `PeerInfo::due(&self, now: DateTime<Utc>) -> bool` is `!syncing && now >= next_attempt`. `should_retry` is removed.
  - `PeerManager::new(router, auth, initial_peers: Vec<String>, sync_interval: Duration)`. The loop ticks every 1 s and starts a session for each due peer. It marks `syncing = true` before spawning. When the session ends it sets `syncing = false` and `next_attempt = now + retry_delay(failed_attempts, sync_interval)`.
  - Incoming sessions register `format!("{}:{}", ip, stats.peer_relay_port)`.
  - `serve` flags: `--delay <secs>` (default 120) and `--sync-interval <secs>` (default 5), passed to the router and the peer manager. Task 6 also passes `--delay` to the API.

- [ ] **Step 1: Write failing tests**

```rust
#[test]
fn retry_delay_backs_off_to_fifteen_seconds() {
    let i = Duration::from_secs(5);
    let got: Vec<u64> = (0..7).map(|n| retry_delay(n, i).as_secs()).collect();
    assert_eq!(got, vec![5, 1, 2, 4, 8, 15, 15]);
}

#[test]
fn peer_not_due_while_syncing() {
    let now = Utc::now();
    let mut p = PeerInfo::new("127.0.0.1:18778".into());
    assert!(p.due(now));
    p.syncing = true;
    assert!(!p.due(now + chrono::Duration::hours(1)));
}

#[test]
fn peer_due_after_next_attempt() {
    let now = Utc::now();
    let mut p = PeerInfo::new("x:1".into());
    p.next_attempt = now + chrono::Duration::seconds(4);
    assert!(!p.due(now));
    assert!(p.due(now + chrono::Duration::seconds(4)));
}
```

- [ ] **Step 2: Run to verify failure**

Run: `cargo test peer`
Expected: compile errors (`retry_delay`, `due`, `syncing`).

- [ ] **Step 3: Implement the scheduling and flags** (interfaces above)

- [ ] **Step 4: Run the tests**

Run: `cargo test`
Expected: all pass.

- [ ] **Step 5: Commit**

```bash
git add src
git commit -m "feat(peer): short capped backoff, one session per peer, --delay and --sync-interval"
```

---

### Task 6: Live playlists with a wall-clock edge

**Files:**
- Create: `src/api/playlist.rs` (declared `mod playlist;` in `api`)
- Modify: `src/api/mod.rs`, `src/main.rs` (`ApiState.delay`)

**Interfaces:**
- Consumes: `BundleMeta`, `BundleStore::list_meta`.
- Produces:
  - `pub const LIVE_WINDOW: usize = 6;`
  - `pub fn live_playlist(content_id: &str, metas: &[BundleMeta], now: DateTime<Utc>, delay: Duration) -> Option<String>`. `metas` is ascending by sequence. Eligible segments have `created_at <= now - delay`. `last` is the max eligible seq. Walk back from `last` while the previous seq is present, up to `LIVE_WINDOW` segments; that run is the playlist. There is never a hole inside a playlist: a hole at the edge holds the run until later segments become eligible, and then `EXT-X-MEDIA-SEQUENCE` jumps forward. Returns `None` if nothing is eligible. There is no `EXT-X-ENDLIST`.
  - `pub fn vod_playlist(content_id: &str, metas: &[BundleMeta]) -> String`: all segments plus `EXT-X-ENDLIST`.
  - Both emit `#EXTM3U`, `#EXT-X-VERSION:3`, `#EXT-X-TARGETDURATION:{ceil(max duration s)}`, `#EXT-X-MEDIA-SEQUENCE:{first seq}`, and per segment `#EXTINF:{secs:.3},` then `/segment/{content_id}/{seq}`.
  - `/play/:content_id` uses `live_playlist` if any meta isn't `Vod`, else `vod_playlist`. `None` → 404 with the body `"Buffering: nothing old enough to play yet"`.
  - `/segment/...` responses add the header `x-cosmosd-created-at: <RFC 3339>` (used by Task 9's player check).
  - `ApiState` gains `pub delay: Duration`, from `serve --delay`.

- [ ] **Step 1: Write failing tests** (`metas(n, now)` builds seqs `0..n`, where seq `i` is created at `now - (10*(n-1-i))` s with 10 000 ms duration)

```rust
#[test]
fn live_edge_is_now_minus_delay() {
    let now = Utc::now();
    let m = metas(20, now); // seq 7 created at now-120s
    let p = live_playlist("m", &m, now, Duration::from_secs(120)).unwrap();
    assert!(p.contains("#EXT-X-MEDIA-SEQUENCE:2\n"));
    assert!(p.contains("/segment/m/7\n"));
    assert!(!p.contains("/segment/m/8\n"));
    assert!(!p.contains("ENDLIST"));
}

#[test]
fn hole_at_edge_shortens_the_run() {
    let now = Utc::now();
    let m: Vec<_> = metas(20, now).into_iter().filter(|x| x.sequence != 5).collect();
    let p = live_playlist("m", &m, now, Duration::from_secs(120)).unwrap();
    assert!(p.contains("#EXT-X-MEDIA-SEQUENCE:6\n"));
    assert!(!p.contains("/segment/m/4\n"));
}

#[test]
fn nothing_old_enough_is_none() {
    let now = Utc::now();
    assert!(live_playlist("m", &metas(5, now), now, Duration::from_secs(120)).is_none());
}

#[test]
fn target_duration_rounds_up() {
    let now = Utc::now();
    let mut m = metas(20, now);
    for x in &mut m { x.duration_ms = 10_010; }
    let p = live_playlist("m", &m, now, Duration::from_secs(120)).unwrap();
    assert!(p.contains("#EXT-X-TARGETDURATION:11\n"));
    assert!(p.contains("#EXTINF:10.010,\n"));
}

#[test]
fn vod_lists_everything_and_ends() {
    let now = Utc::now();
    let p = vod_playlist("f", &metas(3, now));
    assert!(p.contains("#EXT-X-MEDIA-SEQUENCE:0\n"));
    assert!(p.contains("/segment/f/2\n"));
    assert!(p.ends_with("#EXT-X-ENDLIST\n"));
}
```

- [ ] **Step 2: Run to verify failure**

Run: `cargo test playlist`
Expected: compile errors.

- [ ] **Step 3: Implement `playlist.rs` and wire up `/play` and `/segment`**

- [ ] **Step 4: Run the tests**

Run: `cargo test`
Expected: all pass.

- [ ] **Step 5: Commit**

```bash
git add src
git commit -m "feat(api): live playlists that sit --delay behind live"
```

---

### Task 7: Live ingest

**Files:**
- Create: `src/ingest/mod.rs` (file ingest moved out of `main.rs`, plus shared helpers)
- Create: `src/ingest/live.rs`
- Modify: `src/main.rs` (CLI), `src/api/mod.rs` (`IngestRequest.duration_ms`)

**Interfaces:**
- Consumes: `Bundle::with_duration_ms`, `ContentSummary.last_sequence` (served by `/catalog`).
- Produces:
  - `pub struct PlaylistEntry { pub uri: String, pub duration_ms: u32 }` and `pub fn parse_media_playlist(text: &str) -> Vec<PlaylistEntry>`: each `#EXTINF:<secs>,` line paired with the next non-comment line.
  - `pub fn next_sequence(catalog: &serde_json::Value, content_id: &str) -> u32`: `last_sequence + 1` for that content, else 0.
  - `pub async fn ingest_file(file: PathBuf, title: Option<String>, daemon: String, priority: String) -> Result<()>`: today's behavior, but each push carries the duration from `index.m3u8`.
  - `pub fn live_ffmpeg_args(input: &str, out_dir: &Path, max_kbps: u32) -> Vec<String>`, exactly: `-hide_banner -loglevel warning -i <input> -map 0:v:0 -map 0:a:0? -vf scale=-2:720 -c:v libx264 -preset veryfast -tune zerolatency -b:v {v}k -maxrate {v}k -bufsize {2v}k -force_key_frames expr:gte(t,n_forced*10) -c:a aac -b:a 128k -ac 2 -f hls -hls_time 10 -hls_list_size 0 -hls_flags independent_segments -hls_segment_filename <out>/seg%05d.ts <out>/index.m3u8`, where `v = max_kbps - 128`.
  - `pub async fn ingest_live(input: String, content_id: String, title: String, daemon: String, max_kbps: u32) -> Result<()>`: start at `next_sequence` from `/catalog`. Spawn ffmpeg (`tokio::process`, `kill_on_drop(true)`) into a fresh temp dir. Every 500 ms, parse `index.m3u8` and push each new entry with priority `live`, the next sequence and its duration, then delete the `.ts` file. If ffmpeg exits, log it, wait 2 s and restart in a new temp dir, keeping the sequence counter. Runs until Ctrl-C.
  - `IngestRequest` gains `#[serde(default = "default_duration_ms")] duration_ms: u32` (10 000).
  - CLI: `cosmosd ingest [FILE] [--live <URL>] [--content-id <ID>] [--title] [--daemon] [--priority] [--max-bitrate <KBPS>=1200]`. Exactly one of `FILE` and `--live` is required (clap `conflicts_with` / `required_unless_present`). `--live` requires `--content-id`. `--max-bitrate` below 400 is rejected.

- [ ] **Step 1: Write failing tests**

```rust
// src/ingest/mod.rs
#[test]
fn parses_extinf_entries() {
    let t = "#EXTM3U\n#EXT-X-TARGETDURATION:11\n#EXTINF:10.010000,\nseg00000.ts\n#EXTINF:9.5,\nseg00001.ts\n";
    let e = parse_media_playlist(t);
    assert_eq!(e.len(), 2);
    assert_eq!(e[0].uri, "seg00000.ts");
    assert_eq!(e[0].duration_ms, 10_010);
    assert_eq!(e[1].duration_ms, 9_500);
}

#[test]
fn next_sequence_continues_after_catalog() {
    let c = serde_json::json!([{"content_id": "m", "last_sequence": 41}, {"content_id": "x", "last_sequence": 3}]);
    assert_eq!(next_sequence(&c, "m"), 42);
    assert_eq!(next_sequence(&c, "new"), 0);
}

// src/ingest/live.rs
#[test]
fn ffmpeg_args_cap_bitrate_and_cut_ten_seconds() {
    let a = live_ffmpeg_args("udp://127.0.0.1:5000", Path::new("/t"), 1200);
    let has = |k: &str, v: &str| a.windows(2).any(|w| w[0] == k && w[1] == v);
    assert!(has("-b:v", "1072k") && has("-maxrate", "1072k") && has("-bufsize", "2144k"));
    assert!(has("-c:v", "libx264") && has("-hls_time", "10") && has("-b:a", "128k"));
    assert!(has("-i", "udp://127.0.0.1:5000"));
    assert_eq!(a.last().unwrap(), "/t/index.m3u8");
}
```

- [ ] **Step 2: Run to verify failure**

Run: `cargo test ingest`
Expected: compile errors.

- [ ] **Step 3: Implement the module, CLI and `IngestRequest` change**

- [ ] **Step 4: Run the tests**

Run: `cargo test`
Expected: all pass.

- [ ] **Step 5: Smoke test against a real daemon**

Run, in three terminals:
- `cargo run -- serve --store /tmp/cz --no-discovery`
- `ffmpeg -re -f lavfi -i testsrc2=size=1280x720:rate=30 -f lavfi -i sine -c:v libx264 -preset ultrafast -c:a aac -f mpegts 'udp://127.0.0.1:5000?pkt_size=1316'`
- `cargo run -- ingest --live udp://127.0.0.1:5000 --content-id demo`

Expected: after about 30 s, `curl -s localhost:7777/catalog` shows `demo` with `priority: "live"` and a growing `segment_count`. Then kill the ingest and restart it: its first push is `last_sequence + 1`.

- [ ] **Step 6: Commit**

```bash
git add src
git commit -m "feat(ingest): cosmosd ingest --live with capped H.264 transcode"
```

---

### Task 8: Link simulator

**Files:**
- Create: `examples/linksim.rs`

**Interfaces:**
- Produces: the CLI `cargo run --example linksim -- --listen <addr> --upstream <addr> --rate-kbps <n> --latency-ms <n>`.
  - It forwards each accepted connection to `upstream`, both directions.
  - Each direction reads chunks of up to 16 KiB. It paces them with `send_delay(len, rate_kbps)` and delivers each chunk `latency_ms` after reading it, keeping order.
  - `SIGUSR1` cuts the link: every forwarder stops reading and writing, and sockets stay open, so this behaves like a blackhole, not a reset. New connections are accepted but nothing is forwarded. `SIGUSR2` restores the link.
  - It prints `linksim: cut` / `linksim: restored` to stderr.
  - `fn send_delay(bytes: usize, rate_kbps: u32) -> Duration`

- [ ] **Step 1: Write a failing test** (in-file)

```rust
#[test]
fn send_delay_matches_rate() {
    assert_eq!(send_delay(16_000, 3000).as_micros(), 42_666);
    assert_eq!(send_delay(0, 3000), Duration::ZERO);
}
```

- [ ] **Step 2: Run to verify failure**

Run: `cargo test --example linksim`
Expected: compile error.

- [ ] **Step 3: Implement `linksim`**

- [ ] **Step 4: Run the test and a manual check**

Run: `cargo test --example linksim`. Expected: PASS.
Then start `linksim --listen 127.0.0.1:18778 --upstream 127.0.0.1:7778 --rate-kbps 3000 --latency-ms 300` in front of a daemon with live content. Point a second daemon at `--peer 127.0.0.1:18778`. Run `kill -USR1 <pid>`: the second daemon's log shows a sync timeout within 30 s. Run `kill -USR2 <pid>`: syncs resume within 15 s.

- [ ] **Step 5: Commit**

```bash
git add examples/linksim.rs
git commit -m "test: linksim, a pausable rate- and latency-limited TCP proxy"
```

---

### Task 9: Player check and the MVP demo

This task is the MVP gate from the spec.

**Files:**
- Create: `examples/playcheck.rs`
- Create: `scripts/demo-inflight.sh`
- Modify: `README.md` (a "Live over a dropping link" section: the three roles, `ingest --live`, `--delay`, running the ground relay on `--relay-port 443` to get through in-flight firewalls, and how to run the demo)

**Interfaces:**
- Consumes: `/play` live playlists and the `x-cosmosd-created-at` header (Task 6), `linksim` (Task 8), `ingest --live` (Task 7), the `--delay` / `--sync-interval` flags (Task 5).
- Produces:
  - `cargo run --example playcheck -- --url <playlist url> --duration-secs <n>`: a headless player.
    - It waits for the playlist, then starts at `max(first, last - 2)`, like real players do.
    - It "plays" each segment by fetching it (it must return 200) and sleeping for its `EXTINF`.
    - If the next seq isn't listed, it either jumps or stalls. When the playlist's first seq has passed the playhead, it jumps to `max(first, last - 2)` and logs a `jump`. Otherwise it stalls, logs `stall start` and retries each second until `stall end`.
    - Every event line is `t=<elapsed secs> <event> seq=<n> lag=<now - created_at, secs>`.
    - At exit it prints `summary stalls=<n> stall_secs=<n> jumps=<n> final_lag=<secs>`.
  - `scripts/demo-inflight.sh`. It needs ffmpeg, writes everything under `mktemp -d`, and kills every child on exit (`trap`). Steps:
    1. Builds once: `cargo build --release --examples`.
    2. Starts the source: an ffmpeg `testsrc2` 1280x720 at 30 fps with a burned-in `drawtext` clock (`%{localtime}`), plus a `sine` tone, sent to `udp://127.0.0.1:5000`. If `ffmpeg -filters` lacks `drawtext`, it runs without the clock.
    3. Starts the ground node: `serve --port 17777 --relay-port 17778 --node-id ground --no-discovery`, plus `ingest --live udp://127.0.0.1:5000 --content-id demo --daemon http://127.0.0.1:17777`.
    4. Starts `linksim --listen 127.0.0.1:18778 --upstream 127.0.0.1:17778 --rate-kbps 3000 --latency-ms 300`.
    5. Starts the gateway: `serve --port 27777 --relay-port 27778 --node-id gateway --peer 127.0.0.1:18778` (mDNS on).
    6. Starts the cabin: `serve --port 37777 --relay-port 37778 --node-id cabin --bind 0.0.0.0` (mDNS on, no `--peer`).
    7. Runs `playcheck --url http://127.0.0.1:37777/play/demo --duration-secs 960`, logging to `play.log`.
    8. Timeline, in seconds from the start of playcheck: cut at 240, restore at 270, cut at 420, restore at 720.
    9. Prints `curl :37777/peers` once, to show the cabin learned the gateway by mDNS.
    10. Asserts from `play.log`: no `stall start` with `240 ≤ t ≤ 330`; at least one `stall start` with `420 ≤ t ≤ 780`; `final_lag` between 110 and 180. It prints `DEMO PASS` or `DEMO FAIL: <reason>` and exits 0 or 1.
    11. While it runs, it prints `watch: ffplay http://127.0.0.1:37777/play/demo` (or VLC / Safari) so a human can see the clock.

- [ ] **Step 1: Write `playcheck` and the script** (interfaces above)

- [ ] **Step 2: Run the demo**

Run: `scripts/demo-inflight.sh`
Expected (about 17 minutes): `/peers` on the cabin lists the gateway. The `play.log` timeline shows no stall around the 30 s cut, a stall and then a jump after the 5-minute cut, and finally `DEMO PASS`. Watching with ffplay shows the burned-in clock about 2 minutes behind the wall clock, carrying on through the first cut.

- [ ] **Step 3: If it fails, fix the cause with superpowers:systematic-debugging, add a unit test for it to the owning task's module, and rerun until `DEMO PASS`**

- [ ] **Step 4: Commit**

```bash
git add examples/playcheck.rs scripts/demo-inflight.sh README.md
git commit -m "test: in-flight demo: ground, simulated satellite, gateway, cabin"
```

**MVP is done here.** Tasks 10 to 12 are the spec's post-MVP items.

---

### Task 10: Per-direction tokens and the open-relay guard

**Files:**
- Modify: `src/auth/mod.rs`, `src/peer/mod.rs`, `src/main.rs`, `Cargo.toml` (`if-addrs`), `scripts/demo-inflight.sh`, `README.md`

**Interfaces:**
- Produces:
  - `pub fn parse_peer(arg: &str) -> (String, Option<String>)`: splits `host:port@token` at the first `@` (addresses never contain `@`; tokens may).
  - `RelayAuth::new(listen_token: Option<String>)` guards incoming connections only (`verify_incoming`).
  - `pub async fn authenticate_outgoing<S>(stream: &mut S, token: Option<&str>) -> Result<()>` is a free function.
  - `PeerInfo` gains `#[serde(skip)] pub token: Option<String>`, and the router's `sync_with_peer` takes the peer's token instead of `Arc<RelayAuth>`.
  - `pub fn is_public_v4(ip: Ipv4Addr) -> bool`: not private, loopback, link-local, unspecified, or 100.64.0.0/10.
  - `serve` refuses to start, with the error `"refusing an open relay on a public address: set --relay-token or pass --open-relay"`, when there is no `--relay-token`, no `--open-relay`, and some interface has a public IPv4 (`if_addrs::get_if_addrs`).
  - The demo now runs ground with `--relay-token demo-secret` and gateway with `--peer 127.0.0.1:18778@demo-secret`. Cabin has no token.

- [ ] **Step 1: Write failing tests**

```rust
#[test]
fn parse_peer_splits_token() {
    assert_eq!(parse_peer("1.2.3.4:443@s3cr@t"), ("1.2.3.4:443".into(), Some("s3cr@t".into())));
    assert_eq!(parse_peer("10.0.0.2:7778"), ("10.0.0.2:7778".into(), None));
}

#[test]
fn public_v4_detection() {
    assert!(is_public_v4("8.8.8.8".parse().unwrap()));
    for ip in ["10.0.0.1", "192.168.1.5", "172.16.0.1", "100.70.0.1", "127.0.0.1", "169.254.1.1"] {
        assert!(!is_public_v4(ip.parse().unwrap()), "{}", ip);
    }
}

#[tokio::test]
async fn listener_token_does_not_apply_outgoing() {
    let auth = RelayAuth::new(Some("ground-secret".into()));
    let (mut a, mut b) = tokio::io::duplex(1024);
    let (inc, out) = tokio::join!(auth.verify_incoming(&mut a), authenticate_outgoing(&mut b, Some("ground-secret")));
    assert!(inc.is_ok() && out.is_ok());
}
```

- [ ] **Step 2: Run to verify failure.** Run: `cargo test auth peer`. Expected: compile errors.

- [ ] **Step 3: Implement, and update the demo and README**

- [ ] **Step 4: Run.** Run `cargo test`, then `scripts/demo-inflight.sh`. Expected: all pass and `DEMO PASS`.

- [ ] **Step 5: Commit**

```bash
git add Cargo.toml Cargo.lock src scripts README.md
git commit -m "feat(auth): per-direction relay tokens, refuse open relays on public addresses"
```

---

### Task 11: Read-only API beyond localhost

**Files:**
- Modify: `src/api/mod.rs`, `src/main.rs` (`into_make_service_with_connect_info::<SocketAddr>()`), `Cargo.toml` (dev `tower = { version = "0.4", features = ["util"] }`)

**Interfaces:**
- Produces: `POST /ingest` and `POST /peers` answer `403 Forbidden` (body `"write routes are localhost-only"`) unless the remote address is loopback. A `route_layer` / middleware on those routes reads `ConnectInfo<SocketAddr>`. GET routes are unchanged.

- [ ] **Step 1: Write failing tests** using `axum::extract::connect_info::MockConnectInfo` and `tower::ServiceExt::oneshot`

```rust
#[tokio::test]
async fn post_peers_from_lan_is_forbidden() {
    let app = router(test_state()).layer(MockConnectInfo(SocketAddr::from(([192, 168, 1, 9], 5000))));
    let res = app.oneshot(post_json("/peers", r#"{"relay_addr":"1.2.3.4:7778"}"#)).await.unwrap();
    assert_eq!(res.status(), StatusCode::FORBIDDEN);
}

#[tokio::test]
async fn post_peers_from_localhost_is_allowed() {
    let app = router(test_state()).layer(MockConnectInfo(SocketAddr::from(([127, 0, 0, 1], 5000))));
    let res = app.oneshot(post_json("/peers", r#"{"relay_addr":"1.2.3.4:7778"}"#)).await.unwrap();
    assert_eq!(res.status(), StatusCode::OK);
}

#[tokio::test]
async fn catalog_from_lan_is_allowed() {
    let app = router(test_state()).layer(MockConnectInfo(SocketAddr::from(([192, 168, 1, 9], 5000))));
    let res = app.oneshot(get("/catalog")).await.unwrap();
    assert_eq!(res.status(), StatusCode::OK);
}
```

- [ ] **Step 2: Run to verify failure.** Run: `cargo test api`. Expected: the first test gets 200, not 403.

- [ ] **Step 3: Implement**

- [ ] **Step 4: Run.** Run: `cargo test`. Expected: all pass.

- [ ] **Step 5: Commit**

```bash
git add Cargo.toml Cargo.lock src
git commit -m "feat(api): write routes answer only to localhost"
```

---

### Task 12: Phone player page

**Files:**
- Create: `assets/index.html`, `assets/hls.min.js`. The second is vendored hls.js 1.5.17, downloaded once with `curl -fsSL https://cdn.jsdelivr.net/npm/hls.js@1.5.17/dist/hls.min.js -o assets/hls.min.js` and committed.
- Modify: `src/api/mod.rs`

**Interfaces:**
- Produces: `GET /` serves `include_str!("../../assets/index.html")` as `text/html; charset=utf-8`. `GET /hls.min.js` serves `include_bytes!` as `application/javascript`.
- The page fetches `/catalog` and lists each title with its priority. Tapping one plays `/play/{content_id}` in a `<video controls playsinline>`. It uses native HLS when `video.canPlayType('application/vnd.apple.mpegurl')`, else `Hls` from `/hls.min.js`. A 404 "Buffering" response shows that text and retries every 5 s. The page loads nothing external: no fonts, no CDN.

- [ ] **Step 1: Write failing tests** (same helpers as Task 11)

```rust
#[tokio::test]
async fn index_page_has_a_player() {
    let res = router(test_state()).oneshot(get("/")).await.unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    assert!(res.headers()["content-type"].to_str().unwrap().starts_with("text/html"));
    let body = body_string(res).await;
    assert!(body.contains("<video") && body.contains("/hls.min.js") && !body.contains("https://"));
}

#[tokio::test]
async fn hls_js_is_embedded() {
    let res = router(test_state()).oneshot(get("/hls.min.js")).await.unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    assert_eq!(res.headers()["content-type"], "application/javascript");
}
```

- [ ] **Step 2: Run to verify failure.** Run: `cargo test api`. Expected: 404s.

- [ ] **Step 3: Implement the routes and the page**

- [ ] **Step 4: Run and check by hand.** Run `cargo test` (expected: all pass). Then run `scripts/demo-inflight.sh` and open `http://<this Mac's LAN IP>:37777/` on a phone on the same Wi-Fi. Expected: `demo` is listed and plays, with the clock about 2 minutes behind.

- [ ] **Step 5: Commit**

```bash
git add assets src
git commit -m "feat(api): offline phone player page with embedded hls.js"
```
