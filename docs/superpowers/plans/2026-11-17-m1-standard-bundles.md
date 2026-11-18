# M1: Standard Bundles and Signed Manifests Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** cosmosd stores and syncs RFC 9171 bundles, accepts a segment only when a manifest from a paired publisher vouches for its hash, and judges expiry by bundle age instead of wall-clock time.

**Architecture:** The domain `Bundle` is reshaped around a publisher key, a content info block and an age. A new `wire` module converts it to and from RFC 9171 bytes using the `bp7` crate, with our content info in private-use block 192. A new `trust` module holds Ed25519 publisher keys, pairing and signed manifests. The store and router gain an admission gate that checks every segment against a verified manifest.

**Tech Stack:** Rust 2021, tokio, axum, sled, `bp7` 0.12.2 (RFC 9171), `ed25519-dalek` 3.0.0, `blake3`, `ciborium`.

**Spec:** `docs/superpowers/specs/2026-11-16-offline-carry-design.md` (milestone M1: G1, G4, G5)

## Global Constraints

- Bundles are RFC 9171 CBOR. Extension blocks used: bundle age (type 7), hop count (type 10), previous node (type 6). Our content block is private-use type 192.
- All extension block control flags are 0, so other BPv7 software carries a bundle even when it does not understand block 192, and no status reports are requested.
- Destination URI is `dtn://<publisher-key-id>/<content-id>`. A content ID is chosen by the publisher and unique only within that publisher. Identity is the pair of publisher key ID and content ID.
- DTN time zero is allowed as a creation timestamp. Expiry is judged by bundle age, never by comparing wall clocks.
- The BLAKE3 payload hash is not stored in the bundle. It lives in the signed manifest.
- Manifests are signed with Ed25519. A newer signed manifest version replaces an older one for the same publisher and content ID.
- Revocation is manual only: removing a publisher key on a device. No key rotation.
- Sync protocol version bumps to 3. A mismatch fails the session naming both versions (existing pattern).
- Old stores (protocol 2) are refused with a clear message, as the store already does for protocol 1.
- No IPs, MACs, domains or secrets in the repo. Keys live in a key directory outside the repo.
- Docs style: no semicolons, no em dashes, short plain sentences.
- Commits are GPG-signed and carry no Co-Authored-By or other AI attribution trailers.
- Match codebase style: `//!` module docs, `// === Section ===` separators, tests in an in-file `#[cfg(test)] mod tests`.
- Deferred to M2 and not built here: interest lists, the storage cap and its drop order, the unverified-publisher carrier quota, resumable pieces.

## Review Focus

1. **A device with a wrong wall clock** (phone set to 1970 or 2040): a fresh bundle is not dropped and an old one is not kept. Test: `wrong_wall_clock_does_not_change_expiry` (Task 4).
2. **A segment whose manifest never arrives:** it is held for a bounded time, then dropped, never stored as trusted and never held forever. Test: `held_segment_is_dropped_after_hold_time` (Task 5).
3. **Manifest rollback and tampering:** an older signed version arriving after a newer one does not replace it, and a segment whose hash differs from the manifest is rejected. Tests: `older_manifest_does_not_replace_newer`, `segment_with_wrong_hash_is_rejected` (Task 5).
4. **Garbage or hostile bytes** (truncated CBOR, a block of the wrong type, an oversized payload): decode returns an error and never panics. Test: `decode_rejects_garbage_without_panic` (Task 3).
5. **Two publishers using the same content ID** do not collide in the store or overwrite each other. Test: `same_content_id_two_publishers_coexist` (Task 4).

---

### Task 1: Publisher keys, pairing and the key CLI

**Files:**
- Create: `src/trust/mod.rs`
- Modify: `Cargo.toml` (add `ed25519-dalek = "3.0.0"` and whatever it needs for key generation), `src/main.rs` (`mod trust;`, `keys` subcommand, `--key-dir`)
- Test: in-file tests in `src/trust/mod.rs`

**Interfaces:**
- Produces:
  - `pub struct KeyId(pub String)`: 16 lowercase hex chars, the first 8 bytes of BLAKE3 of the public key. `impl Display`, `Clone`, `Eq`, `Hash`, serde.
  - `pub struct PublisherKey` with `generate() -> Self`, `load(dir: &Path) -> Result<Self>`, `save(&self, dir: &Path) -> Result<()>` (file mode 0600), `key_id(&self) -> KeyId`, `public(&self) -> [u8; 32]`, `sign(&self, msg: &[u8]) -> [u8; 64]`.
  - `pub fn verify(public: &[u8; 32], msg: &[u8], sig: &[u8; 64]) -> bool`.
  - `pub fn pairing_string(public: &[u8; 32]) -> String` giving `cosmos-key:<base32, lowercase, no padding>` and `pub fn parse_pairing_string(s: &str) -> Result<[u8; 32]>`.
  - `pub struct PairedKeys` file-backed in the key dir: `open(dir: &Path) -> Result<Self>`, `pair(&mut self, public: [u8; 32]) -> Result<KeyId>`, `unpair(&mut self, id: &KeyId) -> Result<bool>`, `list(&self) -> Vec<KeyId>`.
  - `pub trait KeyLookup { fn lookup(&self, id: &KeyId) -> Option<[u8; 32]>; }` implemented by `PairedKeys` and by a `Trust` struct that also knows the node's own key.
  - CLI: `cosmosd keys init`, `keys show` (prints the pairing string and key ID), `keys pair <string>`, `keys unpair <key-id>`, `keys list`. Default `--key-dir` is `$HOME/.config/cosmosd`.

- [ ] **Step 1: Branch and commit the spec and plan.** Run `git switch -c feat/m1-standard-bundles` from `feat/inflight-live`, then commit the spec and this plan.
- [ ] **Step 2: Write failing tests:** `sign_then_verify_roundtrips`, `verify_rejects_wrong_key`, `key_id_is_16_hex`, `pairing_string_roundtrips`, `parse_pairing_string_rejects_garbage`, `pair_then_lookup_then_unpair` (uses `tempfile`), `saved_key_file_is_0600` (unix only).
- [ ] **Step 3: Run** `cargo test trust` and expect FAIL (module missing).
- [ ] **Step 4: Implement** the interfaces above. Keep the private key as 32 raw bytes in `publisher.key`, paired keys as one pairing string per line in `paired.keys`.
- [ ] **Step 5: Run** `cargo test trust` and expect PASS. Run `cargo run -- keys init --key-dir /tmp/k && cargo run -- keys show --key-dir /tmp/k` and expect a `cosmos-key:` line.
- [ ] **Step 6: Commit** `feat(trust): publisher keys, pairing and the keys CLI`.

---

### Task 2: Signed manifests

**Files:**
- Create: `src/trust/manifest.rs`
- Modify: `src/trust/mod.rs` (`pub mod manifest;`), `Cargo.toml` if needed
- Test: in-file tests in `src/trust/manifest.rs`

**Interfaces:**
- Consumes: `PublisherKey`, `KeyId`, `KeyLookup`, `verify` from Task 1.
- Produces:
  - `pub struct SegmentEntry { pub sequence: u32, pub hash: [u8; 32], pub duration_ms: u32 }`.
  - `pub struct Manifest { pub publisher: KeyId, pub content_id: String, pub title: String, pub version: u64, pub created_at_ms: i64, pub live: bool, pub segments: Vec<SegmentEntry> }`.
  - `pub struct SignedManifest { pub manifest_cbor: Vec<u8>, pub signature: [u8; 64] }` with `sign(m: &Manifest, key: &PublisherKey) -> Result<Self>`, `verify(&self, keys: &dyn KeyLookup) -> Result<Manifest>`, `to_bytes(&self) -> Result<Vec<u8>>`, `from_bytes(&[u8]) -> Result<Self>`.
  - The signature covers `b"cosmos-manifest-v1"` followed by `manifest_cbor`.
  - `impl Manifest { pub fn hash_for(&self, sequence: u32) -> Option<&SegmentEntry> }`.

- [ ] **Step 1: Write failing tests:** `signed_manifest_verifies_with_paired_key`, `verify_fails_for_unpaired_publisher`, `verify_fails_when_manifest_bytes_change` (flip one byte), `verify_fails_with_wrong_domain_prefix`, `manifest_roundtrips_through_bytes`, `hash_for_finds_entry_by_sequence`.
- [ ] **Step 2: Run** `cargo test manifest` and expect FAIL.
- [ ] **Step 3: Implement** the interfaces. `verify` decodes the manifest first only to read the publisher ID, looks the key up, checks the signature, and returns the manifest only if the check passes.
- [ ] **Step 4: Run** `cargo test manifest` and expect PASS.
- [ ] **Step 5: Commit** `feat(trust): Ed25519 signed manifests`.

---

### Task 3: Reshape the bundle and add the RFC 9171 wire format

**Files:**
- Modify: `Cargo.toml` (add `bp7 = "0.12.2"`, consider `default-features = false` and keep only what builds), `src/bundle/mod.rs`, `src/codec.rs`, `src/main.rs` (`mod wire;`)
- Create: `src/wire/mod.rs`, `docs/spec/spec-001-bundle-format.md`
- Test: in-file tests in `src/wire/mod.rs` and `src/bundle/mod.rs`

**Interfaces:**
- Consumes: `KeyId` from Task 1.
- Produces:
  - `pub enum BundleKind { Segment, Manifest }` and `pub struct ContentInfo { pub kind: BundleKind, pub content_id: String, pub title: String, pub sequence: u32, pub duration_ms: u32, pub mime_type: String, pub priority: Priority, pub created_at_ms: i64 }`.
  - `pub struct Bundle { pub publisher: KeyId, pub info: ContentInfo, pub payload: Vec<u8>, pub age_ms: u64, pub lifetime_ms: u64, pub hop_count: u8, pub previous_node: Option<String>, pub source_node: String }`.
  - `BundleId` becomes `<publisher>/<content_id>/<sequence:05>` for segments and `<publisher>/<content_id>/manifest` for manifests.
  - `Bundle::segment(publisher: &KeyId, content_id: &str, title: &str, sequence: u32, mime: &str, priority: Priority, payload: Vec<u8>, source_node: &str) -> Result<Bundle>` (errors when `content_id` is empty or contains `/`) and `Bundle::manifest(publisher: &KeyId, m: &Manifest, signed_bytes: Vec<u8>, source_node: &str) -> Bundle`. Keep `with_duration_ms`.
  - `wire::CONTENT_BLOCK_TYPE: u64 = 192`, `wire::encode_bundle(b: &Bundle) -> Result<Vec<u8>>`, `wire::decode_bundle(bytes: &[u8]) -> Result<Bundle>`, `wire::destination_uri(publisher: &KeyId, content_id: &str) -> String`.
  - Lifetime per priority comes from the existing `Priority::default_ttl_secs`, stored in the primary block. `age_ms` rides in the age block. Hop count and previous node ride in their standard blocks.
  - Expiry helper moves to Task 4. Remove `payload_hash`, `verify`, `expires_at` and `bundle_uuid` from `Bundle`. Fix every caller so the crate compiles (`src/store`, `src/api`, `src/router`, `src/ingest`, `src/api/playlist.rs`). Where a caller only needs to compile, keep behavior identical. Task 4 and 5 change store and gate behavior.

- [ ] **Step 1: Write failing tests:** `encode_decode_roundtrips_all_fields`, `destination_uri_has_publisher_and_content` (expects `dtn://<id>/<content>`), `stock_bp7_decodes_our_bundle_and_keeps_payload` (decode the bytes with plain `bp7::Bundle::try_from` and assert the payload block equals the segment), `unknown_block_192_survives_a_stock_roundtrip` (decode with `bp7`, re-encode, decode ours, content info intact), `age_hop_and_previous_node_blocks_present`, `payload_is_not_inflated` (1 MB payload encodes under 1 MB plus 1 KiB), `content_id_with_slash_is_rejected`, `decode_rejects_garbage_without_panic` (empty, truncated, random bytes, and a bundle with a wrong-typed block 192).
- [ ] **Step 2: Run** `cargo test wire` and expect FAIL.
- [ ] **Step 3: Implement** `src/wire/mod.rs` using `bp7`'s primary and canonical builders and the `CanonicalData::Unknown` variant for block 192. Encode `ContentInfo` as CBOR inside block 192. Primary block creation timestamp is DTN time zero.
- [ ] **Step 4: Reshape `Bundle`** as above and update all callers and existing tests until `cargo test` passes. `codec.rs` keeps its generic encode and decode for peer messages, and drops the bundle-specific tests that no longer apply.
- [ ] **Step 5: Write `docs/spec/spec-001-bundle-format.md`:** the block layout, block 192 fields, the destination URI, the lifetime and age rules and the control flags, in plain short sentences.
- [ ] **Step 6: Run** `cargo test` and expect PASS.
- [ ] **Step 7: Commit** `feat(wire): RFC 9171 bundles with a private-use content block`.

---

### Task 4: Age-based expiry and publisher-scoped storage

**Files:**
- Create: `src/store/age.rs`
- Modify: `src/store/mod.rs`, `src/bundle/mod.rs` (`BundleMeta`), `src/router/mod.rs` (update age on send), `src/main.rs` (reaper flushes the clock)
- Test: in-file tests in `src/store/age.rs` and `src/store/mod.rs`

**Interfaces:**
- Consumes: `Bundle`, `wire::{encode_bundle, decode_bundle}` from Task 3.
- Produces:
  - `pub trait MonoClock: Send + Sync { fn now_ms(&self) -> u64; }`, `pub struct SystemMono` (a persisted base plus `Instant` elapsed in this run, with `flush(&self) -> Result<()>`) and `pub struct FakeMono` for tests with `advance(&self, ms: u64)`.
  - `BundleStore::open_with_clock(path: &str, clock: Arc<dyn MonoClock>) -> Result<Self>` and `open(path)` wrapping it with `SystemMono`.
  - The store keeps encoded RFC 9171 bytes as the value, keyed by the new `BundleId` string. The meta tree stores `BundleMeta { id, publisher, content_id, title, sequence, kind, priority, created_at_ms, duration_ms, lifetime_ms, age_at_insert_ms, inserted_mono_ms }`.
  - `BundleMeta::age_ms(&self, now_mono_ms: u64) -> u64` is `age_at_insert_ms + (now_mono_ms - inserted_mono_ms)`, and `is_valid(now_mono_ms)` is `age_ms < lifetime_ms`.
  - `BundleStore::get` returns the bundle with `age_ms` brought up to date. The router sends bundles with that updated age.
  - Time spent while the process is down is not counted. This is a documented known limit.

- [ ] **Step 1: Write failing tests:** `fresh_bundle_is_valid`, `bundle_expires_when_age_reaches_lifetime` (advance `FakeMono`), `wrong_wall_clock_does_not_change_expiry` (the test sets nothing from `Utc::now`, bundles carry creation time zero, and advancing only the fake clock decides expiry), `age_in_received_bundle_counts_toward_expiry` (insert a bundle with `age_ms` near its lifetime, expect it to expire soon), `same_content_id_two_publishers_coexist`, `old_protocol_2_store_is_refused`.
- [ ] **Step 2: Run** `cargo test store` and expect FAIL.
- [ ] **Step 3: Implement** the clock and the store changes. Bump `codec::PROTOCOL_VERSION` to 3.
- [ ] **Step 4: Run** `cargo test` and expect PASS.
- [ ] **Step 5: Commit** `feat(store): expire by bundle age and key bundles by publisher`.

---

### Task 5: The admission gate

**Files:**
- Modify: `src/store/mod.rs`, `src/router/mod.rs`, `src/main.rs`
- Test: in-file tests in `src/store/mod.rs` and `src/router/mod.rs`

**Interfaces:**
- Consumes: `SignedManifest`, `KeyLookup`, `Trust` from Tasks 1 and 2, `BundleStore` from Task 4.
- Produces:
  - `pub enum Admission { Stored, Duplicate, Held, Rejected(String) }`.
  - `BundleStore::admit(&self, b: &Bundle, trust: &dyn KeyLookup) -> Result<Admission>`.
  - Rules: a manifest bundle from a paired publisher (or the node's own key) is stored if its version is newer than the stored one, and an older or equal version is a `Duplicate`. After a new manifest is stored, held segments for that content are re-checked and promoted. A segment is `Stored` only if a stored manifest lists its sequence and the BLAKE3 of the payload equals the listed hash. A segment with no manifest yet is `Held`. A segment whose hash differs is `Rejected`. A bundle from an unpaired publisher is `Rejected` (the carrier quota arrives in M2).
  - Held segments live in a `pending` tree for `HOLD_SECS: u64 = 600` of monotonic time and are removed by the reaper.
  - The router calls `admit` for every received bundle instead of `verify` and `is_valid`. `ManifestEntry` in the sync manifest now carries the new id string.

- [ ] **Step 1: Write failing tests:** `manifest_then_segment_is_stored`, `segment_then_manifest_is_promoted`, `held_segment_is_dropped_after_hold_time`, `segment_with_wrong_hash_is_rejected`, `unpaired_publisher_is_rejected`, `older_manifest_does_not_replace_newer`, `newer_manifest_replaces_older`, `router_rejects_tampered_segment_over_a_real_sync` (two in-process nodes, flip a payload byte on the sending side).
- [ ] **Step 2: Run** `cargo test admit` and expect FAIL.
- [ ] **Step 3: Implement** `admit` and wire it into the router and the reaper.
- [ ] **Step 4: Run** `cargo test` and expect PASS.
- [ ] **Step 5: Commit** `feat(store): admit segments only through a verified manifest`.

---

### Task 6: Signing ingest, publisher-aware API and demo regression

**Files:**
- Modify: `src/api/mod.rs`, `src/api/playlist.rs`, `src/ingest/mod.rs`, `src/ingest/live.rs`, `src/main.rs`, `README.md`, `TRANSPORT.md`
- Test: in-file tests in `src/api/mod.rs` and `src/ingest/mod.rs`

**Interfaces:**
- Consumes: everything above.
- Produces:
  - `serve` loads the node's `PublisherKey` from `--key-dir` if present and builds a `Trust` from it plus `PairedKeys`.
  - `POST /ingest` signs under the node's key, so the body is unchanged. Without a key it answers 409 with the message `run cosmosd keys init first`.
  - `POST /ingest/finish` takes `{ "content_id": "..." }` and publishes the signed manifest for a VOD item (`live: false`, all segments). For live content ingest publishes a new manifest version after every segment, `live: true`, listing the last 180 segments. `cosmosd ingest <file>` calls finish at the end.
  - `GET /play/:publisher/:content_id` serves the playlist. `GET /play/:content_id` still works and resolves when exactly one publisher has that content ID, otherwise it answers 300 with the matching publisher IDs. `GET /catalog` and `GET /segment/...` carry the publisher.
  - The playlist takes `created_at_ms` from the content info for the live edge.
  - The README and TRANSPORT.md describe pairing, signed content and age-based expiry in plain short sentences.

- [ ] **Step 1: Write failing tests:** `ingest_without_key_returns_409`, `ingest_then_finish_makes_content_playable`, `play_short_url_resolves_single_publisher`, `play_short_url_is_ambiguous_for_two_publishers`, `live_ingest_publishes_a_manifest_per_segment`, `catalog_lists_publisher`.
- [ ] **Step 2: Run** `cargo test api ingest` and expect FAIL.
- [ ] **Step 3: Implement** the API and ingest changes.
- [ ] **Step 4: Run** `cargo test` and expect PASS.
- [ ] **Step 5: Regression:** run the existing in-flight demo from `scripts/` (and `demo.py` if that is the entry point). Expect it to pass as before, including the 30 second and 5 minute cuts.
- [ ] **Step 6: Manual check with VLC:** `cosmosd keys init`, `cosmosd serve`, `cosmosd ingest /path/to/file.mp4`, then `vlc http://localhost:7777/play/<content_id>`. Expect it to play.
- [ ] **Step 7: Commit** `feat(api): signed ingest and publisher-aware playback`.

---

### Task 7: Interop with a stock BPv7 node

**Files:**
- Create: `scripts/interop-ud3tn.sh`, `docs/spec/interop.md`
- Test: the script itself

**Interfaces:**
- Consumes: `wire::encode_bundle`. Add a small hidden CLI command `cosmosd dev-bundle --out <file>` that writes one sample bundle as RFC 9171 bytes.
- Produces: a script that starts µD3TN in Docker, delivers the sample bundle to it by the standard path the image supports (TCPCLv4 preferred, a bundle file import otherwise), and exits 0 only when µD3TN reports it received or stored the bundle.

- [ ] **Step 1: Time-box a spike to 2 hours:** find which convergence layers the current µD3TN image supports and pick TCPCLv4 or file import. Record the choice in `docs/spec/interop.md`.
- [ ] **Step 2: Write the script** to run µD3TN in Docker, send the sample bundle and check µD3TN's own output or API for receipt.
- [ ] **Step 3: Run** `scripts/interop-ud3tn.sh` and expect exit 0 with a line saying the bundle was received.
- [ ] **Step 4: If µD3TN cannot be made to work in the time-box,** use `dtn7-rs` (`bp7dtn`) as the stock node, and say so in `docs/spec/interop.md`. Do not claim µD3TN interop unless the script passed against it.
- [ ] **Step 5: Commit** `test(interop): deliver a cosmosd bundle to a stock BPv7 node`.

---

## Self-review notes

- **Spec coverage for M1:** G1 is Tasks 3, 6 and 7. G4 is Tasks 1, 2 and 5. G5 is Task 4. The M1 acceptance checks map to Task 3 (stock `bp7` decode), Task 7 (independent node), Task 5 (hash and unpaired rejection) and Task 4 (wrong clock).
- **Not in M1 by design:** interest lists, storage cap, resume, carrier quota for unknown publishers, Bluetooth and phone work. These are M2 to M5.
- **Calls made in this plan, open to change:** key ID is 16 hex chars of the BLAKE3 of the public key. The default key directory is `$HOME/.config/cosmosd`. Hold time for segments awaiting a manifest is 600 seconds. Live manifests list the last 180 segments. The pairing string is `cosmos-key:` plus lowercase base32, and a QR code is left for later. `GET /play/:content_id` keeps working when only one publisher has that ID.
