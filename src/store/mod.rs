//! Content-addressed bundle store backed by sled (embedded key-value DB).
//!
//! Bundles are keyed by their BundleId string and stored as RFC 9171 bytes.
//! A second tree, `meta`, holds each bundle's payload-free metadata so
//! listing, the catalog and manifests never decode a payload. Expired bundles
//! are purged lazily on read and periodically by the background reaper task.
//!
//! Bundles from peers enter through [`BundleStore::admit`]. A segment is
//! stored only when a signed manifest from a trusted publisher lists its
//! BLAKE3 hash. A segment that arrives before its manifest waits in the
//! `pending` tree for [`HOLD_SECS`] and is never listed or offered to peers.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use anyhow::{bail, Context, Result};
use serde::{Deserialize, Serialize};
use sled::Transactional;
use tracing::{debug, info, warn};

use crate::bundle::{check_content_id, Bundle, BundleId, BundleKind, BundleMeta};
use crate::codec::{decode, encode};
use crate::trust::manifest::{Manifest, SignedManifest};
use crate::trust::{KeyId, KeyLookup};
use crate::wire::{decode_bundle, encode_bundle};

mod age;
pub use age::{MonoClock, SystemMono};
#[cfg(test)]
pub use age::FakeMono;

/// Seconds of store clock time a segment waits for its manifest.
pub const HOLD_SECS: u64 = 600;

pub struct BundleStore {
    /// id -> RFC 9171 bundle bytes (payload included).
    db: sled::Db,
    /// id -> CBOR `BundleMeta`.
    meta: sled::Tree,
    /// Segment id -> CBOR `HeldSegment`, for segments awaiting a manifest.
    pending: sled::Tree,
    /// Decides expiry together with each bundle's age.
    clock: Arc<dyn MonoClock>,
}

/// Format marker in the `sys` tree. A store without it was written before
/// age-based expiry (sync protocol 2) and holds a different meta layout.
const FORMAT_KEY: &[u8] = b"format";
const FORMAT: u8 = 3;

impl BundleStore {
    pub fn open(path: &str) -> Result<Self> {
        Self::open_inner(path, |sys| Ok(Arc::new(SystemMono::open(sys)?)))
    }

    #[cfg(test)]
    pub fn open_with_clock(path: &str, clock: Arc<dyn MonoClock>) -> Result<Self> {
        Self::open_inner(path, |_| Ok(clock))
    }

    fn open_inner(path: &str, make_clock: impl FnOnce(sled::Tree) -> Result<Arc<dyn MonoClock>>) -> Result<Self> {
        let db = sled::open(path).context("Failed to open bundle store")?;
        let meta = db.open_tree("meta").context("Failed to open bundle meta tree")?;
        let sys = db.open_tree("sys").context("Failed to open bundle sys tree")?;
        let pending = db.open_tree("pending").context("Failed to open bundle pending tree")?;
        // Pre-protocol-2 stores hold JSON bundles in the default tree and no
        // meta tree. `insert` writes both trees atomically, so this layout
        // never arises otherwise. No migration: the content is short-lived.
        if !db.is_empty() && meta.is_empty() {
            bail!("store at {path} is from an older cosmosd (before sync protocol 2): delete it to start fresh");
        }
        match sys.get(FORMAT_KEY)? {
            Some(v) if v.as_ref() == [FORMAT] => {}
            Some(_) => bail!("store at {path} has an unknown format: delete it to start fresh"),
            None if !meta.is_empty() => bail!("store at {path} is from sync protocol 2: delete it to start fresh"),
            None => {
                sys.insert(FORMAT_KEY, &[FORMAT])?;
            }
        }
        let clock = make_clock(sys)?;
        info!("Bundle store opened at {}", path);
        Ok(BundleStore { db, meta, pending, clock })
    }

    /// Persist the expiry clock. Call on a schedule and at shutdown.
    pub fn flush_clock(&self) -> Result<()> {
        self.clock.flush()
    }

    /// Insert a bundle. Returns false if a valid copy is already present
    /// (dedup by BundleId). An expired copy is overwritten. A manifest
    /// replaces the stored one only when its version is higher. Nothing is
    /// verified here: this is for the node's own content. Peers use `admit`.
    pub fn insert(&self, bundle: &Bundle) -> Result<bool> {
        if bundle.info.kind == BundleKind::Manifest {
            let Some(version) = manifest_version_of(bundle) else {
                bail!("manifest {} has an unreadable payload", bundle.id());
            };
            return self.put_manifest(bundle, version);
        }
        let id = bundle.id();
        let key = id.as_str().as_bytes();
        let value = encode_bundle(bundle)?;
        let now = self.clock.now_ms();
        let meta_value = encode(&bundle.meta(now))?;
        // Both trees in one transaction, with the dedup check inside it, so a
        // crash can't leave a payload without its meta entry. An expired (or
        // unreadable) entry counts as absent, so re-ingest replaces it.
        let inserted = (&*self.db, &self.meta)
            .transaction(|(db, meta)| -> sled::transaction::ConflictableTransactionResult<bool, ()> {
                let live = match meta.get(key)? {
                    Some(raw) => decode::<BundleMeta>(&raw).is_ok_and(|m| m.is_valid(now)),
                    None => false,
                };
                if live && db.get(key)?.is_some() {
                    return Ok(false);
                }
                db.insert(key, value.as_slice())?;
                meta.insert(key, meta_value.as_slice())?;
                Ok(true)
            })
            .map_err(|e| anyhow::anyhow!("bundle insert transaction: {e:?}"))?;
        if inserted {
            debug!("Stored bundle: {}", id);
        } else {
            debug!("Duplicate bundle ignored: {}", id);
        }
        Ok(inserted)
    }

    /// Retrieve a bundle by ID with its age brought up to date (age at insert
    /// plus time held). Returns None if missing or expired.
    pub fn get(&self, id: &str) -> Result<Option<Bundle>> {
        let now = self.clock.now_ms();
        let Some(raw_meta) = self.meta.get(id.as_bytes())? else {
            return Ok(None);
        };
        let m: BundleMeta = decode(&raw_meta)?;
        if !m.is_valid(now) {
            self.db.remove(id.as_bytes())?;
            self.meta.remove(id.as_bytes())?;
            return Ok(None);
        }
        let Some(raw) = self.db.get(id.as_bytes())? else {
            return Ok(None);
        };
        let mut bundle = decode_bundle(&raw)?;
        bundle.age_ms = m.age_ms(now);
        Ok(Some(bundle))
    }

    /// Metadata of every valid (non-expired) bundle.
    pub fn all_meta(&self) -> Result<Vec<BundleMeta>> {
        let now = self.clock.now_ms();
        let mut out = Vec::new();
        for item in self.meta.iter() {
            let (_, value) = item?;
            let m: BundleMeta = decode(&value)?;
            if m.is_valid(now) {
                out.push(m);
            }
        }
        Ok(out)
    }

    /// Metadata of the valid segments of one publisher's content, ascending
    /// by sequence. Reads only that content's keys.
    pub fn list_meta(&self, publisher: &KeyId, content_id: &str) -> Result<Vec<BundleMeta>> {
        let now = self.clock.now_ms();
        // Content IDs hold no `/`, so this prefix matches one content only.
        let prefix = format!("{publisher}/{content_id}/");
        let mut out = Vec::new();
        for item in self.meta.scan_prefix(prefix.as_bytes()) {
            let (_, value) = item?;
            let m: BundleMeta = decode(&value)?;
            if m.kind == BundleKind::Segment && m.is_valid(now) {
                out.push(m);
            }
        }
        out.sort_by_key(|m| m.sequence);
        Ok(out)
    }

    /// The stored manifest of a publisher's content, decoded without checking
    /// its signature. Peer manifests were verified by `admit` and the node's
    /// own were signed here. None when absent, expired or unreadable.
    pub fn manifest(&self, publisher: &KeyId, content_id: &str) -> Result<Option<Manifest>> {
        let id = BundleId::manifest(publisher, content_id);
        let Some(b) = self.get(id.as_str())? else {
            return Ok(None);
        };
        let manifest = SignedManifest::from_bytes(&b.payload).and_then(|s| decode::<Manifest>(&s.manifest_cbor));
        Ok(manifest.ok())
    }

    /// Every publisher and content ID in the store with its segment counts.
    /// Two publishers that share a content ID get one entry each.
    pub fn catalog(&self) -> Result<Vec<ContentSummary>> {
        let mut map: HashMap<(KeyId, String), ContentSummary> = HashMap::new();

        for m in self.all_meta()?.into_iter().filter(|m| m.kind == BundleKind::Segment) {
            let key = (m.publisher.clone(), m.content_id.clone());
            let entry = map.entry(key).or_insert_with(|| ContentSummary {
                publisher: m.publisher.clone(),
                content_id: m.content_id.clone(),
                title: m.title.clone(),
                priority: m.priority,
                segment_count: 0,
                last_sequence: m.sequence,
            });
            entry.segment_count += 1;
            entry.last_sequence = entry.last_sequence.max(m.sequence);
        }

        let mut items: Vec<_> = map.into_values().collect();
        items.sort_by(|a, b| (&a.content_id, &a.publisher.0).cmp(&(&b.content_id, &b.publisher.0)));
        Ok(items)
    }

    /// Return all valid bundle IDs in the store (for manifest exchange).
    pub fn all_ids(&self) -> Result<HashSet<String>> {
        Ok(self.all_meta()?.into_iter().map(|m| m.id).collect())
    }

    /// Remove expired bundles. Call periodically from background task.
    pub fn reap_expired(&self) -> Result<usize> {
        let now = self.clock.now_ms();
        let mut removed = 0usize;

        for item in self.meta.iter() {
            let (key, value) = item?;
            let m: BundleMeta = decode(&value)?;
            if !m.is_valid(now) {
                self.db.remove(&key)?;
                self.meta.remove(&key)?;
                removed += 1;
            }
        }

        if removed > 0 {
            info!("Reaped {} expired bundles", removed);
        }

        let mut dropped = 0usize;
        for item in self.pending.iter() {
            let (key, value) = item?;
            if decode::<HeldSegment>(&value).map_or(true, |h| h.hold_expired(now)) {
                self.pending.remove(&key)?;
                dropped += 1;
            }
        }
        if dropped > 0 {
            info!("Dropped {} held segments whose manifest never came", dropped);
        }
        Ok(removed + dropped)
    }
}

// === Admission ===

/// What [`BundleStore::admit`] did with a bundle.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Admission {
    /// Verified and stored.
    Stored,
    /// A valid copy is already stored. For a manifest, the stored version is
    /// the same or newer.
    Duplicate,
    /// A segment from a trusted publisher, waiting in `pending` for its manifest.
    Held,
    /// Refused and not stored. The reason is for logs.
    Rejected(String),
}

fn reject(reason: impl Into<String>) -> Admission {
    Admission::Rejected(reason.into())
}

/// A segment waiting for its manifest, with the store clock reading when it
/// was first held.
#[derive(Serialize, Deserialize)]
struct HeldSegment {
    held_at_ms: u64,
    /// RFC 9171 bundle bytes.
    #[serde(with = "serde_bytes")]
    bundle: Vec<u8>,
}

impl HeldSegment {
    fn hold_expired(&self, now_ms: u64) -> bool {
        now_ms.saturating_sub(self.held_at_ms) >= HOLD_SECS * 1000
    }
}

/// Verify a manifest bundle's signed payload and return the manifest. The
/// manifest must name the bundle's own publisher and content ID.
fn verify_manifest(b: &Bundle, trust: &dyn KeyLookup) -> Result<Manifest> {
    let m = SignedManifest::from_bytes(&b.payload)?.verify(trust)?;
    if m.publisher != b.publisher {
        bail!("manifest names publisher {} but the bundle is from {}", m.publisher, b.publisher);
    }
    if m.content_id != b.info.content_id {
        bail!("manifest is for {:?} but the bundle is for {:?}", m.content_id, b.info.content_id);
    }
    Ok(m)
}

/// The version inside a manifest bundle's payload, without checking the
/// signature. None when it is unreadable.
fn manifest_version_of(b: &Bundle) -> Option<u64> {
    let signed = SignedManifest::from_bytes(&b.payload).ok()?;
    decode::<Manifest>(&signed.manifest_cbor).ok().map(|m| m.version)
}

/// The version of a manifest already in the store. Stored manifests were
/// verified on the way in, so this only decodes. None when it is unreadable.
fn stored_manifest_version(raw: &[u8]) -> Option<u64> {
    manifest_version_of(&decode_bundle(raw).ok()?)
}

/// Check a segment against a verified manifest of the same publisher and content.
fn check_segment(b: &Bundle, m: &Manifest) -> Result<(), String> {
    if b.publisher != m.publisher || b.info.content_id != m.content_id {
        return Err(format!("segment {} does not belong to this manifest", b.id()));
    }
    let Some(entry) = m.hash_for(b.info.sequence) else {
        return Err(format!("sequence {} is not in manifest version {}", b.info.sequence, m.version));
    };
    if blake3::hash(&b.payload).as_bytes() != &entry.hash {
        return Err(format!("payload of {} does not match the manifest hash", b.id()));
    }
    Ok(())
}

impl BundleStore {
    /// The gate for bundles from peers.
    ///
    /// The publisher must be in `trust`. A manifest must carry a valid
    /// signature and is stored only when it is newer than the stored one. A
    /// segment is stored only when the stored manifest lists its sequence with
    /// the BLAKE3 hash of its payload. A segment with no manifest yet is held.
    /// Hostile input gives `Rejected`, and only storage failures are errors.
    pub fn admit(&self, b: &Bundle, trust: &dyn KeyLookup) -> Result<Admission> {
        if !b.valid_on_arrival() {
            return Ok(reject(format!("{} expired before it arrived", b.id())));
        }
        // Held segments are found by a `<publisher>/<content_id>/` prefix.
        if let Err(e) = check_content_id(&b.info.content_id) {
            return Ok(reject(e.to_string()));
        }
        if trust.lookup(&b.publisher).is_none() {
            return Ok(reject(format!("publisher {} is not paired", b.publisher)));
        }
        match b.info.kind {
            BundleKind::Manifest => self.admit_manifest(b, trust),
            BundleKind::Segment => self.admit_segment(b, trust),
        }
    }

    fn admit_manifest(&self, b: &Bundle, trust: &dyn KeyLookup) -> Result<Admission> {
        let m = match verify_manifest(b, trust) {
            Ok(m) => m,
            Err(e) => return Ok(reject(format!("manifest {}: {e}", b.id()))),
        };
        if !self.put_manifest(b, m.version)? {
            debug!("Manifest {} version {} is not newer, ignored", b.id(), m.version);
            return Ok(Admission::Duplicate);
        }
        debug!("Stored manifest {} version {}", b.id(), m.version);
        self.promote_held(&m)?;
        Ok(Admission::Stored)
    }

    /// Store a manifest bundle unless a valid stored one has the same or a
    /// higher version.
    fn put_manifest(&self, b: &Bundle, version: u64) -> Result<bool> {
        let id = b.id();
        let key = id.as_str().as_bytes();
        let value = encode_bundle(b)?;
        let now = self.clock.now_ms();
        let meta_value = encode(&b.meta(now))?;
        // The version check sits inside the transaction, so two manifests
        // arriving at once can never leave the older one stored.
        (&*self.db, &self.meta)
            .transaction(|(db, meta)| -> sled::transaction::ConflictableTransactionResult<bool, ()> {
                let live = match meta.get(key)? {
                    Some(raw) => decode::<BundleMeta>(&raw).is_ok_and(|m| m.is_valid(now)),
                    None => false,
                };
                if live {
                    if let Some(raw) = db.get(key)? {
                        if stored_manifest_version(&raw).is_some_and(|v| v >= version) {
                            return Ok(false);
                        }
                    }
                }
                db.insert(key, value.as_slice())?;
                meta.insert(key, meta_value.as_slice())?;
                Ok(true)
            })
            .map_err(|e| anyhow::anyhow!("manifest insert transaction: {e:?}"))
    }

    fn admit_segment(&self, b: &Bundle, trust: &dyn KeyLookup) -> Result<Admission> {
        let Some(m) = self.current_manifest(&b.publisher, &b.info.content_id, trust)? else {
            return self.hold(b, trust);
        };
        if let Err(reason) = check_segment(b, &m) {
            return Ok(reject(reason));
        }
        Ok(if self.insert(b)? { Admission::Stored } else { Admission::Duplicate })
    }

    /// The stored manifest for a publisher's content, verified again against
    /// `trust`. None when absent, expired, or no longer valid.
    fn current_manifest(&self, publisher: &KeyId, content_id: &str, trust: &dyn KeyLookup) -> Result<Option<Manifest>> {
        let id = BundleId::manifest(publisher, content_id);
        let Some(b) = self.get(id.as_str())? else {
            return Ok(None);
        };
        match verify_manifest(&b, trust) {
            Ok(m) => Ok(Some(m)),
            Err(e) => {
                warn!("Stored manifest {} does not verify: {}", id, e);
                Ok(None)
            }
        }
    }

    /// Put a segment in `pending`. A copy already held keeps its first hold
    /// time, so sending it again never extends the hold.
    fn hold(&self, b: &Bundle, trust: &dyn KeyLookup) -> Result<Admission> {
        let id = b.id();
        let key = id.as_str().as_bytes();
        let now = self.clock.now_ms();
        let already = match self.pending.get(key)? {
            Some(raw) => decode::<HeldSegment>(&raw).is_ok_and(|h| !h.hold_expired(now)),
            None => false,
        };
        if !already {
            let held = HeldSegment { held_at_ms: now, bundle: encode_bundle(b)? };
            self.pending.insert(key, encode(&held)?)?;
            debug!("Holding {} until its manifest arrives", id);
        }
        // The manifest may have been stored, and its held segments promoted,
        // after the lookup in `admit_segment` but before the insert above.
        if let Some(m) = self.current_manifest(&b.publisher, &b.info.content_id, trust)? {
            if let Some(raw) = self.pending.remove(key)? {
                return self.promote_entry(&raw, &m);
            }
        }
        Ok(Admission::Held)
    }

    /// Re-check every held segment of a manifest's content: store those whose
    /// hash matches and drop the rest.
    fn promote_held(&self, m: &Manifest) -> Result<()> {
        let prefix = format!("{}/{}/", m.publisher, m.content_id);
        let (mut stored, mut dropped) = (0usize, 0usize);
        for item in self.pending.scan_prefix(prefix.as_bytes()) {
            let (key, _) = item?;
            // Removing first claims the entry, so a racing promoter skips it.
            let Some(raw) = self.pending.remove(&key)? else {
                continue;
            };
            match self.promote_entry(&raw, m)? {
                Admission::Stored => stored += 1,
                Admission::Rejected(reason) => {
                    warn!("Dropped held segment: {}", reason);
                    dropped += 1;
                }
                Admission::Duplicate | Admission::Held => {}
            }
        }
        if stored + dropped > 0 {
            info!("Manifest {}/{}: promoted {} held segments, dropped {}", m.publisher, m.content_id, stored, dropped);
        }
        Ok(())
    }

    /// Check one held entry, already removed from `pending`, and store it if
    /// it passes. The time it spent held is added to its age.
    fn promote_entry(&self, raw: &[u8], m: &Manifest) -> Result<Admission> {
        let now = self.clock.now_ms();
        let Ok(held) = decode::<HeldSegment>(raw) else {
            return Ok(reject("unreadable held entry"));
        };
        if held.hold_expired(now) {
            return Ok(reject("hold time ran out before the manifest arrived"));
        }
        let mut b = match decode_bundle(&held.bundle) {
            Ok(b) => b,
            Err(e) => return Ok(reject(format!("unreadable held bundle: {e}"))),
        };
        b.age_ms = b.age_ms.saturating_add(now.saturating_sub(held.held_at_ms));
        if !b.valid_on_arrival() {
            return Ok(reject(format!("{} expired while held", b.id())));
        }
        if let Err(reason) = check_segment(&b, m) {
            return Ok(reject(reason));
        }
        Ok(if self.insert(&b)? { Admission::Stored } else { Admission::Duplicate })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bundle::Priority;

    fn publisher() -> KeyId {
        KeyId("0123456789abcdef".into())
    }

    fn make_bundle(content_id: &str, sequence: u32) -> Bundle {
        make_bundle_by(&publisher(), content_id, sequence)
    }

    fn make_bundle_by(publisher: &KeyId, content_id: &str, sequence: u32) -> Bundle {
        Bundle::segment(publisher, content_id, "Test Video", sequence, "video/mp2t", Priority::Vod, b"data".to_vec(), "node-a")
            .unwrap()
    }

    /// A bundle whose age has reached its lifetime.
    fn expired_bundle(content_id: &str, sequence: u32) -> Bundle {
        let mut b = make_bundle(content_id, sequence);
        b.age_ms = b.lifetime_ms;
        b
    }

    /// A store on a fake clock. Keeps the tempdir alive as long as it is.
    struct TestStore {
        store: BundleStore,
        clock: Arc<FakeMono>,
        _dir: tempfile::TempDir,
    }

    impl std::ops::Deref for TestStore {
        type Target = BundleStore;
        fn deref(&self) -> &BundleStore {
            &self.store
        }
    }

    fn temp_store() -> TestStore {
        let dir = tempfile::tempdir().unwrap();
        let clock = Arc::new(FakeMono::default());
        let store = BundleStore::open_with_clock(dir.path().to_str().unwrap(), clock.clone()).unwrap();
        TestStore { store, clock, _dir: dir }
    }

    /// Write a bundle to both trees without the validity checks `insert` skips.
    fn insert_raw(store: &TestStore, b: &Bundle) {
        let id = b.id();
        store.db.insert(id.as_str().as_bytes(), encode_bundle(b).unwrap()).unwrap();
        store.meta.insert(id.as_str().as_bytes(), encode(&b.meta(store.clock.now_ms())).unwrap()).unwrap();
    }

    #[test]
    fn insert_and_get() {
        let store = temp_store();
        let b = make_bundle("show_a", 0);
        assert!(store.insert(&b).unwrap());
        let got = store.get(b.id().as_str()).unwrap().unwrap();
        assert_eq!(got, b);
    }

    #[test]
    fn fresh_bundle_is_valid() {
        let store = temp_store();
        let b = make_bundle("show_a", 0);
        store.insert(&b).unwrap();
        assert_eq!(store.all_meta().unwrap().len(), 1);
        assert!(store.get(b.id().as_str()).unwrap().is_some());
    }

    #[test]
    fn bundle_expires_when_age_reaches_lifetime() {
        let store = temp_store();
        let b = make_bundle("show_a", 0);
        store.insert(&b).unwrap();
        store.clock.advance(b.lifetime_ms - 1);
        assert!(store.get(b.id().as_str()).unwrap().is_some());
        store.clock.advance(1);
        assert!(store.all_meta().unwrap().is_empty());
        assert!(store.get(b.id().as_str()).unwrap().is_none());
    }

    #[test]
    fn wrong_wall_clock_does_not_change_expiry() {
        // Creation times from a clock that is years off in both directions.
        // Nothing here reads the wall clock for expiry: only age and held time.
        for created_at_ms in [0, i64::MAX / 2] {
            let store = temp_store();
            let mut b = make_bundle("show_a", 0);
            b.info.created_at_ms = created_at_ms;
            store.insert(&b).unwrap();
            store.clock.advance(b.lifetime_ms - 1);
            assert!(store.get(b.id().as_str()).unwrap().is_some(), "created_at {created_at_ms}");
            store.clock.advance(1);
            assert!(store.get(b.id().as_str()).unwrap().is_none(), "created_at {created_at_ms}");
        }
    }

    #[test]
    fn age_in_received_bundle_counts_toward_expiry() {
        let store = temp_store();
        let mut b = make_bundle("show_a", 0);
        b.age_ms = b.lifetime_ms - 1_000;
        store.insert(&b).unwrap();
        store.clock.advance(999);
        assert!(store.get(b.id().as_str()).unwrap().is_some());
        store.clock.advance(1);
        assert!(store.get(b.id().as_str()).unwrap().is_none());
    }

    #[test]
    fn get_brings_age_up_to_date() {
        let store = temp_store();
        let mut b = make_bundle("show_a", 0);
        b.age_ms = 500;
        store.insert(&b).unwrap();
        store.clock.advance(2_000);
        assert_eq!(store.get(b.id().as_str()).unwrap().unwrap().age_ms, 2_500);
    }

    #[test]
    fn same_content_id_two_publishers_coexist() {
        let store = temp_store();
        let (p1, p2) = (KeyId("aaaaaaaaaaaaaaaa".into()), KeyId("bbbbbbbbbbbbbbbb".into()));
        let (a, b) = (make_bundle_by(&p1, "show", 0), make_bundle_by(&p2, "show", 0));
        assert!(store.insert(&a).unwrap());
        assert!(store.insert(&b).unwrap());
        assert_ne!(a.id(), b.id());
        assert_eq!(store.get(a.id().as_str()).unwrap().unwrap().publisher, p1);
        assert_eq!(store.get(b.id().as_str()).unwrap().unwrap().publisher, p2);
        assert_eq!(store.all_ids().unwrap().len(), 2);
    }

    #[test]
    fn insert_writes_both_trees_and_dedups() {
        let store = temp_store();
        let b = make_bundle("show_a", 0);
        assert!(store.insert(&b).unwrap());
        let id = b.id();
        let id = id.as_str();
        let (db_raw, meta_raw) = (store.db.get(id).unwrap().unwrap(), store.meta.get(id).unwrap().unwrap());
        assert!(!store.insert(&b).unwrap());
        assert_eq!(store.db.get(id).unwrap().unwrap(), db_raw);
        assert_eq!(store.meta.get(id).unwrap().unwrap(), meta_raw);
        assert_eq!(store.meta.len(), 1);
    }

    #[test]
    fn insert_is_idempotent() {
        let store = temp_store();
        let b = make_bundle("show_a", 0);
        assert!(store.insert(&b).unwrap());
        assert!(!store.insert(&b).unwrap()); // second insert returns false
    }

    #[test]
    fn get_returns_none_for_missing() {
        let store = temp_store();
        assert!(store.get("0123456789abcdef/missing/00000").unwrap().is_none());
    }

    #[test]
    fn get_returns_none_for_expired() {
        let store = temp_store();
        let b = expired_bundle("show_a", 0);
        insert_raw(&store, &b);
        assert!(store.get(b.id().as_str()).unwrap().is_none());
    }

    #[test]
    fn list_meta_returns_sorted_segments() {
        let store = temp_store();
        store.insert(&make_bundle("show_a", 2)).unwrap();
        store.insert(&make_bundle("show_a", 0)).unwrap();
        store.insert(&make_bundle("show_a", 1)).unwrap();
        store.insert(&make_bundle_by(&KeyId("ffffffffffffffff".into()), "show_a", 5)).unwrap();
        let bundles = store.list_meta(&publisher(), "show_a").unwrap();
        assert_eq!(bundles.len(), 3);
        assert_eq!(bundles[0].sequence, 0);
        assert_eq!(bundles[1].sequence, 1);
        assert_eq!(bundles[2].sequence, 2);
    }

    #[test]
    fn catalog_groups_by_content_id() {
        let store = temp_store();
        store.insert(&make_bundle("show_a", 0)).unwrap();
        store.insert(&make_bundle("show_a", 1)).unwrap();
        store.insert(&make_bundle("show_b", 0)).unwrap();
        let catalog = store.catalog().unwrap();
        assert_eq!(catalog.len(), 2);
        let a = catalog.iter().find(|c| c.content_id == "show_a").unwrap();
        assert_eq!(a.segment_count, 2);
    }

    #[test]
    fn list_meta_skips_other_content_and_manifests() {
        let store = temp_store();
        store.insert(&make_bundle("show", 0)).unwrap();
        // "show_b" shares the "show" text prefix but is another content ID.
        store.insert(&make_bundle("show_b", 1)).unwrap();
        let ids: Vec<String> = store.list_meta(&publisher(), "show").unwrap().into_iter().map(|m| m.id).collect();
        assert_eq!(ids, vec!["0123456789abcdef/show/00000".to_string()]);
    }

    #[test]
    fn catalog_keeps_publishers_apart() {
        let store = temp_store();
        let (p1, p2) = (KeyId("aaaaaaaaaaaaaaaa".into()), KeyId("bbbbbbbbbbbbbbbb".into()));
        store.insert(&make_bundle_by(&p2, "show", 0)).unwrap();
        store.insert(&make_bundle_by(&p1, "show", 0)).unwrap();
        store.insert(&make_bundle_by(&p1, "show", 1)).unwrap();
        let c = store.catalog().unwrap();
        let got: Vec<(KeyId, u32)> = c.iter().map(|s| (s.publisher.clone(), s.segment_count)).collect();
        assert_eq!(got, vec![(p1, 2), (p2, 1)]);
    }

    #[test]
    fn all_ids_returns_valid_bundle_ids() {
        let store = temp_store();
        store.insert(&make_bundle("show_a", 0)).unwrap();
        store.insert(&make_bundle("show_a", 1)).unwrap();
        let ids = store.all_ids().unwrap();
        assert_eq!(ids.len(), 2);
        assert!(ids.contains("0123456789abcdef/show_a/00000"));
        assert!(ids.contains("0123456789abcdef/show_a/00001"));
    }

    #[test]
    fn reap_expired_removes_stale_bundles() {
        let store = temp_store();
        let b = expired_bundle("show_a", 0);
        insert_raw(&store, &b);
        assert_eq!(store.reap_expired().unwrap(), 1);
        assert!(store.get(b.id().as_str()).unwrap().is_none());
    }

    #[test]
    fn reap_expires_by_held_time() {
        let store = temp_store();
        let b = make_bundle("show_a", 0);
        store.insert(&b).unwrap();
        assert_eq!(store.reap_expired().unwrap(), 0);
        store.clock.advance(b.lifetime_ms);
        assert_eq!(store.reap_expired().unwrap(), 1);
    }

    #[test]
    fn catalog_reports_last_sequence() {
        let store = temp_store();
        for s in [7, 0, 1] { store.insert(&make_bundle("show_a", s)).unwrap(); }
        let a = store.catalog().unwrap().into_iter().find(|c| c.content_id == "show_a").unwrap();
        assert_eq!(a.last_sequence, 7);
        assert_eq!(a.segment_count, 3);
    }

    #[test]
    fn open_refuses_legacy_store() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().to_str().unwrap();
        {
            // Pre-protocol-2 layout: JSON bundles in the default tree, no meta.
            let db = sled::open(path).unwrap();
            db.insert(b"show_a/00000", br#"{"id":"show_a/00000"}"#.to_vec()).unwrap();
            db.flush().unwrap();
        }
        let err = BundleStore::open(path).err().expect("legacy store must not open");
        assert_eq!(
            err.to_string(),
            format!("store at {path} is from an older cosmosd (before sync protocol 2): delete it to start fresh")
        );
    }

    #[test]
    fn old_protocol_2_store_is_refused() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().to_str().unwrap();
        {
            // Protocol 2 layout: both trees filled, no format marker.
            let db = sled::open(path).unwrap();
            db.insert(b"show_a/00000", b"old".to_vec()).unwrap();
            db.open_tree("meta").unwrap().insert(b"show_a/00000", b"old".to_vec()).unwrap();
            db.flush().unwrap();
        }
        let err = BundleStore::open(path).err().expect("protocol 2 store must not open");
        assert_eq!(
            err.to_string(),
            format!("store at {path} is from sync protocol 2: delete it to start fresh")
        );
    }

    #[test]
    fn own_store_reopens_and_keeps_bundles() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().to_str().unwrap();
        let b = make_bundle("show_a", 0);
        {
            let store = BundleStore::open(path).unwrap();
            store.insert(&b).unwrap();
            store.flush_clock().unwrap();
        }
        let store = BundleStore::open(path).unwrap();
        let got = store.get(b.id().as_str()).unwrap().unwrap();
        assert_eq!(got.payload, b.payload);
        assert_eq!(got.publisher, b.publisher);
    }

    #[test]
    fn insert_replaces_expired_entry() {
        let store = temp_store();
        let old = expired_bundle("show_a", 0);
        insert_raw(&store, &old);
        let fresh = make_bundle("show_a", 0);
        assert!(store.insert(&fresh).unwrap());
        let got = store.get(fresh.id().as_str()).unwrap().expect("fresh bundle readable");
        assert_eq!(got.age_ms, 0);
        assert_eq!(store.list_meta(&publisher(), "show_a").unwrap().len(), 1);
    }

    #[test]
    fn reap_removes_payload_and_meta() {
        let store = temp_store();
        let b = expired_bundle("show_a", 0);
        insert_raw(&store, &b);
        assert_eq!(store.reap_expired().unwrap(), 1);
        assert!(store.db.get(b.id().as_str()).unwrap().is_none());
        assert!(store.meta.get(b.id().as_str()).unwrap().is_none());
    }

    // === Admission ===

    mod admit {
        use super::*;
        use crate::bundle::BundleId;
        use crate::trust::manifest::{Manifest, SegmentEntry, SignedManifest};
        use crate::trust::{KeyLookup, PublisherKey};

        /// Trusts exactly the keys it holds.
        struct Keys(Vec<PublisherKey>);

        impl KeyLookup for Keys {
            fn lookup(&self, id: &KeyId) -> Option<[u8; 32]> {
                self.0.iter().find(|k| k.key_id() == *id).map(|k| k.public())
            }
        }

        fn payload(seq: u32) -> Vec<u8> {
            format!("segment {seq}").into_bytes()
        }

        fn segment(key: &PublisherKey, seq: u32, payload: Vec<u8>) -> Bundle {
            Bundle::segment(&key.key_id(), "show", "Show", seq, "video/mp2t", Priority::Vod, payload, "node-a").unwrap()
        }

        fn manifest(key: &PublisherKey, version: u64, seqs: impl IntoIterator<Item = u32>) -> Manifest {
            Manifest {
                publisher: key.key_id(),
                content_id: "show".into(),
                title: "Show".into(),
                version,
                created_at_ms: 1_700_000_000_000,
                live: false,
                segments: seqs
                    .into_iter()
                    .map(|s| SegmentEntry { sequence: s, hash: *blake3::hash(&payload(s)).as_bytes(), duration_ms: 10_000 })
                    .collect(),
            }
        }

        fn manifest_bundle(key: &PublisherKey, m: &Manifest) -> Bundle {
            let signed = SignedManifest::sign(m, key).unwrap().to_bytes().unwrap();
            Bundle::manifest(&key.key_id(), m, signed, "node-a")
        }

        fn stored_version(store: &BundleStore, key: &PublisherKey) -> u64 {
            let b = store.get(BundleId::manifest(&key.key_id(), "show").as_str()).unwrap().expect("manifest stored");
            let m: Manifest = decode(&SignedManifest::from_bytes(&b.payload).unwrap().manifest_cbor).unwrap();
            m.version
        }

        fn is_stored(store: &BundleStore, b: &Bundle) -> bool {
            store.get(b.id().as_str()).unwrap().is_some()
        }

        #[test]
        fn manifest_then_segment_is_stored() {
            let store = temp_store();
            let key = PublisherKey::generate();
            let keys = Keys(vec![key.clone()]);
            let m = manifest_bundle(&key, &manifest(&key, 1, 0..3));
            assert_eq!(store.admit(&m, &keys).unwrap(), Admission::Stored);
            let s = segment(&key, 1, payload(1));
            assert_eq!(store.admit(&s, &keys).unwrap(), Admission::Stored);
            assert_eq!(store.get(s.id().as_str()).unwrap().unwrap().payload, payload(1));
            assert_eq!(store.admit(&s, &keys).unwrap(), Admission::Duplicate);
            assert!(store.all_ids().unwrap().contains(m.id().as_str()));
        }

        #[test]
        fn segment_then_manifest_is_promoted() {
            let store = temp_store();
            let key = PublisherKey::generate();
            let keys = Keys(vec![key.clone()]);
            let s = segment(&key, 2, payload(2));
            assert_eq!(store.admit(&s, &keys).unwrap(), Admission::Held);
            // Held segments are not offered to peers or listed anywhere.
            assert!(!is_stored(&store, &s));
            assert!(store.all_ids().unwrap().is_empty());
            assert_eq!(store.pending.len(), 1);
            store.clock.advance(5_000);
            let m = manifest_bundle(&key, &manifest(&key, 1, 0..3));
            assert_eq!(store.admit(&m, &keys).unwrap(), Admission::Stored);
            let got = store.get(s.id().as_str()).unwrap().expect("promoted");
            assert_eq!(got.payload, payload(2));
            // Time spent held counts toward the age.
            assert_eq!(got.age_ms, 5_000);
            assert_eq!(store.pending.len(), 0);
        }

        #[test]
        fn held_segment_is_dropped_after_hold_time() {
            let store = temp_store();
            let key = PublisherKey::generate();
            let keys = Keys(vec![key.clone()]);
            let s = segment(&key, 0, payload(0));
            assert_eq!(store.admit(&s, &keys).unwrap(), Admission::Held);
            store.clock.advance(HOLD_SECS * 1000 - 1);
            assert_eq!(store.reap_expired().unwrap(), 0);
            assert_eq!(store.pending.len(), 1);
            store.clock.advance(1);
            assert_eq!(store.reap_expired().unwrap(), 1);
            assert_eq!(store.pending.len(), 0);
            let m = manifest_bundle(&key, &manifest(&key, 1, 0..3));
            assert_eq!(store.admit(&m, &keys).unwrap(), Admission::Stored);
            assert!(!is_stored(&store, &s));
        }

        #[test]
        fn expired_hold_is_not_promoted_before_the_reaper_runs() {
            let store = temp_store();
            let key = PublisherKey::generate();
            let keys = Keys(vec![key.clone()]);
            let s = segment(&key, 0, payload(0));
            assert_eq!(store.admit(&s, &keys).unwrap(), Admission::Held);
            store.clock.advance(HOLD_SECS * 1000);
            let m = manifest_bundle(&key, &manifest(&key, 1, 0..3));
            assert_eq!(store.admit(&m, &keys).unwrap(), Admission::Stored);
            assert!(!is_stored(&store, &s));
            assert_eq!(store.pending.len(), 0);
        }

        #[test]
        fn resending_a_held_segment_does_not_extend_its_hold() {
            let store = temp_store();
            let key = PublisherKey::generate();
            let keys = Keys(vec![key.clone()]);
            let s = segment(&key, 0, payload(0));
            assert_eq!(store.admit(&s, &keys).unwrap(), Admission::Held);
            store.clock.advance(HOLD_SECS * 1000 - 1);
            assert_eq!(store.admit(&s, &keys).unwrap(), Admission::Held);
            store.clock.advance(1);
            assert_eq!(store.reap_expired().unwrap(), 1);
        }

        #[test]
        fn segment_with_wrong_hash_is_rejected() {
            let store = temp_store();
            let key = PublisherKey::generate();
            let keys = Keys(vec![key.clone()]);
            let m = manifest_bundle(&key, &manifest(&key, 1, 0..3));
            assert_eq!(store.admit(&m, &keys).unwrap(), Admission::Stored);
            let mut tampered = payload(1);
            tampered[0] ^= 1;
            let s = segment(&key, 1, tampered);
            assert!(matches!(store.admit(&s, &keys).unwrap(), Admission::Rejected(_)));
            assert!(!is_stored(&store, &s));
            assert_eq!(store.pending.len(), 0);
        }

        #[test]
        fn held_segment_with_wrong_hash_is_dropped_on_promotion() {
            let store = temp_store();
            let key = PublisherKey::generate();
            let keys = Keys(vec![key.clone()]);
            let good = segment(&key, 0, payload(0));
            let bad = segment(&key, 1, b"not segment 1".to_vec());
            assert_eq!(store.admit(&good, &keys).unwrap(), Admission::Held);
            assert_eq!(store.admit(&bad, &keys).unwrap(), Admission::Held);
            let m = manifest_bundle(&key, &manifest(&key, 1, 0..3));
            assert_eq!(store.admit(&m, &keys).unwrap(), Admission::Stored);
            assert!(is_stored(&store, &good));
            assert!(!is_stored(&store, &bad));
            assert_eq!(store.pending.len(), 0);
        }

        #[test]
        fn segment_not_in_manifest_is_rejected() {
            let store = temp_store();
            let key = PublisherKey::generate();
            let keys = Keys(vec![key.clone()]);
            let m = manifest_bundle(&key, &manifest(&key, 1, 0..3));
            assert_eq!(store.admit(&m, &keys).unwrap(), Admission::Stored);
            let s = segment(&key, 7, payload(7));
            assert!(matches!(store.admit(&s, &keys).unwrap(), Admission::Rejected(_)));
            assert!(!is_stored(&store, &s));
        }

        #[test]
        fn unpaired_publisher_is_rejected() {
            let store = temp_store();
            let stranger = PublisherKey::generate();
            let keys = Keys(vec![PublisherKey::generate()]);
            let m = manifest_bundle(&stranger, &manifest(&stranger, 1, 0..3));
            let s = segment(&stranger, 0, payload(0));
            assert!(matches!(store.admit(&s, &keys).unwrap(), Admission::Rejected(_)));
            assert!(matches!(store.admit(&m, &keys).unwrap(), Admission::Rejected(_)));
            assert!(store.all_ids().unwrap().is_empty());
            assert_eq!(store.pending.len(), 0);
        }

        #[test]
        fn manifest_for_another_publisher_or_content_is_rejected() {
            let store = temp_store();
            let (a, b) = (PublisherKey::generate(), PublisherKey::generate());
            let keys = Keys(vec![a.clone(), b.clone()]);
            // Signed and named by A, but carried in a bundle that claims B.
            let m = manifest(&a, 1, 0..3);
            let signed = SignedManifest::sign(&m, &a).unwrap().to_bytes().unwrap();
            let wrong_publisher = Bundle::manifest(&b.key_id(), &m, signed.clone(), "node-a");
            assert!(matches!(store.admit(&wrong_publisher, &keys).unwrap(), Admission::Rejected(_)));
            let mut wrong_content = Bundle::manifest(&a.key_id(), &m, signed, "node-a");
            wrong_content.info.content_id = "other".into();
            assert!(matches!(store.admit(&wrong_content, &keys).unwrap(), Admission::Rejected(_)));
            assert!(store.all_ids().unwrap().is_empty());
        }

        #[test]
        fn hostile_manifest_payloads_are_rejected() {
            let store = temp_store();
            let key = PublisherKey::generate();
            let keys = Keys(vec![key.clone()]);
            let m = manifest(&key, 1, 0..3);
            let mut bad_sig = SignedManifest::sign(&m, &key).unwrap();
            bad_sig.signature[0] ^= 1;
            for junk in [vec![], b"\xff\x00garbage".to_vec(), bad_sig.to_bytes().unwrap()] {
                let b = Bundle::manifest(&key.key_id(), &m, junk, "node-a");
                assert!(matches!(store.admit(&b, &keys).unwrap(), Admission::Rejected(_)));
            }
            assert!(store.all_ids().unwrap().is_empty());
        }

        #[test]
        fn bundle_expired_on_arrival_is_rejected() {
            let store = temp_store();
            let key = PublisherKey::generate();
            let keys = Keys(vec![key.clone()]);
            let mut m = manifest_bundle(&key, &manifest(&key, 1, 0..3));
            m.age_ms = m.lifetime_ms;
            assert!(matches!(store.admit(&m, &keys).unwrap(), Admission::Rejected(_)));
        }

        #[test]
        fn older_manifest_does_not_replace_newer() {
            let store = temp_store();
            let key = PublisherKey::generate();
            let keys = Keys(vec![key.clone()]);
            let newer = manifest_bundle(&key, &manifest(&key, 2, 0..3));
            assert_eq!(store.admit(&newer, &keys).unwrap(), Admission::Stored);
            let older = manifest_bundle(&key, &manifest(&key, 1, 0..5));
            assert_eq!(store.admit(&older, &keys).unwrap(), Admission::Duplicate);
            // Same version with different contents is not newer either.
            let equal = manifest_bundle(&key, &manifest(&key, 2, 0..5));
            assert_eq!(store.admit(&equal, &keys).unwrap(), Admission::Duplicate);
            assert_eq!(stored_version(&store, &key), 2);
            // Segment 4 is listed only by the refused manifests.
            let s = segment(&key, 4, payload(4));
            assert!(matches!(store.admit(&s, &keys).unwrap(), Admission::Rejected(_)));
        }

        #[test]
        fn local_insert_replaces_a_manifest_only_when_newer() {
            let store = temp_store();
            let key = PublisherKey::generate();
            assert!(store.insert(&manifest_bundle(&key, &manifest(&key, 2, 0..3))).unwrap());
            assert!(!store.insert(&manifest_bundle(&key, &manifest(&key, 1, 0..3))).unwrap());
            assert!(!store.insert(&manifest_bundle(&key, &manifest(&key, 2, 0..5))).unwrap());
            assert_eq!(stored_version(&store, &key), 2);
            assert!(store.insert(&manifest_bundle(&key, &manifest(&key, 3, 0..5))).unwrap());
            assert_eq!(stored_version(&store, &key), 3);
            assert_eq!(store.manifest(&key.key_id(), "show").unwrap().unwrap().segments.len(), 5);
            assert!(store.manifest(&key.key_id(), "other").unwrap().is_none());
            let junk = Bundle::manifest(&key.key_id(), &manifest(&key, 4, 0..1), b"junk".to_vec(), "node-a");
            assert!(store.insert(&junk).is_err());
            assert_eq!(stored_version(&store, &key), 3);
        }

        #[test]
        fn newer_manifest_replaces_older() {
            let store = temp_store();
            let key = PublisherKey::generate();
            let keys = Keys(vec![key.clone()]);
            let older = manifest_bundle(&key, &manifest(&key, 1, 0..3));
            assert_eq!(store.admit(&older, &keys).unwrap(), Admission::Stored);
            let newer = manifest_bundle(&key, &manifest(&key, 2, 0..5));
            assert_eq!(store.admit(&newer, &keys).unwrap(), Admission::Stored);
            assert_eq!(stored_version(&store, &key), 2);
            assert_eq!(store.all_ids().unwrap().len(), 1);
            let s = segment(&key, 4, payload(4));
            assert_eq!(store.admit(&s, &keys).unwrap(), Admission::Stored);
        }
    }
}

#[derive(Debug, serde::Serialize)]
pub struct ContentSummary {
    pub publisher: KeyId,
    pub content_id: String,
    pub title: String,
    pub priority: crate::bundle::Priority,
    pub segment_count: u32,
    pub last_sequence: u32,
}
