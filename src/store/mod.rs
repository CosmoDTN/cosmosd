//! Content-addressed bundle store backed by sled (embedded key-value DB).
//!
//! Bundles are keyed by their BundleId string and stored as RFC 9171 bytes.
//! A second tree, `meta`, holds each bundle's payload-free metadata so
//! listing, the catalog and manifests never decode a payload. Expired bundles
//! are purged lazily on read and periodically by the background reaper task.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use anyhow::{bail, Context, Result};
use sled::Transactional;
use tracing::{debug, info};

use crate::bundle::{Bundle, BundleKind, BundleMeta};
use crate::codec::{decode, encode};
use crate::wire::{decode_bundle, encode_bundle};

mod age;
pub use age::{MonoClock, SystemMono};
#[cfg(test)]
pub use age::FakeMono;

pub struct BundleStore {
    /// id -> RFC 9171 bundle bytes (payload included).
    db: sled::Db,
    /// id -> CBOR `BundleMeta`.
    meta: sled::Tree,
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
        Ok(BundleStore { db, meta, clock })
    }

    /// Persist the expiry clock. Call on a schedule and at shutdown.
    pub fn flush_clock(&self) -> Result<()> {
        self.clock.flush()
    }

    /// Insert a bundle. Returns false if a valid copy is already present
    /// (dedup by BundleId); an expired copy is overwritten.
    pub fn insert(&self, bundle: &Bundle) -> Result<bool> {
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

    /// Metadata of the valid segments of one content ID, ascending by
    /// sequence. Matches every publisher until the API takes a publisher.
    pub fn list_meta(&self, content_id: &str) -> Result<Vec<BundleMeta>> {
        let mut out: Vec<BundleMeta> = self
            .all_meta()?
            .into_iter()
            .filter(|m| m.kind == BundleKind::Segment && m.content_id == content_id)
            .collect();
        out.sort_by_key(|m| m.sequence);
        Ok(out)
    }

    /// List all unique content IDs in the store with their segment counts.
    pub fn catalog(&self) -> Result<Vec<ContentSummary>> {
        let mut map: HashMap<String, ContentSummary> = HashMap::new();

        for m in self.all_meta()?.into_iter().filter(|m| m.kind == BundleKind::Segment) {
            let entry = map.entry(m.content_id.clone()).or_insert_with(|| ContentSummary {
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
        items.sort_by(|a, b| a.content_id.cmp(&b.content_id));
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
        Ok(removed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bundle::Priority;
    use crate::trust::KeyId;

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
        let bundles = store.list_meta("show_a").unwrap();
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
        assert_eq!(store.list_meta("show_a").unwrap().len(), 1);
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
}

#[derive(Debug, serde::Serialize)]
pub struct ContentSummary {
    pub content_id: String,
    pub title: String,
    pub priority: crate::bundle::Priority,
    pub segment_count: u32,
    pub last_sequence: u32,
}
