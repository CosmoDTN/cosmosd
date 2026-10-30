//! Content-addressed bundle store backed by sled (embedded key-value DB).
//!
//! Bundles are keyed by their BundleId string and stored as CBOR. A second
//! tree, `meta`, holds each bundle's payload-free metadata so listing, the
//! catalog and manifests never decode a payload. Expired bundles are purged
//! lazily on read and periodically by the background reaper task.

use std::collections::{HashMap, HashSet};

use anyhow::{Context, Result};
use chrono::Utc;
use tracing::{debug, info};

use crate::bundle::{Bundle, BundleMeta};
use crate::codec::{decode, encode};

pub struct BundleStore {
    /// id -> CBOR `Bundle` (payload included).
    db: sled::Db,
    /// id -> CBOR `BundleMeta`.
    meta: sled::Tree,
}

impl BundleStore {
    pub fn open(path: &str) -> Result<Self> {
        let db = sled::open(path).context("Failed to open bundle store")?;
        let meta = db.open_tree("meta").context("Failed to open bundle meta tree")?;
        info!("Bundle store opened at {}", path);
        Ok(BundleStore { db, meta })
    }

    /// Insert a bundle. Returns false if already present (dedup by BundleId).
    pub fn insert(&self, bundle: &Bundle) -> Result<bool> {
        let key = bundle.id.as_str().as_bytes();
        if self.db.contains_key(key)? {
            debug!("Duplicate bundle ignored: {}", bundle.id);
            return Ok(false);
        }
        self.db.insert(key, encode(bundle)?)?;
        self.meta.insert(key, encode(&bundle.meta())?)?;
        debug!("Stored bundle: {}", bundle.id);
        Ok(true)
    }

    /// Retrieve a bundle by ID. Returns None if missing or expired.
    pub fn get(&self, id: &str) -> Result<Option<Bundle>> {
        let Some(raw) = self.db.get(id.as_bytes())? else {
            return Ok(None);
        };
        let bundle: Bundle = decode(&raw)?;
        if !bundle.is_valid() {
            self.db.remove(id.as_bytes())?;
            self.meta.remove(id.as_bytes())?;
            return Ok(None);
        }
        Ok(Some(bundle))
    }

    /// Metadata of every valid (non-expired) bundle.
    pub fn all_meta(&self) -> Result<Vec<BundleMeta>> {
        let mut out = Vec::new();
        for item in self.meta.iter() {
            let (_, value) = item?;
            let m: BundleMeta = decode(&value)?;
            if m.is_valid() {
                out.push(m);
            }
        }
        Ok(out)
    }

    /// Metadata of the valid bundles of one content ID, ascending by sequence.
    pub fn list_meta(&self, content_id: &str) -> Result<Vec<BundleMeta>> {
        let prefix = format!("{}/", content_id);
        let mut out = Vec::new();
        for item in self.meta.scan_prefix(prefix.as_bytes()) {
            let (_, value) = item?;
            let m: BundleMeta = decode(&value)?;
            if m.is_valid() {
                out.push(m);
            }
        }
        out.sort_by_key(|m| m.sequence);
        Ok(out)
    }

    /// List all unique content IDs in the store with their segment counts.
    pub fn catalog(&self) -> Result<Vec<ContentSummary>> {
        let mut map: HashMap<String, ContentSummary> = HashMap::new();

        for m in self.all_meta()? {
            let entry = map.entry(m.content_id.clone()).or_insert_with(|| ContentSummary {
                content_id: m.content_id.clone(),
                title: m.content_title.clone(),
                priority: m.priority,
                segment_count: 0,
                last_sequence: m.sequence,
                expires_at: m.expires_at,
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
        let mut removed = 0usize;

        for item in self.meta.iter() {
            let (key, value) = item?;
            let m: BundleMeta = decode(&value)?;
            if !m.is_valid() {
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

    fn make_bundle(content_id: &str, sequence: u32) -> Bundle {
        Bundle::new(content_id, "Test Video", sequence, "video/mp2t", Priority::Vod, b"data".to_vec(), "node-a")
    }

    /// Write a bundle to both trees without the validity checks `insert` skips.
    fn insert_raw(store: &BundleStore, b: &Bundle) {
        store.db.insert(b.id.as_str().as_bytes(), encode(b).unwrap()).unwrap();
        store.meta.insert(b.id.as_str().as_bytes(), encode(&b.meta()).unwrap()).unwrap();
    }

    fn temp_store() -> BundleStore {
        let dir = tempfile::tempdir().unwrap();
        BundleStore::open(dir.path().to_str().unwrap()).unwrap()
    }

    #[test]
    fn insert_and_get() {
        let store = temp_store();
        let b = make_bundle("show_a", 0);
        assert!(store.insert(&b).unwrap());
        let got = store.get(b.id.as_str()).unwrap().unwrap();
        assert_eq!(got.id, b.id);
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
        assert!(store.get("missing/00000").unwrap().is_none());
    }

    #[test]
    fn get_returns_none_for_expired() {
        let store = temp_store();
        let mut b = make_bundle("show_a", 0);
        b.expires_at = Utc::now() - chrono::Duration::seconds(1);
        insert_raw(&store, &b);
        assert!(store.get(b.id.as_str()).unwrap().is_none());
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
        assert!(ids.contains("show_a/00000"));
        assert!(ids.contains("show_a/00001"));
    }

    #[test]
    fn reap_expired_removes_stale_bundles() {
        let store = temp_store();
        let mut b = make_bundle("show_a", 0);
        b.expires_at = Utc::now() - chrono::Duration::seconds(1);
        insert_raw(&store, &b);
        assert_eq!(store.reap_expired().unwrap(), 1);
        assert!(store.get(b.id.as_str()).unwrap().is_none());
    }

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
}

#[derive(Debug, serde::Serialize)]
pub struct ContentSummary {
    pub content_id: String,
    pub title: String,
    pub priority: crate::bundle::Priority,
    pub segment_count: u32,
    pub last_sequence: u32,
    pub expires_at: chrono::DateTime<Utc>,
}
