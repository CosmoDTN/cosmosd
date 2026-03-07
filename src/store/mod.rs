//! Content-addressed bundle store backed by sled (embedded key-value DB).
//!
//! Bundles are keyed by their BundleId string. Expired bundles are purged
//! lazily on read and periodically by the background reaper task.

use anyhow::{Context, Result};
use chrono::Utc;
use tracing::{debug, info};

use crate::bundle::Bundle;

pub struct BundleStore {
    db: sled::Db,
}

impl BundleStore {
    pub fn open(path: &str) -> Result<Self> {
        let db = sled::open(path).context("Failed to open bundle store")?;
        info!("Bundle store opened at {}", path);
        Ok(BundleStore { db })
    }

    /// Insert a bundle. Returns false if already present (dedup by BundleId).
    pub fn insert(&self, bundle: &Bundle) -> Result<bool> {
        let key = bundle.id.as_str().as_bytes();
        if self.db.contains_key(key)? {
            debug!("Duplicate bundle ignored: {}", bundle.id);
            return Ok(false);
        }
        let value = serde_json::to_vec(bundle)?;
        self.db.insert(key, value)?;
        debug!("Stored bundle: {}", bundle.id);
        Ok(true)
    }

    /// Retrieve a bundle by ID. Returns None if missing or expired.
    pub fn get(&self, id: &str) -> Result<Option<Bundle>> {
        let Some(raw) = self.db.get(id.as_bytes())? else {
            return Ok(None);
        };
        let bundle: Bundle = serde_json::from_slice(&raw)?;
        if !bundle.is_valid() {
            self.db.remove(id.as_bytes())?;
            return Ok(None);
        }
        Ok(Some(bundle))
    }

    /// List all valid (non-expired) bundles for a content ID, sorted by sequence.
    pub fn list_content(&self, content_id: &str) -> Result<Vec<Bundle>> {
        let prefix = format!("{}/", content_id);
        let mut bundles = Vec::new();

        for item in self.db.iter() {
            let (key, value) = item?;
            let key_str = String::from_utf8_lossy(&key);
            if !key_str.starts_with(&prefix) {
                continue;
            }
            let bundle: Bundle = serde_json::from_slice(&value)?;
            if bundle.is_valid() {
                bundles.push(bundle);
            } else {
                self.db.remove(key)?;
            }
        }

        bundles.sort_by_key(|b| b.sequence);
        Ok(bundles)
    }

    /// List all unique content IDs in the store with their segment counts.
    pub fn catalog(&self) -> Result<Vec<ContentSummary>> {
        let mut map: std::collections::HashMap<String, ContentSummary> =
            std::collections::HashMap::new();
        let now = Utc::now();

        for item in self.db.iter() {
            let (_, value) = item?;
            let bundle: Bundle = serde_json::from_slice(&value)?;
            if bundle.expires_at < now {
                continue;
            }
            let content_id = bundle
                .id
                .as_str()
                .split('/')
                .next()
                .unwrap_or("")
                .to_string();
            let entry = map.entry(content_id.clone()).or_insert_with(|| ContentSummary {
                content_id: content_id.clone(),
                title: bundle.content_title.clone(),
                priority: bundle.priority,
                segment_count: 0,
                expires_at: bundle.expires_at,
            });
            entry.segment_count += 1;
        }

        let mut items: Vec<_> = map.into_values().collect();
        items.sort_by(|a, b| a.content_id.cmp(&b.content_id));
        Ok(items)
    }

    /// Return all valid bundle IDs in the store (for manifest exchange).
    pub fn all_ids(&self) -> Result<std::collections::HashSet<String>> {
        let mut ids = std::collections::HashSet::new();
        for item in self.db.iter() {
            let (key, value) = item?;
            let bundle: Bundle = serde_json::from_slice(&value)?;
            if bundle.is_valid() {
                ids.insert(String::from_utf8_lossy(&key).to_string());
            }
        }
        Ok(ids)
    }

    /// Remove expired bundles. Call periodically from background task.
    pub fn reap_expired(&self) -> Result<usize> {
        let now = Utc::now();
        let mut removed = 0usize;

        for item in self.db.iter() {
            let (key, value) = item?;
            let bundle: Bundle = serde_json::from_slice(&value)?;
            if bundle.expires_at < now {
                self.db.remove(key)?;
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
        // Force insert expired bundle directly
        let value = serde_json::to_vec(&b).unwrap();
        store.db.insert(b.id.as_str().as_bytes(), value).unwrap();
        assert!(store.get(b.id.as_str()).unwrap().is_none());
    }

    #[test]
    fn list_content_returns_sorted_segments() {
        let store = temp_store();
        store.insert(&make_bundle("show_a", 2)).unwrap();
        store.insert(&make_bundle("show_a", 0)).unwrap();
        store.insert(&make_bundle("show_a", 1)).unwrap();
        let bundles = store.list_content("show_a").unwrap();
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
        let value = serde_json::to_vec(&b).unwrap();
        store.db.insert(b.id.as_str().as_bytes(), value).unwrap();
        assert_eq!(store.reap_expired().unwrap(), 1);
        assert!(store.get(b.id.as_str()).unwrap().is_none());
    }
}

#[derive(Debug, serde::Serialize)]
pub struct ContentSummary {
    pub content_id: String,
    pub title: String,
    pub priority: crate::bundle::Priority,
    pub segment_count: u32,
    pub expires_at: chrono::DateTime<Utc>,
}
