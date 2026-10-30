//! BPv7-inspired bundle format for CosmosD.
//!
//! A Bundle wraps one HLS segment (or a playlist) with routing metadata.
//! Full BPv7 (RFC 9171) compatibility is a future goal; this is a working subset.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Default HLS segment duration in milliseconds.
pub const DEFAULT_SEGMENT_MS: u32 = 10_000;

// === Priority ===

/// Delivery priority — controls queue ordering and TTL defaults.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Priority {
    /// Live broadcast — route aggressively, short TTL.
    Live,
    /// Near-live — 5–30 min behind broadcast.
    NearLive,
    /// Video on demand — carry indefinitely until delivered.
    Vod,
}

impl Priority {
    pub fn default_ttl_secs(&self) -> i64 {
        match self {
            Priority::Live => 1800,        // 30 minutes
            Priority::NearLive => 21600,   // 6 hours
            Priority::Vod => 2_592_000,    // 30 days
        }
    }
}

// === Bundle ID ===

/// Globally unique identifier for a bundle (content-addressed).
/// Format: `<content_id>/<sequence>` e.g. `ipl_2026_03_07/00042`
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct BundleId(pub String);

impl BundleId {
    pub fn new(content_id: &str, sequence: u32) -> Self {
        BundleId(format!("{}/{:05}", content_id, sequence))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for BundleId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

// === Bundle ===

/// A single transmissible unit — one HLS segment or playlist file.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Bundle {
    /// Unique ID for this bundle.
    pub id: BundleId,

    /// Human-readable content title (e.g. "IPL 2026 — MI vs CSK").
    pub content_title: String,

    /// Sequence number within the content stream (segment index).
    pub sequence: u32,

    /// MIME type: `video/mp2t` for .ts segments, `application/x-mpegURL` for playlists.
    pub mime_type: String,

    /// Delivery priority.
    pub priority: Priority,

    /// Wall-clock time when this bundle expires and should be discarded.
    pub expires_at: DateTime<Utc>,

    /// blake3 hash of `payload` for integrity verification.
    pub payload_hash: String,

    /// Wall-clock time when this bundle was created.
    pub created_at: DateTime<Utc>,

    /// Playback duration of the segment in milliseconds.
    pub duration_ms: u32,

    /// Raw bytes — one HLS .ts segment or .m3u8 playlist.
    #[serde(with = "serde_bytes")]
    pub payload: Vec<u8>,

    /// Node that originally created this bundle.
    pub source_node: String,

    /// Transmission hop count (incremented at each relay).
    pub hop_count: u8,

    /// Internal UUID for deduplication across nodes.
    pub bundle_uuid: Uuid,
}

impl Bundle {
    pub fn new(
        content_id: &str,
        content_title: &str,
        sequence: u32,
        mime_type: &str,
        priority: Priority,
        payload: Vec<u8>,
        source_node: &str,
    ) -> Self {
        let hash = blake3::hash(&payload).to_hex().to_string();
        let created_at = Utc::now();
        let expires_at = created_at + chrono::Duration::seconds(priority.default_ttl_secs());

        Bundle {
            id: BundleId::new(content_id, sequence),
            content_title: content_title.to_string(),
            sequence,
            mime_type: mime_type.to_string(),
            priority,
            expires_at,
            created_at,
            duration_ms: DEFAULT_SEGMENT_MS,
            payload_hash: hash,
            payload,
            source_node: source_node.to_string(),
            hop_count: 0,
            bundle_uuid: Uuid::new_v4(),
        }
    }

    /// Set the segment playback duration.
    #[allow(dead_code)] // used by live ingest in a later task
    pub fn with_duration_ms(mut self, ms: u32) -> Self {
        self.duration_ms = ms;
        self
    }

    /// The content stream this bundle belongs to (the id before the last `/`).
    pub fn content_id(&self) -> &str {
        self.id.as_str().rsplit_once('/').map_or(self.id.as_str(), |(c, _)| c)
    }

    /// Routing fields without the payload.
    pub fn meta(&self) -> BundleMeta {
        BundleMeta {
            id: self.id.0.clone(),
            content_id: self.content_id().to_string(),
            content_title: self.content_title.clone(),
            sequence: self.sequence,
            priority: self.priority,
            created_at: self.created_at,
            expires_at: self.expires_at,
            duration_ms: self.duration_ms,
        }
    }

    /// Returns true if this bundle has not yet expired.
    pub fn is_valid(&self) -> bool {
        Utc::now() < self.expires_at
    }

    /// Returns true if payload integrity check passes.
    pub fn verify(&self) -> bool {
        let actual = blake3::hash(&self.payload).to_hex().to_string();
        actual == self.payload_hash
    }
}

// === BundleMeta ===

/// A bundle's routing metadata, without the payload.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct BundleMeta {
    pub id: String,
    pub content_id: String,
    pub content_title: String,
    pub sequence: u32,
    pub priority: Priority,
    pub created_at: DateTime<Utc>,
    pub expires_at: DateTime<Utc>,
    pub duration_ms: u32,
}

impl BundleMeta {
    /// Returns true if the bundle has not yet expired.
    pub fn is_valid(&self) -> bool {
        Utc::now() < self.expires_at
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_bundle(priority: Priority) -> Bundle {
        Bundle::new("test_content", "Test Video", 0, "video/mp2t", priority, b"hello".to_vec(), "node-a")
    }

    #[test]
    fn bundle_id_format() {
        let id = BundleId::new("my_show", 42);
        assert_eq!(id.as_str(), "my_show/00042");
    }

    #[test]
    fn bundle_verify_passes_on_correct_payload() {
        let b = make_bundle(Priority::Vod);
        assert!(b.verify());
    }

    #[test]
    fn bundle_verify_fails_on_tampered_payload() {
        let mut b = make_bundle(Priority::Vod);
        b.payload = b"tampered".to_vec();
        assert!(!b.verify());
    }

    #[test]
    fn bundle_is_valid_for_fresh_bundle() {
        let b = make_bundle(Priority::Vod);
        assert!(b.is_valid());
    }

    #[test]
    fn bundle_is_invalid_when_expired() {
        let mut b = make_bundle(Priority::Vod);
        b.expires_at = Utc::now() - chrono::Duration::seconds(1);
        assert!(!b.is_valid());
    }

    #[test]
    fn live_priority_has_shorter_ttl_than_vod() {
        assert!(Priority::Live.default_ttl_secs() < Priority::Vod.default_ttl_secs());
    }

    #[test]
    fn meta_carries_routing_fields() {
        let b = make_bundle(Priority::Live).with_duration_ms(9_500);
        let m = b.meta();
        assert_eq!(m.id, "test_content/00000");
        assert_eq!(m.content_id, "test_content");
        assert_eq!(m.duration_ms, 9_500);
        assert_eq!(m.priority, Priority::Live);
    }

    #[test]
    fn bundle_roundtrips_through_json() {
        let b = make_bundle(Priority::NearLive);
        let json = serde_json::to_vec(&b).unwrap();
        let b2: Bundle = serde_json::from_slice(&json).unwrap();
        assert_eq!(b.id, b2.id);
        assert_eq!(b.payload_hash, b2.payload_hash);
        assert_eq!(b.priority, b2.priority);
    }
}
