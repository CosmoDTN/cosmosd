//! Bundles for CosmosD, keyed by publisher.
//!
//! A Bundle carries one HLS segment or one signed manifest, plus the routing
//! fields cosmosd needs. `crate::wire` turns it into RFC 9171 bytes and back.
//! The payload hash is not stored here. It lives in the signed manifest.

use anyhow::{bail, Result};
use serde::{Deserialize, Serialize};

use crate::trust::{
    manifest::{Manifest, SignedManifest},
    KeyId,
};

/// Default HLS segment duration in milliseconds.
pub const DEFAULT_SEGMENT_MS: u32 = 10_000;

/// MIME type of a manifest bundle's payload (a CBOR `SignedManifest`).
pub const MANIFEST_MIME: &str = "application/vnd.cosmos.manifest+cbor";

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

    /// Bundle lifetime in milliseconds, as stored in the primary block.
    pub fn lifetime_ms(&self) -> u64 {
        self.default_ttl_secs() as u64 * 1000
    }
}

// === Content info ===

/// What a bundle carries.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BundleKind {
    Segment,
    Manifest,
}

/// The fields of the private content block (type 192).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ContentInfo {
    pub kind: BundleKind,
    /// Chosen by the publisher. Unique only within that publisher.
    pub content_id: String,
    pub title: String,
    pub sequence: u32,
    pub duration_ms: u32,
    pub mime_type: String,
    pub priority: Priority,
    /// Publisher wall-clock time. Used for the live playlist edge, never for expiry.
    pub created_at_ms: i64,
}

/// Longest content ID, in bytes.
pub const MAX_CONTENT_ID_LEN: usize = 64;

/// A content ID is 1 to 64 characters from `[A-Za-z0-9._-]`. It is one URI
/// segment in bundle destinations, store keys and playback URLs.
pub fn check_content_id(content_id: &str) -> Result<()> {
    if content_id.is_empty() || content_id.len() > MAX_CONTENT_ID_LEN {
        bail!("content id {content_id:?} must be 1 to {MAX_CONTENT_ID_LEN} characters long");
    }
    if !content_id.bytes().all(|c| c.is_ascii_alphanumeric() || matches!(c, b'.' | b'_' | b'-')) {
        bail!("content id {content_id:?} may only use letters, digits, '.', '_' and '-'");
    }
    Ok(())
}

// === Bundle ID ===

/// Store and sync identifier for a bundle.
/// Format: `<publisher>/<content_id>/<sequence:05>` for segments and
/// `<publisher>/<content_id>/manifest` for manifests.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct BundleId(pub String);

impl BundleId {
    pub fn segment(publisher: &KeyId, content_id: &str, sequence: u32) -> Self {
        BundleId(format!("{}/{}/{:05}", publisher, content_id, sequence))
    }

    pub fn manifest(publisher: &KeyId, content_id: &str) -> Self {
        BundleId(format!("{}/{}/manifest", publisher, content_id))
    }

    /// True for a manifest id. Segment ids end in a number, so the suffix is enough.
    pub fn is_manifest_id(id: &str) -> bool {
        id.ends_with("/manifest")
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

/// A single transmissible unit. It holds one HLS segment or one signed manifest.
#[derive(Debug, Clone, PartialEq)]
pub struct Bundle {
    /// Key ID of the publisher that owns the content.
    pub publisher: KeyId,
    pub info: ContentInfo,
    /// Raw bytes of one HLS segment or of a CBOR `SignedManifest`.
    pub payload: Vec<u8>,
    /// Time the bundle has existed, summed over the nodes that held it.
    pub age_ms: u64,
    pub lifetime_ms: u64,
    /// Times the bundle has been sent to a peer. The sender raises it by one.
    pub hop_count: u8,
    /// Node that last sent this bundle to a peer. None until it is first sent.
    pub previous_node: Option<String>,
    /// Node that originally created this bundle.
    pub source_node: String,
}

impl Bundle {
    /// A segment bundle. Errors when `content_id` fails [`check_content_id`].
    pub fn segment(
        publisher: &KeyId,
        content_id: &str,
        title: &str,
        sequence: u32,
        mime: &str,
        priority: Priority,
        payload: Vec<u8>,
        source_node: &str,
    ) -> Result<Bundle> {
        check_content_id(content_id)?;
        let info = ContentInfo {
            kind: BundleKind::Segment,
            content_id: content_id.to_string(),
            title: title.to_string(),
            sequence,
            duration_ms: DEFAULT_SEGMENT_MS,
            mime_type: mime.to_string(),
            priority,
            created_at_ms: chrono::Utc::now().timestamp_millis(),
        };
        Ok(Bundle::fresh(publisher, info, payload, source_node))
    }

    /// A manifest bundle carrying `signed_bytes`, a CBOR `SignedManifest`.
    /// Live manifests get the live lifetime, others the VOD lifetime.
    pub fn manifest(publisher: &KeyId, m: &Manifest, signed_bytes: Vec<u8>, source_node: &str) -> Bundle {
        let info = ContentInfo {
            kind: BundleKind::Manifest,
            content_id: m.content_id.clone(),
            title: m.title.clone(),
            sequence: 0,
            duration_ms: 0,
            mime_type: MANIFEST_MIME.to_string(),
            priority: if m.live { Priority::Live } else { Priority::Vod },
            created_at_ms: m.created_at_ms,
        };
        Bundle::fresh(publisher, info, signed_bytes, source_node)
    }

    fn fresh(publisher: &KeyId, info: ContentInfo, payload: Vec<u8>, source_node: &str) -> Bundle {
        Bundle {
            publisher: publisher.clone(),
            lifetime_ms: info.priority.lifetime_ms(),
            info,
            payload,
            age_ms: 0,
            hop_count: 0,
            previous_node: None,
            source_node: source_node.to_string(),
        }
    }

    /// Set the segment playback duration.
    pub fn with_duration_ms(mut self, ms: u32) -> Self {
        self.info.duration_ms = ms;
        self
    }

    pub fn id(&self) -> BundleId {
        match self.info.kind {
            BundleKind::Segment => BundleId::segment(&self.publisher, &self.info.content_id, self.info.sequence),
            BundleKind::Manifest => BundleId::manifest(&self.publisher, &self.info.content_id),
        }
    }

    /// Routing fields without the payload. `inserted_mono_ms` is the store
    /// clock reading at the moment the bundle is stored.
    pub fn meta(&self, inserted_mono_ms: u64) -> BundleMeta {
        BundleMeta {
            id: self.id().0,
            publisher: self.publisher.clone(),
            content_id: self.info.content_id.clone(),
            title: self.info.title.clone(),
            sequence: self.info.sequence,
            kind: self.info.kind,
            priority: self.info.priority,
            created_at_ms: self.info.created_at_ms,
            duration_ms: self.info.duration_ms,
            lifetime_ms: self.lifetime_ms,
            age_at_insert_ms: self.age_ms,
            inserted_mono_ms,
            version: match self.info.kind {
                BundleKind::Segment => 0,
                BundleKind::Manifest => manifest_version(&self.payload),
            },
        }
    }

    /// True while the age the bundle arrived with is below its lifetime.
    /// This is the age at receipt only. It ignores time held in a store, so
    /// use `BundleMeta::is_valid` for anything already stored.
    pub fn valid_on_arrival(&self) -> bool {
        self.age_ms < self.lifetime_ms
    }
}

/// The version inside a manifest payload, read without checking the
/// signature. 0 when the payload is unreadable. Peers use it only to decide
/// what to pull, and the store verifies whatever arrives.
fn manifest_version(payload: &[u8]) -> u64 {
    SignedManifest::from_bytes(payload)
        .and_then(|s| crate::codec::decode::<Manifest>(&s.manifest_cbor))
        .map_or(0, |m| m.version)
}

// === BundleMeta ===

/// A bundle's routing metadata, without the payload.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct BundleMeta {
    pub id: String,
    pub publisher: KeyId,
    pub content_id: String,
    pub title: String,
    pub sequence: u32,
    pub kind: BundleKind,
    pub priority: Priority,
    pub created_at_ms: i64,
    pub duration_ms: u32,
    pub lifetime_ms: u64,
    /// Age the bundle had when this node stored it.
    pub age_at_insert_ms: u64,
    /// Store clock reading when this node stored it.
    pub inserted_mono_ms: u64,
    /// Signed manifest version for a manifest, 0 for a segment.
    #[serde(default)]
    pub version: u64,
}

impl BundleMeta {
    /// Age now: the age at insert plus the time this node has held it.
    pub fn age_ms(&self, now_mono_ms: u64) -> u64 {
        self.age_at_insert_ms.saturating_add(now_mono_ms.saturating_sub(self.inserted_mono_ms))
    }

    /// True while the bundle's age is below its lifetime.
    pub fn is_valid(&self, now_mono_ms: u64) -> bool {
        self.age_ms(now_mono_ms) < self.lifetime_ms
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn publisher() -> KeyId {
        KeyId("0123456789abcdef".into())
    }

    fn make_bundle(priority: Priority) -> Bundle {
        Bundle::segment(&publisher(), "test_content", "Test Video", 0, "video/mp2t", priority, b"hello".to_vec(), "node-a")
            .unwrap()
    }

    fn manifest(live: bool) -> Manifest {
        Manifest {
            publisher: publisher(),
            content_id: "show".into(),
            title: "Show".into(),
            version: 3,
            created_at_ms: 1_700_000_000_000,
            live,
            segments: vec![],
        }
    }

    #[test]
    fn content_id_allows_only_safe_chars() {
        for ok in ["a", "Show_1.v2-final", &"x".repeat(64)] {
            assert!(check_content_id(ok).is_ok(), "{ok:?}");
        }
        for bad in ["", &"x".repeat(65), "a/b", "a b", "a?b", "a%2Fb", "film\u{e9}", "a\nb"] {
            assert!(check_content_id(bad).is_err(), "{bad:?}");
        }
        let make = |id: &str| Bundle::segment(&publisher(), id, "t", 0, "video/mp2t", Priority::Vod, vec![], "n");
        assert!(make("good_id").is_ok());
        assert!(make("bad id").is_err());
    }

    #[test]
    fn bundle_id_format() {
        assert_eq!(BundleId::segment(&publisher(), "my_show", 42).as_str(), "0123456789abcdef/my_show/00042");
        assert_eq!(BundleId::manifest(&publisher(), "my_show").as_str(), "0123456789abcdef/my_show/manifest");
    }

    #[test]
    fn bundle_is_valid_for_fresh_bundle() {
        let b = make_bundle(Priority::Vod);
        assert_eq!(b.age_ms, 0);
        assert!(b.valid_on_arrival());
    }

    #[test]
    fn bundle_is_invalid_when_age_reaches_lifetime() {
        let mut b = make_bundle(Priority::Live);
        b.age_ms = b.lifetime_ms - 1;
        assert!(b.valid_on_arrival());
        b.age_ms = b.lifetime_ms;
        assert!(!b.valid_on_arrival());
    }

    #[test]
    fn lifetime_comes_from_priority() {
        assert_eq!(make_bundle(Priority::Live).lifetime_ms, 1_800_000);
        assert!(Priority::Live.default_ttl_secs() < Priority::Vod.default_ttl_secs());
    }

    #[test]
    fn meta_carries_routing_fields() {
        let b = make_bundle(Priority::Live).with_duration_ms(9_500);
        let m = b.meta(0);
        assert_eq!(m.id, "0123456789abcdef/test_content/00000");
        assert_eq!(m.publisher, publisher());
        assert_eq!(m.content_id, "test_content");
        assert_eq!(m.kind, BundleKind::Segment);
        assert_eq!(m.duration_ms, 9_500);
        assert_eq!(m.priority, Priority::Live);
    }

    #[test]
    fn meta_carries_manifest_version() {
        let key = crate::trust::PublisherKey::generate();
        let mut m = manifest(true);
        m.publisher = key.key_id();
        let signed = SignedManifest::sign(&m, &key).unwrap().to_bytes().unwrap();
        assert_eq!(Bundle::manifest(&key.key_id(), &m, signed, "node-a").meta(0).version, 3);
        assert_eq!(Bundle::manifest(&key.key_id(), &m, b"junk".to_vec(), "node-a").meta(0).version, 0);
        assert_eq!(make_bundle(Priority::Live).meta(0).version, 0);
        assert!(BundleId::is_manifest_id("0123456789abcdef/show/manifest"));
        assert!(!BundleId::is_manifest_id("0123456789abcdef/show/00001"));
    }

    #[test]
    fn manifest_bundle_carries_signed_bytes() {
        let b = Bundle::manifest(&publisher(), &manifest(true), b"signed".to_vec(), "node-a");
        assert_eq!(b.id().as_str(), "0123456789abcdef/show/manifest");
        assert_eq!(b.info.kind, BundleKind::Manifest);
        assert_eq!(b.info.priority, Priority::Live);
        assert_eq!(b.info.created_at_ms, 1_700_000_000_000);
        assert_eq!(b.payload, b"signed");
        let vod = Bundle::manifest(&publisher(), &manifest(false), vec![], "node-a");
        assert_eq!(vod.lifetime_ms, Priority::Vod.lifetime_ms());
    }
}
