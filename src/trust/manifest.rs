//! Signed content manifests.
//!
//! A manifest lists the segments of one piece of content with their hashes.
//! The publisher signs the CBOR bytes of the manifest. The signature covers a
//! fixed domain prefix followed by those bytes, so it can not be replayed as
//! any other kind of signed message.

use anyhow::{anyhow, bail, Result};
use serde::{Deserialize, Serialize};

use super::{verify, KeyId, KeyLookup, PublisherKey};
use crate::codec;

/// Domain separation prefix for manifest signatures.
const DOMAIN: &[u8] = b"cosmos-manifest-v1";

// === Fixed byte arrays as CBOR byte strings ===

/// Serde helpers so `[u8; N]` encodes as one byte string, not an array of numbers.
mod byte_array {
    use serde::{de::Error, Deserialize, Deserializer, Serializer};
    use serde_bytes::ByteBuf;

    pub fn serialize<S: Serializer, const N: usize>(v: &[u8; N], s: S) -> Result<S::Ok, S::Error> {
        s.serialize_bytes(v)
    }

    pub fn deserialize<'de, D: Deserializer<'de>, const N: usize>(d: D) -> Result<[u8; N], D::Error> {
        let buf = ByteBuf::deserialize(d)?;
        <[u8; N]>::try_from(buf.as_slice())
            .map_err(|_| D::Error::custom(format!("expected {N} bytes, got {}", buf.len())))
    }
}

// === Manifest ===

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SegmentEntry {
    pub sequence: u32,
    #[serde(with = "byte_array")]
    pub hash: [u8; 32],
    pub duration_ms: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Manifest {
    pub publisher: KeyId,
    pub content_id: String,
    pub title: String,
    /// Chosen by the caller. A higher version replaces a lower one in the store.
    pub version: u64,
    pub created_at_ms: i64,
    pub live: bool,
    pub segments: Vec<SegmentEntry>,
}

impl Manifest {
    pub fn hash_for(&self, sequence: u32) -> Option<&SegmentEntry> {
        self.segments.iter().find(|s| s.sequence == sequence)
    }
}

// === Signed manifest ===

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SignedManifest {
    #[serde(with = "serde_bytes")]
    pub manifest_cbor: Vec<u8>,
    #[serde(with = "byte_array")]
    pub signature: [u8; 64],
}

fn signed_message(manifest_cbor: &[u8]) -> Vec<u8> {
    let mut msg = Vec::with_capacity(DOMAIN.len() + manifest_cbor.len());
    msg.extend_from_slice(DOMAIN);
    msg.extend_from_slice(manifest_cbor);
    msg
}

impl SignedManifest {
    pub fn sign(m: &Manifest, key: &PublisherKey) -> Result<Self> {
        let manifest_cbor = codec::encode(m)?;
        let signature = key.sign(&signed_message(&manifest_cbor));
        Ok(SignedManifest { manifest_cbor, signature })
    }

    /// Check the signature against the publisher's key and return the manifest.
    /// The manifest is decoded first only to read the publisher ID.
    pub fn verify(&self, keys: &dyn KeyLookup) -> Result<Manifest> {
        let m: Manifest = codec::decode(&self.manifest_cbor)?;
        let public = keys
            .lookup(&m.publisher)
            .ok_or_else(|| anyhow!("unknown publisher {}", m.publisher))?;
        if !verify(&public, &signed_message(&self.manifest_cbor), &self.signature) {
            bail!("bad manifest signature");
        }
        Ok(m)
    }

    pub fn to_bytes(&self) -> Result<Vec<u8>> {
        codec::encode(self)
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self> {
        codec::decode(bytes)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::trust::PairedKeys;

    fn manifest_for(key: &PublisherKey, segments: u32) -> Manifest {
        Manifest {
            publisher: key.key_id(),
            content_id: "match".into(),
            title: "Match".into(),
            version: 1,
            created_at_ms: 1_700_000_000_000,
            live: false,
            segments: (0..segments)
                .map(|i| SegmentEntry {
                    sequence: i,
                    hash: *blake3::hash(&i.to_le_bytes()).as_bytes(),
                    duration_ms: 10_000,
                })
                .collect(),
        }
    }

    /// A lookup that trusts exactly the given key, via the real paired store.
    fn paired(key: &PublisherKey) -> (tempfile::TempDir, PairedKeys) {
        let dir = tempfile::tempdir().unwrap();
        let mut p = PairedKeys::open(dir.path()).unwrap();
        p.pair(key.public()).unwrap();
        (dir, p)
    }

    #[test]
    fn signed_manifest_verifies_with_paired_key() {
        let k = PublisherKey::generate();
        let (_d, keys) = paired(&k);
        let m = manifest_for(&k, 3);
        let signed = SignedManifest::sign(&m, &k).unwrap();
        assert_eq!(signed.verify(&keys).unwrap(), m);
    }

    #[test]
    fn verify_fails_for_unpaired_publisher() {
        let k = PublisherKey::generate();
        let other = PublisherKey::generate();
        let (_d, keys) = paired(&other);
        let signed = SignedManifest::sign(&manifest_for(&k, 3), &k).unwrap();
        assert!(signed.verify(&keys).is_err());
    }

    #[test]
    fn verify_fails_when_manifest_bytes_change() {
        let k = PublisherKey::generate();
        let (_d, keys) = paired(&k);
        let mut signed = SignedManifest::sign(&manifest_for(&k, 3), &k).unwrap();
        let last = signed.manifest_cbor.len() - 1;
        signed.manifest_cbor[last] ^= 1;
        assert!(signed.verify(&keys).is_err());
    }

    #[test]
    fn verify_fails_with_wrong_domain_prefix() {
        let k = PublisherKey::generate();
        let (_d, keys) = paired(&k);
        let m = manifest_for(&k, 3);
        let manifest_cbor = codec::encode(&m).unwrap();
        let mut msg = b"cosmos-other-v1".to_vec();
        msg.extend_from_slice(&manifest_cbor);
        let forged = SignedManifest { signature: k.sign(&msg), manifest_cbor };
        assert!(forged.verify(&keys).is_err());
    }

    #[test]
    fn manifest_roundtrips_through_bytes() {
        let k = PublisherKey::generate();
        let signed = SignedManifest::sign(&manifest_for(&k, 5), &k).unwrap();
        let back = SignedManifest::from_bytes(&signed.to_bytes().unwrap()).unwrap();
        assert_eq!(back, signed);
    }

    #[test]
    fn hash_for_finds_entry_by_sequence() {
        let k = PublisherKey::generate();
        let m = manifest_for(&k, 4);
        assert_eq!(m.hash_for(2).unwrap().sequence, 2);
        assert_eq!(m.hash_for(2).unwrap().hash, m.segments[2].hash);
        assert!(m.hash_for(9).is_none());
    }

    #[test]
    fn hashes_encode_as_byte_strings() {
        let k = PublisherKey::generate();
        let n = codec::encode(&manifest_for(&k, 1000)).unwrap().len();
        // A byte-string hash costs 34 bytes. Field names and small numbers bring an
        // entry to about 70 bytes. A number array would add about 55 more per hash.
        assert!(n < 1000 * 85, "encoded {n} bytes");
    }

    #[test]
    fn from_bytes_rejects_garbage_without_panic() {
        let k = PublisherKey::generate();
        let (_d, keys) = paired(&k);
        let good = SignedManifest::sign(&manifest_for(&k, 3), &k).unwrap().to_bytes().unwrap();
        assert!(SignedManifest::from_bytes(&[]).is_err());
        assert!(SignedManifest::from_bytes(&good[..good.len() / 2]).is_err());
        let noise: Vec<u8> = (0..200u32).map(|i| (i.wrapping_mul(2654435761) >> 13) as u8).collect();
        assert!(SignedManifest::from_bytes(&noise).is_err());
        // Valid envelope around junk manifest bytes must fail verify, not panic.
        let junk = SignedManifest { manifest_cbor: noise, signature: [0u8; 64] };
        assert!(junk.verify(&keys).is_err());
    }
}
