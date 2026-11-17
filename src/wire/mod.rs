//! RFC 9171 wire format for cosmosd bundles.
//!
//! A bundle on the wire is a standard BPv7 bundle. The destination names the
//! content as `dtn://<publisher-key-id>/<content-id>`. The creation time is
//! DTN time zero and the age block carries elapsed time. Our content info
//! rides in private-use block 192 as a CBOR map. Every block control flag is
//! 0, so other BPv7 software carries the bundle without knowing block 192.
//! See `docs/spec/spec-001-bundle-format.md`.

use std::time::Duration;

use anyhow::{anyhow, bail, Context, Result};
use bp7::bundle::BundleBuilder;
use bp7::canonical::{
    new_bundle_age_block, new_payload_block, new_previous_node_block, CanonicalBlockBuilder, CanonicalData,
    BUNDLE_AGE_BLOCK, HOP_COUNT_BLOCK,
};
use bp7::crc::CrcValue;
use bp7::dtntime::CreationTimestamp;
use bp7::eid::EndpointID;
use bp7::flags::{BlockControlFlags, BundleControlFlags};
use bp7::primary::PrimaryBlockBuilder;

use crate::bundle::{check_content_id, Bundle, ContentInfo};
use crate::codec;
use crate::trust::KeyId;

/// Private-use block type for our content info.
pub const CONTENT_BLOCK_TYPE: u64 = 192;

/// Every block control flag is 0. See the module docs.
const NO_FLAGS: BlockControlFlags = BlockControlFlags::empty();

/// Hop limit written in the hop count block. cosmosd does not enforce it yet.
const HOP_LIMIT: u8 = 255;

// Block numbers. The payload block is always 1.
const CONTENT_BLOCK_NUMBER: u64 = 2;
const AGE_BLOCK_NUMBER: u64 = 3;
const HOP_BLOCK_NUMBER: u64 = 4;
const PREVIOUS_BLOCK_NUMBER: u64 = 5;

// === Encode ===

/// `dtn://<publisher-key-id>/<content-id>`.
pub fn destination_uri(publisher: &KeyId, content_id: &str) -> String {
    format!("dtn://{publisher}/{content_id}")
}

pub fn encode_bundle(b: &Bundle) -> Result<Vec<u8>> {
    check_content_id(&b.info.content_id)?;
    check_node_name("publisher", &b.publisher.0)?;
    check_node_name("source node", &b.source_node)?;
    let destination = EndpointID::try_from(destination_uri(&b.publisher, &b.info.content_id).as_str())
        .map_err(|e| anyhow!("bad destination: {e}"))?;
    let source = node_eid(&b.source_node)?;

    let primary = PrimaryBlockBuilder::default()
        .bundle_control_flags(BundleControlFlags::BUNDLE_MUST_NOT_FRAGMENTED.bits())
        .crc(CrcValue::Crc32Empty)
        .destination(destination)
        .source(source)
        .creation_timestamp(CreationTimestamp::with_time_and_seq(0, creation_seqno(b)))
        .lifetime(Duration::from_millis(b.lifetime_ms))
        .build()
        .map_err(|e| anyhow!("primary block: {e}"))?;

    let content = CanonicalBlockBuilder::default()
        .block_type(CONTENT_BLOCK_TYPE)
        .block_number(CONTENT_BLOCK_NUMBER)
        .block_control_flags(NO_FLAGS.bits())
        .data(CanonicalData::Unknown(codec::encode(&b.info)?))
        .build()
        .map_err(|e| anyhow!("content block: {e}"))?;
    let hops = CanonicalBlockBuilder::default()
        .block_type(HOP_COUNT_BLOCK)
        .block_number(HOP_BLOCK_NUMBER)
        .block_control_flags(NO_FLAGS.bits())
        .data(CanonicalData::HopCount(HOP_LIMIT, b.hop_count))
        .build()
        .map_err(|e| anyhow!("hop count block: {e}"))?;
    let mut canonicals = vec![content, new_bundle_age_block(AGE_BLOCK_NUMBER, NO_FLAGS, b.age_ms), hops];
    if let Some(prev) = &b.previous_node {
        check_node_name("previous node", prev)?;
        canonicals.push(new_previous_node_block(PREVIOUS_BLOCK_NUMBER, NO_FLAGS, node_eid(prev)?));
    }
    canonicals.push(new_payload_block(NO_FLAGS, b.payload.clone()));

    let mut bundle = BundleBuilder::default()
        .primary(primary)
        .canonicals(canonicals)
        .build()
        .map_err(|e| anyhow!("bundle: {e}"))?;
    Ok(bundle.to_cbor())
}

/// With creation time zero, the sequence number alone keeps the RFC 9171
/// bundle ID unique. It is derived from our bundle ID and payload, so the
/// same bundle always encodes the same way.
fn creation_seqno(b: &Bundle) -> u64 {
    let mut h = blake3::Hasher::new();
    h.update(b.id().as_str().as_bytes());
    h.update(&[0]);
    h.update(&b.payload);
    let bytes: [u8; 8] = h.finalize().as_bytes()[..8].try_into().expect("8 bytes");
    u64::from_be_bytes(bytes)
}

/// `dtn://<name>/`, the node ID of a named node.
fn node_eid(name: &str) -> Result<EndpointID> {
    EndpointID::with_dtn(name).map_err(|e| anyhow!("bad node name {name:?}: {e}"))
}

/// A node name is one non-empty URI segment.
fn check_node_name(what: &str, name: &str) -> Result<()> {
    if name.is_empty() || name.contains('/') {
        bail!("{what} {name:?} must be non-empty and free of '/'");
    }
    Ok(())
}

// === Decode ===

/// Decode and validate RFC 9171 bytes. Errors, never panics, on bad input.
pub fn decode_bundle(bytes: &[u8]) -> Result<Bundle> {
    let bundle = bp7::bundle::Bundle::try_from(bytes).map_err(|e| anyhow!("not an RFC 9171 bundle: {e}"))?;
    bundle.validate().map_err(|errs| anyhow!("invalid bundle: {errs:?}"))?;

    let dest = &bundle.primary.destination;
    let (Some(publisher), Some(content_id)) = (dest.node(), dest.service_name()) else {
        bail!("destination {dest} is not dtn://<publisher>/<content-id>");
    };
    if dest.scheme() != "dtn" {
        bail!("destination {dest} is not a dtn URI");
    }
    check_content_id(&content_id)?;

    let block = bundle
        .extension_block_by_type(CONTENT_BLOCK_TYPE)
        .ok_or_else(|| anyhow!("no content block (type {CONTENT_BLOCK_TYPE})"))?;
    let CanonicalData::Unknown(raw) = block.data() else {
        bail!("content block has unexpected data");
    };
    let info: ContentInfo = codec::decode(raw).context("content block is not a content info map")?;
    if info.content_id != content_id {
        bail!("content block names {:?} but the destination names {content_id:?}", info.content_id);
    }

    let age = bundle
        .extension_block_by_type(BUNDLE_AGE_BLOCK)
        .and_then(|b| b.bundle_age_get())
        .ok_or_else(|| anyhow!("no bundle age block"))?;
    let hop_count = bundle
        .extension_block_by_type(HOP_COUNT_BLOCK)
        .and_then(|b| b.hop_count_get())
        .map_or(0, |(_, count)| count);
    let source_node = bundle.primary.source.node().ok_or_else(|| anyhow!("bundle has no source node"))?;
    let payload = bundle.payload().ok_or_else(|| anyhow!("no payload block"))?.clone();

    Ok(Bundle {
        publisher: KeyId(publisher),
        info,
        payload,
        age_ms: u64::try_from(age).unwrap_or(u64::MAX),
        lifetime_ms: u64::try_from(bundle.primary.lifetime.as_millis()).unwrap_or(u64::MAX),
        hop_count,
        previous_node: bundle.previous_node().and_then(|e| e.node()),
        source_node,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bundle::{Bundle, BundleKind, Priority};
    use crate::trust::KeyId;
    use bp7::canonical::{CanonicalData, BUNDLE_AGE_BLOCK, HOP_COUNT_BLOCK};
    use bp7::eid::EndpointID;

    fn publisher() -> KeyId {
        KeyId("0123456789abcdef".into())
    }

    /// A segment with every field away from its default.
    fn sample(payload: Vec<u8>) -> Bundle {
        let mut b = Bundle::segment(&publisher(), "match_1", "Match", 7, "video/mp2t", Priority::Live, payload, "ground")
            .unwrap()
            .with_duration_ms(9_500);
        b.age_ms = 1_234;
        b.hop_count = 3;
        b.previous_node = Some("relay-1".into());
        b
    }

    fn stock(bytes: &[u8]) -> bp7::bundle::Bundle {
        bp7::bundle::Bundle::try_from(bytes).expect("stock bp7 decodes our bundle")
    }

    #[test]
    fn encode_decode_roundtrips_all_fields() {
        let seg = sample(vec![7u8; 1000]);
        assert_eq!(decode_bundle(&encode_bundle(&seg).unwrap()).unwrap(), seg);

        let mut man = seg.clone();
        man.info.kind = BundleKind::Manifest;
        man.previous_node = None;
        assert_eq!(decode_bundle(&encode_bundle(&man).unwrap()).unwrap(), man);
    }

    #[test]
    fn destination_uri_has_publisher_and_content() {
        assert_eq!(destination_uri(&publisher(), "match_1"), "dtn://0123456789abcdef/match_1");
        let raw = encode_bundle(&sample(b"ts".to_vec())).unwrap();
        assert_eq!(stock(&raw).primary.destination.to_string(), "dtn://0123456789abcdef/match_1");
    }

    #[test]
    fn stock_bp7_decodes_our_bundle_and_keeps_payload() {
        let b = sample(vec![0x47; 4096]);
        let s = stock(&encode_bundle(&b).unwrap());
        s.validate().expect("valid RFC 9171 bundle");
        assert_eq!(s.payload().unwrap(), &b.payload);
        assert_eq!(s.primary.creation_timestamp.dtntime(), 0);
        assert_eq!(s.primary.lifetime.as_millis() as u64, b.lifetime_ms);
    }

    #[test]
    fn unknown_block_192_survives_a_stock_roundtrip() {
        let b = sample(b"segment".to_vec());
        let mut s = stock(&encode_bundle(&b).unwrap());
        let block = s.extension_block_by_type(CONTENT_BLOCK_TYPE).expect("block 192");
        assert!(matches!(block.data(), CanonicalData::Unknown(_)));
        let again = s.to_cbor();
        assert_eq!(decode_bundle(&again).unwrap(), b);
    }

    #[test]
    fn age_hop_and_previous_node_blocks_present() {
        let s = stock(&encode_bundle(&sample(b"x".to_vec())).unwrap());
        let age = s.extension_block_by_type(BUNDLE_AGE_BLOCK).unwrap();
        assert_eq!(age.bundle_age_get(), Some(1_234));
        let hop = s.extension_block_by_type(HOP_COUNT_BLOCK).unwrap();
        assert_eq!(hop.hop_count_get().map(|(_, count)| count), Some(3));
        assert_eq!(s.previous_node(), Some(&EndpointID::with_dtn("relay-1").unwrap()));
        assert!(s.canonicals.iter().all(|c| c.block_control_flags == 0));
    }

    #[test]
    fn payload_is_not_inflated() {
        let n = encode_bundle(&sample(vec![0xAB; 1_000_000])).unwrap().len();
        assert!(n < 1_000_000 + 1024, "encoded {n} bytes");
    }

    #[test]
    fn content_id_with_slash_is_rejected() {
        let make = |id: &str| Bundle::segment(&publisher(), id, "t", 0, "video/mp2t", Priority::Vod, vec![], "ground");
        assert!(make("a/b").is_err());
        assert!(make("").is_err());
        let mut b = sample(vec![]);
        b.info.content_id = "a/b".into();
        assert!(encode_bundle(&b).is_err());
        b.info.content_id = "a~b".into();
        assert!(encode_bundle(&b).is_err());
    }

    #[test]
    fn decode_rejects_a_content_id_outside_the_allowed_set() {
        // Another BPv7 node could send `~`, which is fine in a URI but not in a content id.
        let mut s = stock(&encode_bundle(&sample(b"x".to_vec())).unwrap());
        s.primary.destination = EndpointID::try_from("dtn://0123456789abcdef/a~b").unwrap();
        let block = s.canonicals.iter_mut().find(|c| c.block_type == CONTENT_BLOCK_TYPE).unwrap();
        let mut info = sample(vec![]).info;
        info.content_id = "a~b".into();
        block.set_data(CanonicalData::Unknown(codec::encode(&info).unwrap()));
        let err = decode_bundle(&s.to_cbor()).unwrap_err().to_string();
        assert!(err.contains("content id"), "{err}");
    }

    #[test]
    fn decode_rejects_garbage_without_panic() {
        let good = encode_bundle(&sample(vec![1u8; 300])).unwrap();
        assert!(decode_bundle(&[]).is_err());
        for cut in [1, 10, good.len() / 2, good.len() - 1] {
            assert!(decode_bundle(&good[..cut]).is_err(), "cut at {cut}");
        }
        // Deterministic noise from a simple LCG.
        let mut x: u64 = 0x2545_f491_4f6c_dd1d;
        for len in [1usize, 7, 64, 1024] {
            let noise: Vec<u8> = (0..len)
                .map(|_| {
                    x = x.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
                    (x >> 56) as u8
                })
                .collect();
            assert!(decode_bundle(&noise).is_err(), "noise of {len} bytes");
        }
        // A stock bundle whose block 192 holds a CBOR integer, not a content map.
        let mut s = stock(&good);
        let block = s.canonicals.iter_mut().find(|c| c.block_type == CONTENT_BLOCK_TYPE).unwrap();
        block.set_data(CanonicalData::Unknown(vec![0x18, 0x2a]));
        assert!(decode_bundle(&s.to_cbor()).is_err());
    }
}
