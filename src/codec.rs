//! CBOR wire codec for bundles and peer messages.

use anyhow::{anyhow, Result};
use serde::{de::DeserializeOwned, Serialize};

/// Peer protocol version carried in the handshake.
pub const PROTOCOL_VERSION: u16 = 2;

// === Encode / decode ===

pub fn encode<T: Serialize>(v: &T) -> Result<Vec<u8>> {
    let mut out = Vec::new();
    ciborium::into_writer(v, &mut out).map_err(|e| anyhow!("cbor encode: {e}"))?;
    Ok(out)
}

pub fn decode<T: DeserializeOwned>(bytes: &[u8]) -> Result<T> {
    ciborium::from_reader(bytes).map_err(|e| anyhow!("cbor decode: {e}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bundle::{Bundle, Priority};

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
}
