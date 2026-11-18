//! CBOR codec for peer messages and store metadata.
//!
//! Bundles themselves use the RFC 9171 format in `crate::wire`.

use anyhow::{anyhow, Result};
use serde::{de::DeserializeOwned, Serialize};

/// Peer protocol version carried in the handshake.
pub const PROTOCOL_VERSION: u16 = 3;

// === Encode / decode ===

pub fn encode<T: Serialize>(v: &T) -> Result<Vec<u8>> {
    let mut out = Vec::new();
    ciborium::into_writer(v, &mut out).map_err(|e| anyhow!("cbor encode: {e}"))?;
    Ok(out)
}

pub fn decode<T: DeserializeOwned>(bytes: &[u8]) -> Result<T> {
    ciborium::from_reader(bytes).map_err(|e| anyhow!("cbor decode: {e}"))
}

