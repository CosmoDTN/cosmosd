//! Convergence layer abstraction — the pluggable transport interface.
//!
//! Implement `ConvergenceLayer` to add any transport medium to CosmosD:
//! LoRa, Reticulum, ham radio, BLE, USB sneakernet, laser comms.
//!
//! The TCP transport ships as the built-in reference implementation.

pub mod tcp;

use anyhow::Result;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use crate::bundle::Bundle;

/// Address of a remote CosmosD peer.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct PeerAddr(pub String);

impl std::fmt::Display for PeerAddr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// A bidirectional stream for sending/receiving bundles with a peer.
#[async_trait]
pub trait BundleStream: Send + Sync {
    async fn send(&mut self, bundle: &Bundle) -> Result<()>;
    async fn recv(&mut self) -> Result<Option<Bundle>>;
    fn peer_addr(&self) -> &PeerAddr;
}

/// A listener that accepts incoming peer connections.
#[async_trait]
pub trait BundleListener: Send {
    async fn accept(&mut self) -> Result<Box<dyn BundleStream>>;
}

/// Core trait — implement this to add a new transport to CosmosD.
///
/// # Example
/// ```rust
/// struct LoRaTransport { /* radio config */ }
///
/// #[async_trait]
/// impl ConvergenceLayer for LoRaTransport {
///     fn name(&self) -> &str { "lora" }
///     async fn connect(&self, peer: &PeerAddr) -> Result<Box<dyn BundleStream>> { ... }
///     async fn listen(&self) -> Result<Box<dyn BundleListener>> { ... }
/// }
/// ```
#[async_trait]
pub trait ConvergenceLayer: Send + Sync {
    /// Short name identifying this transport (e.g. "tcp", "lora", "reticulum").
    fn name(&self) -> &str;

    /// Open a connection to a remote peer.
    async fn connect(&self, peer: &PeerAddr) -> Result<Box<dyn BundleStream>>;

    /// Start listening for incoming peer connections.
    async fn listen(&self) -> Result<Box<dyn BundleListener>>;
}
