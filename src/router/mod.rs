//! Epidemic routing — automatic bundle replication between peers.
//!
//! When two cosmosd nodes connect, they exchange manifests (what bundles
//! each has), compute the diff, and pull missing bundles. This is how
//! content spreads through the mesh without any central coordination.
//!
//! Protocol (over any TCP connection on the relay port):
//!   1. Both sides send their manifest simultaneously
//!   2. Initiator computes diff, sends a pull request
//!   3. Responder streams requested bundles
//!   4. Connection closes

use std::collections::HashSet;
use std::sync::Arc;

use anyhow::Result;
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tracing::{debug, info, warn};

use crate::auth::RelayAuth;
use crate::bundle::Bundle;
use crate::store::BundleStore;

// === Wire types ===

#[derive(Serialize, Deserialize)]
struct Manifest {
    node_id: String,
    bundle_ids: Vec<String>,
}

#[derive(Serialize, Deserialize)]
struct PullRequest {
    bundle_ids: Vec<String>,
}

// === Frame I/O (4-byte length prefix + payload) ===

const MAX_FRAME: usize = 60 * 1024 * 1024; // 60 MB hard cap

async fn send_frame(stream: &mut TcpStream, data: &[u8]) -> Result<()> {
    let len = data.len() as u32;
    stream.write_all(&len.to_be_bytes()).await?;
    stream.write_all(data).await?;
    Ok(())
}

async fn recv_frame(stream: &mut TcpStream) -> Result<Vec<u8>> {
    let mut len_buf = [0u8; 4];
    stream.read_exact(&mut len_buf).await?;
    let len = u32::from_be_bytes(len_buf) as usize;
    if len > MAX_FRAME {
        anyhow::bail!("Frame too large: {} bytes", len);
    }
    let mut buf = vec![0u8; len];
    stream.read_exact(&mut buf).await?;
    Ok(buf)
}

// === Sync stats ===

pub struct SyncStats {
    pub peer_node_id: String,
    pub received: usize,
    pub sent: usize,
}

// === Epidemic router ===

pub struct EpidemicRouter {
    pub store: Arc<BundleStore>,
    pub node_id: String,
}

impl EpidemicRouter {
    pub fn new(store: Arc<BundleStore>, node_id: String) -> Self {
        EpidemicRouter { store, node_id }
    }

    /// Initiate a sync session with a remote peer.
    ///
    /// Called by the peer manager on a schedule for each known peer.
    pub async fn sync_with_peer(&self, addr: &str, auth: Arc<RelayAuth>) -> Result<SyncStats> {
        debug!("Connecting to peer relay: {}", addr);
        let mut stream = TcpStream::connect(addr).await?;
        auth.authenticate_outgoing(&mut stream).await?;

        // 1. Send our manifest
        let our_ids = self.store.all_ids()?;
        let manifest = serde_json::to_vec(&Manifest {
            node_id: self.node_id.clone(),
            bundle_ids: our_ids.iter().cloned().collect(),
        })?;
        send_frame(&mut stream, &manifest).await?;

        // 2. Receive their manifest
        let raw = recv_frame(&mut stream).await?;
        let their_manifest: Manifest = serde_json::from_slice(&raw)?;
        let peer_id = their_manifest.node_id.clone();

        // 3. Compute what we're missing
        let their_ids: HashSet<String> = their_manifest.bundle_ids.into_iter().collect();
        let missing: Vec<String> = their_ids
            .difference(&our_ids)
            .cloned()
            .collect();

        info!(
            "Peer {}: they have {}, we have {}, missing {}",
            peer_id,
            their_ids.len(),
            our_ids.len(),
            missing.len()
        );

        // 4. Send pull request
        let pull = serde_json::to_vec(&PullRequest { bundle_ids: missing.clone() })?;
        send_frame(&mut stream, &pull).await?;

        // 5. Receive requested bundles
        let mut received = 0usize;
        for id in &missing {
            let raw = recv_frame(&mut stream).await?;
            if raw.is_empty() {
                debug!("Peer {} doesn't have bundle {}", peer_id, id);
                continue;
            }
            let bundle: Bundle = match serde_json::from_slice(&raw) {
                Ok(b) => b,
                Err(e) => {
                    warn!("Bad bundle {} from {}: {}", id, peer_id, e);
                    continue;
                }
            };
            if !bundle.verify() {
                warn!("Integrity check failed for bundle {} from {}", id, peer_id);
                continue;
            }
            if bundle.is_valid() {
                self.store.insert(&bundle)?;
                received += 1;
            }
        }

        info!("Sync complete with {}: received {} bundles", peer_id, received);
        Ok(SyncStats { peer_node_id: peer_id, received, sent: 0 })
    }

    /// Handle an incoming sync session from a remote peer.
    ///
    /// Called by the relay listener for each accepted connection.
    pub async fn handle_incoming(&self, mut stream: TcpStream) -> Result<SyncStats> {
        // 1. Receive their manifest
        let raw = recv_frame(&mut stream).await?;
        let their_manifest: Manifest = serde_json::from_slice(&raw)?;
        let peer_id = their_manifest.node_id.clone();
        let their_ids: HashSet<String> = their_manifest.bundle_ids.into_iter().collect();

        debug!("Incoming sync from {}: they have {} bundles", peer_id, their_ids.len());

        // 2. Send our manifest
        let our_ids = self.store.all_ids()?;
        let manifest = serde_json::to_vec(&Manifest {
            node_id: self.node_id.clone(),
            bundle_ids: our_ids.iter().cloned().collect(),
        })?;
        send_frame(&mut stream, &manifest).await?;

        // 3. Receive their pull request
        let raw = recv_frame(&mut stream).await?;
        let pull: PullRequest = serde_json::from_slice(&raw)?;

        // 4. Send requested bundles (empty frame if we don't have it)
        let mut sent = 0usize;
        for id in &pull.bundle_ids {
            match self.store.get(id)? {
                Some(bundle) => {
                    let data = serde_json::to_vec(&bundle)?;
                    send_frame(&mut stream, &data).await?;
                    sent += 1;
                }
                None => {
                    send_frame(&mut stream, &[]).await?;
                }
            }
        }

        info!("Incoming sync from {}: sent {} bundles", peer_id, sent);
        Ok(SyncStats { peer_node_id: peer_id, received: 0, sent })
    }
}
