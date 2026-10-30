//! Epidemic routing — automatic bundle replication between peers.
//!
//! When two cosmosd nodes connect, they exchange manifests (what bundles
//! each has), compute the diff, and pull missing bundles. This is how
//! content spreads through the mesh without any central coordination.
//!
//! Protocol v2 (CBOR frames, over any stream on the relay port):
//!   1. Both sides send their manifest (version, relay port, id + priority)
//!   2. Initiator orders the diff live-first and sends a pull request
//!   3. Responder streams requested bundles, in that order
//!   4. Connection closes
//!
//! Every read is bounded by `FRAME_TIMEOUT`, so a blackholed link fails the
//! session instead of hanging it.

use std::sync::Arc;
use std::time::Duration;

use anyhow::{anyhow, bail, Result};
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::time::timeout;
use tracing::{debug, info, warn};

use crate::auth::RelayAuth;
use crate::bundle::Bundle;
use crate::codec::{self, PROTOCOL_VERSION};
use crate::store::BundleStore;

mod order;
pub use order::*;

/// Longest we wait for a TCP connection to a peer.
pub const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
/// Longest we wait for the next frame from a peer.
pub const FRAME_TIMEOUT: Duration = Duration::from_secs(30);

// === Wire types ===

#[derive(Serialize, Deserialize)]
struct Manifest {
    protocol: u16,
    node_id: String,
    relay_port: u16,
    entries: Vec<ManifestEntry>,
}

#[derive(Serialize, Deserialize)]
struct PullRequest {
    bundle_ids: Vec<String>,
}

// === Frame I/O (4-byte length prefix + payload) ===

const MAX_FRAME: usize = 60 * 1024 * 1024; // 60 MB hard cap

async fn send_frame<S: AsyncWrite + Unpin>(stream: &mut S, data: &[u8]) -> Result<()> {
    let len = data.len() as u32;
    stream.write_all(&len.to_be_bytes()).await?;
    stream.write_all(data).await?;
    stream.flush().await?;
    Ok(())
}

/// Read one frame, giving up after `FRAME_TIMEOUT` so a dead link cannot hang us.
async fn recv_frame<S: AsyncRead + Unpin>(stream: &mut S) -> Result<Vec<u8>> {
    timeout(FRAME_TIMEOUT, async {
        let mut len_buf = [0u8; 4];
        stream.read_exact(&mut len_buf).await?;
        let len = u32::from_be_bytes(len_buf) as usize;
        if len > MAX_FRAME {
            bail!("Frame too large: {} bytes", len);
        }
        let mut buf = vec![0u8; len];
        stream.read_exact(&mut buf).await?;
        Ok(buf)
    })
    .await
    .map_err(|_| anyhow!("timed out after {:?} waiting for a frame", FRAME_TIMEOUT))?
}

// === Sync stats ===

#[derive(Debug)]
pub struct SyncStats {
    pub peer_node_id: String,
    pub peer_relay_port: u16,
    pub received: usize,
    pub sent: usize,
}

// === Epidemic router ===

pub struct EpidemicRouter {
    pub store: Arc<BundleStore>,
    pub node_id: String,
    relay_port: u16,
    delay: Duration,
}

impl EpidemicRouter {
    pub fn new(store: Arc<BundleStore>, node_id: String, relay_port: u16, delay: Duration) -> Self {
        EpidemicRouter { store, node_id, relay_port, delay }
    }

    /// Our manifest: every valid bundle we hold, with its priority.
    fn manifest(&self) -> Result<Manifest> {
        let entries = self
            .store
            .all_meta()?
            .into_iter()
            .map(|m| ManifestEntry { id: m.id.to_string(), priority: m.priority })
            .collect();
        Ok(Manifest {
            protocol: PROTOCOL_VERSION,
            node_id: self.node_id.clone(),
            relay_port: self.relay_port,
            entries,
        })
    }

    /// Decode a peer's manifest and refuse one from another protocol version.
    fn parse_manifest(raw: &[u8]) -> Result<Manifest> {
        let m: Manifest = codec::decode(raw)
            .map_err(|e| anyhow!("peer sent an unreadable manifest (older protocol than {}?): {}", PROTOCOL_VERSION, e))?;
        if m.protocol != PROTOCOL_VERSION {
            bail!("protocol mismatch: peer speaks v{}, we speak v{}", m.protocol, PROTOCOL_VERSION);
        }
        Ok(m)
    }

    /// Initiate a sync session with a remote peer.
    ///
    /// Called by the peer manager on a schedule for each known peer.
    pub async fn sync_with_peer(&self, addr: &str, auth: Arc<RelayAuth>) -> Result<SyncStats> {
        debug!("Connecting to peer relay: {}", addr);
        let mut stream = timeout(CONNECT_TIMEOUT, TcpStream::connect(addr))
            .await
            .map_err(|_| anyhow!("timed out connecting to {}", addr))??;
        auth.authenticate_outgoing(&mut stream).await?;
        self.initiate(stream).await
    }

    /// Run the initiating side of a sync over an authenticated stream.
    pub async fn initiate<S: AsyncRead + AsyncWrite + Unpin>(&self, mut stream: S) -> Result<SyncStats> {
        // 1. Send our manifest
        send_frame(&mut stream, &codec::encode(&self.manifest()?)?).await?;

        // 2. Receive their manifest
        let theirs = Self::parse_manifest(&recv_frame(&mut stream).await?)?;
        let peer_id = theirs.node_id.clone();

        // 3. Work out what we're missing, most urgent first
        let our_ids = self.store.all_ids()?;
        let missing = pull_order(&theirs.entries, &our_ids, self.delay);

        info!(
            "Peer {}: they have {}, we have {}, missing {}",
            peer_id,
            theirs.entries.len(),
            our_ids.len(),
            missing.len()
        );

        // 4. Send pull request
        send_frame(&mut stream, &codec::encode(&PullRequest { bundle_ids: missing.clone() })?).await?;

        // 5. Receive requested bundles, storing each as it arrives
        let mut received = 0usize;
        for id in &missing {
            let raw = recv_frame(&mut stream).await?;
            if raw.is_empty() {
                debug!("Peer {} doesn't have bundle {}", peer_id, id);
                continue;
            }
            let bundle: Bundle = match codec::decode(&raw) {
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
        Ok(SyncStats { peer_node_id: peer_id, peer_relay_port: theirs.relay_port, received, sent: 0 })
    }

    /// Handle an incoming sync session from a remote peer.
    ///
    /// Called by the relay listener for each accepted connection.
    pub async fn handle_incoming<S: AsyncRead + AsyncWrite + Unpin>(&self, mut stream: S) -> Result<SyncStats> {
        // 1. Receive their manifest
        let raw = recv_frame(&mut stream).await?;

        // 2. Send our manifest (even to a mismatched peer, so it can report the mismatch too)
        send_frame(&mut stream, &codec::encode(&self.manifest()?)?).await?;

        let theirs = Self::parse_manifest(&raw)?;
        let peer_id = theirs.node_id.clone();
        debug!("Incoming sync from {}: they have {} bundles", peer_id, theirs.entries.len());

        // 3. Receive their pull request
        let pull: PullRequest = codec::decode(&recv_frame(&mut stream).await?)?;

        // 4. Send requested bundles (empty frame if we don't have it)
        let mut sent = 0usize;
        for id in &pull.bundle_ids {
            match self.store.get(id)? {
                Some(bundle) => {
                    send_frame(&mut stream, &codec::encode(&bundle)?).await?;
                    sent += 1;
                }
                None => {
                    send_frame(&mut stream, &[]).await?;
                }
            }
        }

        info!("Incoming sync from {}: sent {} bundles", peer_id, sent);
        Ok(SyncStats { peer_node_id: peer_id, peer_relay_port: theirs.relay_port, received: 0, sent })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bundle::Priority;

    /// A router on a temp store; keeps the tempdir alive as long as it is.
    struct TestRouter {
        router: EpidemicRouter,
        _dir: tempfile::TempDir,
    }

    impl std::ops::Deref for TestRouter {
        type Target = EpidemicRouter;
        fn deref(&self) -> &EpidemicRouter {
            &self.router
        }
    }

    fn router(name: &str, port: u16) -> TestRouter {
        let dir = tempfile::tempdir().unwrap();
        let store = Arc::new(BundleStore::open(dir.path().to_str().unwrap()).unwrap());
        let router = EpidemicRouter::new(store, name.to_string(), port, Duration::from_secs(120));
        TestRouter { router, _dir: dir }
    }

    fn live_bundle(content: &str, seq: u32) -> Bundle {
        Bundle::new(content, "Match", seq, "video/mp2t", Priority::Live, vec![seq as u8; 64], "ground")
    }

    #[tokio::test]
    async fn sync_pulls_missing_bundles() {
        let ground = router("ground", 17778);
        let gateway = router("gateway", 27778);
        for s in 0..3 {
            ground.store.insert(&live_bundle("m", s)).unwrap();
        }
        let (a, b) = tokio::io::duplex(1 << 20);
        let (got, served) = tokio::join!(gateway.initiate(a), ground.handle_incoming(b));
        assert_eq!(got.unwrap().received, 3);
        assert_eq!(served.unwrap().sent, 3);
        assert!(gateway.store.get("m/00002").unwrap().is_some());
    }

    #[tokio::test]
    async fn sync_reports_peer_relay_port() {
        let ground = router("ground", 17778);
        let gateway = router("gateway", 27778);
        let (a, b) = tokio::io::duplex(1 << 16);
        let (out, inc) = tokio::join!(gateway.initiate(a), ground.handle_incoming(b));
        assert_eq!(out.unwrap().peer_relay_port, 17778);
        assert_eq!(inc.unwrap().peer_relay_port, 27778);
    }

    #[tokio::test]
    async fn protocol_mismatch_is_reported() {
        let gateway = router("gateway", 27778);
        let (a, mut b) = tokio::io::duplex(1 << 16);
        let peer = tokio::spawn(async move {
            let _ = recv_frame(&mut b).await;
            let m = Manifest { protocol: 1, node_id: "old".into(), relay_port: 7778, entries: vec![] };
            send_frame(&mut b, &codec::encode(&m).unwrap()).await.unwrap();
            b
        });
        let err = gateway.initiate(a).await.unwrap_err().to_string();
        assert!(err.contains("protocol") && err.contains('1') && err.contains('2'), "{}", err);
        drop(peer.await);
    }

    #[tokio::test(start_paused = true)]
    async fn initiate_times_out_on_stalled_peer() {
        let gateway = router("gateway", 27778);
        let (a, _b) = tokio::io::duplex(1 << 16); // peer never answers, never closes
        let res = gateway.initiate(a).await;
        assert!(res.is_err());
    }
}
