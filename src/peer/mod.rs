//! Peer manager — tracks known peers, drives the epidemic sync loop.
//!
//! Every 30 seconds, connects to each known peer and runs a sync session
//! via the epidemic router. Failed connections are retried with backoff.
//! Peers can be added via CLI flag (--peer) or the HTTP API at runtime.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use chrono::{DateTime, Utc};
use serde::Serialize;
use tokio::net::TcpListener;
use tokio::sync::RwLock;
use tracing::{debug, info, warn};

use crate::auth::RelayAuth;
use crate::router::EpidemicRouter;

// === Peer state ===

#[derive(Debug, Clone, Serialize)]
pub struct PeerInfo {
    pub relay_addr: String,
    pub last_seen: Option<DateTime<Utc>>,
    pub last_sync: Option<DateTime<Utc>>,
    pub bundles_received: usize,
    pub failed_attempts: u32,
}

impl PeerInfo {
    fn new(relay_addr: String) -> Self {
        PeerInfo {
            relay_addr,
            last_seen: None,
            last_sync: None,
            bundles_received: 0,
            failed_attempts: 0,
        }
    }

    /// Exponential backoff: skip this peer if it failed recently.
    fn should_retry(&self) -> bool {
        let Some(last_sync) = self.last_sync else {
            return true; // never tried
        };
        let backoff_secs = (2u64).pow(self.failed_attempts.min(6)) * 30;
        let elapsed = Utc::now().signed_duration_since(last_sync).num_seconds();
        elapsed as u64 >= backoff_secs
    }
}

// === Peer manager ===

pub struct PeerManager {
    peers: Arc<RwLock<HashMap<String, PeerInfo>>>,
    router: Arc<EpidemicRouter>,
    auth: Arc<RelayAuth>,
    sync_interval: Duration,
}

impl PeerManager {
    pub fn new(router: Arc<EpidemicRouter>, auth: Arc<RelayAuth>, initial_peers: Vec<String>) -> Self {
        let mut map = HashMap::new();
        for addr in initial_peers {
            map.insert(addr.clone(), PeerInfo::new(addr));
        }
        PeerManager {
            peers: Arc::new(RwLock::new(map)),
            router,
            auth,
            sync_interval: Duration::from_secs(30),
        }
    }

    /// Add a peer at runtime (called from HTTP API or discovery).
    pub async fn add_peer(&self, relay_addr: String) {
        let mut peers = self.peers.write().await;
        peers
            .entry(relay_addr.clone())
            .or_insert_with(|| PeerInfo::new(relay_addr.clone()));
        info!("Peer added: {}", relay_addr);
    }

    /// List all known peers and their status.
    pub async fn list_peers(&self) -> Vec<PeerInfo> {
        self.peers.read().await.values().cloned().collect()
    }

    /// Outgoing sync loop — runs forever, syncs all peers on interval.
    pub async fn run_sync_loop(&self) {
        info!("Peer sync loop started (interval: {}s)", self.sync_interval.as_secs());
        loop {
            tokio::time::sleep(self.sync_interval).await;

            let peer_addrs: Vec<String> = {
                let peers = self.peers.read().await;
                peers
                    .values()
                    .filter(|p| p.should_retry())
                    .map(|p| p.relay_addr.clone())
                    .collect()
            };

            for addr in peer_addrs {
                let router = self.router.clone();
                let peers = self.peers.clone();
                let addr_clone = addr.clone();

                let auth = self.auth.clone();
                tokio::spawn(async move {
                    match router.sync_with_peer(&addr_clone, auth).await {
                        Ok(stats) => {
                            let mut map = peers.write().await;
                            if let Some(peer) = map.get_mut(&addr_clone) {
                                peer.last_seen = Some(Utc::now());
                                peer.last_sync = Some(Utc::now());
                                peer.bundles_received += stats.received;
                                peer.failed_attempts = 0;
                            }
                        }
                        Err(e) => {
                            debug!("Sync failed with {}: {}", addr_clone, e);
                            let mut map = peers.write().await;
                            if let Some(peer) = map.get_mut(&addr_clone) {
                                peer.last_sync = Some(Utc::now());
                                peer.failed_attempts += 1;
                            }
                        }
                    }
                });
            }
        }
    }

    /// Incoming relay listener — accepts peer connections, runs sync sessions.
    pub async fn run_relay_listener(&self, bind_addr: String) -> Result<()> {
        let listener = TcpListener::bind(&bind_addr).await?;
        info!("Relay listener on {} (epidemic routing)", bind_addr);

        loop {
            let (mut stream, peer_addr) = listener.accept().await?;
            info!("Incoming sync from {}", peer_addr);

            let router = self.router.clone();
            let peers = self.peers.clone();
            let auth = self.auth.clone();

            tokio::spawn(async move {
                if let Err(e) = auth.verify_incoming(&mut stream).await {
                    tracing::warn!("Auth rejected from {}: {}", peer_addr, e);
                    return;
                }
                match router.handle_incoming(stream).await {
                    Ok(stats) => {
                        // Register peer if not already known
                        let relay_addr = format!("{}:{}", peer_addr.ip(), 7778);
                        let mut map = peers.write().await;
                        let peer = map
                            .entry(relay_addr.clone())
                            .or_insert_with(|| PeerInfo::new(relay_addr));
                        peer.last_seen = Some(Utc::now());
                        info!("Served {} bundles to {}", stats.sent, peer_addr);
                    }
                    Err(e) => {
                        warn!("Relay session error from {}: {}", peer_addr, e);
                    }
                }
            });
        }
    }
}
