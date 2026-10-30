//! Peer manager — tracks known peers, drives the epidemic sync loop.
//!
//! Ticks every second and runs one sync session per due peer via the
//! epidemic router. After a success a peer is next tried one sync interval
//! later; after a failure it backs off 1 s, 2 s, 4 s, 8 s, capped at 15 s,
//! so a link that drops is picked up again within seconds.
//! Peers can be added via CLI flag (--peer) or the HTTP API at runtime, and,
//! unless discovery is off, by connecting to us.

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

/// Delay before the next attempt: the sync interval after a success, then a
/// short capped backoff (1, 2, 4, 8, 15 s) while the peer keeps failing.
pub fn retry_delay(failed_attempts: u32, sync_interval: Duration) -> Duration {
    match failed_attempts {
        0 => sync_interval,
        1..=4 => Duration::from_secs(1 << (failed_attempts - 1)),
        _ => Duration::from_secs(15),
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct PeerInfo {
    pub relay_addr: String,
    /// Token we present when dialing this peer. Never serialized or logged.
    #[serde(skip)]
    pub token: Option<String>,
    pub last_seen: Option<DateTime<Utc>>,
    pub last_sync: Option<DateTime<Utc>>,
    pub bundles_received: usize,
    pub failed_attempts: u32,
    /// A session with this peer is in flight.
    pub syncing: bool,
    pub next_attempt: DateTime<Utc>,
}

impl PeerInfo {
    fn new(relay_addr: String) -> Self {
        PeerInfo {
            relay_addr,
            token: None,
            last_seen: None,
            last_sync: None,
            bundles_received: 0,
            failed_attempts: 0,
            syncing: false,
            next_attempt: Utc::now(),
        }
    }

    /// Ready for a new session: none in flight and the backoff has elapsed.
    fn due(&self, now: DateTime<Utc>) -> bool {
        !self.syncing && now >= self.next_attempt
    }
}

/// Record an incoming session from the peer relay at `relay_addr`. A known
/// peer is marked seen; an unknown one is added only if `register` is set.
///
/// With discovery off (the ground node) it isn't: a gateway behind plane
/// Wi-Fi can't be dialed back, and on one machine dialing back would hand the
/// gateway a direct route to the ground that skips the satellite link.
fn note_incoming(map: &mut HashMap<String, PeerInfo>, relay_addr: String, register: bool) {
    if register {
        map.entry(relay_addr.clone()).or_insert_with(|| PeerInfo::new(relay_addr.clone()));
    }
    if let Some(peer) = map.get_mut(&relay_addr) {
        peer.last_seen = Some(Utc::now());
    }
}

// === Peer manager ===

pub struct PeerManager {
    peers: Arc<RwLock<HashMap<String, PeerInfo>>>,
    router: Arc<EpidemicRouter>,
    auth: Arc<RelayAuth>,
    sync_interval: Duration,
    /// Add peers that connect to us (off with `--no-discovery`).
    register_incoming: bool,
}

impl PeerManager {
    pub fn new(
        router: Arc<EpidemicRouter>,
        auth: Arc<RelayAuth>,
        initial_peers: Vec<(String, Option<String>)>,
        sync_interval: Duration,
    ) -> Self {
        let mut map = HashMap::new();
        for (addr, token) in initial_peers {
            let mut info = PeerInfo::new(addr.clone());
            info.token = token;
            map.insert(addr, info);
        }
        PeerManager {
            peers: Arc::new(RwLock::new(map)),
            router,
            auth,
            sync_interval,
            register_incoming: true,
        }
    }

    /// Whether peers that connect to us are added to the peer list.
    pub fn with_incoming_registration(mut self, on: bool) -> Self {
        self.register_incoming = on;
        self
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

    /// Outgoing sync loop — runs forever, ticking once a second and starting
    /// a session for every due peer (at most one in flight per peer).
    pub async fn run_sync_loop(&self) {
        info!("Peer sync loop started (interval: {}s)", self.sync_interval.as_secs());
        let mut tick = tokio::time::interval(Duration::from_secs(1));
        loop {
            tick.tick().await;

            // Select and mark under one lock so two ticks never double-start a peer.
            let due: Vec<(String, Option<String>)> = {
                let now = Utc::now();
                let mut peers = self.peers.write().await;
                peers
                    .values_mut()
                    .filter(|p| p.due(now))
                    .map(|p| {
                        p.syncing = true;
                        (p.relay_addr.clone(), p.token.clone())
                    })
                    .collect()
            };

            for (addr, token) in due {
                let router = self.router.clone();
                let peers = self.peers.clone();
                let sync_interval = self.sync_interval;

                tokio::spawn(async move {
                    let result = router.sync_with_peer(&addr, token.as_deref()).await;
                    let now = Utc::now();
                    let mut map = peers.write().await;
                    if let Some(peer) = map.get_mut(&addr) {
                        peer.last_sync = Some(now);
                        match result {
                            Ok(stats) => {
                                peer.last_seen = Some(now);
                                peer.bundles_received += stats.received;
                                peer.failed_attempts = 0;
                            }
                            Err(e) => {
                                debug!("Sync failed with {}: {}", addr, e);
                                peer.failed_attempts += 1;
                            }
                        }
                        peer.syncing = false;
                        let delay = retry_delay(peer.failed_attempts, sync_interval);
                        peer.next_attempt = now
                            + chrono::Duration::from_std(delay).unwrap_or_else(|_| chrono::Duration::seconds(15));
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
            let register = self.register_incoming;

            tokio::spawn(async move {
                if let Err(e) = auth.verify_incoming(&mut stream).await {
                    tracing::warn!("Auth rejected from {}: {}", peer_addr, e);
                    return;
                }
                match router.handle_incoming(stream).await {
                    Ok(stats) => {
                        // At the relay port it advertised
                        let relay_addr = format!("{}:{}", peer_addr.ip(), stats.peer_relay_port);
                        note_incoming(&mut *peers.write().await, relay_addr, register);
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn retry_delay_backs_off_to_fifteen_seconds() {
        let i = Duration::from_secs(5);
        let got: Vec<u64> = (0..7).map(|n| retry_delay(n, i).as_secs()).collect();
        assert_eq!(got, vec![5, 1, 2, 4, 8, 15, 15]);
    }

    #[test]
    fn peer_not_due_while_syncing() {
        let mut p = PeerInfo::new("127.0.0.1:18778".into());
        let now = Utc::now(); // after `new`, whose next_attempt is its creation time
        assert!(p.due(now));
        p.syncing = true;
        assert!(!p.due(now + chrono::Duration::hours(1)));
    }

    #[test]
    fn incoming_peer_registered_only_when_discovering() {
        let mut map = HashMap::new();
        note_incoming(&mut map, "127.0.0.1:17778".into(), false);
        assert!(map.is_empty(), "a --no-discovery node must not dial back whoever dialed it");
        note_incoming(&mut map, "127.0.0.1:27778".into(), true);
        assert!(map["127.0.0.1:27778"].last_seen.is_some());
    }

    #[test]
    fn known_incoming_peer_is_seen_even_without_discovery() {
        let mut map = HashMap::new();
        map.insert("10.0.0.2:7778".to_string(), PeerInfo::new("10.0.0.2:7778".into()));
        note_incoming(&mut map, "10.0.0.2:7778".into(), false);
        assert!(map["10.0.0.2:7778"].last_seen.is_some());
    }

    #[test]
    fn peer_token_is_not_serialized() {
        let mut p = PeerInfo::new("x:1".into());
        p.token = Some("hunter2".into());
        assert!(!serde_json::to_string(&p).unwrap().contains("hunter2"));
    }

    #[test]
    fn peer_due_after_next_attempt() {
        let now = Utc::now();
        let mut p = PeerInfo::new("x:1".into());
        p.next_attempt = now + chrono::Duration::seconds(4);
        assert!(!p.due(now));
        assert!(p.due(now + chrono::Duration::seconds(4)));
    }
}
