//! mDNS peer discovery — auto-discover cosmosd nodes on the local network.
//!
//! Announces this node as `_cosmosd._tcp.local.` and listens for other nodes.
//! Discovered peers are automatically added to the peer manager.
//!
//! This enables zero-config proximity mesh on any shared WiFi or LAN —
//! two devices running cosmosd on the same network find each other instantly.

use std::sync::Arc;

use anyhow::Result;
use mdns_sd::{ServiceDaemon, ServiceEvent, ServiceInfo};
use tracing::{info, warn};

use crate::peer::PeerManager;

const SERVICE_TYPE: &str = "_cosmosd._tcp.local.";

/// Announce this node and discover peers via mDNS.
/// Runs forever — call from a background tokio task.
pub async fn run(node_id: &str, relay_port: u16, peers: Arc<PeerManager>) -> Result<()> {
    let mdns = ServiceDaemon::new()?;

    // 1. Announce ourselves
    let host = format!("{}.local.", node_id.replace(' ', "-"));
    let service = ServiceInfo::new(
        SERVICE_TYPE,
        node_id,
        &host,
        "",        // IP auto-detected by mdns-sd
        relay_port,
        None,
    )?;
    mdns.register(service)?;
    info!("mDNS: announced {} on port {}", node_id, relay_port);

    // 2. Browse for other cosmosd nodes
    let receiver = mdns.browse(SERVICE_TYPE)?;

    loop {
        match receiver.recv_async().await {
            Ok(ServiceEvent::ServiceResolved(info)) => {
                let port = info.get_port();
                for addr in info.get_addresses() {
                    let relay_addr = format!("{}:{}", addr, port);
                    // Don't peer with ourselves
                    if info.get_fullname().contains(node_id) {
                        continue;
                    }
                    info!("mDNS: discovered peer {} at {}", info.get_fullname(), relay_addr);
                    peers.add_peer(relay_addr).await;
                }
            }
            Ok(ServiceEvent::ServiceRemoved(_, fullname)) => {
                info!("mDNS: peer left — {}", fullname);
                // NOTE: peer manager keeps the peer but backoff handles unreachable nodes
            }
            Ok(_) => {}
            Err(e) => {
                warn!("mDNS receive error: {}", e);
            }
        }
    }
}
