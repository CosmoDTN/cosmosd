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

/// Our service record. Without addresses no peer can resolve it, so mdns-sd
/// must fill them in from the host's interfaces (`addr_auto`).
fn announcement(node_id: &str, relay_port: u16) -> Result<ServiceInfo> {
    let host = format!("{}.local.", node_id.replace(' ', "-"));
    Ok(ServiceInfo::new(SERVICE_TYPE, node_id, &host, "", relay_port, None)?.enable_addr_auto())
}

/// Announce this node and discover peers via mDNS.
/// Runs forever — call from a background tokio task.
pub async fn run(node_id: &str, relay_port: u16, peers: Arc<PeerManager>) -> Result<()> {
    let mdns = ServiceDaemon::new()?;

    // 1. Announce ourselves
    mdns.register(announcement(node_id, relay_port)?)?;
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn announcement_gets_host_addresses() {
        let info = announcement("cabin", 37778).unwrap();
        assert!(info.is_addr_auto(), "a record with no addresses never resolves");
        assert_eq!(info.get_port(), 37778);
        assert_eq!(info.get_fullname(), "cabin._cosmosd._tcp.local.");
    }
}
