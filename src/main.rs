//! cosmosd — Delay-Tolerant Streaming Daemon
//!
//! Stores HLS video bundles, relays them to peers, and serves them
//! to local apps via HTTP. Part of the CosmosD open-source framework.
//!
//! Usage:
//!   cosmosd serve                  # Start daemon (default port 7777)
//!   cosmosd serve --port 8888      # Custom port
//!   cosmosd serve --store /data    # Custom store path

mod api;
mod auth;
mod bundle;
mod discovery;
mod peer;
mod router;
mod store;
mod transport;

use std::sync::Arc;

use anyhow::Result;
use clap::{Parser, Subcommand};
use tracing::info;
use tracing_subscriber::EnvFilter;

use api::ApiState;
use auth::RelayAuth;
use peer::PeerManager;
use router::EpidemicRouter;
use store::BundleStore;

// === CLI ===

#[derive(Parser)]
#[command(name = "cosmosd", about = "Stream across the void — CosmosD daemon")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Start the cosmosd daemon.
    Serve {
        /// HTTP API port (SDK + ingest connect here).
        #[arg(long, default_value = "7777")]
        port: u16,

        /// TCP relay port — peers connect here for epidemic sync.
        #[arg(long, default_value = "7778")]
        relay_port: u16,

        /// Known peers to sync with (relay addresses, e.g. 192.168.1.5:7778).
        /// Pass multiple times: --peer A --peer B
        #[arg(long = "peer")]
        peers: Vec<String>,

        /// Path to bundle store directory.
        #[arg(long, default_value = ".cosmosd/store")]
        store: String,

        /// Human-readable node ID (defaults to hostname).
        #[arg(long)]
        node_id: Option<String>,

        /// Optional shared secret for relay authentication.
        /// If set, peers must supply the same token to sync.
        #[arg(long)]
        relay_token: Option<String>,

        /// Disable mDNS peer discovery (useful in controlled environments).
        #[arg(long, default_value = "false")]
        no_discovery: bool,
    },
}

// === Entry point ===

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env().add_directive("cosmosd=info".parse()?))
        .init();

    let cli = Cli::parse();

    match cli.command {
        Command::Serve { port, relay_port, store, node_id, peers, relay_token, no_discovery } => {
            serve(port, relay_port, store, node_id, peers, relay_token, no_discovery).await?;
        }
    }

    Ok(())
}

async fn serve(
    port: u16,
    relay_port: u16,
    store_path: String,
    node_id: Option<String>,
    initial_peers: Vec<String>,
    relay_token: Option<String>,
    no_discovery: bool,
) -> Result<()> {
    let node_id = node_id.unwrap_or_else(|| {
        hostname::get()
            .ok()
            .and_then(|h| h.into_string().ok())
            .unwrap_or_else(|| "cosmosd-node".to_string())
    });

    info!("CosmosD node:  {}", node_id);
    info!("Bundle store:  {}", store_path);
    info!("HTTP API:      http://127.0.0.1:{}", port);
    info!("Relay port:    {}", relay_port);

    let store = Arc::new(BundleStore::open(&store_path)?);

    // 1. Relay auth
    let relay_auth = Arc::new(RelayAuth::new(relay_token));

    // 2. Epidemic router — shared by peer manager
    let epidemic_router = Arc::new(EpidemicRouter::new(store.clone(), node_id.clone()));

    // 3. Peer manager — outgoing sync + incoming relay
    let peer_manager = Arc::new(PeerManager::new(epidemic_router, relay_auth, initial_peers));

    // 4. Background: reap expired bundles every 5 minutes
    {
        let store = store.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(std::time::Duration::from_secs(300));
            loop {
                interval.tick().await;
                let _ = store.reap_expired();
            }
        });
    }

    // 5. Background: incoming relay listener (epidemic sync from peers)
    {
        let pm = peer_manager.clone();
        let relay_addr = format!("0.0.0.0:{}", relay_port);
        tokio::spawn(async move {
            if let Err(e) = pm.run_relay_listener(relay_addr).await {
                tracing::error!("Relay listener error: {}", e);
            }
        });
    }

    // 6. Background: outgoing sync loop (push/pull with known peers)
    {
        let pm = peer_manager.clone();
        tokio::spawn(async move {
            pm.run_sync_loop().await;
        });
    }

    // 7. Background: mDNS peer discovery (auto-discover nodes on local network)
    if !no_discovery {
        let pm = peer_manager.clone();
        let nid = node_id.clone();
        tokio::spawn(async move {
            if let Err(e) = discovery::run(&nid, relay_port, pm).await {
                tracing::warn!("mDNS discovery error: {}", e);
            }
        });
        info!("mDNS discovery enabled — auto-discovering peers on local network");
    }

    // 8. HTTP API server (local only)
    let state = ApiState { store, node_id, peers: peer_manager };
    let app = api::router(state);
    let addr = format!("127.0.0.1:{}", port);
    let listener = tokio::net::TcpListener::bind(&addr).await?;

    info!("Ready. Endpoints:");
    info!("  GET  http://{}/health", addr);
    info!("  GET  http://{}/catalog", addr);
    info!("  GET  http://{}/peers", addr);
    info!("  POST http://{}/ingest", addr);

    axum::serve(listener, app).await?;
    Ok(())
}
