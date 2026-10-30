//! cosmosd — Delay-Tolerant Streaming Daemon
//!
//! Stores HLS video bundles, relays them to peers, and serves them
//! to local apps via HTTP. Part of the CosmosD open-source framework.
//!
//! Usage:
//!   cosmosd serve                                    # Start daemon (default port 7777)
//!   cosmosd serve --port 8888                        # Custom port
//!   cosmosd serve --bind 0.0.0.0                     # Expose API on all interfaces
//!   cosmosd ingest /path/to/movie.mkv                # Ingest a local file
//!   cosmosd ingest /path/to/movie.mkv --title "Dune" # With custom title
//!   cosmosd ingest --live udp://127.0.0.1:5000 --content-id demo  # Live stream

mod api;
mod auth;
mod bundle;
// Used by the store, router and API from Tasks 2/4 onward.
mod codec;
mod discovery;
mod ingest;
mod peer;
mod router;
mod store;
mod transport;

use std::path::PathBuf;
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
        /// Pass multiple times: --peer A --peer B.
        /// A token follows the first `@` (host:port@token); it is presented
        /// only to that peer.
        #[arg(long = "peer")]
        peers: Vec<String>,

        /// Path to bundle store directory.
        #[arg(long, default_value = ".cosmosd/store")]
        store: String,

        /// Human-readable node ID (defaults to hostname).
        #[arg(long)]
        node_id: Option<String>,

        /// Optional shared secret guarding our relay port: peers that dial us
        /// must present it. Tokens for peers we dial go in `--peer host:port@token`.
        #[arg(long)]
        relay_token: Option<String>,

        /// Disable peer discovery: no mDNS, and peers that connect to us are not
        /// dialed back. Only --peer and POST /peers peers are synced with.
        #[arg(long, default_value = "false")]
        no_discovery: bool,

        /// Run the relay without a --relay-token even if this machine has a
        /// public IPv4 address.
        #[arg(long, default_value = "false")]
        open_relay: bool,

        /// Bind address for the HTTP API.
        /// Use 0.0.0.0 to expose the API on all interfaces (e.g. for remote access).
        #[arg(long, default_value = "127.0.0.1")]
        bind: String,

        /// Seconds of delay the link tolerates before a bundle is considered late.
        #[arg(long, default_value = "120")]
        delay: u64,

        /// Seconds between syncs with a healthy peer.
        #[arg(long, default_value = "5")]
        sync_interval: u64,
    },

    /// Ingest a local video file, or a live stream (--live), into the running
    /// cosmosd daemon. Requires ffmpeg to be installed. The daemon must be running.
    Ingest {
        /// Path to the video file (mkv, mp4, avi, etc.).
        #[arg(required_unless_present = "live", conflicts_with = "live")]
        file: Option<PathBuf>,

        /// Live input URL for ffmpeg (e.g. udp://127.0.0.1:5000). Needs --content-id.
        #[arg(long, requires = "content_id")]
        live: Option<String>,

        /// Content ID for a live stream.
        #[arg(long)]
        content_id: Option<String>,

        /// Content title. Defaults to the filename stem.
        #[arg(long)]
        title: Option<String>,

        /// cosmosd HTTP API URL.
        #[arg(long, default_value = "http://127.0.0.1:7777")]
        daemon: String,

        /// Bundle priority: vod, near_live, or live.
        #[arg(long, default_value = "vod")]
        priority: String,

        /// Live only: total bitrate cap in kbps (video + 128 kbps audio).
        #[arg(long, default_value = "1200", value_parser = clap::value_parser!(u32).range(400..))]
        max_bitrate: u32,
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
        Command::Serve { port, relay_port, store, node_id, peers, relay_token, no_discovery, open_relay, bind, delay, sync_interval } => {
            serve(port, relay_port, store, node_id, peers, relay_token, no_discovery, open_relay, bind, delay, sync_interval).await?;
        }
        Command::Ingest { file, live, content_id, title, daemon, priority, max_bitrate } => {
            match (file, live) {
                (_, Some(url)) => {
                    let content_id = content_id.expect("clap enforces --content-id with --live");
                    let title = title.unwrap_or_else(|| content_id.clone());
                    ingest::ingest_live(url, content_id, title, daemon, max_bitrate).await?;
                }
                (Some(file), None) => ingest::ingest_file(file, title, daemon, priority).await?,
                (None, None) => unreachable!("clap requires FILE or --live"),
            }
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
    open_relay: bool,
    bind: String,
    delay: u64,
    sync_interval: u64,
) -> Result<()> {
    let node_id = node_id.unwrap_or_else(|| {
        hostname::get()
            .ok()
            .and_then(|h| h.into_string().ok())
            .unwrap_or_else(|| "cosmosd-node".to_string())
    });

    auth::check_open_relay(relay_token.is_some(), open_relay)?;

    info!("CosmosD node:  {}", node_id);
    info!("Bundle store:  {}", store_path);
    info!("HTTP API:      http://{}:{}", bind, port);
    info!("Relay port:    {}", relay_port);

    let store = Arc::new(BundleStore::open(&store_path)?);

    // 1. Relay auth
    let relay_auth = Arc::new(RelayAuth::new(relay_token));

    // 2. Epidemic router — shared by peer manager
    let epidemic_router = Arc::new(EpidemicRouter::new(
        store.clone(),
        node_id.clone(),
        relay_port,
        std::time::Duration::from_secs(delay),
    ));

    // 3. Peer manager — outgoing sync + incoming relay
    let peer_manager = Arc::new(PeerManager::new(
        epidemic_router,
        relay_auth,
        initial_peers.iter().map(|a| auth::parse_peer(a)).collect(),
        std::time::Duration::from_secs(sync_interval),
    )
    .with_incoming_registration(!no_discovery));

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

    // 8. HTTP API server
    let state = ApiState { store, node_id, peers: peer_manager, delay: std::time::Duration::from_secs(delay) };
    let app = api::router(state);
    let addr = format!("{}:{}", bind, port);
    let listener = tokio::net::TcpListener::bind(&addr).await?;

    info!("Ready. Endpoints:");
    info!("  GET  http://{}/health", addr);
    info!("  GET  http://{}/catalog", addr);
    info!("  GET  http://{}/peers", addr);
    info!("  POST http://{}/ingest", addr);

    axum::serve(listener, app.into_make_service_with_connect_info::<std::net::SocketAddr>()).await?;
    Ok(())
}
