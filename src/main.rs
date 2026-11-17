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
//!   cosmosd keys init                                # Create a publisher key
//!   cosmosd keys show                                # Print pairing string and key ID
//!   cosmosd keys pair cosmos-key:...                 # Trust a publisher

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
mod trust;
mod wire;

use std::path::PathBuf;
use std::sync::Arc;

use anyhow::{Context, Result};
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
        #[arg(long, value_parser = auth::non_blank_token)]
        relay_token: Option<String>,

        /// Disable peer discovery: no mDNS, and peers that connect to us are not
        /// dialed back. Only --peer and POST /peers peers are synced with.
        #[arg(long, default_value = "false")]
        no_discovery: bool,

        /// Run the relay without a --relay-token even with --no-discovery (the
        /// ground role) or on a machine that has a public IPv4 address.
        #[arg(long, default_value = "false")]
        open_relay: bool,

        /// Bind address for the HTTP API.
        /// Use 0.0.0.0 to expose the API on all interfaces (e.g. for remote access).
        #[arg(long, default_value = "127.0.0.1")]
        bind: String,

        /// Seconds the viewer plays behind live (live playlists list only
        /// segments this old). Also the window of newest segments a gateway
        /// pulls first. Use the same value on the gateway and cabin nodes.
        #[arg(long, default_value = "120")]
        delay: u64,

        /// Seconds between syncs with a healthy peer.
        #[arg(long, default_value = "5")]
        sync_interval: u64,

        /// Key directory (default $HOME/.config/cosmosd). Bundles from peers
        /// are accepted only from this node's key and the keys paired here.
        #[arg(long)]
        key_dir: Option<PathBuf>,
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

    /// Manage publisher keys and the keys this node trusts.
    Keys {
        /// Key directory (default $HOME/.config/cosmosd).
        #[arg(long, global = true)]
        key_dir: Option<PathBuf>,

        #[command(subcommand)]
        action: KeysCmd,
    },
}

#[derive(Subcommand)]
enum KeysCmd {
    /// Create this node's publisher key.
    Init,
    /// Print this node's pairing string and key ID.
    Show,
    /// Trust a publisher, given its pairing string.
    Pair { pairing: String },
    /// Stop trusting a publisher, given its key ID.
    Unpair { key_id: String },
    /// List the paired key IDs.
    List,
}

// === Entry point ===

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env().add_directive("cosmosd=info".parse()?))
        .init();

    let cli = Cli::parse();

    match cli.command {
        Command::Serve { port, relay_port, store, node_id, peers, relay_token, no_discovery, open_relay, bind, delay, sync_interval, key_dir } => {
            let key_dir = match key_dir {
                Some(d) => d,
                None => default_key_dir()?,
            };
            serve(port, relay_port, store, node_id, peers, relay_token, no_discovery, open_relay, bind, delay, sync_interval, key_dir).await?;
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
        Command::Keys { key_dir, action } => keys_command(key_dir, action)?,
    }

    Ok(())
}

fn default_key_dir() -> Result<PathBuf> {
    let home = std::env::var_os("HOME").ok_or_else(|| anyhow::anyhow!("HOME is not set, pass --key-dir"))?;
    Ok(PathBuf::from(home).join(".config/cosmosd"))
}

fn keys_command(key_dir: Option<PathBuf>, action: KeysCmd) -> Result<()> {
    use trust::{pairing_string, parse_pairing_string, KeyId, PairedKeys, PublisherKey};

    let dir = match key_dir {
        Some(d) => d,
        None => default_key_dir()?,
    };
    match action {
        KeysCmd::Init => {
            let key = PublisherKey::generate();
            // save refuses to overwrite, so an existing key is never clobbered.
            key.save(&dir).with_context(|| format!("a publisher key may already exist in {}", dir.display()))?;
            println!("created publisher key {}", key.key_id());
        }
        KeysCmd::Show => {
            let key = PublisherKey::load(&dir)?;
            println!("{}", pairing_string(&key.public()));
            println!("key id: {}", key.key_id());
        }
        KeysCmd::Pair { pairing } => {
            let id = PairedKeys::open(&dir)?.pair(parse_pairing_string(&pairing)?)?;
            println!("paired {id}");
        }
        KeysCmd::Unpair { key_id } => {
            if PairedKeys::open(&dir)?.unpair(&KeyId(key_id.clone()))? {
                println!("unpaired {key_id}");
            } else {
                anyhow::bail!("no paired key {key_id}");
            }
        }
        KeysCmd::List => {
            for id in PairedKeys::open(&dir)?.list() {
                println!("{id}");
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
    key_dir: PathBuf,
) -> Result<()> {
    let node_id = node_id.unwrap_or_else(|| {
        hostname::get()
            .ok()
            .and_then(|h| h.into_string().ok())
            .unwrap_or_else(|| "cosmosd-node".to_string())
    });

    auth::check_open_relay(relay_token.is_some(), open_relay, no_discovery)?;

    info!("CosmosD node:  {}", node_id);
    info!("Bundle store:  {}", store_path);
    info!("HTTP API:      http://{}:{}", bind, port);
    info!("Relay port:    {}", relay_port);

    let peers = initial_peers.iter().map(|a| auth::parse_peer(a)).collect::<Result<Vec<_>>>()?;
    let store = Arc::new(BundleStore::open(&store_path)?);
    let trust = load_trust(&key_dir)?;

    // 1. Relay auth
    let relay_auth = Arc::new(RelayAuth::new(relay_token));

    // 2. Epidemic router — shared by peer manager
    let epidemic_router = Arc::new(EpidemicRouter::new(
        store.clone(),
        node_id.clone(),
        relay_port,
        std::time::Duration::from_secs(delay),
        trust,
    ));

    // 3. Peer manager — outgoing sync + incoming relay
    let peer_manager = Arc::new(PeerManager::new(
        epidemic_router,
        relay_auth,
        peers,
        std::time::Duration::from_secs(sync_interval),
    )
    .with_incoming_registration(!no_discovery));

    // 4. Background: reap expired bundles and flush the expiry clock every
    //    5 minutes. There is no clean shutdown path yet, so the flush here
    //    bounds the time lost on exit.
    {
        let store = store.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(std::time::Duration::from_secs(300));
            loop {
                interval.tick().await;
                let _ = store.reap_expired();
                if let Err(e) = store.flush_clock() {
                    tracing::warn!("Failed to flush the store clock: {}", e);
                }
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
    let flush_store = store.clone();
    let state = ApiState { store, node_id, peers: peer_manager, delay: std::time::Duration::from_secs(delay) };
    let app = api::router(state);
    let addr = format!("{}:{}", bind, port);
    let listener = tokio::net::TcpListener::bind(&addr).await?;

    info!("Ready. Endpoints:");
    info!("  GET  http://{}/health", addr);
    info!("  GET  http://{}/catalog", addr);
    info!("  GET  http://{}/peers", addr);
    info!("  POST http://{}/ingest", addr);

    axum::serve(listener, app.into_make_service_with_connect_info::<std::net::SocketAddr>())
        .with_graceful_shutdown(shutdown_signal())
        .await?;
    // Clean stop: persist the expiry clock so held time is not lost.
    flush_store.flush_clock()?;
    info!("Store clock flushed, shutting down");
    Ok(())
}

/// The node's own key, if it has one, plus the keys paired in `key_dir`. A
/// missing key dir gives a node that trusts nobody.
fn load_trust(key_dir: &std::path::Path) -> Result<Arc<trust::Trust>> {
    let own = trust::PublisherKey::load_if_present(key_dir)?;
    let paired = trust::PairedKeys::open(key_dir)?;
    match &own {
        Some(k) => info!("Publisher key: {}", k.key_id()),
        None => info!("Publisher key: none in {}", key_dir.display()),
    }
    info!("Paired keys:   {}", paired.list().len());
    Ok(Arc::new(trust::Trust::new(own, paired)))
}

/// Resolves on Ctrl+C or, on unix, SIGTERM.
async fn shutdown_signal() {
    let ctrl_c = async {
        let _ = tokio::signal::ctrl_c().await;
    };
    #[cfg(unix)]
    let term = async {
        match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
            Ok(mut s) => {
                s.recv().await;
            }
            Err(_) => std::future::pending::<()>().await,
        }
    };
    #[cfg(not(unix))]
    let term = std::future::pending::<()>();
    tokio::select! {
        _ = ctrl_c => {},
        _ = term => {},
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn relay_token_must_not_be_blank() {
        for token in ["", "   "] {
            assert!(Cli::try_parse_from(["cosmosd", "serve", "--relay-token", token]).is_err(), "{token:?}");
        }
        assert!(Cli::try_parse_from(["cosmosd", "serve", "--relay-token", "s3cret"]).is_ok());
    }
}
