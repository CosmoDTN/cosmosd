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

mod api;
mod auth;
mod bundle;
mod discovery;
mod peer;
mod router;
mod store;
mod transport;

use std::path::PathBuf;
use std::sync::Arc;

use anyhow::{Context, Result};
use base64::Engine;
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

        /// Bind address for the HTTP API.
        /// Use 0.0.0.0 to expose the API on all interfaces (e.g. for remote access).
        #[arg(long, default_value = "127.0.0.1")]
        bind: String,
    },

    /// Ingest a local video file into the running cosmosd daemon.
    /// Requires ffmpeg to be installed. The daemon must be running.
    Ingest {
        /// Path to the video file (mkv, mp4, avi, etc.).
        file: PathBuf,

        /// Content title. Defaults to the filename stem.
        #[arg(long)]
        title: Option<String>,

        /// cosmosd HTTP API URL.
        #[arg(long, default_value = "http://127.0.0.1:7777")]
        daemon: String,

        /// Bundle priority: vod, near_live, or live.
        #[arg(long, default_value = "vod")]
        priority: String,
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
        Command::Serve { port, relay_port, store, node_id, peers, relay_token, no_discovery, bind } => {
            serve(port, relay_port, store, node_id, peers, relay_token, no_discovery, bind).await?;
        }
        Command::Ingest { file, title, daemon, priority } => {
            ingest_file(file, title, daemon, priority).await?;
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
    bind: String,
) -> Result<()> {
    let node_id = node_id.unwrap_or_else(|| {
        hostname::get()
            .ok()
            .and_then(|h| h.into_string().ok())
            .unwrap_or_else(|| "cosmosd-node".to_string())
    });

    info!("CosmosD node:  {}", node_id);
    info!("Bundle store:  {}", store_path);
    info!("HTTP API:      http://{}:{}", bind, port);
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

    // 8. HTTP API server
    let state = ApiState { store, node_id, peers: peer_manager };
    let app = api::router(state);
    let addr = format!("{}:{}", bind, port);
    let listener = tokio::net::TcpListener::bind(&addr).await?;

    info!("Ready. Endpoints:");
    info!("  GET  http://{}/health", addr);
    info!("  GET  http://{}/catalog", addr);
    info!("  GET  http://{}/peers", addr);
    info!("  POST http://{}/ingest", addr);

    axum::serve(listener, app).await?;
    Ok(())
}

// === Ingest subcommand ===

async fn ingest_file(
    file: PathBuf,
    title: Option<String>,
    daemon: String,
    priority: String,
) -> Result<()> {
    let file = file.canonicalize().context("File not found")?;
    let title = title.unwrap_or_else(|| {
        file.file_stem()
            .unwrap_or_default()
            .to_string_lossy()
            .to_string()
    });
    let content_id = file
        .file_stem()
        .unwrap_or_default()
        .to_string_lossy()
        .to_lowercase()
        .replace(' ', "_")
        .chars()
        .filter(|c| c.is_alphanumeric() || *c == '_')
        .take(40)
        .collect::<String>();

    // 1. Check ffmpeg is available
    which_ffmpeg().context("ffmpeg not found — install it: apt install ffmpeg / brew install ffmpeg")?;

    // 2. Check daemon is reachable
    let client = reqwest::Client::new();
    client
        .get(format!("{}/health", daemon))
        .send()
        .await
        .context("Cannot reach cosmosd — is it running? cosmosd serve")?;

    info!("Ingesting: {}", title);
    info!("File:      {}", file.display());
    info!("Content ID:{}", content_id);
    info!("Daemon:    {}", daemon);

    // 3. Chunk into HLS segments with ffmpeg
    let tmp = tempfile::tempdir()?;
    let hls_dir = tmp.path();

    info!("Chunking with ffmpeg...");
    let status = std::process::Command::new("ffmpeg")
        .args([
            "-y", "-i", file.to_str().unwrap(),
            "-map", "0:v:0",   // first video stream only
            "-map", "0:a:0",   // first audio stream only
            "-codec", "copy",
            "-hls_time", "10",
            "-hls_list_size", "0",
            "-hls_segment_filename", &format!("{}/seg%05d.ts", hls_dir.display()),
            "-f", "hls",
            &format!("{}/index.m3u8", hls_dir.display()),
        ])
        .status()
        .context("ffmpeg failed to run")?;

    anyhow::ensure!(status.success(), "ffmpeg exited with error");

    // 4. Push each segment to cosmosd
    let mut segments: Vec<_> = std::fs::read_dir(hls_dir)?
        .filter_map(|e| e.ok())
        .filter(|e| e.path().extension().map(|x| x == "ts").unwrap_or(false))
        .map(|e| e.path())
        .collect();
    segments.sort();

    info!("Pushing {} segments to {}...", segments.len(), daemon);

    for (i, seg_path) in segments.iter().enumerate() {
        let payload = std::fs::read(seg_path)?;
        let b64 = base64::engine::general_purpose::STANDARD.encode(&payload);

        let resp = client
            .post(format!("{}/ingest", daemon))
            .json(&serde_json::json!({
                "content_id":    content_id,
                "content_title": title,
                "sequence":      i,
                "mime_type":     "video/mp2t",
                "priority":      priority,
                "payload_b64":   b64,
            }))
            .send()
            .await
            .with_context(|| format!("Failed to push segment {}", i))?;

        let data: serde_json::Value = resp.json().await?;
        info!("  seg{:05} → {} (stored={})", i, data["bundle_id"], data["stored"]);
    }

    info!("Done: {} ({} segments)", title, segments.len());
    info!("Play: ffplay {}/play/{}", daemon, content_id);
    Ok(())
}

fn which_ffmpeg() -> Option<()> {
    std::process::Command::new("ffmpeg")
        .arg("-version")
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status()
        .ok()
        .filter(|s| s.success())
        .map(|_| ())
}
