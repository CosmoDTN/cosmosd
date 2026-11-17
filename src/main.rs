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
//!   cosmosd dev-bundle --out sample.bundle           # Hidden: one sample bundle for interop tests

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

        /// Content ID for a live stream: 1 to 64 of A-Z a-z 0-9 . _ -
        #[arg(long, value_parser = parse_content_id)]
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

    /// Write one sample RFC 9171 segment bundle (--out), or decode a bundle
    /// file and print its fields (--check). For interop tests with other
    /// BPv7 nodes, see scripts/interop-ud3tn.sh.
    #[command(hide = true)]
    DevBundle {
        /// File to write the sample bundle to.
        #[arg(long, required_unless_present = "check", conflicts_with = "check")]
        out: Option<PathBuf>,

        /// Bundle file to decode with wire::decode_bundle.
        #[arg(long)]
        check: Option<PathBuf>,
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
        Command::DevBundle { out, check } => dev_bundle_command(out, check)?,
    }

    Ok(())
}

/// clap parser for `--content-id`.
fn parse_content_id(s: &str) -> Result<String> {
    bundle::check_content_id(s)?;
    Ok(s.to_string())
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

// === Dev bundle ===

/// Content ID, payload and source node of the sample bundle.
const DEV_CONTENT_ID: &str = "interop-sample";
const DEV_PAYLOAD: &[u8] = b"cosmosd interop sample segment\n";
const DEV_SOURCE_NODE: &str = "cosmosd-dev";

fn dev_bundle_command(out: Option<PathBuf>, check: Option<PathBuf>) -> Result<()> {
    match (out, check) {
        (Some(out), None) => {
            std::fs::write(&out, dev_bundle_bytes()?).with_context(|| format!("writing {}", out.display()))?;
            println!("wrote {}", out.display());
        }
        (None, Some(check)) => {
            let bytes = std::fs::read(&check).with_context(|| format!("reading {}", check.display()))?;
            print!("{}", describe_bundle(&bytes)?);
        }
        _ => unreachable!("clap requires exactly one of --out and --check"),
    }
    Ok(())
}

/// One segment bundle as RFC 9171 bytes. The publisher is a throwaway key
/// that is never saved, so nothing will admit the segment. It is for
/// carrying only.
fn dev_bundle_bytes() -> Result<Vec<u8>> {
    let publisher = trust::PublisherKey::generate().key_id();
    let b = bundle::Bundle::segment(
        &publisher,
        DEV_CONTENT_ID,
        "Interop sample",
        0,
        "video/mp2t",
        bundle::Priority::Live,
        DEV_PAYLOAD.to_vec(),
        DEV_SOURCE_NODE,
    )?;
    wire::encode_bundle(&b)
}

/// Decode with `wire::decode_bundle` and list the fields, one `key=value`
/// per line. The first five lines are the identity a carrier must not change.
fn describe_bundle(bytes: &[u8]) -> Result<String> {
    let b = wire::decode_bundle(bytes)?;
    let kind = match b.info.kind {
        bundle::BundleKind::Segment => "segment",
        bundle::BundleKind::Manifest => "manifest",
    };
    Ok(format!(
        "publisher={}\ncontent_id={}\nkind={kind}\nsequence={}\npayload_blake3={}\n\
         payload_len={}\nsource_node={}\nage_ms={}\nhop_count={}\nprevious_node={}\n",
        b.publisher,
        b.info.content_id,
        b.info.sequence,
        blake3::hash(&b.payload).to_hex(),
        b.payload.len(),
        b.source_node,
        b.age_ms,
        b.hop_count,
        b.previous_node.as_deref().unwrap_or("none"),
    ))
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
    let node_id = resolve_node_id(node_id)?;

    auth::check_open_relay(relay_token.is_some(), open_relay, no_discovery)?;

    info!("CosmosD node:  {}", node_id);
    info!("Bundle store:  {}", store_path);
    info!("HTTP API:      http://{}:{}", bind, port);
    info!("Relay port:    {}", relay_port);

    let peers = initial_peers.iter().map(|a| auth::parse_peer(a)).collect::<Result<Vec<_>>>()?;
    let store = Arc::new(BundleStore::open(&store_path)?);
    let (own_key, trust) = load_trust(&key_dir)?;
    // A publisher unpaired since the last run loses its content here.
    let purged = store.purge_untrusted(&*trust)?;
    info!("Untrusted:     removed {} bundles", purged);

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
    let state = ApiState::new(store, node_id, peer_manager, std::time::Duration::from_secs(delay), own_key);
    let app = api::router(state);
    let addr = format!("{}:{}", bind, port);
    let listener = tokio::net::TcpListener::bind(&addr).await?;

    info!("Ready. Endpoints:");
    info!("  GET  http://{}/health", addr);
    info!("  GET  http://{}/catalog", addr);
    info!("  GET  http://{}/peers", addr);
    info!("  GET  http://{}/play/<publisher>/<content_id>", addr);
    info!("  POST http://{}/ingest", addr);
    info!("  POST http://{}/ingest/finish", addr);

    axum::serve(listener, app.into_make_service_with_connect_info::<std::net::SocketAddr>())
        .with_graceful_shutdown(shutdown_signal())
        .await?;
    // Clean stop: persist the expiry clock so held time is not lost.
    flush_store.flush_clock()?;
    info!("Store clock flushed, shutting down");
    Ok(())
}

/// `--node-id`, or the hostname, checked now: it goes into every bundle
/// this node creates or forwards, so a bad one would fail every send.
fn resolve_node_id(node_id: Option<String>) -> Result<String> {
    let node_id = node_id.unwrap_or_else(|| {
        hostname::get()
            .ok()
            .and_then(|h| h.into_string().ok())
            .unwrap_or_else(|| "cosmosd-node".to_string())
    });
    wire::check_node_id(&node_id).context("pass a valid --node-id")?;
    Ok(node_id)
}

/// The node's own key, if it has one, and a `Trust` of that key plus the keys
/// paired in `key_dir`. A missing key dir gives a node that trusts nobody and
/// can not ingest. Pairings are read once, so changes need a restart.
fn load_trust(key_dir: &std::path::Path) -> Result<(Option<trust::PublisherKey>, Arc<trust::Trust>)> {
    let own = trust::PublisherKey::load_if_present(key_dir)?;
    let paired = trust::PairedKeys::open(key_dir)?;
    match &own {
        Some(k) => info!("Publisher key: {}", k.key_id()),
        None => info!("Publisher key: none in {}", key_dir.display()),
    }
    info!("Paired keys:   {}", paired.list().len());
    Ok((own.clone(), Arc::new(trust::Trust::new(own, paired))))
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

    #[test]
    fn node_id_is_checked_at_startup() {
        assert_eq!(resolve_node_id(Some("iss-node-1".into())).unwrap(), "iss-node-1");
        for bad in ["", "a/b"] {
            let err = resolve_node_id(Some(bad.into())).unwrap_err().to_string();
            assert!(err.contains("--node-id"), "{bad:?}: {err}");
        }
        assert!(resolve_node_id(None).is_ok(), "the hostname is a valid node id here");
    }

    #[test]
    fn live_content_id_must_be_valid() {
        let parse = |id: &str| Cli::try_parse_from(["cosmosd", "ingest", "--live", "udp://x", "--content-id", id]);
        assert!(parse("match_1").is_ok());
        for bad in ["bad id", "a/b", ""] {
            assert!(parse(bad).is_err(), "{bad:?}");
        }
    }

    #[test]
    fn dev_bundle_needs_out_or_check() {
        assert!(Cli::try_parse_from(["cosmosd", "dev-bundle"]).is_err());
        assert!(Cli::try_parse_from(["cosmosd", "dev-bundle", "--out", "a", "--check", "b"]).is_err());
        assert!(Cli::try_parse_from(["cosmosd", "dev-bundle", "--out", "a"]).is_ok());
        assert!(Cli::try_parse_from(["cosmosd", "dev-bundle", "--check", "b"]).is_ok());
    }

    #[test]
    fn dev_bundle_writes_a_bundle_both_decoders_accept() {
        let dir = tempfile::tempdir().unwrap();
        let out = dir.path().join("sample.bundle");
        dev_bundle_command(Some(out.clone()), None).unwrap();
        let bytes = std::fs::read(&out).unwrap();

        let ours = wire::decode_bundle(&bytes).expect("wire::decode_bundle accepts the sample");
        assert_eq!(ours.info.kind, bundle::BundleKind::Segment);
        assert_eq!(ours.info.content_id, DEV_CONTENT_ID);
        assert_eq!(ours.payload, DEV_PAYLOAD);
        assert_eq!(ours.source_node, DEV_SOURCE_NODE);

        let stock = bp7::bundle::Bundle::try_from(bytes.as_slice()).expect("stock bp7 decodes the sample");
        stock.validate().expect("stock bp7 validates the sample");
        assert!(stock.extension_block_by_type(wire::CONTENT_BLOCK_TYPE).is_some());
        assert_eq!(stock.payload().unwrap(), &DEV_PAYLOAD.to_vec());
    }

    #[test]
    fn dev_bundle_uses_a_new_key_each_time() {
        let a = wire::decode_bundle(&dev_bundle_bytes().unwrap()).unwrap();
        let b = wire::decode_bundle(&dev_bundle_bytes().unwrap()).unwrap();
        assert_ne!(a.publisher, b.publisher);
    }

    #[test]
    fn describe_bundle_lists_identity_first() {
        let bytes = dev_bundle_bytes().unwrap();
        let b = wire::decode_bundle(&bytes).unwrap();
        let text = describe_bundle(&bytes).unwrap();
        let lines: Vec<&str> = text.lines().collect();
        assert_eq!(lines[0], format!("publisher={}", b.publisher));
        assert_eq!(lines[1], format!("content_id={DEV_CONTENT_ID}"));
        assert_eq!(lines[2], "kind=segment");
        assert_eq!(lines[3], "sequence=0");
        assert_eq!(lines[4], format!("payload_blake3={}", blake3::hash(DEV_PAYLOAD).to_hex()));
        assert!(text.contains("age_ms=0\n"));
        assert!(text.contains("hop_count=0\n"));
        assert!(text.contains("previous_node=none\n"));
        assert!(describe_bundle(b"not a bundle").is_err());
    }
}
