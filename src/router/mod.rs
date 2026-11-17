//! Epidemic routing — automatic bundle replication between peers.
//!
//! When two cosmosd nodes connect, they exchange manifests (what bundles
//! each has), compute the diff, and pull missing bundles. This is how
//! content spreads through the mesh without any central coordination.
//!
//! Protocol v3 (CBOR frames, over any stream on the relay port):
//!   1. Both sides send their manifest (version, relay port, and per bundle
//!      its id, priority and content manifest version)
//!   2. Initiator works out what it lacks, plus content manifests newer than
//!      its own, orders them manifests first then live-first, and sends a
//!      pull request
//!   3. Responder streams requested bundles, in that order. Each bundle is
//!      RFC 9171 bytes (`crate::wire`) inside a CBOR byte string.
//!   4. Connection closes
//!
//! Every received bundle goes through `BundleStore::admit`, so a segment is
//! kept only when a signed manifest from a trusted publisher vouches for it.
//!
//! Every read is bounded by `FRAME_TIMEOUT`, so a blackholed link fails the
//! session instead of hanging it.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{anyhow, bail, Result};
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::time::timeout;
use tracing::{debug, info, warn};

use crate::auth::authenticate_outgoing;
use crate::codec::{self, PROTOCOL_VERSION};
use crate::store::{Admission, BundleStore};
use crate::trust::KeyLookup;
use crate::wire;

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
const READ_CHUNK: usize = 64 * 1024;

async fn send_frame<S: AsyncWrite + Unpin>(stream: &mut S, data: &[u8]) -> Result<()> {
    let len = data.len() as u32;
    stream.write_all(&len.to_be_bytes()).await?;
    stream.write_all(data).await?;
    stream.flush().await?;
    Ok(())
}

/// Read exactly `buf.len()` bytes; every individual read must make progress
/// within `FRAME_TIMEOUT` (an idle timeout, so a slow but live link is fine).
async fn read_idle<S: AsyncRead + Unpin>(stream: &mut S, buf: &mut [u8]) -> Result<()> {
    let mut filled = 0;
    while filled < buf.len() {
        let end = buf.len().min(filled + READ_CHUNK);
        let n = timeout(FRAME_TIMEOUT, stream.read(&mut buf[filled..end]))
            .await
            .map_err(|_| anyhow!("timed out: no bytes from peer for {:?}", FRAME_TIMEOUT))??;
        if n == 0 {
            bail!("connection closed mid-frame");
        }
        filled += n;
    }
    Ok(())
}

/// Read one frame. Gives up after `FRAME_TIMEOUT` of silence, not of total
/// transfer time, so big bundles survive slow links and dead links still fail.
async fn recv_frame<S: AsyncRead + Unpin>(stream: &mut S) -> Result<Vec<u8>> {
    let mut len_buf = [0u8; 4];
    read_idle(stream, &mut len_buf).await?;
    let len = u32::from_be_bytes(len_buf) as usize;
    if len > MAX_FRAME {
        bail!("Frame too large: {} bytes", len);
    }
    let mut buf = vec![0u8; len];
    read_idle(stream, &mut buf).await?;
    Ok(buf)
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
    /// Publishers whose bundles we accept from peers.
    trust: Arc<dyn KeyLookup + Send + Sync>,
}

impl EpidemicRouter {
    pub fn new(
        store: Arc<BundleStore>,
        node_id: String,
        relay_port: u16,
        delay: Duration,
        trust: Arc<dyn KeyLookup + Send + Sync>,
    ) -> Self {
        EpidemicRouter { store, node_id, relay_port, delay, trust }
    }

    /// Our manifest: every valid bundle we hold, with its priority.
    fn manifest(&self) -> Result<Manifest> {
        let entries = self
            .store
            .all_meta()?
            .into_iter()
            .map(|m| ManifestEntry { id: m.id, priority: m.priority, version: m.version })
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
    pub async fn sync_with_peer(&self, addr: &str, token: Option<&str>) -> Result<SyncStats> {
        debug!("Connecting to peer relay: {}", addr);
        let mut stream = timeout(CONNECT_TIMEOUT, TcpStream::connect(addr))
            .await
            .map_err(|_| anyhow!("timed out connecting to {}", addr))??;
        authenticate_outgoing(&mut stream, token).await?;
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
        let our_ids: HashMap<String, u64> = self.store.all_meta()?.into_iter().map(|m| (m.id, m.version)).collect();
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

        // 5. Receive requested bundles and pass each through the admission gate
        let mut received = 0usize;
        let (mut held, mut rejected) = (Vec::new(), 0usize);
        for id in &missing {
            let raw = recv_frame(&mut stream).await?;
            if raw.is_empty() {
                debug!("Peer {} doesn't have bundle {}", peer_id, id);
                continue;
            }
            let decoded = codec::decode::<serde_bytes::ByteBuf>(&raw).and_then(|bytes| wire::decode_bundle(&bytes));
            let bundle = match decoded {
                Ok(b) => b,
                Err(e) => {
                    warn!("Bad bundle {} from {}: {}", id, peer_id, e);
                    continue;
                }
            };
            match self.store.admit(&bundle, &*self.trust)? {
                Admission::Stored => received += 1,
                Admission::Duplicate => debug!("Already have bundle {}", id),
                Admission::Held => held.push(id),
                Admission::Rejected(reason) => {
                    warn!("Rejected bundle {} from {}: {}", id, peer_id, reason);
                    rejected += 1;
                }
            }
        }
        // Held segments that a manifest later in this session promoted count as received.
        let promoted = if held.is_empty() {
            0
        } else {
            let ours = self.store.all_ids()?;
            held.iter().filter(|id| ours.contains(id.as_str())).count()
        };
        received += promoted;

        info!(
            "Sync complete with {}: received {} bundles, {} held for a manifest, {} rejected",
            peer_id,
            received,
            held.len() - promoted,
            rejected
        );
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
                    let bytes = wire::encode_bundle(&bundle)?;
                    send_frame(&mut stream, &codec::encode(&serde_bytes::Bytes::new(&bytes))?).await?;
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
    use crate::bundle::{Bundle, Priority};
    use crate::trust::manifest::{Manifest as ContentManifest, SegmentEntry, SignedManifest};
    use crate::trust::{KeyId, PublisherKey};

    /// Trusts exactly the keys it holds.
    struct Keys(Vec<PublisherKey>);

    impl KeyLookup for Keys {
        fn lookup(&self, id: &KeyId) -> Option<[u8; 32]> {
            self.0.iter().find(|k| k.key_id() == *id).map(|k| k.public())
        }
    }

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
        router_trusting(name, port, vec![])
    }

    fn router_trusting(name: &str, port: u16, keys: Vec<PublisherKey>) -> TestRouter {
        let dir = tempfile::tempdir().unwrap();
        let store = Arc::new(BundleStore::open(dir.path().to_str().unwrap()).unwrap());
        let router = EpidemicRouter::new(store, name.to_string(), port, Duration::from_secs(120), Arc::new(Keys(keys)));
        TestRouter { router, _dir: dir }
    }

    fn live_payload(seq: u32) -> Vec<u8> {
        vec![seq as u8; 64]
    }

    fn live_bundle(key: &PublisherKey, content: &str, seq: u32) -> Bundle {
        Bundle::segment(&key.key_id(), content, "Match", seq, "video/mp2t", Priority::Live, live_payload(seq), "ground")
            .unwrap()
    }

    /// A signed live manifest, version 1, listing `seqs` with their honest hashes.
    fn live_manifest(key: &PublisherKey, content: &str, seqs: std::ops::Range<u32>) -> Bundle {
        live_manifest_v(key, content, 1, seqs)
    }

    fn live_manifest_v(key: &PublisherKey, content: &str, version: u64, seqs: std::ops::Range<u32>) -> Bundle {
        let m = ContentManifest {
            publisher: key.key_id(),
            content_id: content.into(),
            title: "Match".into(),
            version,
            created_at_ms: 1_700_000_000_000,
            live: true,
            segments: seqs
                .map(|s| SegmentEntry { sequence: s, hash: *blake3::hash(&live_payload(s)).as_bytes(), duration_ms: 10_000 })
                .collect(),
        };
        let signed = SignedManifest::sign(&m, key).unwrap().to_bytes().unwrap();
        Bundle::manifest(&key.key_id(), &m, signed, "ground")
    }

    #[tokio::test]
    async fn sync_pulls_missing_bundles() {
        let key = PublisherKey::generate();
        let ground = router("ground", 17778);
        let gateway = router_trusting("gateway", 27778, vec![key.clone()]);
        ground.store.insert(&live_manifest(&key, "m", 0..3)).unwrap();
        for s in 0..3 {
            ground.store.insert(&live_bundle(&key, "m", s)).unwrap();
        }
        let (a, b) = tokio::io::duplex(1 << 20);
        let (got, served) = tokio::join!(gateway.initiate(a), ground.handle_incoming(b));
        // The manifest, pulled first, plus three segments.
        assert_eq!(got.unwrap().received, 4);
        assert_eq!(served.unwrap().sent, 4);
        let id = format!("{}/m/00002", key.key_id());
        let got = gateway.store.get(&id).unwrap().expect("pulled bundle");
        assert_eq!(got.payload, live_payload(2));
        assert_eq!(got.info.sequence, 2);
    }

    #[tokio::test]
    async fn router_rejects_tampered_segment_over_a_real_sync() {
        let key = PublisherKey::generate();
        let ground = router("ground", 17778);
        let gateway = router_trusting("gateway", 27778, vec![key.clone()]);
        let tampered = |seq: u32| {
            let mut b = live_bundle(&key, "m", seq);
            b.payload[10] ^= 0x01;
            b
        };

        // The manifest lists 0..4 with honest hashes. Segment 2 is flipped on
        // the sending side. The manifest is pulled first, so each segment is
        // checked as it arrives. The held path is covered in the store tests.
        ground.store.insert(&live_manifest(&key, "m", 0..4)).unwrap();
        for s in 0..2 {
            ground.store.insert(&live_bundle(&key, "m", s)).unwrap();
        }
        ground.store.insert(&tampered(2)).unwrap();
        let (a, b) = tokio::io::duplex(1 << 20);
        let (got, _) = tokio::join!(gateway.initiate(a), ground.handle_incoming(b));
        assert_eq!(got.unwrap().received, 3);

        // The manifest is now in place on the receiver. Segment 3 arrives
        // flipped and must be refused straight away.
        ground.store.insert(&tampered(3)).unwrap();
        let (a, b) = tokio::io::duplex(1 << 20);
        let (got, _) = tokio::join!(gateway.initiate(a), ground.handle_incoming(b));
        assert_eq!(got.unwrap().received, 0);

        let id = |s: u32| format!("{}/m/{:05}", key.key_id(), s);
        assert!(gateway.store.get(&id(0)).unwrap().is_some());
        assert!(gateway.store.get(&id(1)).unwrap().is_some());
        assert!(gateway.store.get(&id(2)).unwrap().is_none(), "tampered segment 2 stored");
        assert!(gateway.store.get(&id(3)).unwrap().is_none(), "tampered segment stored");
    }

    /// One session with `from` serving `to`. Returns (received, sent).
    async fn sync(to: &EpidemicRouter, from: &EpidemicRouter) -> (usize, usize) {
        let (a, b) = tokio::io::duplex(1 << 20);
        let (got, served) = tokio::join!(to.initiate(a), from.handle_incoming(b));
        (got.unwrap().received, served.unwrap().sent)
    }

    /// The version of the content manifest a router holds for `m`.
    fn held_version(r: &EpidemicRouter, key: &PublisherKey) -> u64 {
        let b = r.store.get(&format!("{}/m/manifest", key.key_id())).unwrap().expect("manifest held");
        let m: ContentManifest = codec::decode(&SignedManifest::from_bytes(&b.payload).unwrap().manifest_cbor).unwrap();
        m.version
    }

    #[tokio::test]
    async fn peer_pulls_newer_manifest_version() {
        let key = PublisherKey::generate();
        let keys = Keys(vec![key.clone()]);
        let a = router_trusting("a", 17778, vec![key.clone()]);
        let b = router("b", 27778);
        assert_eq!(a.store.admit(&live_manifest_v(&key, "m", 1, 0..1), &keys).unwrap(), Admission::Stored);
        b.store.insert(&live_manifest_v(&key, "m", 2, 0..2)).unwrap();
        assert_eq!(sync(&a, &b).await, (1, 1));
        assert_eq!(held_version(&a, &key), 2);
    }

    #[tokio::test]
    async fn peer_does_not_pull_equal_or_older_manifest() {
        let key = PublisherKey::generate();
        let keys = Keys(vec![key.clone()]);
        let a = router_trusting("a", 17778, vec![key.clone()]);
        assert_eq!(a.store.admit(&live_manifest_v(&key, "m", 2, 0..1), &keys).unwrap(), Admission::Stored);
        for v in [1, 2] {
            let b = router("b", 27778);
            b.store.insert(&live_manifest_v(&key, "m", v, 0..3)).unwrap();
            assert_eq!(sync(&a, &b).await, (0, 0), "version {v}");
        }
        assert_eq!(held_version(&a, &key), 2);
    }

    #[tokio::test]
    async fn live_sequence_stores_segment_added_by_newer_manifest() {
        let key = PublisherKey::generate();
        let keys = Keys(vec![key.clone()]);
        let a = router_trusting("a", 17778, vec![key.clone()]);
        let b = router_trusting("b", 27778, vec![key.clone()]);
        // B publishes version 1 with segments 0..2 (sequences 0, 1 and 2).
        assert_eq!(b.store.admit(&live_manifest_v(&key, "m", 1, 0..3), &keys).unwrap(), Admission::Stored);
        for s in 0..3 {
            assert_eq!(b.store.admit(&live_bundle(&key, "m", s), &keys).unwrap(), Admission::Stored);
        }
        assert_eq!(sync(&a, &b).await, (4, 4));

        // B publishes version 2 adding segment 3.
        assert_eq!(b.store.admit(&live_manifest_v(&key, "m", 2, 0..4), &keys).unwrap(), Admission::Stored);
        assert_eq!(b.store.admit(&live_bundle(&key, "m", 3), &keys).unwrap(), Admission::Stored);
        assert_eq!(sync(&a, &b).await, (2, 2));
        assert_eq!(held_version(&a, &key), 2);
        let got = a.store.get(&format!("{}/m/00003", key.key_id())).unwrap().expect("segment 3 stored");
        assert_eq!(got.payload, live_payload(3));
    }

    #[tokio::test]
    async fn sync_rejects_unpaired_publisher() {
        let key = PublisherKey::generate();
        let ground = router("ground", 17778);
        let gateway = router("gateway", 27778);
        ground.store.insert(&live_manifest(&key, "m", 0..1)).unwrap();
        ground.store.insert(&live_bundle(&key, "m", 0)).unwrap();
        let (a, b) = tokio::io::duplex(1 << 20);
        let (got, _) = tokio::join!(gateway.initiate(a), ground.handle_incoming(b));
        assert_eq!(got.unwrap().received, 0);
        assert!(gateway.store.all_ids().unwrap().is_empty());
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
        assert!(err.contains("v1") && err.contains("v3"), "{}", err);
        drop(peer.await);
    }

    #[tokio::test(start_paused = true)]
    async fn initiate_times_out_on_stalled_peer() {
        let gateway = router("gateway", 27778);
        let (a, _b) = tokio::io::duplex(1 << 16); // peer never answers, never closes
        let err = gateway.initiate(a).await.unwrap_err().to_string();
        assert!(err.contains("timed out"), "{}", err);
    }

    #[tokio::test(start_paused = true)]
    async fn slow_but_live_frame_is_received() {
        let (mut a, mut b) = tokio::io::duplex(1 << 20);
        let payload = vec![7u8; 500_000];
        let sent = payload.clone();
        tokio::spawn(async move {
            let mut framed = (sent.len() as u32).to_be_bytes().to_vec();
            framed.extend_from_slice(&sent);
            for piece in framed.chunks(100_000) {
                a.write_all(piece).await.unwrap();
                tokio::time::sleep(Duration::from_secs(20)).await;
            }
            a
        });
        let start = tokio::time::Instant::now();
        let got = recv_frame(&mut b).await.unwrap();
        assert_eq!(got, payload);
        assert!(start.elapsed() > FRAME_TIMEOUT);
    }
}
