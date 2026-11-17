//! Local HTTP API — used by cosmoscast-sdk and cosmoscast-ingest.
//!
//! Binds to localhost by default. With `--bind` on a LAN address the read
//! routes are open to the network, but the write routes (`POST /ingest`,
//! `POST /ingest/finish`, `POST /peers`) answer only to loopback peers.
//!
//! Ingest signs under the node's publisher key. Playback URLs name the
//! publisher: `/play/<publisher>/<content_id>`. The short `/play/<content_id>`
//! works while exactly one publisher has that content ID.

use std::{
    collections::HashMap,
    net::SocketAddr,
    sync::{Arc, Mutex},
    time::Duration,
};

use axum::{
    Router,
    extract::{ConnectInfo, DefaultBodyLimit, Path, Request, State},
    http::{header::CONTENT_TYPE, StatusCode},
    middleware::{self, Next},
    response::{IntoResponse, Response},
    routing::{get, post},
    Json,
};
use serde::{Deserialize, Serialize};
use tracing::info;

use crate::{
    bundle::{check_content_id, Bundle, BundleId, Priority},
    peer::PeerManager,
    store::BundleStore,
    trust::{
        manifest::{Manifest, SegmentEntry, SignedManifest},
        KeyId, PublisherKey,
    },
};

mod playlist;

/// Most segments a live manifest lists: 30 minutes of 10 s segments, the
/// live lifetime.
const LIVE_MANIFEST_SEGMENTS: usize = 180;

// === App state ===

#[derive(Clone)]
pub struct ApiState {
    pub store: Arc<BundleStore>,
    pub node_id: String,
    pub peers: Arc<PeerManager>,
    /// How far behind live the playlist edge sits (`serve --delay`).
    pub delay: Duration,
    /// The node's publisher key. Ingest signs with it. None until `keys init`.
    pub key: Option<PublisherKey>,
    /// Held while a manifest is built and stored, so two ingests never pick
    /// the same version.
    publish: Arc<Mutex<()>>,
}

impl ApiState {
    pub fn new(
        store: Arc<BundleStore>,
        node_id: String,
        peers: Arc<PeerManager>,
        delay: Duration,
        key: Option<PublisherKey>,
    ) -> Self {
        ApiState { store, node_id, peers, delay, key, publish: Arc::new(Mutex::new(())) }
    }
}

// === Error type ===

/// An error with the HTTP status it answers with. Plain conversions give 500.
struct ApiError {
    status: StatusCode,
    error: anyhow::Error,
}

impl ApiError {
    fn new(status: StatusCode, msg: impl std::fmt::Display) -> Self {
        ApiError { status, error: anyhow::anyhow!("{msg}") }
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        (self.status, Json(serde_json::json!({ "error": self.error.to_string() }))).into_response()
    }
}

impl<E: Into<anyhow::Error>> From<E> for ApiError {
    fn from(e: E) -> Self {
        ApiError { status: StatusCode::INTERNAL_SERVER_ERROR, error: e.into() }
    }
}

type ApiResult<T> = Result<Json<T>, ApiError>;

// === Routes ===

/// Largest `POST /ingest` body. axum's 2 MB default turns away a single 10 s
/// live segment once base64'd; this fits a 60 MB relay frame plus base64.
const INGEST_BODY_LIMIT: usize = 80 * 1024 * 1024;

pub fn router(state: ApiState) -> Router {
    let writes = Router::new()
        .route("/ingest", post(ingest).layer(DefaultBodyLimit::max(INGEST_BODY_LIMIT)))
        .route("/ingest/finish", post(finish))
        .route("/peers", post(add_peer))
        .route_layer(middleware::from_fn(localhost_only));

    Router::new()
        .route("/", get(index_page))
        .route("/hls.min.js", get(hls_js))
        .route("/health", get(health))
        .route("/catalog", get(catalog))
        .route("/play/:content_id", get(play_short))
        .route("/play/:publisher/:content_id", get(play))
        .route("/segment/:content_id/:sequence", get(segment_short))
        .route("/segment/:publisher/:content_id/:sequence", get(segment))
        .route("/peers", get(list_peers))
        .merge(writes)
        .with_state(state)
}

// === Phone player ===

/// The offline player page and its vendored hls.js (1.5.17), baked into the
/// binary so a phone on the cabin LAN needs no internet.
async fn index_page() -> impl IntoResponse {
    ([(CONTENT_TYPE, "text/html; charset=utf-8")], include_str!("../../assets/index.html"))
}

async fn hls_js() -> impl IntoResponse {
    ([(CONTENT_TYPE, "application/javascript")], include_bytes!("../../assets/hls.min.js").as_slice())
}

/// Loopback peer, counting an IPv4-mapped IPv6 loopback (`::ffff:127.0.0.1`).
fn is_local(peer: SocketAddr) -> bool {
    peer.ip().to_canonical().is_loopback()
}

/// Guard for the write routes: with the API bound to the LAN, only the
/// machine itself may change state. Only these routes read `ConnectInfo`.
async fn localhost_only(
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    req: Request,
    next: Next,
) -> Response {
    if is_local(peer) {
        next.run(req).await
    } else {
        (StatusCode::FORBIDDEN, "write routes are localhost-only").into_response()
    }
}

// === Read handlers ===

async fn health(State(state): State<ApiState>) -> impl IntoResponse {
    Json(serde_json::json!({
        "ok": true,
        "node_id": state.node_id,
        "version": env!("CARGO_PKG_VERSION"),
        "publisher": state.key.as_ref().map(|k| k.key_id()),
    }))
}

async fn catalog(State(state): State<ApiState>) -> ApiResult<Vec<crate::store::ContentSummary>> {
    let items = state.store.catalog()?;
    Ok(Json(items))
}

/// The one publisher holding `content_id`, for the short URLs. Otherwise the
/// response to send: 404 for none, 300 with the publisher IDs for several.
fn resolve_publisher(state: &ApiState, content_id: &str) -> Result<Result<KeyId, Response>, ApiError> {
    let mut publishers: Vec<KeyId> = state
        .store
        .catalog()?
        .into_iter()
        .filter(|c| c.content_id == content_id)
        .map(|c| c.publisher)
        .collect();
    Ok(match publishers.len() {
        0 => Err((StatusCode::NOT_FOUND, "Content not found").into_response()),
        1 => Ok(publishers.remove(0)),
        _ => Err((StatusCode::MULTIPLE_CHOICES, Json(publishers)).into_response()),
    })
}

/// GET /play/{content_id}: the playlist, when one publisher has this content.
async fn play_short(State(state): State<ApiState>, Path(content_id): Path<String>) -> Result<Response, ApiError> {
    match resolve_publisher(&state, &content_id)? {
        Ok(publisher) => serve_playlist(&state, &publisher, &content_id),
        Err(resp) => Ok(resp),
    }
}

/// GET /play/{publisher}/{content_id}: HLS M3U8 playlist.
async fn play(
    State(state): State<ApiState>,
    Path((publisher, content_id)): Path<(String, String)>,
) -> Result<Response, ApiError> {
    serve_playlist(&state, &KeyId(publisher), &content_id)
}

fn serve_playlist(state: &ApiState, publisher: &KeyId, content_id: &str) -> Result<Response, ApiError> {
    let metas = state.store.list_meta(publisher, content_id)?;
    if metas.is_empty() {
        return Ok((StatusCode::NOT_FOUND, "Content not found").into_response());
    }
    let body = if metas.iter().all(|m| m.priority == Priority::Vod) {
        playlist::vod_playlist(&metas)
    } else {
        match playlist::live_playlist(&metas, chrono::Utc::now(), state.delay) {
            Some(p) => p,
            None => {
                return Ok((
                    StatusCode::NOT_FOUND,
                    "Buffering: nothing old enough to play yet",
                )
                    .into_response());
            }
        }
    };
    Ok(([("Content-Type", "application/x-mpegURL")], body).into_response())
}

/// GET /segment/{content_id}/{sequence}: a segment, when one publisher has
/// this content.
async fn segment_short(
    State(state): State<ApiState>,
    Path((content_id, sequence)): Path<(String, u32)>,
) -> Result<Response, ApiError> {
    match resolve_publisher(&state, &content_id)? {
        Ok(publisher) => serve_segment(&state, &publisher, &content_id, sequence),
        Err(resp) => Ok(resp),
    }
}

/// GET /segment/{publisher}/{content_id}/{sequence}: raw .ts segment bytes.
async fn segment(
    State(state): State<ApiState>,
    Path((publisher, content_id, sequence)): Path<(String, String, u32)>,
) -> Result<Response, ApiError> {
    serve_segment(&state, &KeyId(publisher), &content_id, sequence)
}

fn serve_segment(state: &ApiState, publisher: &KeyId, content_id: &str, sequence: u32) -> Result<Response, ApiError> {
    let id = BundleId::segment(publisher, content_id, sequence);
    let Some(bundle) = state.store.get(id.as_str())? else {
        return Ok((StatusCode::NOT_FOUND, "Segment not found").into_response());
    };
    let created_at = chrono::DateTime::from_timestamp_millis(bundle.info.created_at_ms).unwrap_or_default();
    Ok((
        [
            ("Content-Type", "video/mp2t".to_string()),
            ("x-cosmosd-created-at", created_at.to_rfc3339()),
        ],
        bundle.payload,
    )
        .into_response())
}

/// List all known peers and their sync status.
async fn list_peers(State(state): State<ApiState>) -> impl IntoResponse {
    let peers = state.peers.list_peers().await;
    Json(peers)
}

// === Write handlers ===

/// Add a peer at runtime — triggers sync on next interval.
#[derive(Deserialize)]
struct AddPeerRequest {
    relay_addr: String,
}

async fn add_peer(
    State(state): State<ApiState>,
    Json(req): Json<AddPeerRequest>,
) -> impl IntoResponse {
    state.peers.add_peer(req.relay_addr.clone()).await;
    Json(serde_json::json!({ "added": req.relay_addr }))
}

/// Ingest a new segment from the content pipeline.
#[derive(Deserialize)]
struct IngestRequest {
    content_id: String,
    content_title: String,
    sequence: u32,
    mime_type: String,
    priority: Priority,
    /// Segment duration in milliseconds.
    #[serde(default = "default_duration_ms")]
    duration_ms: u32,
    /// Base64-encoded segment payload.
    payload_b64: String,
}

fn default_duration_ms() -> u32 {
    10_000
}

#[derive(Serialize)]
struct IngestResponse {
    bundle_id: String,
    stored: bool,
}

/// The node's key, or 409 when it has none.
fn own_key(state: &ApiState) -> Result<&PublisherKey, ApiError> {
    state.key.as_ref().ok_or_else(|| ApiError::new(StatusCode::CONFLICT, "run cosmosd keys init first"))
}

fn checked_content_id(content_id: &str) -> Result<(), ApiError> {
    check_content_id(content_id).map_err(|e| ApiError::new(StatusCode::BAD_REQUEST, e))
}

/// Store a segment under the node's key. A live segment also publishes the
/// next version of the content's signed manifest.
async fn ingest(
    State(state): State<ApiState>,
    Json(req): Json<IngestRequest>,
) -> ApiResult<IngestResponse> {
    use base64::Engine;
    let key = own_key(&state)?;
    checked_content_id(&req.content_id)?;
    let payload = base64::engine::general_purpose::STANDARD
        .decode(&req.payload_b64)
        .map_err(|e| ApiError::new(StatusCode::BAD_REQUEST, format!("Invalid base64: {e}")))?;

    let bundle = Bundle::segment(
        &key.key_id(),
        &req.content_id,
        &req.content_title,
        req.sequence,
        &req.mime_type,
        req.priority,
        payload,
        &state.node_id,
    )?
    .with_duration_ms(req.duration_ms);
    let bundle_id = bundle.id().to_string();
    let stored = state.store.insert(&bundle)?;
    info!("Ingest: {} (stored={})", bundle_id, stored);

    if req.priority == Priority::Live {
        publish_manifest(&state, key, &req.content_id, Publish::Live { just_ingested: req.sequence })?;
    }
    Ok(Json(IngestResponse { bundle_id, stored }))
}

#[derive(Deserialize)]
struct FinishRequest {
    content_id: String,
}

#[derive(Serialize)]
struct FinishResponse {
    manifest_id: String,
    version: u64,
    segments: usize,
}

/// Publish the signed manifest of a finished (non-live) item: every segment.
async fn finish(
    State(state): State<ApiState>,
    Json(req): Json<FinishRequest>,
) -> ApiResult<FinishResponse> {
    let key = own_key(&state)?;
    checked_content_id(&req.content_id)?;
    let m = publish_manifest(&state, key, &req.content_id, Publish::Vod)?;
    Ok(Json(FinishResponse {
        manifest_id: BundleId::manifest(&m.publisher, &m.content_id).to_string(),
        version: m.version,
        segments: m.segments.len(),
    }))
}

// === Manifest publishing ===

enum Publish {
    /// All segments, `live: false`.
    Vod,
    /// The newest [`LIVE_MANIFEST_SEGMENTS`] segments, `live: true`.
    Live { just_ingested: u32 },
}

/// Sign and store the next manifest version for one of the node's own
/// content items, listing the segments it has stored.
///
/// A live manifest reuses the hashes in the previous version, so each new
/// segment costs one hash. The segment just ingested is always hashed again.
fn publish_manifest(state: &ApiState, key: &PublisherKey, content_id: &str, mode: Publish) -> Result<Manifest, ApiError> {
    let _publishing = state.publish.lock().unwrap_or_else(|e| e.into_inner());
    let publisher = key.key_id();
    let mut metas = state.store.list_meta(&publisher, content_id)?;
    if metas.is_empty() {
        return Err(ApiError::new(StatusCode::NOT_FOUND, format!("no segments of {content_id:?} to publish")));
    }
    let previous = state.store.manifest(&publisher, content_id)?;

    let (live, known, fresh) = match mode {
        Publish::Vod => (false, HashMap::new(), None),
        Publish::Live { just_ingested } => {
            metas.drain(..metas.len().saturating_sub(LIVE_MANIFEST_SEGMENTS));
            let known: HashMap<u32, [u8; 32]> =
                previous.iter().flat_map(|p| &p.segments).map(|e| (e.sequence, e.hash)).collect();
            (true, known, Some(just_ingested))
        }
    };

    let mut segments = Vec::with_capacity(metas.len());
    for m in &metas {
        let hash = match known.get(&m.sequence).filter(|_| fresh != Some(m.sequence)) {
            Some(h) => *h,
            None => match state.store.get(&m.id)? {
                Some(b) => *blake3::hash(&b.payload).as_bytes(),
                // Expired since it was listed.
                None => continue,
            },
        };
        segments.push(SegmentEntry { sequence: m.sequence, hash, duration_ms: m.duration_ms });
    }

    let manifest = Manifest {
        publisher: publisher.clone(),
        content_id: content_id.to_string(),
        title: metas.last().map(|m| m.title.clone()).unwrap_or_default(),
        version: previous.map_or(1, |p| p.version + 1),
        created_at_ms: chrono::Utc::now().timestamp_millis(),
        live,
        segments,
    };
    let signed = SignedManifest::sign(&manifest, key)?.to_bytes()?;
    let bundle = Bundle::manifest(&publisher, &manifest, signed, &state.node_id);
    if !state.store.insert(&bundle)? {
        return Err(anyhow::anyhow!("manifest version {} is not newer than the stored one", manifest.version).into());
    }
    info!(
        "Published manifest {} version {} ({} segments)",
        bundle.id(),
        manifest.version,
        manifest.segments.len()
    );
    Ok(manifest)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bundle::BundleId;
    use crate::store::Admission;
    use crate::trust::manifest::{Manifest, SignedManifest};
    use crate::trust::{PairedKeys, PublisherKey, Trust};
    use crate::{auth::RelayAuth, router::EpidemicRouter};
    use axum::{body::Body, extract::connect_info::MockConnectInfo, http::Request};
    use std::net::SocketAddr;
    use tower::ServiceExt;

    fn test_state() -> (ApiState, tempfile::TempDir) {
        test_state_with_key(None)
    }

    fn test_state_with_key(key: Option<PublisherKey>) -> (ApiState, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let store = Arc::new(BundleStore::open(dir.path().to_str().unwrap()).unwrap());
        let delay = Duration::from_secs(120);
        let trust = Arc::new(Trust::new(key.clone(), PairedKeys::open(dir.path()).unwrap()));
        let router = Arc::new(EpidemicRouter::new(store.clone(), "t".into(), 0, delay, trust));
        let peers = Arc::new(PeerManager::new(router, Arc::new(RelayAuth::new(None)), vec![], delay));
        (ApiState::new(store, "t".into(), peers, delay, key), dir)
    }

    fn get(path: &str) -> Request<Body> {
        Request::get(path).body(Body::empty()).unwrap()
    }

    fn post_json(path: &str, json: &str) -> Request<Body> {
        Request::post(path)
            .header("content-type", "application/json")
            .body(Body::from(json.to_owned()))
            .unwrap()
    }

    async fn body_string(res: Response) -> String {
        let bytes = axum::body::to_bytes(res.into_body(), usize::MAX).await.unwrap();
        String::from_utf8(bytes.to_vec()).unwrap()
    }

    fn from_ip(ip: [u8; 4]) -> MockConnectInfo<SocketAddr> {
        MockConnectInfo(SocketAddr::from((ip, 5000)))
    }

    /// Serve the API, with a publisher key, on an ephemeral port. Keeps the
    /// store's tempdir alive.
    async fn serve() -> (String, tempfile::TempDir) {
        let (state, dir) = test_state_with_key(Some(PublisherKey::generate()));
        let app = super::router(state);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app.into_make_service_with_connect_info::<SocketAddr>())
                .await
                .unwrap()
        });
        (format!("http://{}", addr), dir)
    }

    const ADD_PEER: &str = r#"{"relay_addr":"1.2.3.4:7778"}"#;

    #[tokio::test]
    async fn post_peers_from_lan_is_forbidden() {
        let (state, _dir) = test_state();
        let app = router(state).layer(from_ip([192, 168, 1, 9]));
        let res = app.oneshot(post_json("/peers", ADD_PEER)).await.unwrap();
        assert_eq!(res.status(), StatusCode::FORBIDDEN);
        let body = axum::body::to_bytes(res.into_body(), usize::MAX).await.unwrap();
        assert_eq!(&body[..], b"write routes are localhost-only");
    }

    #[tokio::test]
    async fn post_ingest_from_lan_is_forbidden() {
        let (state, _dir) = test_state();
        let app = router(state).layer(from_ip([192, 168, 1, 9]));
        let res = app.oneshot(post_json("/ingest", "{}")).await.unwrap();
        assert_eq!(res.status(), StatusCode::FORBIDDEN);
    }

    #[tokio::test]
    async fn post_peers_from_localhost_is_allowed() {
        let (state, _dir) = test_state();
        let app = router(state).layer(from_ip([127, 0, 0, 1]));
        let res = app.oneshot(post_json("/peers", ADD_PEER)).await.unwrap();
        assert_eq!(res.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn read_routes_from_lan_are_allowed() {
        for path in ["/catalog", "/peers"] {
            let (state, _dir) = test_state();
            let app = router(state).layer(from_ip([192, 168, 1, 9]));
            let res = app.oneshot(get(path)).await.unwrap();
            assert_eq!(res.status(), StatusCode::OK, "{path}");
        }
    }

    #[tokio::test]
    async fn read_routes_need_no_connect_info() {
        let (state, _dir) = test_state();
        let res = router(state).oneshot(get("/health")).await.unwrap();
        assert_eq!(res.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn index_page_has_a_player() {
        let (state, _dir) = test_state();
        let res = router(state).oneshot(get("/")).await.unwrap();
        assert_eq!(res.status(), StatusCode::OK);
        assert!(res.headers()["content-type"].to_str().unwrap().starts_with("text/html"));
        let body = body_string(res).await;
        assert!(body.contains("<video") && body.contains("/hls.min.js") && !body.contains("https://"));
        assert!(body.contains("++gen"), "play() needs a generation guard against stale responses");
    }

    /// The player must never use the short `/play/<content_id>` form: it
    /// answers 300 once a second publisher shares the content ID, which
    /// would break a button or a live reload mid-playback.
    #[test]
    fn index_page_plays_publisher_qualified_urls() {
        let page = include_str!("../../assets/index.html");
        let builds: Vec<&str> = page.match_indices("'/play/' +").map(|(i, _)| &page[i..]).collect();
        assert!(!builds.is_empty(), "the page builds a play URL");
        for b in builds {
            assert!(
                b.starts_with("'/play/' + encodeURIComponent(c.publisher) + '/' + encodeURIComponent(c.content_id)"),
                "play URL not publisher-qualified: {}",
                &b[..b.len().min(80)]
            );
        }
        assert!(!page.contains("'/segment/'"), "the page must leave segment URLs to the playlist");
    }

    #[tokio::test]
    async fn catalog_entries_all_carry_publisher() {
        let key = PublisherKey::generate();
        let (state, _dir) = test_state_with_key(Some(key.clone()));
        insert_segment(&state.store, P1, "other", 0);
        let app = local_app(state);
        let res = send(&app, post_json("/ingest", &ingest_body("show", 0, "vod", &seg_payload(0)))).await;
        assert_eq!(res.status(), StatusCode::OK);
        let items: serde_json::Value =
            serde_json::from_str(&body_string(send(&app, get("/catalog")).await).await).unwrap();
        let items = items.as_array().unwrap();
        assert_eq!(items.len(), 2);
        for c in items {
            let publisher = c["publisher"].as_str().unwrap_or_else(|| panic!("no publisher in {c}"));
            assert_eq!(publisher.len(), 16, "{c}");
        }
        assert!(items.iter().any(|c| c["publisher"] == key.key_id().0.as_str()));
    }

    #[tokio::test]
    async fn hls_js_is_embedded() {
        let (state, _dir) = test_state();
        let res = router(state).oneshot(get("/hls.min.js")).await.unwrap();
        assert_eq!(res.status(), StatusCode::OK);
        assert_eq!(res.headers()["content-type"], "application/javascript");
    }

    #[test]
    fn ipv6_and_mapped_loopback_count_as_local() {
        assert!(is_local("[::1]:1".parse().unwrap()));
        assert!(is_local("[::ffff:127.0.0.1]:1".parse().unwrap()));
        assert!(!is_local("[::ffff:192.168.1.9]:1".parse().unwrap()));
    }

    /// A 10 s live segment at 1.2 Mbps is about 1.5 MB, 2 MB once base64'd:
    /// over axum's default body limit.
    #[tokio::test]
    async fn ingest_accepts_a_full_size_live_segment() {
        use base64::Engine;
        let (base, _dir) = serve().await;
        let payload = vec![0x47u8; 3 * 1024 * 1024];
        let body = serde_json::json!({
            "content_id": "demo", "content_title": "demo", "sequence": 0,
            "mime_type": "video/mp2t", "priority": "live",
            "payload_b64": base64::engine::general_purpose::STANDARD.encode(&payload),
        });
        let resp = reqwest::Client::new()
            .post(format!("{}/ingest", base))
            .json(&body)
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), reqwest::StatusCode::OK);
    }

    // === Signed ingest and publisher-aware playback ===

    fn ingest_body(content_id: &str, sequence: u32, priority: &str, payload: &[u8]) -> String {
        use base64::Engine;
        serde_json::json!({
            "content_id": content_id, "content_title": "Show", "sequence": sequence,
            "mime_type": "video/mp2t", "priority": priority, "duration_ms": 9_000 + sequence,
            "payload_b64": base64::engine::general_purpose::STANDARD.encode(payload),
        })
        .to_string()
    }

    fn finish_body(content_id: &str) -> String {
        serde_json::json!({ "content_id": content_id }).to_string()
    }

    fn seg_payload(seq: u32) -> Vec<u8> {
        format!("segment {seq}").into_bytes()
    }

    /// A router that sees every request as coming from localhost.
    fn local_app(state: ApiState) -> Router {
        router(state).layer(from_ip([127, 0, 0, 1]))
    }

    async fn send(app: &Router, req: Request<Body>) -> Response {
        app.clone().oneshot(req).await.unwrap()
    }

    /// The manifest the node stored for its own content, checked against its key.
    fn stored_manifest(store: &BundleStore, key: &PublisherKey, content_id: &str) -> Option<Manifest> {
        let id = BundleId::manifest(&key.key_id(), content_id);
        let b = store.get(id.as_str()).unwrap()?;
        let trust = Trust::new(Some(key.clone()), PairedKeys::open(tempfile::tempdir().unwrap().path()).unwrap());
        Some(SignedManifest::from_bytes(&b.payload).unwrap().verify(&trust).unwrap())
    }

    fn insert_segment(store: &BundleStore, publisher: &str, content_id: &str, seq: u32) {
        let b = Bundle::segment(
            &KeyId(publisher.into()), content_id, "Show", seq, "video/mp2t", Priority::Vod, seg_payload(seq), "n",
        )
        .unwrap();
        store.insert(&b).unwrap();
    }

    const P1: &str = "aaaaaaaaaaaaaaaa";
    const P2: &str = "bbbbbbbbbbbbbbbb";

    #[tokio::test]
    async fn ingest_without_key_returns_409() {
        let (state, _dir) = test_state();
        let app = local_app(state);
        for (path, body) in [("/ingest", ingest_body("show", 0, "vod", b"ts")), ("/ingest/finish", finish_body("show"))] {
            let res = send(&app, post_json(path, &body)).await;
            assert_eq!(res.status(), StatusCode::CONFLICT, "{path}");
            assert!(body_string(res).await.contains("run cosmosd keys init first"), "{path}");
        }
    }

    #[tokio::test]
    async fn ingest_then_finish_makes_content_playable() {
        let key = PublisherKey::generate();
        let (state, _dir) = test_state_with_key(Some(key.clone()));
        let store = state.store.clone();
        let app = local_app(state);
        for seq in 0..3 {
            let res = send(&app, post_json("/ingest", &ingest_body("show", seq, "vod", &seg_payload(seq)))).await;
            assert_eq!(res.status(), StatusCode::OK);
        }
        // VOD content gets its manifest only at finish.
        assert!(stored_manifest(&store, &key, "show").is_none());
        let res = send(&app, post_json("/ingest/finish", &finish_body("show"))).await;
        assert_eq!(res.status(), StatusCode::OK);

        let m = stored_manifest(&store, &key, "show").expect("manifest stored");
        assert_eq!((m.version, m.live, m.publisher.clone()), (1, false, key.key_id()));
        let seqs: Vec<u32> = m.segments.iter().map(|s| s.sequence).collect();
        assert_eq!(seqs, vec![0, 1, 2]);
        for e in &m.segments {
            assert_eq!(e.hash, *blake3::hash(&seg_payload(e.sequence)).as_bytes());
            assert_eq!(e.duration_ms, 9_000 + e.sequence);
        }

        let pid = key.key_id().to_string();
        let res = send(&app, get(&format!("/play/{pid}/show"))).await;
        assert_eq!(res.status(), StatusCode::OK);
        let playlist = body_string(res).await;
        assert!(playlist.contains(&format!("/segment/{pid}/show/2\n")), "{playlist}");
        assert!(playlist.ends_with("#EXT-X-ENDLIST\n"));
        let res = send(&app, get(&format!("/segment/{pid}/show/1"))).await;
        assert_eq!(res.status(), StatusCode::OK);
        assert_eq!(body_string(res).await.into_bytes(), seg_payload(1));

        // A peer that paired this node admits the manifest and every segment.
        let peer_dir = tempfile::tempdir().unwrap();
        let peer = BundleStore::open(peer_dir.path().join("store").to_str().unwrap()).unwrap();
        let mut paired = PairedKeys::open(peer_dir.path()).unwrap();
        paired.pair(key.public()).unwrap();
        let peer_trust = Trust::new(None, paired);
        let manifest = store.get(BundleId::manifest(&key.key_id(), "show").as_str()).unwrap().unwrap();
        assert_eq!(peer.admit(&manifest, &peer_trust).unwrap(), Admission::Stored);
        for seq in 0..3 {
            let s = store.get(BundleId::segment(&key.key_id(), "show", seq).as_str()).unwrap().unwrap();
            assert_eq!(peer.admit(&s, &peer_trust).unwrap(), Admission::Stored, "seq {seq}");
        }

        // Finishing again publishes the next version.
        let res = send(&app, post_json("/ingest/finish", &finish_body("show"))).await;
        assert_eq!(res.status(), StatusCode::OK);
        assert_eq!(stored_manifest(&store, &key, "show").unwrap().version, 2);
    }

    #[tokio::test]
    async fn finish_without_segments_is_404() {
        let (state, _dir) = test_state_with_key(Some(PublisherKey::generate()));
        let res = send(&local_app(state), post_json("/ingest/finish", &finish_body("nothing"))).await;
        assert_eq!(res.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn bad_content_id_is_400() {
        let (state, _dir) = test_state_with_key(Some(PublisherKey::generate()));
        let app = local_app(state);
        let long = "x".repeat(65);
        for cid in ["a/b", "", "bad id!", "caf\u{e9}", long.as_str()] {
            let res = send(&app, post_json("/ingest", &ingest_body(cid, 0, "vod", b"ts"))).await;
            assert_eq!(res.status(), StatusCode::BAD_REQUEST, "ingest {cid:?}");
            let res = send(&app, post_json("/ingest/finish", &finish_body(cid))).await;
            assert_eq!(res.status(), StatusCode::BAD_REQUEST, "finish {cid:?}");
        }
    }

    #[tokio::test]
    async fn live_ingest_publishes_a_manifest_per_segment() {
        let key = PublisherKey::generate();
        let (state, _dir) = test_state_with_key(Some(key.clone()));
        let store = state.store.clone();
        let app = local_app(state);
        for seq in 0..3 {
            let res = send(&app, post_json("/ingest", &ingest_body("demo", seq, "live", &seg_payload(seq)))).await;
            assert_eq!(res.status(), StatusCode::OK);
            let m = stored_manifest(&store, &key, "demo").expect("manifest after each live segment");
            assert_eq!(m.version, u64::from(seq) + 1);
            assert!(m.live);
            assert_eq!(m.segments.len(), seq as usize + 1);
            let last = m.segments.last().unwrap();
            assert_eq!(last.sequence, seq);
            assert_eq!(last.hash, *blake3::hash(&seg_payload(seq)).as_bytes());
        }
    }

    #[tokio::test]
    async fn live_manifest_lists_the_last_180_segments() {
        let key = PublisherKey::generate();
        let (state, _dir) = test_state_with_key(Some(key.clone()));
        let store = state.store.clone();
        let app = local_app(state);
        for seq in 0..182 {
            let res = send(&app, post_json("/ingest", &ingest_body("demo", seq, "live", &seg_payload(seq)))).await;
            assert_eq!(res.status(), StatusCode::OK);
        }
        let m = stored_manifest(&store, &key, "demo").unwrap();
        assert_eq!(m.version, 182);
        assert_eq!(m.segments.len(), 180);
        assert_eq!(m.segments.first().unwrap().sequence, 2);
        assert_eq!(m.segments.last().unwrap().sequence, 181);
        for e in &m.segments {
            assert_eq!(e.hash, *blake3::hash(&seg_payload(e.sequence)).as_bytes(), "seq {}", e.sequence);
        }
    }

    #[tokio::test]
    async fn play_short_url_resolves_single_publisher() {
        let (state, _dir) = test_state();
        insert_segment(&state.store, P1, "show", 0);
        insert_segment(&state.store, P1, "show", 1);
        let app = router(state);
        let res = send(&app, get("/play/show")).await;
        assert_eq!(res.status(), StatusCode::OK);
        // Segment URLs name the publisher, so playback never hits a 300.
        let playlist = body_string(res).await;
        assert!(playlist.contains(&format!("/segment/{P1}/show/1\n")), "{playlist}");
        let res = send(&app, get("/segment/show/1")).await;
        assert_eq!(res.status(), StatusCode::OK);
        assert_eq!(body_string(res).await.into_bytes(), seg_payload(1));
        assert_eq!(send(&app, get("/play/missing")).await.status(), StatusCode::NOT_FOUND);
        assert_eq!(send(&app, get("/segment/missing/0")).await.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn play_short_url_is_ambiguous_for_two_publishers() {
        let (state, _dir) = test_state();
        insert_segment(&state.store, P1, "show", 0);
        insert_segment(&state.store, P2, "show", 0);
        let app = router(state);
        for path in ["/play/show", "/segment/show/0"] {
            let res = send(&app, get(path)).await;
            assert_eq!(res.status(), StatusCode::MULTIPLE_CHOICES, "{path}");
            let ids: Vec<String> = serde_json::from_str(&body_string(res).await).unwrap();
            assert_eq!(ids, vec![P1.to_string(), P2.to_string()], "{path}");
        }
        let res = send(&app, get(&format!("/play/{P1}/show"))).await;
        assert_eq!(res.status(), StatusCode::OK);
        let playlist = body_string(res).await;
        assert!(playlist.contains(&format!("/segment/{P1}/show/0")) && !playlist.contains(P2), "{playlist}");
    }

    #[tokio::test]
    async fn catalog_lists_publisher() {
        let (state, _dir) = test_state();
        insert_segment(&state.store, P1, "show", 0);
        insert_segment(&state.store, P2, "show", 0);
        insert_segment(&state.store, P2, "show", 1);
        let res = send(&router(state), get("/catalog")).await;
        assert_eq!(res.status(), StatusCode::OK);
        let items: serde_json::Value = serde_json::from_str(&body_string(res).await).unwrap();
        let items = items.as_array().unwrap();
        assert_eq!(items.len(), 2, "two publishers sharing a content id stay apart");
        assert_eq!((items[0]["publisher"].as_str(), items[0]["segment_count"].as_u64()), (Some(P1), Some(1)));
        assert_eq!((items[1]["publisher"].as_str(), items[1]["segment_count"].as_u64()), (Some(P2), Some(2)));
    }

    #[tokio::test]
    async fn health_reports_the_publisher_key() {
        let key = PublisherKey::generate();
        let (state, _dir) = test_state_with_key(Some(key.clone()));
        let health: serde_json::Value =
            serde_json::from_str(&body_string(send(&router(state), get("/health")).await).await).unwrap();
        assert_eq!(health["publisher"].as_str(), Some(key.key_id().0.as_str()));
        let (state, _dir) = test_state();
        let health: serde_json::Value =
            serde_json::from_str(&body_string(send(&router(state), get("/health")).await).await).unwrap();
        assert!(health["publisher"].is_null());
    }
}
