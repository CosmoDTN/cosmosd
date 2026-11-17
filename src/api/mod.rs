//! Local HTTP API — used by cosmoscast-sdk and cosmoscast-ingest.
//!
//! Binds to localhost by default. With `--bind` on a LAN address the read
//! routes are open to the network, but the write routes (`POST /ingest`,
//! `POST /peers`) answer only to loopback peers.

use std::{net::SocketAddr, sync::Arc, time::Duration};

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
    bundle::{Bundle, Priority},
    peer::PeerManager,
    store::BundleStore,
    trust::KeyId,
};

mod playlist;

/// Publisher for `POST /ingest` until ingest signs under the node's key.
/// Not a real key ID. Every node uses the same value for now.
const UNSIGNED_PUBLISHER: &str = "unsigned";

// === App state ===

#[derive(Clone)]
pub struct ApiState {
    pub store: Arc<BundleStore>,
    pub node_id: String,
    pub peers: Arc<PeerManager>,
    /// How far behind live the playlist edge sits (`serve --delay`).
    pub delay: Duration,
}

// === Error type ===

struct ApiError(anyhow::Error);

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({ "error": self.0.to_string() })),
        )
            .into_response()
    }
}

impl<E: Into<anyhow::Error>> From<E> for ApiError {
    fn from(e: E) -> Self {
        ApiError(e.into())
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
        .route("/peers", post(add_peer))
        .route_layer(middleware::from_fn(localhost_only));

    Router::new()
        .route("/", get(index_page))
        .route("/hls.min.js", get(hls_js))
        .route("/health", get(health))
        .route("/catalog", get(catalog))
        .route("/play/:content_id", get(hls_playlist_handler))
        .route("/segment/:content_id/:sequence", get(hls_segment_handler))
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

// === Handlers ===

async fn health(State(state): State<ApiState>) -> impl IntoResponse {
    Json(serde_json::json!({
        "ok": true,
        "node_id": state.node_id,
        "version": env!("CARGO_PKG_VERSION"),
    }))
}

async fn catalog(State(state): State<ApiState>) -> ApiResult<Vec<crate::store::ContentSummary>> {
    let items = state.store.catalog()?;
    Ok(Json(items))
}

/// GET /play/{content_id} — HLS M3U8 playlist
async fn hls_playlist_handler(
    State(state): State<ApiState>,
    Path(content_id): Path<String>,
) -> Result<Response, ApiError> {
    serve_playlist(state, content_id).await
}

/// GET /segment/{content_id}/{sequence} — raw .ts segment bytes
async fn hls_segment_handler(
    State(state): State<ApiState>,
    Path((content_id, sequence)): Path<(String, u32)>,
) -> Result<Response, ApiError> {
    let meta = state.store.list_meta(&content_id)?.into_iter().find(|m| m.sequence == sequence);
    let Some(bundle) = meta.map(|m| state.store.get(&m.id)).transpose()?.flatten() else {
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

async fn serve_playlist(state: ApiState, content_id: String) -> Result<Response, ApiError> {
    let metas = state.store.list_meta(&content_id)?;
    if metas.is_empty() {
        return Ok((StatusCode::NOT_FOUND, "Content not found").into_response());
    }
    let body = if metas.iter().all(|m| m.priority == Priority::Vod) {
        playlist::vod_playlist(&content_id, &metas)
    } else {
        match playlist::live_playlist(&content_id, &metas, chrono::Utc::now(), state.delay) {
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

/// List all known peers and their sync status.
async fn list_peers(State(state): State<ApiState>) -> impl IntoResponse {
    let peers = state.peers.list_peers().await;
    Json(peers)
}

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

/// Ingest a new bundle from the content pipeline.
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

async fn ingest(
    State(state): State<ApiState>,
    Json(req): Json<IngestRequest>,
) -> ApiResult<IngestResponse> {
    use base64::Engine;
    let payload = base64::engine::general_purpose::STANDARD
        .decode(&req.payload_b64)
        .map_err(|e| anyhow::anyhow!("Invalid base64: {}", e))?;

    let bundle = Bundle::segment(
        &KeyId(UNSIGNED_PUBLISHER.into()),
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
    Ok(Json(IngestResponse { bundle_id, stored }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::trust::{PairedKeys, Trust};
    use crate::{auth::RelayAuth, router::EpidemicRouter};
    use axum::{body::Body, extract::connect_info::MockConnectInfo, http::Request};
    use std::net::SocketAddr;
    use tower::ServiceExt;

    fn test_state() -> (ApiState, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let store = Arc::new(BundleStore::open(dir.path().to_str().unwrap()).unwrap());
        let delay = Duration::from_secs(120);
        let trust = Arc::new(Trust::new(None, PairedKeys::open(dir.path()).unwrap()));
        let router = Arc::new(EpidemicRouter::new(store.clone(), "t".into(), 0, delay, trust));
        let peers = Arc::new(PeerManager::new(router, Arc::new(RelayAuth::new(None)), vec![], delay));
        (ApiState { store, node_id: "t".into(), peers, delay }, dir)
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

    /// Serve the API on an ephemeral port; keeps the store's tempdir alive.
    async fn serve() -> (String, tempfile::TempDir) {
        let (state, dir) = test_state();
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
}
