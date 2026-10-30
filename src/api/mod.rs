//! Local HTTP API — used by cosmoscast-sdk and cosmoscast-ingest.
//!
//! All endpoints bind to localhost only. No authentication needed
//! since this is a local daemon API (not exposed to the network).

use std::sync::Arc;

use axum::{
    Router,
    extract::{Path, State},
    http::StatusCode,
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
};

// === App state ===

#[derive(Clone)]
pub struct ApiState {
    pub store: Arc<BundleStore>,
    pub node_id: String,
    pub peers: Arc<PeerManager>,
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

pub fn router(state: ApiState) -> Router {
    Router::new()
        .route("/health", get(health))
        .route("/catalog", get(catalog))
        .route("/play/:content_id", get(hls_playlist_handler))
        .route("/segment/:content_id/:sequence", get(hls_segment_handler))
        .route("/ingest", post(ingest))
        .route("/peers", get(list_peers))
        .route("/peers", post(add_peer))
        .with_state(state)
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
    let bundle_id = format!("{}/{:05}", content_id, sequence);
    let Some(bundle) = state.store.get(&bundle_id)? else {
        return Ok((StatusCode::NOT_FOUND, "Segment not found").into_response());
    };
    if !bundle.verify() {
        return Ok((StatusCode::INTERNAL_SERVER_ERROR, "Integrity check failed").into_response());
    }
    Ok(([("Content-Type", "video/mp2t")], bundle.payload).into_response())
}

async fn serve_playlist(state: ApiState, content_id: String) -> Result<Response, ApiError> {
    let bundles = state.store.list_meta(&content_id)?;
    if bundles.is_empty() {
        return Ok((StatusCode::NOT_FOUND, "Content not found").into_response());
    }
    let mut m3u8 = String::from(
        "#EXTM3U\n#EXT-X-VERSION:3\n#EXT-X-TARGETDURATION:10\n#EXT-X-MEDIA-SEQUENCE:0\n",
    );
    for bundle in &bundles {
        m3u8.push_str(&format!(
            "#EXTINF:10.0,\n/segment/{}/{}\n",
            content_id, bundle.sequence
        ));
    }
    // NOTE: no EXT-X-ENDLIST for live — added only when all segments are VOD
    if bundles.iter().all(|b| b.priority == Priority::Vod) {
        m3u8.push_str("#EXT-X-ENDLIST\n");
    }
    Ok(([("Content-Type", "application/x-mpegURL")], m3u8).into_response())
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
    /// Base64-encoded segment payload.
    payload_b64: String,
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

    let bundle = Bundle::new(
        &req.content_id,
        &req.content_title,
        req.sequence,
        &req.mime_type,
        req.priority,
        payload,
        &state.node_id,
    );
    let bundle_id = bundle.id.to_string();
    let stored = state.store.insert(&bundle)?;

    info!("Ingest: {} (stored={})", bundle_id, stored);
    Ok(Json(IngestResponse { bundle_id, stored }))
}
