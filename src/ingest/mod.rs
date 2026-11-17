//! Ingest — turn video into bundles and push them to a running daemon.
//!
//! `ingest_file` chunks a finished file; `live` transcodes a live stream
//! continuously. Both share the single-segment push helper below.

pub mod live;

use std::path::PathBuf;

use anyhow::{Context, Result};
use base64::Engine;
use tracing::info;

pub use live::ingest_live;

// === Playlist parsing ===

/// One segment listed in a media playlist.
#[derive(Debug, Clone, PartialEq)]
pub struct PlaylistEntry {
    pub uri: String,
    pub duration_ms: u32,
}

/// Pair each `#EXTINF:<secs>,` line with the next non-comment line.
pub fn parse_media_playlist(text: &str) -> Vec<PlaylistEntry> {
    let mut entries = Vec::new();
    let mut pending: Option<u32> = None;
    for line in text.lines().map(str::trim) {
        if let Some(rest) = line.strip_prefix("#EXTINF:") {
            let secs = rest.split(',').next().unwrap_or("").trim();
            pending = secs.parse::<f64>().ok().map(|s| (s * 1000.0).round() as u32);
        } else if line.is_empty() || line.starts_with('#') {
            continue;
        } else if let Some(duration_ms) = pending.take() {
            entries.push(PlaylistEntry { uri: line.to_string(), duration_ms });
        }
    }
    entries
}

// === Catalog ===

/// `last_sequence + 1` for this publisher's `content_id` in a `/catalog`
/// response, else 0. Other publishers may use the same content ID.
pub fn next_sequence(catalog: &serde_json::Value, publisher: &str, content_id: &str) -> u32 {
    catalog
        .as_array()
        .into_iter()
        .flatten()
        .find(|c| c["publisher"] == publisher && c["content_id"] == content_id)
        .and_then(|c| c["last_sequence"].as_u64())
        .map_or(0, |s| s as u32 + 1)
}

// === Shared push ===

/// POST one segment to the daemon's `/ingest`; returns the JSON response.
pub(crate) async fn push_segment(
    client: &reqwest::Client,
    daemon: &str,
    content_id: &str,
    title: &str,
    sequence: u32,
    priority: &str,
    duration_ms: u32,
    payload: &[u8],
) -> Result<serde_json::Value> {
    let resp = client
        .post(format!("{}/ingest", daemon))
        .json(&serde_json::json!({
            "content_id":    content_id,
            "content_title": title,
            "sequence":      sequence,
            "mime_type":     "video/mp2t",
            "priority":      priority,
            "duration_ms":   duration_ms,
            "payload_b64":   base64::engine::general_purpose::STANDARD.encode(payload),
        }))
        .send()
        .await
        .with_context(|| format!("Failed to push segment {}", sequence))?;
    json_or_reason(resp, &format!("segment {sequence}")).await
}

/// POST `/ingest/finish`: the daemon signs and stores the manifest listing
/// every segment of `content_id`. Returns the JSON response.
pub(crate) async fn finish(client: &reqwest::Client, daemon: &str, content_id: &str) -> Result<serde_json::Value> {
    let resp = client
        .post(format!("{}/ingest/finish", daemon))
        .json(&serde_json::json!({ "content_id": content_id }))
        .send()
        .await
        .with_context(|| format!("Failed to finish {}", content_id))?;
    json_or_reason(resp, &format!("finish of {content_id}")).await
}

/// The JSON body of a success, else an error carrying the daemon's reason
/// (e.g. the 403 for a non-localhost ingest or the 409 for a missing key).
async fn json_or_reason(resp: reqwest::Response, what: &str) -> Result<serde_json::Value> {
    let status = resp.status();
    if !status.is_success() {
        let body = resp.text().await.unwrap_or_default();
        anyhow::bail!("Daemon rejected {}: {}: {}", what, status, body.trim());
    }
    Ok(resp.json().await?)
}

pub(crate) fn which_ffmpeg() -> Option<()> {
    std::process::Command::new("ffmpeg")
        .arg("-version")
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status()
        .ok()
        .filter(|s| s.success())
        .map(|_| ())
}

/// Check the daemon is up and has a publisher key. Returns its key ID.
pub(crate) async fn check_daemon(client: &reqwest::Client, daemon: &str) -> Result<String> {
    let health: serde_json::Value = client
        .get(format!("{}/health", daemon))
        .send()
        .await
        .context("Cannot reach cosmosd. Is it running? Start it with cosmosd serve")?
        .json()
        .await
        .context("Bad /health response")?;
    health["publisher"]
        .as_str()
        .map(str::to_string)
        .context("The daemon has no publisher key: run cosmosd keys init first, then restart cosmosd serve")
}

// === File ingest ===

/// A valid content ID made from a file name: lowercase ASCII letters, digits
/// and `_`, at most 40 characters. A name with none of those gives `untitled`.
pub fn content_id_from_name(name: &str) -> String {
    let id: String = name
        .to_lowercase()
        .replace(' ', "_")
        .chars()
        .filter(|c| c.is_ascii_alphanumeric() || *c == '_')
        .take(40)
        .collect();
    if id.is_empty() { "untitled".to_string() } else { id }
}

pub async fn ingest_file(
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
    let content_id = content_id_from_name(&file.file_stem().unwrap_or_default().to_string_lossy());

    // 1. Check ffmpeg is available
    which_ffmpeg().context("ffmpeg not found. Install it with apt install ffmpeg or brew install ffmpeg")?;

    // 2. Check daemon is reachable and can sign
    let client = reqwest::Client::new();
    let publisher = check_daemon(&client, &daemon).await?;

    info!("Ingesting: {}", title);
    info!("File:      {}", file.display());
    info!("Content ID:{}", content_id);
    info!("Daemon:    {}", daemon);
    info!("Publisher: {}", publisher);

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

    // 4. Push each segment (with its playlist duration) to cosmosd
    let playlist = std::fs::read_to_string(hls_dir.join("index.m3u8"))
        .context("ffmpeg produced no index.m3u8")?;
    let entries = parse_media_playlist(&playlist);

    info!("Pushing {} segments to {}...", entries.len(), daemon);

    for (i, entry) in entries.iter().enumerate() {
        let payload = std::fs::read(hls_dir.join(&entry.uri))?;
        let data = push_segment(
            &client, &daemon, &content_id, &title, i as u32, &priority, entry.duration_ms, &payload,
        )
        .await?;
        info!("  seg{:05} → {} (stored={})", i, data["bundle_id"], data["stored"]);
    }

    // 5. Publish the signed manifest, so peers accept the segments
    let data = finish(&client, &daemon, &content_id).await?;
    info!("Manifest {} version {}", data["manifest_id"], data["version"]);

    info!("Done: {} ({} segments)", title, entries.len());
    info!("Play: ffplay -extension_picky 0 {}/play/{}/{}", daemon, publisher, content_id);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn content_id_from_name_is_always_valid() {
        assert_eq!(content_id_from_name("My Movie (2024)"), "my_movie_2024");
        assert_eq!(content_id_from_name("Am\u{e9}lie"), "amlie");
        assert_eq!(content_id_from_name("\u{65e5}\u{672c}"), "untitled");
        assert_eq!(content_id_from_name(""), "untitled");
        let long = content_id_from_name(&"a".repeat(100));
        assert_eq!(long.len(), 40);
        for name in ["My Movie (2024)", "Am\u{e9}lie", "\u{65e5}\u{672c}", "a/b\\c", &"z".repeat(100)] {
            let id = content_id_from_name(name);
            assert!(crate::bundle::check_content_id(&id).is_ok(), "{name:?} gave {id:?}");
        }
    }

    #[test]
    fn parses_extinf_entries() {
        let t = "#EXTM3U\n#EXT-X-TARGETDURATION:11\n#EXTINF:10.010000,\nseg00000.ts\n#EXTINF:9.5,\nseg00001.ts\n";
        let e = parse_media_playlist(t);
        assert_eq!(e.len(), 2);
        assert_eq!(e[0].uri, "seg00000.ts");
        assert_eq!(e[0].duration_ms, 10_010);
        assert_eq!(e[1].duration_ms, 9_500);
    }

    #[tokio::test]
    async fn push_segment_error_carries_the_daemon_body() {
        use axum::{http::StatusCode, routing::post, Router};
        let app = Router::new().route("/ingest", post(|| async { (StatusCode::FORBIDDEN, "write routes are localhost-only") }));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let daemon = format!("http://{}", listener.local_addr().unwrap());
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });

        let client = reqwest::Client::new();
        let err = push_segment(&client, &daemon, "m", "M", 0, "live", 10_000, b"ts").await.unwrap_err();
        let msg = format!("{err:#}");
        assert!(msg.contains("403") && msg.contains("write routes are localhost-only"), "{msg}");
    }

    #[test]
    fn next_sequence_continues_after_catalog() {
        let c = serde_json::json!([
            {"publisher": "p", "content_id": "m", "last_sequence": 41},
            {"publisher": "p", "content_id": "x", "last_sequence": 3},
        ]);
        assert_eq!(next_sequence(&c, "p", "m"), 42);
        assert_eq!(next_sequence(&c, "p", "new"), 0);
    }

    #[test]
    fn next_sequence_ignores_other_publishers() {
        let c = serde_json::json!([
            {"publisher": "other", "content_id": "m", "last_sequence": 900},
            {"publisher": "mine", "content_id": "m", "last_sequence": 4},
        ]);
        assert_eq!(next_sequence(&c, "mine", "m"), 5);
        assert_eq!(next_sequence(&c, "nobody", "m"), 0);
    }

    #[tokio::test]
    async fn finish_posts_the_content_id() {
        use axum::{routing::post, Json, Router};
        let app = Router::new().route(
            "/ingest/finish",
            post(|Json(body): Json<serde_json::Value>| async move { Json(serde_json::json!({ "got": body })) }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let daemon = format!("http://{}", listener.local_addr().unwrap());
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });

        let data = finish(&reqwest::Client::new(), &daemon, "show").await.unwrap();
        assert_eq!(data["got"], serde_json::json!({ "content_id": "show" }));
    }

    #[tokio::test]
    async fn finish_error_carries_the_daemon_body() {
        use axum::{http::StatusCode, routing::post, Router};
        let app = Router::new().route("/ingest/finish", post(|| async { (StatusCode::CONFLICT, "run cosmosd keys init first") }));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let daemon = format!("http://{}", listener.local_addr().unwrap());
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });

        let err = finish(&reqwest::Client::new(), &daemon, "show").await.unwrap_err();
        let msg = format!("{err:#}");
        assert!(msg.contains("409") && msg.contains("run cosmosd keys init first"), "{msg}");
    }
}
