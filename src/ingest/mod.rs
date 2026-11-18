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

/// `last_sequence + 1` for `content_id` in a `/catalog` response, else 0.
pub fn next_sequence(catalog: &serde_json::Value, content_id: &str) -> u32 {
    catalog
        .as_array()
        .into_iter()
        .flatten()
        .find(|c| c["content_id"] == content_id)
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
    // Keep the daemon's reason (e.g. the 403 for a non-localhost ingest).
    let status = resp.status();
    if !status.is_success() {
        let body = resp.text().await.unwrap_or_default();
        anyhow::bail!("Daemon rejected segment {}: {}: {}", sequence, status, body.trim());
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

pub(crate) async fn check_daemon(client: &reqwest::Client, daemon: &str) -> Result<()> {
    client
        .get(format!("{}/health", daemon))
        .send()
        .await
        .context("Cannot reach cosmosd — is it running? cosmosd serve")?;
    Ok(())
}

// === File ingest ===

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
    check_daemon(&client, &daemon).await?;

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

    info!("Done: {} ({} segments)", title, entries.len());
    info!("Play: ffplay {}/play/{}", daemon, content_id);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

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
        let c = serde_json::json!([{"content_id": "m", "last_sequence": 41}, {"content_id": "x", "last_sequence": 3}]);
        assert_eq!(next_sequence(&c, "m"), 42);
        assert_eq!(next_sequence(&c, "new"), 0);
    }
}
