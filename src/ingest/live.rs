//! Live ingest — transcode a live stream to capped H.264/AAC HLS and push
//! each finished segment to the daemon as a `live` bundle. The daemon signs
//! each one and publishes the next version of the live manifest.

use std::collections::HashSet;
use std::path::Path;
use std::process::Stdio;
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use tokio::process::Command;
use tracing::{info, warn};

use super::{check_daemon, next_sequence, parse_media_playlist, push_segment, which_ffmpeg};

const POLL_INTERVAL: Duration = Duration::from_millis(500);
const RESTART_DELAY: Duration = Duration::from_secs(2);
const AUDIO_KBPS: u32 = 128;

// === ffmpeg ===

/// ffmpeg arguments: 720p H.264 capped at `max_kbps` total, 10 s HLS segments.
/// `ts_offset` shifts output timestamps so a restarted run continues past the
/// previous one instead of starting over near zero.
pub fn live_ffmpeg_args(input: &str, out_dir: &Path, max_kbps: u32, ts_offset: Duration) -> Vec<String> {
    let v = max_kbps - AUDIO_KBPS;
    let out = out_dir.display();
    [
        "-hide_banner", "-loglevel", "warning",
        "-i", input,
        "-output_ts_offset", &format!("{:.3}", ts_offset.as_secs_f64()),
        "-map", "0:v:0", "-map", "0:a:0?",
        "-vf", "scale=-2:720",
        "-c:v", "libx264", "-preset", "veryfast", "-tune", "zerolatency",
        "-b:v", &format!("{v}k"),
        "-maxrate", &format!("{v}k"),
        "-bufsize", &format!("{}k", 2 * v),
        "-force_key_frames", "expr:gte(t,n_forced*10)",
        "-c:a", "aac", "-b:a", "128k", "-ac", "2",
        "-f", "hls",
        "-hls_time", "10",
        "-hls_list_size", "0",
        "-hls_flags", "independent_segments",
        "-hls_segment_filename", &format!("{out}/seg%05d.ts"),
        &format!("{out}/index.m3u8"),
    ]
    .iter()
    .map(|s| s.to_string())
    .collect()
}

// === Live loop ===

pub async fn ingest_live(
    input: String,
    content_id: String,
    title: String,
    daemon: String,
    max_kbps: u32,
) -> Result<()> {
    which_ffmpeg().context("ffmpeg not found — install it: apt install ffmpeg / brew install ffmpeg")?;
    let client = reqwest::Client::new();
    let publisher = check_daemon(&client, &daemon).await?;

    let catalog: serde_json::Value = client
        .get(format!("{}/catalog", daemon))
        .send()
        .await?
        .json()
        .await
        .context("Bad /catalog response")?;
    let mut sequence = next_sequence(&catalog, &publisher, &content_id);

    info!("Live ingest: {} ({})", title, content_id);
    info!("Input:      {}", input);
    info!("Daemon:     {}", daemon);
    info!("Publisher:  {}", publisher);
    info!("Max rate:   {} kbps, first sequence {}", max_kbps, sequence);

    // Dropping `run` on either signal kills ffmpeg (kill_on_drop) and
    // removes its tempdir.
    let terminate = terminate_signal()?;
    tokio::select! {
        r = run(&input, &content_id, &title, &daemon, max_kbps, &client, &mut sequence) => r,
        _ = tokio::signal::ctrl_c() => {
            info!("Stopping live ingest (SIGINT)");
            Ok(())
        }
        _ = terminate => {
            info!("Stopping live ingest (SIGTERM)");
            Ok(())
        }
    }
}

/// Resolves on SIGTERM; never on platforms without it.
#[cfg(unix)]
fn terminate_signal() -> Result<impl std::future::Future<Output = ()>> {
    use tokio::signal::unix::{signal, SignalKind};
    let mut term = signal(SignalKind::terminate()).context("cannot listen for SIGTERM")?;
    Ok(async move {
        term.recv().await;
    })
}

#[cfg(not(unix))]
fn terminate_signal() -> Result<impl std::future::Future<Output = ()>> {
    Ok(std::future::pending())
}

/// Run ffmpeg and forward its segments, restarting forever. Each run's
/// timestamps are offset by the time since the first run started.
async fn run(
    input: &str,
    content_id: &str,
    title: &str,
    daemon: &str,
    max_kbps: u32,
    client: &reqwest::Client,
    sequence: &mut u32,
) -> Result<()> {
    let first_start = Instant::now();
    loop {
        let tmp = tempfile::tempdir()?;
        let dir = tmp.path();
        let mut child = Command::new("ffmpeg")
            .args(live_ffmpeg_args(input, dir, max_kbps, first_start.elapsed()))
            .stdin(Stdio::null())
            .kill_on_drop(true)
            .spawn()
            .context("ffmpeg failed to run")?;

        let mut pushed: HashSet<String> = HashSet::new();
        loop {
            push_new(dir, &mut pushed, content_id, title, daemon, client, sequence).await;
            if let Some(status) = child.try_wait()? {
                // Flush whatever ffmpeg finished before it exited.
                push_new(dir, &mut pushed, content_id, title, daemon, client, sequence).await;
                warn!("ffmpeg exited ({}); restarting in {:?}", status, RESTART_DELAY);
                break;
            }
            tokio::time::sleep(POLL_INTERVAL).await;
        }
        tokio::time::sleep(RESTART_DELAY).await;
    }
}

/// Push every playlist entry not yet pushed in this ffmpeg run.
async fn push_new(
    dir: &Path,
    pushed: &mut HashSet<String>,
    content_id: &str,
    title: &str,
    daemon: &str,
    client: &reqwest::Client,
    sequence: &mut u32,
) {
    let Ok(text) = std::fs::read_to_string(dir.join("index.m3u8")) else {
        return;
    };
    for entry in parse_media_playlist(&text) {
        if pushed.contains(&entry.uri) {
            continue;
        }
        let path = dir.join(&entry.uri);
        let payload = match std::fs::read(&path) {
            Ok(p) => p,
            Err(e) => {
                warn!("Skipping {}: {}", entry.uri, e);
                pushed.insert(entry.uri);
                continue;
            }
        };
        match push_segment(client, daemon, content_id, title, *sequence, "live", entry.duration_ms, &payload).await {
            Ok(data) => {
                info!("  {} → seq {} {} (stored={})", entry.uri, *sequence, data["bundle_id"], data["stored"]);
                *sequence += 1;
                pushed.insert(entry.uri);
                let _ = std::fs::remove_file(&path);
            }
            // Leave it unmarked so the next poll retries the same sequence.
            Err(e) => {
                warn!("Push of {} failed: {:#}", entry.uri, e);
                return;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ffmpeg_args_cap_bitrate_and_cut_ten_seconds() {
        let a = live_ffmpeg_args("udp://127.0.0.1:5000", Path::new("/t"), 1200, Duration::ZERO);
        let has = |k: &str, v: &str| a.windows(2).any(|w| w[0] == k && w[1] == v);
        assert!(has("-b:v", "1072k") && has("-maxrate", "1072k") && has("-bufsize", "2144k"));
        assert!(has("-c:v", "libx264") && has("-hls_time", "10") && has("-b:a", "128k"));
        assert!(has("-i", "udp://127.0.0.1:5000"));
        assert!(has("-output_ts_offset", "0.000"));
        assert_eq!(a.last().unwrap(), "/t/index.m3u8");
    }

    #[test]
    fn ffmpeg_args_offset_timestamps_after_a_restart() {
        let a = live_ffmpeg_args("udp://127.0.0.1:5000", Path::new("/t"), 1200, Duration::from_millis(12_500));
        let at = a.iter().position(|s| s == "-output_ts_offset").expect("offset flag");
        assert_eq!(a[at + 1], "12.500");
        // An output option: after the input, before the output path.
        assert!(at > a.iter().position(|s| s == "-i").unwrap());
        assert!(at < a.len() - 1);
    }
}
