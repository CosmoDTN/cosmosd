//! playcheck: a headless HLS live player for checking playback.
//!
//! Waits for the playlist, starts at `max(first, last - 2)` like real players,
//! then "plays" each segment by fetching it (it must return 200) and sleeping
//! for its `EXTINF`. When the next segment isn't listed it either jumps (the
//! playlist has slid past the playhead) or stalls, retrying every second.
//!
//! Every event line is `t=<elapsed secs> <event> seq=<n> lag=<secs>`, where lag
//! is how far the playhead is behind the segment's `x-cosmosd-created-at`. At
//! exit it prints `summary stalls=<n> stall_secs=<n> jumps=<n> final_lag=<secs>`.

use std::time::{Duration, Instant};

use anyhow::{bail, Context, Result};
use chrono::{DateTime, Utc};
use clap::Parser;

const RETRY: Duration = Duration::from_secs(1);

#[derive(Parser)]
#[command(about = "Headless HLS live player: logs stalls, jumps and lag behind live")]
struct Args {
    /// Live playlist URL, e.g. http://127.0.0.1:37777/play/demo
    #[arg(long)]
    url: String,
    /// How long to run, counted from start (including the wait for the playlist)
    #[arg(long)]
    duration_secs: u64,
}

// === Playlist ===

#[derive(Debug, Clone, PartialEq)]
struct Segment {
    seq: u64,
    secs: f64,
    uri: String,
}

/// Parse a media playlist: `EXT-X-MEDIA-SEQUENCE` numbers the first segment.
fn parse_playlist(text: &str) -> Vec<Segment> {
    let mut seq = 0u64;
    let mut secs = None;
    let mut out = Vec::new();
    for line in text.lines().map(str::trim) {
        if let Some(v) = line.strip_prefix("#EXT-X-MEDIA-SEQUENCE:") {
            seq = v.parse().unwrap_or(0);
        } else if let Some(v) = line.strip_prefix("#EXTINF:") {
            secs = v.split(',').next().and_then(|d| d.parse().ok());
        } else if !line.is_empty() && !line.starts_with('#') {
            out.push(Segment { seq, secs: secs.take().unwrap_or(10.0), uri: line.to_string() });
            seq += 1;
        }
    }
    out
}

/// Where a player starts (or rejoins): two segments back from the live edge.
fn join_point(list: &[Segment]) -> Option<&Segment> {
    let last = list.last()?;
    let target = last.seq.saturating_sub(2).max(list[0].seq);
    list.iter().find(|s| s.seq == target)
}

// === Player ===

struct Player {
    client: reqwest::Client,
    url: reqwest::Url,
    start: Instant,
    stalls: u32,
    stall_secs: f64,
    jumps: u32,
    /// Created-at of the segment playing and when it started playing.
    playing: Option<(DateTime<Utc>, Instant, f64)>,
}

impl Player {
    fn t(&self) -> f64 {
        self.start.elapsed().as_secs_f64()
    }

    /// Playhead lag behind live: now minus the playing segment's creation,
    /// less how far into that segment playback has got.
    fn lag(&self) -> f64 {
        match self.playing {
            Some((created, started, secs)) => {
                let behind = (Utc::now() - created).num_milliseconds() as f64 / 1000.0;
                behind - started.elapsed().as_secs_f64().min(secs)
            }
            None => 0.0,
        }
    }

    fn event(&self, name: &str, seq: u64) {
        println!("t={:.1} {} seq={} lag={:.1}", self.t(), name, seq, self.lag());
    }

    /// The playlist, or None while it isn't there (404 buffering, error).
    async fn playlist(&self) -> Option<Vec<Segment>> {
        let resp = self.client.get(self.url.clone()).send().await.ok()?;
        if !resp.status().is_success() {
            return None;
        }
        let list = parse_playlist(&resp.text().await.ok()?);
        (!list.is_empty()).then_some(list)
    }

    /// Fetch one segment (must be 200) and note its creation time.
    async fn fetch(&mut self, seg: &Segment) -> Result<()> {
        let url = self.url.join(&seg.uri)?;
        let resp = self.client.get(url.clone()).send().await?;
        if resp.status() != reqwest::StatusCode::OK {
            bail!("segment {} returned {}", url, resp.status());
        }
        let created = resp
            .headers()
            .get("x-cosmosd-created-at")
            .and_then(|v| v.to_str().ok())
            .and_then(|v| DateTime::parse_from_rfc3339(v).ok())
            .with_context(|| format!("segment {} has no x-cosmosd-created-at", url))?
            .with_timezone(&Utc);
        resp.bytes().await?;
        self.playing = Some((created, Instant::now(), seg.secs));
        Ok(())
    }

    async fn run(&mut self, until: Duration) -> Result<()> {
        // 1. Wait for the playlist, then join two segments back from the edge.
        let mut next = loop {
            if self.start.elapsed() >= until {
                return Ok(());
            }
            if let Some(list) = self.playlist().await {
                break join_point(&list).expect("non-empty").seq;
            }
            tokio::time::sleep(RETRY).await;
        };
        let mut first_play = true;
        let mut stalled: Option<Instant> = None;

        // 2. Play, stall or jump until time is up.
        while self.start.elapsed() < until {
            let list = self.playlist().await.unwrap_or_default();
            let jump_to = match list.first() {
                Some(first) if first.seq > next => join_point(&list).cloned(),
                _ => None,
            };
            let seg = list.iter().find(|s| s.seq == next).cloned().or(jump_to.clone());
            let Some(seg) = seg else {
                if stalled.is_none() {
                    self.stalls += 1;
                    stalled = Some(Instant::now());
                    self.event("stall start", next);
                }
                tokio::time::sleep(RETRY).await;
                continue;
            };
            if let Some(since) = stalled.take() {
                self.stall_secs += since.elapsed().as_secs_f64();
                self.event("stall end", next);
            }
            self.fetch(&seg).await?;
            if jump_to.is_some() {
                self.jumps += 1;
                self.event("jump", seg.seq);
            } else if first_play {
                self.event("start", seg.seq);
            } else {
                self.event("play", seg.seq);
            }
            first_play = false;
            next = seg.seq + 1;
            let left = until.saturating_sub(self.start.elapsed());
            tokio::time::sleep(Duration::from_secs_f64(seg.secs).min(left)).await;
        }
        if let Some(since) = stalled {
            self.stall_secs += since.elapsed().as_secs_f64();
        }
        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    let mut player = Player {
        client: reqwest::Client::builder().timeout(Duration::from_secs(10)).build()?,
        url: reqwest::Url::parse(&args.url).context("bad --url")?,
        start: Instant::now(),
        stalls: 0,
        stall_secs: 0.0,
        jumps: 0,
        playing: None,
    };
    let result = player.run(Duration::from_secs(args.duration_secs)).await;
    println!(
        "summary stalls={} stall_secs={:.0} jumps={} final_lag={:.1}",
        player.stalls,
        player.stall_secs,
        player.jumps,
        player.lag()
    );
    result
}

#[cfg(test)]
mod tests {
    use super::*;

    const LIST: &str = "#EXTM3U\n#EXT-X-VERSION:3\n#EXT-X-TARGETDURATION:10\n#EXT-X-MEDIA-SEQUENCE:4\n\
        #EXTINF:10.010,\n/segment/demo/4\n#EXTINF:9.500,\n/segment/demo/5\n#EXTINF:10.000,\n/segment/demo/6\n\
        #EXTINF:10.000,\n/segment/demo/7\n";

    #[test]
    fn parses_sequence_and_durations() {
        let l = parse_playlist(LIST);
        assert_eq!(l.len(), 4);
        assert_eq!(l[1], Segment { seq: 5, secs: 9.5, uri: "/segment/demo/5".into() });
    }

    #[test]
    fn joins_two_back_from_the_edge() {
        let l = parse_playlist(LIST);
        assert_eq!(join_point(&l).unwrap().seq, 5);
        assert_eq!(join_point(&l[..1]).unwrap().seq, 4);
    }
}
