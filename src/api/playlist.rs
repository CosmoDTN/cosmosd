//! HLS playlist building.
//!
//! Live playlists sit `--delay` behind the newest segment: only segments at
//! least `delay` old are listed, so everything between that edge and the newest
//! segment is buffer that rides out link drops. A playlist never has a hole.

use std::collections::BTreeSet;
use std::time::Duration;

use chrono::{DateTime, Utc};

use crate::bundle::BundleMeta;

/// Most segments a live playlist lists.
pub const LIVE_WINDOW: usize = 6;

// === Builders ===

/// Live playlist for the window ending at the wall-clock edge `now - delay`.
///
/// `metas` is ascending by sequence. Returns `None` if nothing is old enough.
pub fn live_playlist(
    content_id: &str,
    metas: &[BundleMeta],
    now: DateTime<Utc>,
    delay: Duration,
) -> Option<String> {
    let edge = now - chrono::Duration::from_std(delay).ok()?;
    let last = metas.iter().filter(|m| m.created_at <= edge).map(|m| m.sequence).max()?;
    let present: BTreeSet<u32> = metas.iter().map(|m| m.sequence).collect();

    // Walk back from the edge while the previous sequence is present.
    let mut first = last;
    while ((last - first) as usize) + 1 < LIVE_WINDOW
        && first > 0
        && present.contains(&(first - 1))
    {
        first -= 1;
    }
    let run: Vec<&BundleMeta> = metas
        .iter()
        .filter(|m| (first..=last).contains(&m.sequence))
        .collect();
    Some(render(content_id, &run, false))
}

/// Playlist of every segment, closed with `EXT-X-ENDLIST`.
pub fn vod_playlist(content_id: &str, metas: &[BundleMeta]) -> String {
    let all: Vec<&BundleMeta> = metas.iter().collect();
    render(content_id, &all, true)
}

fn render(content_id: &str, segs: &[&BundleMeta], endlist: bool) -> String {
    let max_ms = segs.iter().map(|m| m.duration_ms).max().unwrap_or(0);
    let first = segs.first().map_or(0, |m| m.sequence);
    let mut out = format!(
        "#EXTM3U\n#EXT-X-VERSION:3\n#EXT-X-TARGETDURATION:{}\n#EXT-X-MEDIA-SEQUENCE:{}\n",
        max_ms.div_ceil(1000),
        first
    );
    for m in segs {
        out.push_str(&format!(
            "#EXTINF:{:.3},\n/segment/{}/{}\n",
            m.duration_ms as f64 / 1000.0,
            content_id,
            m.sequence
        ));
    }
    if endlist {
        out.push_str("#EXT-X-ENDLIST\n");
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bundle::Priority;

    fn metas(n: u32, now: DateTime<Utc>) -> Vec<BundleMeta> {
        (0..n)
            .map(|i| BundleMeta {
                id: format!("m/{:05}", i),
                content_id: "m".into(),
                content_title: "t".into(),
                sequence: i,
                priority: Priority::Live,
                created_at: now - chrono::Duration::seconds(10 * (n - 1 - i) as i64),
                expires_at: now + chrono::Duration::hours(1),
                duration_ms: 10_000,
            })
            .collect()
    }

    #[test]
    fn live_edge_is_now_minus_delay() {
        let now = Utc::now();
        let m = metas(20, now); // seq 7 created at now-120s
        let p = live_playlist("m", &m, now, Duration::from_secs(120)).unwrap();
        assert!(p.contains("#EXT-X-MEDIA-SEQUENCE:2\n"));
        assert!(p.contains("/segment/m/7\n"));
        assert!(!p.contains("/segment/m/8\n"));
        assert!(!p.contains("ENDLIST"));
    }

    #[test]
    fn hole_at_edge_shortens_the_run() {
        let now = Utc::now();
        let m: Vec<_> = metas(20, now).into_iter().filter(|x| x.sequence != 5).collect();
        let p = live_playlist("m", &m, now, Duration::from_secs(120)).unwrap();
        assert!(p.contains("#EXT-X-MEDIA-SEQUENCE:6\n"));
        assert!(!p.contains("/segment/m/4\n"));
    }

    #[test]
    fn nothing_old_enough_is_none() {
        let now = Utc::now();
        assert!(live_playlist("m", &metas(5, now), now, Duration::from_secs(120)).is_none());
    }

    #[test]
    fn target_duration_rounds_up() {
        let now = Utc::now();
        let mut m = metas(20, now);
        for x in &mut m {
            x.duration_ms = 10_010;
        }
        let p = live_playlist("m", &m, now, Duration::from_secs(120)).unwrap();
        assert!(p.contains("#EXT-X-TARGETDURATION:11\n"));
        assert!(p.contains("#EXTINF:10.010,\n"));
    }

    #[test]
    fn vod_lists_everything_and_ends() {
        let now = Utc::now();
        let p = vod_playlist("f", &metas(3, now));
        assert!(p.contains("#EXT-X-MEDIA-SEQUENCE:0\n"));
        assert!(p.contains("/segment/f/2\n"));
        assert!(p.ends_with("#EXT-X-ENDLIST\n"));
    }
}
