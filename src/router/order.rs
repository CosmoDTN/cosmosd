//! Pull order — what a viewer needs soonest comes first.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::time::Duration;

use serde::{Deserialize, Serialize};

use crate::bundle::Priority;

// === Types ===

/// One line of a peer's manifest: a bundle id and how urgently it is wanted.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ManifestEntry {
    pub id: String,
    pub priority: Priority,
}

// === Ordering ===

/// Split `<content_id>/<seq>` at the last `/`.
fn parse_id(id: &str) -> Option<(&str, u32)> {
    let (content, seq) = id.rsplit_once('/')?;
    Some((content, seq.parse().ok()?))
}

/// Most non-urgent bundles (backfill, VOD, unparsable) one session pulls.
pub const NON_URGENT_PER_SESSION: usize = 2;

/// What one sync session pulls, most urgent first.
///
/// Every session pulls all of its urgent bundles: the live and near-live
/// segments missing from the delay window (the newest `delay` of segments),
/// ascending. It then pulls at most [`NON_URGENT_PER_SESSION`] more, taken in
/// order from live backfill (newest first), then VOD ascending, then
/// unparsable ids. This holds whether or not the session had urgent work.
///
/// The window starts exactly `delay` behind the newest segment: after a long
/// outage players rejoin at the oldest segment they find near the edge, so
/// anything older pulled first would leave them further behind for good.
/// The bounded tail keeps a long backlog from holding up live segments made
/// while a session transfers, yet still moves backfill and VOD forward every
/// session, even while live segments keep arriving.
pub fn pull_order(their: &[ManifestEntry], ours: &HashSet<String>, delay: Duration) -> Vec<String> {
    // Newest seq per live/near-live content, over everything they have.
    let mut newest: HashMap<&str, u32> = HashMap::new();
    for e in their.iter().filter(|e| e.priority != Priority::Vod) {
        if let Some((content, seq)) = parse_id(&e.id) {
            let n = newest.entry(content).or_insert(seq);
            *n = (*n).max(seq);
        }
    }
    let window = delay.as_secs() as u32 / 10; // in 10 s segments

    // (priority, content) sorts Live before NearLive, then by content id.
    let mut urgent: BTreeMap<(Priority, &str), Vec<(u32, &str)>> = BTreeMap::new();
    let mut backfill: BTreeMap<(Priority, &str), Vec<(u32, &str)>> = BTreeMap::new();
    let mut vod: Vec<(&str, u32, &str)> = Vec::new();
    let mut unparsable: Vec<&str> = Vec::new();

    for e in their.iter().filter(|e| !ours.contains(&e.id)) {
        let Some((content, seq)) = parse_id(&e.id) else {
            unparsable.push(&e.id);
            continue;
        };
        if e.priority == Priority::Vod {
            vod.push((content, seq, &e.id));
            continue;
        }
        let urgent_from = newest[content].saturating_sub(window);
        let set = if seq >= urgent_from { &mut urgent } else { &mut backfill };
        set.entry((e.priority, content)).or_default().push((seq, &e.id));
    }

    let mut out = Vec::new();
    for mut ids in urgent.into_values() {
        ids.sort();
        out.extend(ids.into_iter().map(|(_, id)| id.to_string()));
    }
    let mut tail = Vec::new();
    for mut ids in backfill.into_values() {
        ids.sort_by(|a, b| b.cmp(a));
        tail.extend(ids.into_iter().map(|(_, id)| id.to_string()));
    }
    vod.sort();
    tail.extend(vod.into_iter().map(|(_, _, id)| id.to_string()));
    tail.extend(unparsable.into_iter().map(String::from));
    out.extend(tail.into_iter().take(NON_URGENT_PER_SESSION));
    out
}

// === Tests ===

#[cfg(test)]
mod tests {
    use super::*;

    fn live(id: &str) -> ManifestEntry { ManifestEntry { id: id.into(), priority: Priority::Live } }
    fn vod(id: &str) -> ManifestEntry { ManifestEntry { id: id.into(), priority: Priority::Vod } }

    #[test]
    fn urgent_work_then_two_backfill() {
        let mut their: Vec<_> = (0..100).map(|s| live(&format!("m/{:05}", s))).collect();
        their.push(vod("f/00001"));
        their.push(vod("f/00000"));
        let ours: HashSet<String> = (0..50).map(|s| format!("m/{:05}", s)).collect();
        let order = pull_order(&their, &ours, Duration::from_secs(120));
        // newest 99, delay 12 segments -> urgent from 87, then 2 backfill, newest first
        let mut expect: Vec<String> = (87..100).map(|s| format!("m/{:05}", s)).collect();
        expect.extend(["m/00086".to_string(), "m/00085".to_string()]);
        assert_eq!(order, expect);
    }

    #[test]
    fn no_urgent_work_pulls_two_backfill() {
        let mut their: Vec<_> = (0..100).map(|s| live(&format!("m/{:05}", s))).collect();
        their.push(vod("f/00001"));
        their.push(vod("f/00000"));
        let ours: HashSet<String> = (0..30).chain(87..100).map(|s| format!("m/{:05}", s)).collect();
        let order = pull_order(&their, &ours, Duration::from_secs(120));
        assert_eq!(order, vec!["m/00086".to_string(), "m/00085".to_string()]);
    }

    #[test]
    fn tail_runs_backfill_then_vod_then_unparsable() {
        // newest 13, window 12 -> urgent from 1, all held; backfill is just 0
        let mut their: Vec<_> = (0..14).map(|s| live(&format!("m/{:05}", s))).collect();
        their.push(vod("f/00000"));
        their.push(live("junk"));
        let ours: HashSet<String> = (1..14).map(|s| format!("m/{:05}", s)).collect();
        let order = pull_order(&their, &ours, Duration::from_secs(120));
        assert_eq!(order, vec!["m/00000".to_string(), "f/00000".to_string()]);

        let their = vec![live("junk"), vod("f/00001"), vod("f/00000")];
        let order = pull_order(&their, &HashSet::new(), Duration::from_secs(120));
        assert_eq!(order, vec!["f/00000".to_string(), "f/00001".to_string()]);

        let their = vec![live("junk"), vod("f/00000")];
        let order = pull_order(&their, &HashSet::new(), Duration::from_secs(120));
        assert_eq!(order, vec!["f/00000".to_string(), "junk".to_string()]);
    }

    /// After an outage longer than the delay, a player rejoins at the oldest
    /// segment it finds near the edge, so the first one pulled must be the
    /// one `delay` behind the newest, not older.
    #[test]
    fn long_outage_pulls_the_rejoin_point_first() {
        let their: Vec<_> = (0..72).map(|s| live(&format!("demo/{:05}", s))).collect();
        let ours: HashSet<String> = (0..40).map(|s| format!("demo/{:05}", s)).collect();
        let order = pull_order(&their, &ours, Duration::from_secs(120));
        assert_eq!(order[0], "demo/00059"); // newest 71 minus 12 segments of delay
        assert_eq!(order[12], "demo/00071");
        // then only NON_URGENT_PER_SESSION of the backfill, newest first
        assert_eq!(&order[13..], &["demo/00058".to_string(), "demo/00057".to_string()]);
    }

    #[test]
    fn nothing_missing_means_nothing_to_pull() {
        let their = vec![live("m/00000")];
        let ours: HashSet<String> = ["m/00000".to_string()].into();
        assert!(pull_order(&their, &ours, Duration::from_secs(120)).is_empty());
    }
}
