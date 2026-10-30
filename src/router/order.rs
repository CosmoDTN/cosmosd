//! Pull order — what a viewer needs soonest comes first.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::time::Duration;

use serde::{Deserialize, Serialize};

use crate::bundle::Priority;

// === Types ===

/// One line of a peer's manifest: a bundle id and how urgently it is wanted.
// Used by Task 4 sync protocol.
#[allow(dead_code)]
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

/// Order the bundles we lack so the ones a viewer needs soonest come first:
/// the live window (newest segments, ascending), then live backfill (newest
/// first), then VOD ascending. Unparsable ids go last.
// Used by Task 4 sync protocol.
#[allow(dead_code)]
pub fn pull_order(their: &[ManifestEntry], ours: &HashSet<String>, delay: Duration) -> Vec<String> {
    // Newest seq per live/near-live content, over everything they have.
    let mut newest: HashMap<&str, u32> = HashMap::new();
    for e in their.iter().filter(|e| e.priority != Priority::Vod) {
        if let Some((content, seq)) = parse_id(&e.id) {
            let n = newest.entry(content).or_insert(seq);
            *n = (*n).max(seq);
        }
    }
    let margin = delay.as_secs() as u32 / 10 + 6;

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
        let urgent_from = newest[content].saturating_sub(margin);
        let set = if seq >= urgent_from { &mut urgent } else { &mut backfill };
        set.entry((e.priority, content)).or_default().push((seq, &e.id));
    }

    let mut out = Vec::new();
    for mut ids in urgent.into_values() {
        ids.sort();
        out.extend(ids.into_iter().map(|(_, id)| id.to_string()));
    }
    for mut ids in backfill.into_values() {
        ids.sort_by(|a, b| b.cmp(a));
        out.extend(ids.into_iter().map(|(_, id)| id.to_string()));
    }
    vod.sort();
    out.extend(vod.into_iter().map(|(_, _, id)| id.to_string()));
    out.extend(unparsable.into_iter().map(String::from));
    out
}

// === Tests ===

#[cfg(test)]
mod tests {
    use super::*;

    fn live(id: &str) -> ManifestEntry { ManifestEntry { id: id.into(), priority: Priority::Live } }
    fn vod(id: &str) -> ManifestEntry { ManifestEntry { id: id.into(), priority: Priority::Vod } }

    #[test]
    fn live_window_first_then_backfill_then_vod() {
        let mut their: Vec<_> = (0..100).map(|s| live(&format!("m/{:05}", s))).collect();
        their.push(vod("f/00001"));
        their.push(vod("f/00000"));
        let ours: HashSet<String> = (0..50).map(|s| format!("m/{:05}", s)).collect();
        let order = pull_order(&their, &ours, Duration::from_secs(120));
        // newest 99, delay 12 segments + 6 margin -> urgent from 81
        let expect_urgent: Vec<String> = (81..100).map(|s| format!("m/{:05}", s)).collect();
        assert_eq!(&order[..19], &expect_urgent[..]);
        assert_eq!(order[19], "m/00080");
        assert_eq!(order[49], "m/00050");
        assert_eq!(&order[50..], &["f/00000".to_string(), "f/00001".to_string()]);
    }

    #[test]
    fn nothing_missing_means_nothing_to_pull() {
        let their = vec![live("m/00000")];
        let ours: HashSet<String> = ["m/00000".to_string()].into();
        assert!(pull_order(&their, &ours, Duration::from_secs(120)).is_empty());
    }
}
