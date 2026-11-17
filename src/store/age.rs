//! Monotonic clocks for bundle expiry.
//!
//! Wall clocks on a drifting node cannot be trusted, so expiry never reads
//! one. A bundle carries its age, and the store adds the time it has held the
//! bundle, measured on a clock that only moves forward.

#[cfg(test)]
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

use anyhow::{Context, Result};

/// A clock that only moves forward. Its zero point has no meaning, only
/// differences between two readings do.
pub trait MonoClock: Send + Sync {
    fn now_ms(&self) -> u64;

    /// Persist the clock so the next run continues from here.
    fn flush(&self) -> Result<()> {
        Ok(())
    }
}

// === SystemMono ===

const CLOCK_KEY: &[u8] = b"mono_ms";

/// A persisted base plus the time elapsed in this run.
///
/// Known limits: time the process is down, or the OS is suspended, is not
/// counted. After a restart the clock resumes from the last flush, so a bundle
/// held across an outage looks younger than it is. The reaper flushes every 5
/// minutes and a clean stop (Ctrl+C, SIGTERM) flushes once. An unclean stop
/// (crash, power loss, kill -9) can lose up to the 5 minute flush interval of
/// counted hold time, on each occurrence.
pub struct SystemMono {
    tree: sled::Tree,
    base_ms: u64,
    started: Instant,
}

impl SystemMono {
    /// Load the base from `tree`, or start at zero when it has none.
    pub fn open(tree: sled::Tree) -> Result<Self> {
        let base_ms = match tree.get(CLOCK_KEY)? {
            Some(raw) => u64::from_be_bytes(raw.as_ref().try_into().context("stored clock is not 8 bytes")?),
            None => 0,
        };
        Ok(SystemMono { tree, base_ms, started: Instant::now() })
    }
}

impl MonoClock for SystemMono {
    fn now_ms(&self) -> u64 {
        self.base_ms + u64::try_from(self.started.elapsed().as_millis()).unwrap_or(u64::MAX)
    }

    fn flush(&self) -> Result<()> {
        self.tree.insert(CLOCK_KEY, &self.now_ms().to_be_bytes())?;
        self.tree.flush()?;
        Ok(())
    }
}

// === FakeMono ===

/// A clock the test moves by hand.
#[cfg(test)]
#[derive(Default)]
pub struct FakeMono(AtomicU64);

#[cfg(test)]
impl FakeMono {
    pub fn advance(&self, ms: u64) {
        self.0.fetch_add(ms, Ordering::SeqCst);
    }
}

#[cfg(test)]
impl MonoClock for FakeMono {
    fn now_ms(&self) -> u64 {
        self.0.load(Ordering::SeqCst)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fake_clock_advances_by_hand() {
        let c = FakeMono::default();
        assert_eq!(c.now_ms(), 0);
        c.advance(5);
        c.advance(7);
        assert_eq!(c.now_ms(), 12);
    }

    #[test]
    fn system_clock_resumes_from_its_last_flush() {
        let db = sled::Config::new().temporary(true).open().unwrap();
        let tree = db.open_tree("sys").unwrap();
        let first = SystemMono::open(tree.clone()).unwrap();
        assert!(first.now_ms() < 1_000);
        tree.insert(CLOCK_KEY, &10_000u64.to_be_bytes()).unwrap();
        let second = SystemMono::open(tree.clone()).unwrap();
        assert!(second.now_ms() >= 10_000 && second.now_ms() < 11_000);
        second.flush().unwrap();
        let third = SystemMono::open(tree).unwrap();
        assert!(third.now_ms() >= 10_000);
    }

    #[test]
    fn store_flush_persists_elapsed_time_across_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().to_str().unwrap();
        {
            let store = crate::store::BundleStore::open(path).unwrap();
            std::thread::sleep(std::time::Duration::from_millis(50));
            store.flush_clock().unwrap();
        }
        // The flush ran after 50 ms of elapsed time, so the base is at least that.
        let store = crate::store::BundleStore::open(path).unwrap();
        assert!(store.clock.now_ms() >= 50);
    }
}
