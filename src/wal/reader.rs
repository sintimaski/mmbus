//! Read-only view over a WAL directory.
//!
//! Subscribers use this to replay messages older than the ring's
//! oldest cursor without holding the publisher's writer-side `Wal`
//! handle.  Pure scan + parse — never runs recover_truncate, never
//! opens the active segment for writing, never spawns a flusher
//! thread; safe to call while a publisher is writing the same
//! directory.

use std::collections::BTreeMap;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};

use crate::wal::wal::{WalError, WalReplayer};

/// Read-only handle on a WAL directory's segment list.  Captures a
/// snapshot at `open()`-time; subsequent rotations on the publisher
/// side are NOT followed (the subscriber re-opens or transitions
/// to the live ring once caught up).
pub struct WalReader {
    /// `first_cursor → segment_path`, sorted ascending.
    segments: BTreeMap<u64, PathBuf>,
}

impl WalReader {
    /// Scan `dir/wal/*.seg` and build the segment index.  Returns an
    /// empty reader if the directory doesn't exist (caller decides
    /// whether to treat that as `CursorTooOld` or "no WAL").
    pub fn open(dir: &Path) -> io::Result<Self> {
        let wal_dir = dir.join("wal");
        let mut segments = BTreeMap::new();
        let entries = match fs::read_dir(&wal_dir) {
            Ok(e) => e,
            Err(e) if e.kind() == io::ErrorKind::NotFound => {
                return Ok(Self { segments });
            }
            Err(e) => return Err(e),
        };
        for entry in entries {
            let entry = entry?;
            let path = entry.path();
            let name = match path.file_name().and_then(|s| s.to_str()) {
                Some(n) => n,
                None => continue,
            };
            let stem = match name.strip_suffix(".seg") {
                Some(s) => s,
                None => continue,
            };
            if let Ok(first) = stem.parse::<u64>() {
                segments.insert(first, path);
            }
        }
        Ok(Self { segments })
    }

    /// First cursor still on disk, or `None` if the WAL is empty.
    pub fn oldest_cursor(&self) -> Option<u64> {
        self.segments.keys().next().copied()
    }

    /// Build a replayer starting at `cursor` (inclusive).  Returns
    /// `CursorTooOld` if `cursor` predates the oldest segment.
    pub fn read_from(&self, cursor: u64) -> Result<WalReplayer, WalError> {
        let oldest = match self.oldest_cursor() {
            Some(v) => v,
            None => return Ok(WalReplayer::new(Vec::new(), cursor)),
        };
        if cursor < oldest {
            return Err(WalError::CursorTooOld { requested: cursor, oldest });
        }
        let containing_first = self
            .segments
            .range(..=cursor)
            .next_back()
            .map(|(&k, _)| k)
            .unwrap_or(oldest);
        let segments: Vec<PathBuf> = self
            .segments
            .range(containing_first..)
            .map(|(_, p)| p.clone())
            .collect();
        Ok(WalReplayer::new(segments, cursor))
    }

    pub fn segments(&self) -> usize {
        self.segments.len()
    }
}
