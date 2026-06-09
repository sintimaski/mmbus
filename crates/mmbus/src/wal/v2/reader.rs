//! Read-only directory scanner over a v2 WAL (W2-6).
//!
//! Mirrors [`crate::wal::reader::WalReader`]'s public API so the
//! subscriber's replay path swaps backends transparently behind the
//! `wal_v2` feature flag.  Pure scan + parse — never writes, never
//! runs recovery, safe to call while a publisher is appending to
//! the same directory.

use std::collections::BTreeMap;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};

use crate::wal::v2::wal::{WalError, WalReplayer};

/// Read-only handle on a WAL v2 directory's segment list.  Captures
/// a snapshot at `open()`-time; subsequent rotations are NOT
/// followed (the subscriber re-opens or transitions to the live
/// ring once caught up).
pub struct WalReader {
    /// `first_cursor → segment_path`, sorted ascending.
    segments: BTreeMap<u64, PathBuf>,
}

impl WalReader {
    /// Scan `dir/wal/*.seg` and build the segment index.  Returns an
    /// empty reader if the directory doesn't exist.
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

    pub fn oldest_cursor(&self) -> Option<u64> {
        self.segments.keys().next().copied()
    }

    pub fn read_from(&self, cursor: u64) -> Result<WalReplayer, WalError> {
        let oldest = match self.oldest_cursor() {
            Some(v) => v,
            None => return Ok(WalReplayer::new(Vec::new(), cursor)),
        };
        if cursor < oldest {
            return Err(WalError::CursorTooOld {
                requested: cursor,
                oldest,
            });
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::wal::config::{FsyncPolicy, WalConfig};
    use crate::wal::v2::wal::Wal;
    use tempfile::TempDir;

    fn tmpdir() -> TempDir {
        tempfile::tempdir().expect("tempdir")
    }

    fn cfg() -> WalConfig {
        WalConfig {
            enabled: true,
            fsync_policy: FsyncPolicy::None,
            segment_size_max: 4096,
            retention_bytes: 1 << 20,
            ..WalConfig::default()
        }
    }

    #[test]
    fn open_on_missing_dir_returns_empty_reader() {
        let dir = tmpdir();
        let r = WalReader::open(dir.path()).unwrap();
        assert_eq!(r.segments(), 0);
        assert!(r.oldest_cursor().is_none());
    }

    #[test]
    fn open_after_writes_indexes_segments() {
        let dir = tmpdir();
        {
            let w = Wal::open(dir.path(), cfg()).unwrap();
            for i in 0..5u64 {
                w.append(i, 0, b"x").unwrap();
            }
        }
        let r = WalReader::open(dir.path()).unwrap();
        assert!(r.segments() >= 1);
        assert_eq!(r.oldest_cursor(), Some(0));
    }

    #[test]
    fn read_from_below_oldest_returns_cursor_too_old() {
        // Build a WAL whose oldest cursor is non-zero (rotate +
        // prune by setting tight retention).
        let dir = tmpdir();
        let mut c = cfg();
        c.segment_size_max = 112;
        c.retention_bytes = 200;
        {
            let w = Wal::open(dir.path(), c).unwrap();
            for i in 0..6u64 {
                w.append(i, 0, &i.to_le_bytes()).unwrap();
            }
        }
        let r = WalReader::open(dir.path()).unwrap();
        let oldest = r.oldest_cursor().unwrap();
        assert!(
            oldest > 0,
            "expected pruning to advance oldest, got {oldest}"
        );
        match r.read_from(0) {
            Err(WalError::CursorTooOld { requested: 0, .. }) => (),
            Err(e) => panic!("expected CursorTooOld, got Err({e:?})"),
            Ok(_) => panic!("expected CursorTooOld, got Ok(_)"),
        }
    }

    #[test]
    fn read_from_replays_committed_records() {
        let dir = tmpdir();
        {
            let w = Wal::open(dir.path(), cfg()).unwrap();
            for i in 0..3u64 {
                w.append(i, 0, format!("rec-{i}").as_bytes()).unwrap();
            }
        }
        let r = WalReader::open(dir.path()).unwrap();
        let records: Vec<_> = r.read_from(0).unwrap().map(|r| r.unwrap()).collect();
        assert_eq!(records.len(), 3);
        for (i, rec) in records.iter().enumerate() {
            assert_eq!(rec.cursor, i as u64);
            assert_eq!(rec.payload, format!("rec-{i}").as_bytes());
        }
    }
}
