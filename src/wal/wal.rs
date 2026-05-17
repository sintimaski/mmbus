//! `Wal` — multi-segment aggregator.
//!
//! Owns a directory of segment files plus the per-segment writer for
//! the "active" segment (the most recent one).  Handles rotation
//! (close + roll over when active hits `segment_size_max`), retention
//! (background thread deletes the oldest segment when total bytes
//! exceed `retention_bytes`), the in-memory index
//! (`BTreeMap<first_cursor -> path>`), and the batched flusher when
//! `FsyncPolicy::Batched` is configured.
//!
//! Single-writer (mmbus is SPMC) — relies on the existing
//! `producer.lock` for cross-process exclusion.

use std::collections::BTreeMap;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use crate::wal::config::{FsyncPolicy, WalConfig};
use crate::wal::record::{Record, MAX_PAYLOAD_LEN};
use crate::wal::segment_reader::{recover_truncate, ReaderError, SegmentReader};
use crate::wal::segment_writer::{SegmentWriter, WriterError};
use crate::wal::stats::WalStats;

#[derive(Debug, thiserror::Error)]
pub enum WalError {
    #[error("I/O error: {0}")]
    Io(#[from] io::Error),

    #[error("segment writer: {0}")]
    Writer(#[from] WriterError),

    #[error("segment reader: {0}")]
    Reader(#[from] ReaderError),

    #[error("cursor {requested} is older than the oldest in-WAL slot ({oldest})")]
    CursorTooOld { requested: u64, oldest: u64 },

    #[error("WAL flusher thread died; further appends would silently lose durability")]
    Poisoned,

    #[error("payload too large: {payload_len} > MAX_PAYLOAD_LEN ({MAX_PAYLOAD_LEN})")]
    PayloadTooLarge { payload_len: usize },
}

/// Multi-segment write-ahead log.  See module docs for the threading
/// model.
pub struct Wal {
    dir: PathBuf,
    cfg: WalConfig,
    /// Index of all segments on disk (active + retained), keyed by
    /// first_cursor.  The active segment is the entry with the
    /// largest key; reads walk the map in ascending key order.
    /// Protected by the same `Mutex` as the writer because both
    /// rotation and retention mutate this map.
    inner: Arc<Mutex<Inner>>,
    /// Highest cursor durable on disk.  Updated by `fsync()` (in
    /// Each + by the flusher thread under Batched).  Subscribers
    /// under Batched are clamped to this value.
    durable_cursor: Arc<AtomicU64>,
    /// Flusher thread shutdown flag (only spawned under Batched).
    shutdown: Arc<AtomicBool>,
    /// Flusher thread handle.  None when fsync_policy != Batched.
    flusher_thread: Option<JoinHandle<()>>,
    /// True when the flusher thread observed an error and stopped;
    /// subsequent appends should return `WalError::Poisoned` rather
    /// than silently dropping durability.
    poisoned: Arc<AtomicBool>,
}

/// State guarded by the inner Mutex.  `writer` is `Option` because
/// rotation briefly drops + recreates it (close old, open new).
struct Inner {
    /// `first_cursor → segment_path`, sorted ascending.  Includes
    /// the active segment as the last entry.
    segments: BTreeMap<u64, PathBuf>,
    /// Bytes-per-segment cache, parallel to `segments`.  Updated on
    /// rotation + retention to avoid `stat`-ing every time we compute
    /// total_wal_bytes for stats / retention checks.
    segment_sizes: BTreeMap<u64, u64>,
    /// Writer for the active segment.  Always present once the WAL
    /// has been opened; rotation transiently sets it to None while
    /// the old writer is closed and the new one created.
    writer: Option<SegmentWriter>,
}

impl Wal {
    /// Open the WAL rooted at `dir`.  Creates `dir/wal/` if needed,
    /// runs `recover_truncate` on every existing segment, builds the
    /// in-memory index.  When `cfg.fsync_policy == Batched` also
    /// spawns the flusher thread.
    pub fn open(dir: &Path, cfg: WalConfig) -> Result<Self, WalError> {
        let wal_dir = dir.join("wal");
        fs::create_dir_all(&wal_dir)?;

        // Scan + recover existing segments.
        let mut segments = BTreeMap::new();
        let mut segment_sizes = BTreeMap::new();
        for entry in fs::read_dir(&wal_dir)? {
            let entry = entry?;
            let path = entry.path();
            let name = path
                .file_name()
                .and_then(|s| s.to_str())
                .unwrap_or_default()
                .to_string();
            if !name.ends_with(".seg") {
                continue;
            }
            let stem = &name[..name.len() - 4];
            let first_cursor: u64 = match stem.parse() {
                Ok(v) => v,
                Err(_) => continue, // ignore stray files
            };
            let _ = recover_truncate(&path)?;
            let len = fs::metadata(&path)?.len();
            segments.insert(first_cursor, path);
            segment_sizes.insert(first_cursor, len);
        }

        let (writer, pending_cursor) = if let Some((&last_first_cursor, last_path)) =
            segments.iter().next_back()
        {
            // Existing active segment — compute the next cursor from
            // its contents, then rotate to a fresh segment named after
            // that cursor.  We don't append into the existing file
            // (SegmentWriter::create uses create_new, which would
            // collide); rotating is conservative but keeps the writer
            // logic simple (one segment per process lifetime, plus
            // whatever existed before).
            let mut last_cursor: Option<u64> = None;
            let mut reader = SegmentReader::open(last_path)?;
            for r in reader.iter().flatten() {
                last_cursor = Some(r.cursor);
            }
            let next_cursor = last_cursor.map(|c| c + 1).unwrap_or(last_first_cursor);
            // If the prior segment was empty (no records) we can reuse
            // its slot rather than rotating into another empty one.
            if next_cursor == last_first_cursor {
                // Empty segment — leave it; defer writer creation
                // until the first append, like the fresh-WAL path.
                (None, last_first_cursor)
            } else {
                let new_path = wal_dir.join(format!("{next_cursor:020}.seg"));
                let writer = SegmentWriter::create(&new_path, next_cursor)?;
                segments.insert(next_cursor, new_path.clone());
                segment_sizes.insert(next_cursor, writer.bytes_written());
                (Some(writer), next_cursor)
            }
        } else {
            // Fresh WAL — defer the writer creation to the first
            // append so an opened-but-never-used Wal doesn't leave
            // an empty segment file behind.
            (None, 0)
        };

        let durable_cursor = Arc::new(AtomicU64::new(pending_cursor));
        let inner = Arc::new(Mutex::new(Inner { segments, segment_sizes, writer }));
        let shutdown = Arc::new(AtomicBool::new(false));
        let poisoned = Arc::new(AtomicBool::new(false));

        let flusher_thread = if cfg.enabled && matches!(cfg.fsync_policy, FsyncPolicy::Batched) {
            Some(spawn_flusher_thread(
                inner.clone(),
                durable_cursor.clone(),
                shutdown.clone(),
                poisoned.clone(),
                cfg.fsync_interval,
                cfg.fsync_batch_bytes,
            ))
        } else {
            None
        };

        Ok(Self {
            dir: wal_dir,
            cfg,
            inner,
            durable_cursor,
            shutdown,
            flusher_thread,
            poisoned,
        })
    }

    /// Append one record.  Rotates the active segment if it would
    /// exceed `segment_size_max`; runs retention if total bytes
    /// exceed `retention_bytes`.  Under `FsyncPolicy::Each`, fsyncs
    /// inline before returning.  Returns `WalError::Poisoned` if the
    /// flusher thread has died (Batched only).
    pub fn append(
        &self,
        cursor: u64,
        ts_unix_nanos: u64,
        payload: &[u8],
    ) -> Result<(), WalError> {
        if !self.cfg.enabled {
            return Ok(()); // no-op when WAL is disabled
        }
        if payload.len() > MAX_PAYLOAD_LEN {
            return Err(WalError::PayloadTooLarge { payload_len: payload.len() });
        }
        if self.poisoned.load(Ordering::Acquire) {
            return Err(WalError::Poisoned);
        }

        let mut inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());

        // Ensure we have an active writer (deferred creation from open).
        if inner.writer.is_none() {
            let path = self.dir.join(format!("{cursor:020}.seg"));
            let w = SegmentWriter::create(&path, cursor)?;
            inner.segments.insert(cursor, path.clone());
            inner.segment_sizes.insert(cursor, w.bytes_written());
            inner.writer = Some(w);
        }

        // Rotate if the active segment would exceed segment_size_max.
        // We rotate BEFORE appending so the new record always lands in
        // the new segment.
        let needs_rotate = {
            let w = inner.writer.as_ref().unwrap();
            (w.bytes_written() + crate::wal::record::RECORD_FRAMING as u64 + payload.len() as u64)
                > self.cfg.segment_size_max as u64
        };
        if needs_rotate {
            self.rotate_locked(&mut inner, cursor)?;
        }

        // Append.
        {
            let w = inner.writer.as_mut().unwrap();
            w.append(cursor, ts_unix_nanos, payload)?;
            let first = w.first_cursor();
            let bytes = w.bytes_written();
            inner.segment_sizes.insert(first, bytes);
        }

        // Under Each, fsync inline and advance durable_cursor.
        if matches!(self.cfg.fsync_policy, FsyncPolicy::Each) {
            let w = inner.writer.as_mut().unwrap();
            let d = w.fsync()?;
            self.durable_cursor.store(d, Ordering::Release);
        }

        // Retention: delete oldest segments while total > cap.  Never
        // delete the active segment.
        self.enforce_retention_locked(&mut inner)?;

        Ok(())
    }

    /// Force-flush the active segment regardless of fsync_policy.
    /// Useful from tests; in production the flusher thread or the
    /// Each policy handles this.
    pub fn fsync(&self) -> Result<(), WalError> {
        let mut inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(w) = inner.writer.as_mut() {
            let d = w.fsync()?;
            self.durable_cursor.store(d, Ordering::Release);
        }
        Ok(())
    }

    /// Force-rotate the active segment (if any).  Used by the
    /// publisher on generation bump per RFC §6 + §9.2 so a fresh
    /// publisher never appends to the dead one's tail.
    pub fn bump_generation(&self) -> Result<(), WalError> {
        let mut inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(w) = inner.writer.as_ref() {
            let next_cursor = w.pending_cursor();
            self.rotate_locked(&mut inner, next_cursor)?;
        }
        Ok(())
    }

    /// Highest cursor durable on disk.  Subscribers under Batched
    /// must clamp their read position to this value.
    pub fn durable_cursor(&self) -> u64 {
        self.durable_cursor.load(Ordering::Acquire)
    }

    /// First cursor still in any segment on disk.  Returns 0 when
    /// the WAL is empty (no segments yet).
    pub fn oldest_cursor(&self) -> u64 {
        let inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        inner.segments.keys().next().copied().unwrap_or(0)
    }

    /// Read records starting from `cursor` (inclusive).  Returns
    /// `Err(CursorTooOld)` if `cursor` predates the oldest segment.
    ///
    /// The returned iterator lazily opens segments in order; if the
    /// retention thread deletes a segment under us mid-walk, the
    /// next read surfaces a `WalError::Io` (the W1-e replayer
    /// translates that into `CursorTooOld` to the subscriber).
    pub fn read_from(&self, cursor: u64) -> Result<WalReplayer, WalError> {
        let inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        let oldest = match inner.segments.keys().next().copied() {
            Some(v) => v,
            None => return Ok(WalReplayer::new(Vec::new(), cursor)),
        };
        if cursor < oldest {
            return Err(WalError::CursorTooOld { requested: cursor, oldest });
        }
        // Containing segment = largest first_cursor <= cursor (or oldest
        // if cursor is past all segments' first_cursors — still safe).
        let containing_first = inner
            .segments
            .range(..=cursor)
            .next_back()
            .map(|(&k, _)| k)
            .unwrap_or(oldest);
        let segments: Vec<PathBuf> = inner
            .segments
            .range(containing_first..)
            .map(|(_, p)| p.clone())
            .collect();
        Ok(WalReplayer::new(segments, cursor))
    }

    /// Snapshot of internal state for monitoring.
    pub fn stats(&self) -> WalStats {
        let inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        let pending_cursor = inner.writer.as_ref().map(|w| w.pending_cursor()).unwrap_or(0);
        let oldest_cursor = inner.segments.keys().next().copied().unwrap_or(0);
        let active_segment_bytes =
            inner.writer.as_ref().map(|w| w.bytes_written()).unwrap_or(0);
        let total_wal_bytes: u64 = inner.segment_sizes.values().sum();
        let segments = inner.segments.len();
        WalStats {
            pending_cursor,
            durable_cursor: self.durable_cursor.load(Ordering::Acquire),
            oldest_cursor,
            active_segment_bytes,
            total_wal_bytes,
            segments,
        }
    }

    // ── Internals ──────────────────────────────────────────────────────────

    fn rotate_locked(
        &self,
        inner: &mut Inner,
        new_first_cursor: u64,
    ) -> Result<(), WalError> {
        if let Some(w) = inner.writer.take() {
            let first = w.first_cursor();
            let final_bytes = w.bytes_written();
            w.close()?;
            inner.segment_sizes.insert(first, final_bytes);
        }
        let path = self.dir.join(format!("{new_first_cursor:020}.seg"));
        let writer = SegmentWriter::create(&path, new_first_cursor)?;
        inner.segments.insert(new_first_cursor, path.clone());
        inner.segment_sizes.insert(new_first_cursor, writer.bytes_written());
        inner.writer = Some(writer);
        Ok(())
    }

    fn enforce_retention_locked(&self, inner: &mut Inner) -> Result<(), WalError> {
        loop {
            let total: u64 = inner.segment_sizes.values().sum();
            if total <= self.cfg.retention_bytes {
                return Ok(());
            }
            // Find the oldest segment that is NOT the active one.
            let active_first =
                inner.writer.as_ref().map(|w| w.first_cursor()).unwrap_or(u64::MAX);
            let oldest_first = inner
                .segments
                .keys()
                .copied()
                .find(|&k| k != active_first);
            match oldest_first {
                Some(k) => {
                    if let Some(path) = inner.segments.remove(&k) {
                        let _ = fs::remove_file(&path);
                    }
                    inner.segment_sizes.remove(&k);
                }
                None => return Ok(()), // only the active segment left
            }
        }
    }
}

impl Drop for Wal {
    fn drop(&mut self) {
        self.shutdown.store(true, Ordering::Release);
        if let Some(h) = self.flusher_thread.take() {
            let _ = h.join();
        }
        // Best-effort final fsync.
        if self.cfg.enabled {
            let _ = self.fsync();
        }
    }
}

/// Iterator returned by [`Wal::read_from`].  Lazily opens each segment.
pub struct WalReplayer {
    segments: Vec<PathBuf>,
    seg_idx: usize,
    cursor_floor: u64,
    current: Option<SegmentReader>,
}

impl WalReplayer {
    fn new(segments: Vec<PathBuf>, cursor_floor: u64) -> Self {
        Self { segments, seg_idx: 0, cursor_floor, current: None }
    }
}

impl Iterator for WalReplayer {
    type Item = Result<Record, WalError>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if self.current.is_none() {
                if self.seg_idx >= self.segments.len() {
                    return None;
                }
                let path = &self.segments[self.seg_idx];
                self.seg_idx += 1;
                match SegmentReader::open(path) {
                    Ok(r) => self.current = Some(r),
                    Err(e) => return Some(Err(WalError::from(e))),
                }
            }
            let reader = self.current.as_mut().unwrap();
            match reader.next_record() {
                Some(Ok(r)) => {
                    if r.cursor < self.cursor_floor {
                        // Skip records before the requested cursor.
                        continue;
                    }
                    return Some(Ok(r));
                }
                Some(Err(e)) => return Some(Err(WalError::from(e))),
                None => {
                    // End of this segment — advance to next.
                    self.current = None;
                }
            }
        }
    }
}

// ── Flusher thread ────────────────────────────────────────────────────────────

fn spawn_flusher_thread(
    inner: Arc<Mutex<Inner>>,
    durable_cursor: Arc<AtomicU64>,
    shutdown: Arc<AtomicBool>,
    poisoned: Arc<AtomicBool>,
    fsync_interval: Duration,
    _fsync_batch_bytes: usize,
) -> JoinHandle<()> {
    thread::spawn(move || {
        let mut next_tick = Instant::now() + fsync_interval;
        while !shutdown.load(Ordering::Acquire) {
            let now = Instant::now();
            if now < next_tick {
                thread::sleep((next_tick - now).min(Duration::from_millis(50)));
                continue;
            }
            next_tick = now + fsync_interval;
            let mut guard = match inner.lock() {
                Ok(g) => g,
                Err(p) => p.into_inner(),
            };
            if let Some(w) = guard.writer.as_mut() {
                match w.fsync() {
                    Ok(d) => durable_cursor.store(d, Ordering::Release),
                    Err(_) => {
                        poisoned.store(true, Ordering::Release);
                        return;
                    }
                }
            }
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tmpdir() -> tempfile::TempDir {
        tempfile::tempdir().expect("tempdir")
    }

    fn cfg(segment_size_max: usize, retention_bytes: u64) -> WalConfig {
        WalConfig {
            enabled: true,
            fsync_policy: FsyncPolicy::Each, // synchronous = deterministic in tests
            fsync_interval: Duration::from_millis(5),
            fsync_batch_bytes: 1024,
            segment_size_max,
            retention_bytes,
        }
    }

    #[test]
    fn open_empty_dir_creates_no_segment_yet() {
        let dir = tmpdir();
        let wal = Wal::open(dir.path(), cfg(1024, 1_000_000)).unwrap();
        let s = wal.stats();
        assert_eq!(s.segments, 0);
        assert_eq!(s.pending_cursor, 0);
    }

    #[test]
    fn append_creates_first_segment_on_demand() {
        let dir = tmpdir();
        let wal = Wal::open(dir.path(), cfg(1024, 1_000_000)).unwrap();
        wal.append(0, 0, b"x").unwrap();
        let s = wal.stats();
        assert_eq!(s.segments, 1);
        assert_eq!(s.pending_cursor, 1);
        assert_eq!(s.oldest_cursor, 0);
        assert!(s.active_segment_bytes > 0);
    }

    #[test]
    fn rotates_when_active_exceeds_size_cap() {
        let dir = tmpdir();
        // Very small cap so a few records force a rotation.
        let wal = Wal::open(dir.path(), cfg(200, 1_000_000)).unwrap();
        // Each record: RECORD_FRAMING (28) + payload (16) = 44 bytes.
        // Header: 32 bytes. After ~3 records the active will be near cap.
        for i in 0..6u64 {
            wal.append(i, 0, &[0xAB; 16]).unwrap();
        }
        let s = wal.stats();
        assert!(s.segments >= 2, "expected at least 2 segments, got {}", s.segments);
        assert_eq!(s.pending_cursor, 6);
    }

    #[test]
    fn retention_deletes_oldest_when_total_exceeds_cap() {
        let dir = tmpdir();
        // Force frequent rotation (small segments) + tight retention.
        let wal = Wal::open(dir.path(), cfg(150, 300)).unwrap();
        for i in 0..30u64 {
            wal.append(i, 0, &[0xCD; 32]).unwrap();
        }
        let s = wal.stats();
        assert!(
            s.total_wal_bytes <= 300 + 150, /* loose: at most one over-cap segment */
            "retention failed: total={}, oldest={}",
            s.total_wal_bytes,
            s.oldest_cursor
        );
        assert!(s.oldest_cursor > 0, "oldest segment must have been deleted");
    }

    #[test]
    fn read_from_walks_records_in_order() {
        let dir = tmpdir();
        let wal = Wal::open(dir.path(), cfg(1024, 1_000_000)).unwrap();
        for i in 0..5u64 {
            wal.append(i, 0, &i.to_le_bytes()).unwrap();
        }
        let records: Vec<_> = wal
            .read_from(0)
            .unwrap()
            .collect::<Result<_, _>>()
            .unwrap();
        assert_eq!(records.len(), 5);
        for (i, r) in records.iter().enumerate() {
            assert_eq!(r.cursor, i as u64);
            assert_eq!(r.payload, (i as u64).to_le_bytes().to_vec());
        }
    }

    #[test]
    fn read_from_skips_records_before_requested_cursor() {
        let dir = tmpdir();
        let wal = Wal::open(dir.path(), cfg(1024, 1_000_000)).unwrap();
        for i in 0..5u64 {
            wal.append(i, 0, &i.to_le_bytes()).unwrap();
        }
        let records: Vec<_> = wal
            .read_from(2)
            .unwrap()
            .collect::<Result<_, _>>()
            .unwrap();
        assert_eq!(records.len(), 3);
        assert_eq!(records[0].cursor, 2);
    }

    #[test]
    fn read_from_cursor_too_old_after_retention() {
        let dir = tmpdir();
        let wal = Wal::open(dir.path(), cfg(150, 300)).unwrap();
        for i in 0..30u64 {
            wal.append(i, 0, &[0xEF; 32]).unwrap();
        }
        let oldest = wal.oldest_cursor();
        assert!(oldest > 0);
        match wal.read_from(0) {
            Err(WalError::CursorTooOld { requested: 0, oldest: got }) => {
                assert_eq!(got, oldest);
            }
            other => panic!("expected CursorTooOld, got {:?}", other.map(|_| "Ok")),
        }
    }

    #[test]
    fn read_from_walks_across_segment_boundary() {
        let dir = tmpdir();
        let wal = Wal::open(dir.path(), cfg(120, 100_000)).unwrap();
        for i in 0..10u64 {
            wal.append(i, 0, &i.to_le_bytes()).unwrap();
        }
        let s = wal.stats();
        assert!(s.segments >= 2, "test setup needs multi-segment WAL");
        let records: Vec<_> = wal
            .read_from(0)
            .unwrap()
            .collect::<Result<_, _>>()
            .unwrap();
        assert_eq!(records.len(), 10);
    }

    #[test]
    fn reopen_resumes_after_existing_segments() {
        let dir = tmpdir();
        {
            let wal = Wal::open(dir.path(), cfg(1024, 1_000_000)).unwrap();
            wal.append(0, 0, b"a").unwrap();
            wal.append(1, 0, b"b").unwrap();
            wal.append(2, 0, b"c").unwrap();
            drop(wal);
        }
        let wal = Wal::open(dir.path(), cfg(1024, 1_000_000)).unwrap();
        let s = wal.stats();
        assert!(s.pending_cursor >= 3, "reopen must resume past existing records");
        // Reading from 0 still yields the originals.
        let records: Vec<_> = wal
            .read_from(0)
            .unwrap()
            .collect::<Result<_, _>>()
            .unwrap();
        assert_eq!(records.len(), 3);
        assert_eq!(records[0].cursor, 0);
        assert_eq!(records[2].payload, b"c");
    }

    #[test]
    fn bump_generation_rotates_immediately() {
        let dir = tmpdir();
        let wal = Wal::open(dir.path(), cfg(1024, 1_000_000)).unwrap();
        wal.append(0, 0, b"x").unwrap();
        let before = wal.stats().segments;
        wal.bump_generation().unwrap();
        let after = wal.stats().segments;
        assert_eq!(after, before + 1, "bump_generation must add one segment");
    }

    #[test]
    fn stats_durable_cursor_matches_each_policy() {
        let dir = tmpdir();
        let wal = Wal::open(dir.path(), cfg(1024, 1_000_000)).unwrap();
        for i in 0..3u64 {
            wal.append(i, 0, b"x").unwrap();
        }
        let s = wal.stats();
        assert_eq!(s.durable_cursor, 3, "Each policy: durable == pending");
        assert_eq!(s.pending_cursor, 3);
    }

    #[test]
    fn batched_flusher_advances_durable_cursor() {
        let dir = tmpdir();
        let mut c = cfg(1024, 1_000_000);
        c.fsync_policy = FsyncPolicy::Batched;
        c.fsync_interval = Duration::from_millis(20);
        let wal = Wal::open(dir.path(), c).unwrap();
        for i in 0..3u64 {
            wal.append(i, 0, b"x").unwrap();
        }
        // durable_cursor lags pending until flusher ticks.  Poll for
        // up to 1 s.
        let deadline = std::time::Instant::now() + Duration::from_secs(1);
        loop {
            let s = wal.stats();
            if s.durable_cursor == s.pending_cursor {
                break;
            }
            if std::time::Instant::now() >= deadline {
                panic!(
                    "flusher did not advance durable_cursor: pending={}, durable={}",
                    s.pending_cursor, s.durable_cursor
                );
            }
            std::thread::sleep(Duration::from_millis(5));
        }
    }
}
