//! `wal::v2::Wal` — multi-segment aggregator for the lock-free WAL.
//!
//! Mirrors the v0.1 [`crate::wal::Wal`] public API so the publisher /
//! subscriber integration in W2-5 / W2-6 reduces to swapping the
//! constructor.  Internal implementation uses
//! [`MmapSegmentWriter`] (lock-free append),
//! [`MmapSegmentReader`] (seqlock-aware read), and
//! [`ActiveCoord`] (subscriber rotation discovery), wired with
//! [`rotation::rotate`].
//!
//! Hot-path append:
//!
//! ```text
//! lock(inner)        ← contended only by concurrent appends from the
//!                      same process; cross-process is excluded by
//!                      producer.lock outside this struct
//! mmap_writer.append ← lock-free: CAS on tail + memcpy + 2 atomic stores
//! release lock
//! ```
//!
//! No `BufWriter`, no `write(2)` on the hot path.  Per-platform
//! `msync` for the `Each` and `Batched` policies arrives in W2-7;
//! today's stub just calls `flush_async` and treats every successful
//! append as durable, which is the conservative shape for tests.

use std::collections::BTreeMap;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use crate::wal::config::{FsyncPolicy, WalConfig};
use crate::wal::record::{Record, MAX_PAYLOAD_LEN, RECORD_FRAMING};
use crate::wal::stats::WalStats;
use crate::wal::v2::active::ActiveCoord;
use crate::wal::v2::mmap_segment_reader::{
    MmapSegmentReader, ReadOutcome, ReaderError as MmapReaderError,
};
use crate::wal::v2::mmap_segment_writer::{
    align_record_len, AppendOutcome, MmapSegmentWriter, WriterError as MmapWriterError,
};
use crate::wal::v2::rotation::{rotate, segment_path, RotateError};

/// Errors returned by the v2 [`Wal`].  Reuses [`crate::wal::WalError`]'s
/// shape where possible so the publisher's `Error::Wal` mapping stays
/// unchanged when W2-5 swaps backends.
#[derive(Debug, thiserror::Error)]
pub enum WalError {
    #[error("I/O error: {0}")]
    Io(#[from] io::Error),

    #[error("segment writer: {0}")]
    Writer(#[from] MmapWriterError),

    #[error("segment reader: {0}")]
    Reader(#[from] MmapReaderError),

    #[error("rotation: {0}")]
    Rotation(#[from] RotateError),

    #[error("cursor {requested} is older than the oldest in-WAL slot ({oldest})")]
    CursorTooOld { requested: u64, oldest: u64 },

    #[error("WAL flusher thread died; further appends would silently lose durability")]
    Poisoned,

    #[error("payload too large: {payload_len} > MAX_PAYLOAD_LEN ({MAX_PAYLOAD_LEN})")]
    PayloadTooLarge { payload_len: usize },
}

/// Lock-free multi-segment WAL.  Single-publisher (SPMC) — see
/// module docs.
pub struct Wal {
    dir: PathBuf,
    cfg: WalConfig,
    inner: Arc<Mutex<Inner>>,
    /// 16-byte mmap'd coord file that names the active segment.
    /// Subscribers open their own `ActiveCoord` and load-acquire the
    /// `first_cursor` field to follow rotations.
    active: Arc<ActiveCoord>,
    /// Highest cursor msync'd to disk.  Until W2-7 wires real msync,
    /// this mirrors `pending_cursor` after each successful append.
    durable_cursor: Arc<AtomicU64>,
    /// Next cursor `append()` will assign.  Set after each append;
    /// survives the deferred-writer case (Wal::open with no segments
    /// holds this at the right value before the first append).
    pending_cursor: Arc<AtomicU64>,
    shutdown: Arc<AtomicBool>,
    flusher_thread: Option<JoinHandle<()>>,
    poisoned: Arc<AtomicBool>,
}

struct Inner {
    /// `first_cursor → segment_path`, sorted ascending.  Includes
    /// the active segment as the last entry.
    segments: BTreeMap<u64, PathBuf>,
    /// Bytes for every RETIRED (non-active) segment, keyed by
    /// first_cursor.  Updated only on rotation + retention — never
    /// on the hot append path.
    segment_sizes: BTreeMap<u64, u64>,
    /// Live tail (= byte count) of the active segment.  Mirrored
    /// from `writer.current_tail()` after every append so retention
    /// can be evaluated without an atomic load per check.
    active_bytes: u64,
    /// Sum of `segment_sizes` (retained-segment bytes), maintained
    /// incrementally on rotation/retention so the per-publish
    /// retention check is a single add + compare.
    retained_bytes: u64,
    /// Writer for the active segment.  Always present once any
    /// append has happened; deferred until first append for fresh
    /// empty WALs.
    writer: Option<MmapSegmentWriter>,
}

impl Wal {
    /// Open the WAL rooted at `dir`.  Creates `<dir>/wal/` if needed,
    /// scans existing segment files, opens `active.dat`, and runs
    /// recovery: if `active.dat`'s `first_cursor` points at a
    /// segment that doesn't exist (e.g. crash mid-rotation), the
    /// largest first_cursor on disk wins and `active.dat` is
    /// rewritten to match.
    pub fn open(dir: &Path, cfg: WalConfig) -> Result<Self, WalError> {
        let wal_dir = dir.join("wal");
        fs::create_dir_all(&wal_dir)?;

        // Scan existing segment files.
        let mut segments = BTreeMap::new();
        let mut segment_sizes = BTreeMap::new();
        for entry in fs::read_dir(&wal_dir)? {
            let entry = entry?;
            let path = entry.path();
            let name = match path.file_name().and_then(|s| s.to_str()) {
                Some(n) => n.to_string(),
                None => continue,
            };
            if !name.ends_with(".seg") {
                continue;
            }
            let stem = &name[..name.len() - 4];
            let first_cursor: u64 = match stem.parse() {
                Ok(v) => v,
                Err(_) => continue,
            };
            let len = fs::metadata(&path)?.len();
            segments.insert(first_cursor, path);
            segment_sizes.insert(first_cursor, len);
        }

        let active = Arc::new(ActiveCoord::open_or_create(&wal_dir)?);

        let (writer, pending_cursor, active_bytes) = if let Some((
            &last_first_cursor,
            last_path,
        )) = segments.iter().next_back()
        {
            // Existing newest segment — scan to find the highest
            // committed cursor, then rotate to a fresh segment so
            // we don't try to re-open the existing one for append
            // (MmapSegmentWriter::create uses create_new).
            //
            // Recovery tolerance: if the segment file is corrupt
            // (truncated, bad header, mid-record CRC mismatch from
            // a crash mid-write), treat everything from the bad
            // boundary onwards as not-committed.  Mirrors v0.1's
            // recover_truncate semantics without an in-place
            // ftruncate (we just don't bump last_cursor past the
            // last clean record).
            let mut last_cursor: Option<u64> = None;
            if let Ok(mut reader) = MmapSegmentReader::open(last_path) {
                // AwaitMore / EndOfSegment / Err all stop recovery at
                // the last good cursor.  An Err here would have been
                // a partial write before the system died; treat it
                // as if the in-flight record never happened.
                while let ReadOutcome::Record(r) = reader.next_record() {
                    last_cursor = Some(r.cursor);
                }
            }
            // else: segment header unparseable (file truncated below
            // 32 bytes or magic destroyed).  Treat as empty —
            // last_cursor stays None.
            let next_cursor = last_cursor.map(|c| c + 1).unwrap_or(last_first_cursor);
            if next_cursor == last_first_cursor {
                // Empty segment — leave it; defer writer creation
                // until the first append.
                active.store_active(last_first_cursor)?;
                (None, last_first_cursor, 0)
            } else {
                let new_path = segment_path(&wal_dir, next_cursor);
                let writer = MmapSegmentWriter::create(
                    &new_path,
                    cfg.segment_size_max,
                    next_cursor,
                )?;
                segments.insert(next_cursor, new_path.clone());
                let bytes = writer.current_tail();
                segment_sizes.insert(next_cursor, bytes);
                active.store_active(next_cursor)?;
                (Some(writer), next_cursor, bytes)
            }
        } else {
            // Fresh WAL — defer creation to first append.
            (None, 0, 0)
        };

        // `segment_sizes` should hold RETIRED segments only.  Pull
        // out the active one if we just created it.
        let retained_bytes: u64 = if let Some(active_first) =
            segments.iter().next_back().map(|(k, _)| *k).filter(|_| active_bytes > 0)
        {
            segment_sizes.remove(&active_first);
            segment_sizes.values().sum()
        } else {
            segment_sizes.values().sum()
        };

        let durable_cursor = Arc::new(AtomicU64::new(pending_cursor));
        let pending_cursor_atomic = Arc::new(AtomicU64::new(pending_cursor));
        let inner = Arc::new(Mutex::new(Inner {
            segments,
            segment_sizes,
            active_bytes,
            retained_bytes,
            writer,
        }));
        let shutdown = Arc::new(AtomicBool::new(false));
        let poisoned = Arc::new(AtomicBool::new(false));

        let flusher_thread = if cfg.enabled && matches!(cfg.fsync_policy, FsyncPolicy::Batched) {
            Some(spawn_flusher_thread(
                inner.clone(),
                durable_cursor.clone(),
                pending_cursor_atomic.clone(),
                shutdown.clone(),
                poisoned.clone(),
                cfg.fsync_interval,
            ))
        } else {
            None
        };

        Ok(Self {
            dir: wal_dir,
            cfg,
            inner,
            active,
            durable_cursor,
            pending_cursor: pending_cursor_atomic,
            shutdown,
            flusher_thread,
            poisoned,
        })
    }

    /// Append one record.  Rotates the active segment if the record
    /// won't fit.  Under [`FsyncPolicy::Each`] flushes inline.
    pub fn append(
        &self,
        cursor: u64,
        ts_unix_nanos: u64,
        payload: &[u8],
    ) -> Result<(), WalError> {
        if !self.cfg.enabled {
            return Ok(());
        }
        if payload.len() > MAX_PAYLOAD_LEN {
            return Err(WalError::PayloadTooLarge { payload_len: payload.len() });
        }
        if self.poisoned.load(Ordering::Acquire) {
            return Err(WalError::Poisoned);
        }

        let mut inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());

        if inner.writer.is_none() {
            let path = segment_path(&self.dir, cursor);
            let w = MmapSegmentWriter::create(&path, self.cfg.segment_size_max, cursor)?;
            inner.segments.insert(cursor, path);
            inner.active_bytes = w.current_tail();
            inner.writer = Some(w);
            self.active.store_active(cursor)?;
        }

        // Cheap pre-check: would this record fit?  We don't have to
        // be exact — the writer's CAS handles the genuine overflow
        // case — but cheap rejection saves a doomed append + rollback.
        let record_bytes = align_record_len(RECORD_FRAMING + payload.len()) as u64;
        if inner.active_bytes + record_bytes > self.cfg.segment_size_max as u64 {
            self.rotate_locked(&mut inner, cursor)?;
        }

        // Append.  If the writer surprises us with SegmentFull (can
        // happen if our active_bytes cache drifted from reality, or
        // a tail-CAS race ate into the slack), rotate + retry once.
        let first_outcome = {
            let w = inner.writer.as_ref().unwrap();
            w.append(cursor, ts_unix_nanos, payload)?
        };
        let next_cursor = match first_outcome {
            AppendOutcome::Ok { next_cursor, .. } => {
                inner.active_bytes = inner.writer.as_ref().unwrap().current_tail();
                next_cursor
            }
            AppendOutcome::SegmentFull => {
                self.rotate_locked(&mut inner, cursor)?;
                let w = inner.writer.as_ref().unwrap();
                match w.append(cursor, ts_unix_nanos, payload)? {
                    AppendOutcome::Ok { next_cursor, .. } => {
                        inner.active_bytes = w.current_tail();
                        next_cursor
                    }
                    AppendOutcome::SegmentFull => {
                        // Genuinely doesn't fit even in a fresh
                        // segment — payload exceeds segment_size_max.
                        return Err(WalError::PayloadTooLarge {
                            payload_len: payload.len(),
                        });
                    }
                }
            }
        };
        self.pending_cursor.store(next_cursor, Ordering::Release);

        if matches!(self.cfg.fsync_policy, FsyncPolicy::Each) {
            // W2-7 will wire real msync(MS_SYNC) + per-platform
            // file-level sync here; for now flush_async + advance
            // durable_cursor so subscribers gating on it behave as
            // expected in tests.
            if let Some(w) = inner.writer.as_ref() {
                w.flush_async()?;
            }
            self.durable_cursor.store(next_cursor, Ordering::Release);
        }

        if inner.retained_bytes + inner.active_bytes > self.cfg.retention_bytes {
            self.enforce_retention_locked(&mut inner)?;
        }

        Ok(())
    }

    /// Force-flush — until W2-7, calls `flush_async` and advances
    /// `durable_cursor` to `pending_cursor`.
    pub fn fsync(&self) -> Result<(), WalError> {
        let inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(w) = inner.writer.as_ref() {
            w.flush_async()?;
        }
        self.durable_cursor
            .store(self.pending_cursor.load(Ordering::Acquire), Ordering::Release);
        Ok(())
    }

    /// Force-rotate the active segment (if any).  Used by the
    /// publisher on generation bump so a fresh publisher never
    /// appends to the dead one's tail.
    pub fn bump_generation(&self) -> Result<(), WalError> {
        let mut inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        if inner.writer.is_some() {
            let next_cursor = self.pending_cursor.load(Ordering::Acquire);
            self.rotate_locked(&mut inner, next_cursor)?;
        }
        Ok(())
    }

    pub fn durable_cursor(&self) -> u64 {
        self.durable_cursor.load(Ordering::Acquire)
    }

    pub fn pending_cursor(&self) -> u64 {
        self.pending_cursor.load(Ordering::Acquire)
    }

    pub fn oldest_cursor(&self) -> u64 {
        let inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        inner.segments.keys().next().copied().unwrap_or(0)
    }

    /// Iterator over records starting at `cursor` (inclusive).
    /// Returns `Err(CursorTooOld)` if `cursor` predates the oldest
    /// segment.
    pub fn read_from(&self, cursor: u64) -> Result<WalReplayer, WalError> {
        let inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        let oldest = match inner.segments.keys().next().copied() {
            Some(v) => v,
            None => return Ok(WalReplayer::new(Vec::new(), cursor)),
        };
        if cursor < oldest {
            return Err(WalError::CursorTooOld { requested: cursor, oldest });
        }
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

    pub fn stats(&self) -> WalStats {
        let inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        let oldest_cursor = inner.segments.keys().next().copied().unwrap_or(0);
        let segments = inner.segments.len();
        WalStats {
            pending_cursor: self.pending_cursor.load(Ordering::Acquire),
            durable_cursor: self.durable_cursor.load(Ordering::Acquire),
            oldest_cursor,
            active_segment_bytes: inner.active_bytes,
            total_wal_bytes: inner.retained_bytes + inner.active_bytes,
            segments,
        }
    }

    // ── Internals ──────────────────────────────────────────────────────────

    fn rotate_locked(&self, inner: &mut Inner, new_first_cursor: u64) -> Result<(), WalError> {
        if let Some(old_w) = inner.writer.take() {
            let first = old_w.first_cursor();
            let final_bytes = old_w.current_tail();
            // Use `rotate` helper to mark + create + publish.
            let new_w = rotate(
                &self.dir,
                &old_w,
                &self.active,
                new_first_cursor,
                self.cfg.segment_size_max,
            )?;
            // Old segment retires; record its (final) size.
            inner.segment_sizes.insert(first, final_bytes);
            inner.retained_bytes += final_bytes;
            // New segment becomes active.
            inner.segments.insert(new_first_cursor, segment_path(&self.dir, new_first_cursor));
            inner.active_bytes = new_w.current_tail();
            inner.writer = Some(new_w);
            drop(old_w);
        } else {
            // No writer yet — first append after a deferred-create.
            // Just open the segment + publish via active.dat.
            let path = segment_path(&self.dir, new_first_cursor);
            let w = MmapSegmentWriter::create(&path, self.cfg.segment_size_max, new_first_cursor)?;
            inner.segments.insert(new_first_cursor, path);
            inner.active_bytes = w.current_tail();
            inner.writer = Some(w);
            self.active.store_active(new_first_cursor)?;
        }
        Ok(())
    }

    fn enforce_retention_locked(&self, inner: &mut Inner) -> Result<(), WalError> {
        loop {
            if inner.retained_bytes + inner.active_bytes <= self.cfg.retention_bytes {
                return Ok(());
            }
            let active_first =
                inner.writer.as_ref().map(|w| w.first_cursor()).unwrap_or(u64::MAX);
            let oldest_first =
                inner.segments.keys().copied().find(|&k| k != active_first);
            match oldest_first {
                Some(k) => {
                    if let Some(path) = inner.segments.remove(&k) {
                        let _ = fs::remove_file(&path);
                    }
                    if let Some(size) = inner.segment_sizes.remove(&k) {
                        inner.retained_bytes = inner.retained_bytes.saturating_sub(size);
                    }
                }
                None => return Ok(()),
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
        if self.cfg.enabled {
            let _ = self.fsync();
        }
    }
}

/// Iterator returned by [`Wal::read_from`].  Lazily opens each
/// segment via [`MmapSegmentReader`] and yields committed records
/// from `cursor_floor` onwards.
pub struct WalReplayer {
    segments: Vec<PathBuf>,
    seg_idx: usize,
    cursor_floor: u64,
    current: Option<MmapSegmentReader>,
}

impl WalReplayer {
    pub(crate) fn new(segments: Vec<PathBuf>, cursor_floor: u64) -> Self {
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
                match MmapSegmentReader::open(path) {
                    Ok(r) => self.current = Some(r),
                    Err(e) => return Some(Err(WalError::from(e))),
                }
            }
            let reader = self.current.as_mut().unwrap();
            match reader.next_record() {
                ReadOutcome::Record(r) => {
                    if r.cursor < self.cursor_floor {
                        continue;
                    }
                    return Some(Ok(r));
                }
                ReadOutcome::Err(e) => return Some(Err(WalError::from(e))),
                // Live tail OR rotated marker → advance to next segment.
                ReadOutcome::AwaitMore | ReadOutcome::EndOfSegment => {
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
    pending_cursor: Arc<AtomicU64>,
    shutdown: Arc<AtomicBool>,
    poisoned: Arc<AtomicBool>,
    fsync_interval: Duration,
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

            // Snapshot the pending cursor BEFORE flushing so anything
            // that lands after our msync isn't falsely marked durable.
            let snapshot = pending_cursor.load(Ordering::Acquire);

            let flush_result: io::Result<()> = {
                let guard = match inner.lock() {
                    Ok(g) => g,
                    Err(p) => p.into_inner(),
                };
                if let Some(w) = guard.writer.as_ref() {
                    w.flush_async()
                } else {
                    Ok(())
                }
            };

            match flush_result {
                Ok(()) => {
                    durable_cursor.store(snapshot, Ordering::Release);
                }
                Err(_) => {
                    poisoned.store(true, Ordering::Release);
                    return;
                }
            }
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn tmpdir() -> TempDir {
        tempfile::tempdir().expect("tempdir")
    }

    fn cfg(segment_size_max: usize, retention_bytes: u64) -> WalConfig {
        WalConfig {
            enabled: true,
            fsync_policy: FsyncPolicy::None,
            segment_size_max,
            retention_bytes,
            ..WalConfig::default()
        }
    }

    #[test]
    fn disabled_wal_is_noop() {
        let dir = tmpdir();
        let c = WalConfig { enabled: false, ..WalConfig::default() };
        let w = Wal::open(dir.path(), c).unwrap();
        w.append(0, 0, b"ignored").unwrap();
        assert_eq!(w.pending_cursor(), 0);
        assert_eq!(w.stats().segments, 0);
    }

    #[test]
    fn open_empty_dir_creates_no_segment_yet() {
        let dir = tmpdir();
        let w = Wal::open(dir.path(), cfg(4096, 1 << 20)).unwrap();
        assert_eq!(w.stats().segments, 0);
        assert_eq!(w.pending_cursor(), 0);
        assert_eq!(w.oldest_cursor(), 0);
    }

    #[test]
    fn append_creates_first_segment_on_demand() {
        let dir = tmpdir();
        let w = Wal::open(dir.path(), cfg(4096, 1 << 20)).unwrap();
        w.append(0, 0xDEAD, b"hello").unwrap();
        let stats = w.stats();
        assert_eq!(stats.segments, 1);
        assert_eq!(stats.pending_cursor, 1);
        assert!(stats.active_segment_bytes > 0);
    }

    #[test]
    fn read_from_returns_appended_records() {
        let dir = tmpdir();
        let w = Wal::open(dir.path(), cfg(4096, 1 << 20)).unwrap();
        for i in 0..5u64 {
            w.append(i, i * 1000, format!("rec-{i}").as_bytes()).unwrap();
        }
        let replayer = w.read_from(0).unwrap();
        let records: Vec<Record> = replayer.map(|r| r.unwrap()).collect();
        assert_eq!(records.len(), 5);
        for (i, r) in records.iter().enumerate() {
            assert_eq!(r.cursor, i as u64);
            assert_eq!(r.ts_unix_nanos, (i as u64) * 1000);
            assert_eq!(r.payload, format!("rec-{i}").as_bytes());
        }
    }

    #[test]
    fn read_from_skips_below_floor() {
        let dir = tmpdir();
        let w = Wal::open(dir.path(), cfg(4096, 1 << 20)).unwrap();
        for i in 0..5u64 {
            w.append(i, 0, b"x").unwrap();
        }
        let records: Vec<Record> = w.read_from(3).unwrap().map(|r| r.unwrap()).collect();
        assert_eq!(records.iter().map(|r| r.cursor).collect::<Vec<_>>(), vec![3, 4]);
    }

    #[test]
    fn rotation_triggers_when_segment_size_max_exceeded() {
        // Tiny segments so we can rotate with a handful of small
        // appends.  RECORD_FRAMING=28 + payload 8 → aligned 40.
        let dir = tmpdir();
        // header(32) + 2 records (40 each) = 112; cap segment at 112
        // so the 3rd append rotates.
        let w = Wal::open(dir.path(), cfg(112, 1 << 20)).unwrap();
        for i in 0..4u64 {
            w.append(i, 0, &i.to_le_bytes()).unwrap();
        }
        let stats = w.stats();
        assert!(stats.segments >= 2, "expected rotation, got {} segments", stats.segments);
        // All 4 records readable.
        let records: Vec<Record> = w.read_from(0).unwrap().map(|r| r.unwrap()).collect();
        assert_eq!(records.len(), 4);
        assert_eq!(
            records.iter().map(|r| r.cursor).collect::<Vec<_>>(),
            vec![0, 1, 2, 3]
        );
    }

    #[test]
    fn read_from_cursor_too_old_after_retention() {
        let dir = tmpdir();
        // Small segments + tiny retention so the first segment gets
        // pruned after a couple of rotations.
        let w = Wal::open(dir.path(), cfg(112, 200)).unwrap();
        for i in 0..6u64 {
            w.append(i, 0, &i.to_le_bytes()).unwrap();
        }
        let oldest = w.oldest_cursor();
        assert!(oldest > 0, "expected retention to prune segment 0, oldest = {oldest}");
        match w.read_from(0) {
            Err(WalError::CursorTooOld { requested: 0, .. }) => (),
            Err(e) => panic!("expected CursorTooOld, got Err({e:?})"),
            Ok(_) => panic!("expected CursorTooOld, got Ok(replayer)"),
        }
    }

    #[test]
    fn fsync_each_advances_durable_cursor_inline() {
        let dir = tmpdir();
        let mut c = cfg(4096, 1 << 20);
        c.fsync_policy = FsyncPolicy::Each;
        let w = Wal::open(dir.path(), c).unwrap();
        assert_eq!(w.durable_cursor(), 0);
        w.append(0, 0, b"a").unwrap();
        assert_eq!(w.durable_cursor(), 1, "Each should advance durable inline");
        w.append(1, 0, b"b").unwrap();
        assert_eq!(w.durable_cursor(), 2);
    }

    #[test]
    fn batched_flusher_eventually_advances_durable_cursor() {
        let dir = tmpdir();
        let c = WalConfig {
            enabled: true,
            fsync_policy: FsyncPolicy::Batched,
            fsync_interval: Duration::from_millis(2),
            segment_size_max: 4096,
            retention_bytes: 1 << 20,
            ..WalConfig::default()
        };
        let w = Wal::open(dir.path(), c).unwrap();
        for i in 0..10u64 {
            w.append(i, 0, b"x").unwrap();
        }
        let deadline = Instant::now() + Duration::from_millis(500);
        while w.durable_cursor() < 10 {
            if Instant::now() >= deadline {
                panic!("flusher never advanced durable_cursor (got {})", w.durable_cursor());
            }
            thread::sleep(Duration::from_millis(2));
        }
    }

    #[test]
    fn bump_generation_rotates_active_segment() {
        let dir = tmpdir();
        let w = Wal::open(dir.path(), cfg(4096, 1 << 20)).unwrap();
        w.append(0, 0, b"a").unwrap();
        let segments_before = w.stats().segments;
        w.bump_generation().unwrap();
        let segments_after = w.stats().segments;
        assert_eq!(segments_after, segments_before + 1, "bump should rotate");
        // Subsequent append lands in the new segment.
        w.append(1, 0, b"b").unwrap();
        let records: Vec<Record> = w.read_from(0).unwrap().map(|r| r.unwrap()).collect();
        assert_eq!(records.iter().map(|r| r.cursor).collect::<Vec<_>>(), vec![0, 1]);
    }

    #[test]
    fn reopen_picks_up_where_we_left_off() {
        let dir = tmpdir();
        {
            let w = Wal::open(dir.path(), cfg(4096, 1 << 20)).unwrap();
            for i in 0..3u64 {
                w.append(i, 0, b"x").unwrap();
            }
            assert_eq!(w.pending_cursor(), 3);
        }
        // Reopen.
        let w = Wal::open(dir.path(), cfg(4096, 1 << 20)).unwrap();
        assert_eq!(w.pending_cursor(), 3, "pending_cursor must survive reopen");
        w.append(3, 0, b"after").unwrap();
        assert_eq!(w.pending_cursor(), 4);
        // All four records readable across both segments.
        let records: Vec<Record> = w.read_from(0).unwrap().map(|r| r.unwrap()).collect();
        assert_eq!(records.len(), 4);
        assert_eq!(
            records.iter().map(|r| r.cursor).collect::<Vec<_>>(),
            vec![0, 1, 2, 3]
        );
    }

    #[test]
    fn stats_reflect_segment_count_and_bytes() {
        let dir = tmpdir();
        let w = Wal::open(dir.path(), cfg(112, 1 << 20)).unwrap();
        for i in 0..5u64 {
            w.append(i, 0, &i.to_le_bytes()).unwrap();
        }
        let s = w.stats();
        assert_eq!(s.pending_cursor, 5);
        assert!(s.segments >= 2);
        assert!(s.total_wal_bytes >= s.active_segment_bytes);
    }
}
