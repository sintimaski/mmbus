/// Lock-free SPMC ring buffer over a memory-mapped file.
///
/// # Wire format (v4 — SPMC + generation + per-slot seqlock)
///
/// ```text
/// Bytes 0..64  — fixed header
///   0   u64    magic
///   8   u32    version (= 4)
///  12   u32    capacity (slot count)
///  16   u32    slot_payload_size (max bytes per message)
///  20   u32    max_subscribers
///  24   u64    generation  (AtomicU64, incremented on publisher restart)
///  32   u64    tail        (AtomicU64, producer cursor)
///  40   u24    _pad to 64-byte cache line
///
/// Bytes 64 .. 64+8*max_subscribers — subscriber cursor table
///   cursor[i]  u64  (AtomicU64)
///                   CURSOR_UNCLAIMED (u64::MAX) = slot free
///                   any other value = subscriber's next-read position
///
/// Bytes ALIGN64(64+8*max_subscribers) onwards — ring slots
///   slot[i]  [u64 seq][u32 len][payload (slot_payload_size bytes)]
///
///   `seq` carries the publisher's tail value at the moment this slot was
///   written.  Subscribers Acquire-load it before AND after copying the
///   payload (seqlock pattern): a mismatch means the publisher overwrote
///   the slot during the read, so the subscriber re-resolves its position
///   from the new seq and retries.  Under `BackpressurePolicy::DropOldest`
///   this is how skipped-message detection happens — no force-advance of
///   the cursor table is needed.
/// ```
///
/// # SPMC invariant
///
/// The producer may only advance `tail` as long as:
///   tail - min(active_cursors) < capacity
///
/// Each subscriber atomically updates its own cursor slot after reading.
/// No shared `head` exists: every subscriber is independent.
use memmap2::MmapMut;
use std::cell::UnsafeCell;
use std::fs::OpenOptions;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};

pub const MAGIC: u64 = 0x6D6D_6275_7300_0004; // "mmbus" + format version 4
pub const CURSOR_UNCLAIMED: u64 = u64::MAX;
pub const MAX_SUBSCRIBERS_DEFAULT: u32 = 16;

const OFF_MAGIC: usize = 0;
const OFF_VERSION: usize = 8;
const OFF_CAPACITY: usize = 12;
const OFF_SLOT_SIZE: usize = 16;
const OFF_MAX_SUBS: usize = 20;
const OFF_GENERATION: usize = 24; // AtomicU64 — incremented on publisher restart
const OFF_TAIL: usize = 32; // AtomicU64 — producer cursor
const OFF_CURSORS: usize = 64; // AtomicU64[max_subscribers]

/// First byte offset of the ring slots (64-byte aligned, after cursor table).
pub fn slots_offset(max_subscribers: u32) -> usize {
    let raw = OFF_CURSORS + 8 * max_subscribers as usize;
    (raw + 63) & !63
}

/// On-disk slot stride: 8 B seq + 4 B len + `slot_payload_size` payload bytes,
/// rounded up to an 8-byte multiple so every slot's `seq: AtomicU64` is naturally
/// aligned.
const SLOT_OVERHEAD: usize = 8 + 4;
const SLOT_ALIGN: usize = 8;

fn slot_stride(slot_payload_size: u32) -> usize {
    let raw = SLOT_OVERHEAD + slot_payload_size as usize;
    (raw + SLOT_ALIGN - 1) & !(SLOT_ALIGN - 1)
}

pub fn mmap_size(capacity: u32, slot_payload_size: u32, max_subscribers: u32) -> usize {
    slots_offset(max_subscribers) + slot_stride(slot_payload_size) * capacity as usize
}

pub struct RingBuffer {
    inner: UnsafeCell<MmapMut>,
    pub capacity: u32,
    pub slot_payload_size: u32,
    pub max_subscribers: u32,
    slots_off: usize,
}

// SAFETY (Send + Sync): `RingBuffer` wraps `UnsafeCell<MmapMut>`.  We never
// hand out `&mut MmapMut`; every mutation goes through atomic ops on
// pointer-derived `AtomicU64` references or through `write_unaligned` /
// `copy_nonoverlapping` on disjoint slot regions (publisher writes its slot
// before bumping `tail`; subscribers only read slots strictly less than
// `tail`).  Cross-process sharing of the same mmap pages is the design.
unsafe impl Send for RingBuffer {}
unsafe impl Sync for RingBuffer {}

impl RingBuffer {
    pub fn create(
        path: &Path,
        capacity: u32,
        slot_payload_size: u32,
        max_subscribers: u32,
    ) -> std::io::Result<Self> {
        let total = mmap_size(capacity, slot_payload_size, max_subscribers);
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(path)?;
        file.set_len(total as u64)?;

        // SAFETY: file was `set_len(total)`-ed above; memmap2 maps exactly
        // that many bytes.  We're the only mmap of this file (we just
        // created it), so there's no concurrent reader yet.
        let mut mmap = unsafe { MmapMut::map_mut(&file)? };
        let p = mmap.as_mut_ptr();

        // SAFETY: `p` points to the first byte of a `total`-byte mmap
        // (>= 64 B header).  Every OFF_* is < 64; `write_unaligned`
        // handles any alignment (in practice all offsets are naturally
        // aligned).  OFF_GENERATION is 8-byte aligned (offset 24) so
        // the AtomicU64 cast is sound.  No other thread holds a
        // reference to the mmap yet (we just returned from `map_mut`).
        unsafe {
            p.add(OFF_MAGIC).cast::<u64>().write_unaligned(MAGIC);
            p.add(OFF_VERSION).cast::<u32>().write_unaligned(4);
            p.add(OFF_CAPACITY).cast::<u32>().write_unaligned(capacity);
            p.add(OFF_SLOT_SIZE).cast::<u32>().write_unaligned(slot_payload_size);
            p.add(OFF_MAX_SUBS).cast::<u32>().write_unaligned(max_subscribers);
            // generation starts at 1 (file is zero-initialized; bump from 0).
            (*(p.add(OFF_GENERATION) as *mut AtomicU64)).store(1, Ordering::Release);
            // tail starts at 0 (file is zero-initialized).
        }

        // The file is zero-initialized; cursor slots need CURSOR_UNCLAIMED (u64::MAX).
        for i in 0..max_subscribers as usize {
            // SAFETY: i < max_subscribers, so OFF_CURSORS + i*8 < slots_offset
            // <= total.  Each cursor slot is at an 8-byte offset from
            // OFF_CURSORS (= 64), hence 8-byte aligned.  Single-threaded
            // init; Relaxed is fine.
            unsafe {
                let cp = p.add(OFF_CURSORS + i * 8) as *mut AtomicU64;
                (*cp).store(CURSOR_UNCLAIMED, Ordering::Relaxed);
            }
        }

        let slots_off = slots_offset(max_subscribers);
        Ok(Self { inner: UnsafeCell::new(mmap), capacity, slot_payload_size, max_subscribers, slots_off })
    }

    pub fn open(path: &Path) -> std::io::Result<Self> {
        let file = OpenOptions::new().read(true).write(true).open(path)?;
        // SAFETY: the file was created with `mmap_size(...)` bytes by
        // `create()` (we trust the magic check below to confirm it's
        // ours). Other processes may concurrently mmap the same file —
        // that's the design; all mutation goes through atomics + disjoint
        // slot writes.
        let mmap = unsafe { MmapMut::map_mut(&file)? };

        // SAFETY: the file is at least 64 B (we set_len'd to mmap_size,
        // and the smallest mmap_size starts at slots_offset(0) = 64).
        // All header offsets are < 32 so well within the mapped region.
        // read_unaligned tolerates any alignment.
        let p = mmap.as_ptr();
        let magic = unsafe { p.add(OFF_MAGIC).cast::<u64>().read_unaligned() };
        if magic != MAGIC {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("mmbus: unrecognised magic {magic:#018x} (want {MAGIC:#018x}; is this an older format?)"),
            ));
        }
        // SAFETY: same as above — offsets within the header region.
        let capacity = unsafe { p.add(OFF_CAPACITY).cast::<u32>().read_unaligned() };
        let slot_payload_size = unsafe { p.add(OFF_SLOT_SIZE).cast::<u32>().read_unaligned() };
        let max_subscribers = unsafe { p.add(OFF_MAX_SUBS).cast::<u32>().read_unaligned() };
        let slots_off = slots_offset(max_subscribers);

        Ok(Self { inner: UnsafeCell::new(mmap), capacity, slot_payload_size, max_subscribers, slots_off })
    }

    /// Publisher entry point: reuse an existing compatible ring (bumping its
    /// `generation`) or create a fresh one.  Reusing avoids `ftruncate(0)`,
    /// which would otherwise invalidate any existing subscriber's mmap and
    /// risk SIGBUS — instead, existing subscribers see the bumped generation
    /// on their next wakeup and shut down cleanly via `Error::Io(UnexpectedEof)`.
    pub fn create_or_reuse(
        path: &Path,
        capacity: u32,
        slot_payload_size: u32,
        max_subscribers: u32,
    ) -> std::io::Result<Self> {
        // Reuse path: a compatible v3 ring already exists on disk.
        if let Ok(existing) = Self::open(path) {
            if existing.capacity == capacity
                && existing.slot_payload_size == slot_payload_size
                && existing.max_subscribers == max_subscribers
            {
                existing.generation_atomic().fetch_add(1, Ordering::AcqRel);
                existing.tail_atomic().store(0, Ordering::Release);
                // Cursors are NOT reset: leaving them claimed forces stale
                // subscribers to detect the generation bump and self-release
                // their slots via `Drop`.
                return Ok(existing);
            }
        }
        // Fresh path: wrong shape, wrong version, or no file.
        Self::create(path, capacity, slot_payload_size, max_subscribers)
    }

    /// Current publisher generation.  Bumped each time the same on-disk ring
    /// is reused by a new `Publisher::create` (i.e. after the previous
    /// publisher crashed or shut down).
    pub fn generation(&self) -> u64 {
        self.generation_atomic().load(Ordering::Acquire)
    }

    fn generation_atomic(&self) -> &AtomicU64 {
        // SAFETY: `base()` is the mmap start, OFF_GENERATION=24 is 8-byte
        // aligned, and the header (64 B) is always mapped — see `create()`
        // and `open()`.  Cast to `&AtomicU64` because every read/write of
        // this field crate-wide goes through atomic ops.
        unsafe { &*(self.base().add(OFF_GENERATION) as *const AtomicU64) }
    }

    fn stride(&self) -> usize {
        slot_stride(self.slot_payload_size)
    }

    fn base(&self) -> *mut u8 {
        // SAFETY: UnsafeCell::get returns a raw `*mut MmapMut` valid for
        // the lifetime of `self`.  We never construct a `&mut MmapMut`
        // elsewhere, so this is the unique mutable access.  The MmapMut
        // we deref into is owned by self, so the &mut is short-lived
        // and discarded as soon as `as_mut_ptr()` returns the byte ptr.
        unsafe { (&mut *self.inner.get()).as_mut_ptr() }
    }

    fn tail_atomic(&self) -> &AtomicU64 {
        // SAFETY: OFF_TAIL=32 is 8-byte aligned; the header is always
        // mapped (see `generation_atomic` SAFETY for the full rationale).
        unsafe { &*(self.base().add(OFF_TAIL) as *const AtomicU64) }
    }

    fn cursor_atomic(&self, idx: usize) -> &AtomicU64 {
        debug_assert!(idx < self.max_subscribers as usize);
        // SAFETY: idx < max_subscribers (debug-checked); cursor table
        // ends at OFF_CURSORS + 8*max_subscribers <= slots_offset, so
        // the access is within the mapped header region.  Cursor slots
        // are 8-byte aligned (OFF_CURSORS=64 plus 8-byte stride).
        unsafe { &*(self.base().add(OFF_CURSORS + idx * 8) as *const AtomicU64) }
    }

    // ── Subscriber cursor management ──────────────────────────────────────────

    /// Claim a free cursor slot, initialising it to `initial_cursor`.
    /// Returns the slot index on success, or `None` if all slots are taken.
    pub fn claim_cursor(&self, initial_cursor: u64) -> Option<usize> {
        (0..self.max_subscribers as usize).find(|&i| {
            self.cursor_atomic(i)
                .compare_exchange(
                    CURSOR_UNCLAIMED,
                    initial_cursor,
                    Ordering::AcqRel,
                    Ordering::Relaxed,
                )
                .is_ok()
        })
    }

    /// Release a cursor slot (subscriber disconnecting or being dropped).
    /// Overwrite a previously-claimed cursor slot.  Used by the subscriber
    /// to re-synchronise its position with the current tail after the
    /// publisher handshake completes (in case the publisher restarted and
    /// reset the tail between `claim_cursor` and the socket connect).
    pub fn set_cursor(&self, idx: usize, value: u64) {
        self.cursor_atomic(idx).store(value, Ordering::Release);
    }

    pub fn release_cursor(&self, idx: usize) {
        self.cursor_atomic(idx).store(CURSOR_UNCLAIMED, Ordering::Release);
    }

    // ── Producer helpers ──────────────────────────────────────────────────────

    /// Minimum cursor across all claimed subscriber slots, or `None` if no
    /// subscribers are active.
    fn min_active_cursor(&self) -> Option<u64> {
        let mut min: Option<u64> = None;
        for i in 0..self.max_subscribers as usize {
            let c = self.cursor_atomic(i).load(Ordering::Acquire);
            if c != CURSOR_UNCLAIMED {
                min = Some(min.map_or(c, |m| m.min(c)));
            }
        }
        min
    }

    /// Write `data` to the next available slot.
    /// Returns `false` if the ring is full (slowest subscriber is too far behind).
    pub fn try_publish(&self, data: &[u8]) -> bool {
        assert!(
            data.len() <= self.slot_payload_size as usize,
            "data len {} exceeds slot_payload_size {}",
            data.len(),
            self.slot_payload_size,
        );

        let tail = self.tail_atomic().load(Ordering::Relaxed);
        let effective_min = self.min_active_cursor().unwrap_or(tail);

        if tail.wrapping_sub(effective_min) >= self.capacity as u64 {
            return false;
        }

        self.write_slot(tail, data);
        self.tail_atomic().store(tail + 1, Ordering::Release);
        true
    }

    /// Like `try_publish` but always succeeds: when the ring is full the
    /// publisher overwrites the slot containing the oldest unread message.
    /// Subscribers detect the overwrite via the slot's seq field and skip
    /// forward — no cursor-table force-advance is needed.
    pub fn publish_drop_oldest(&self, data: &[u8]) -> bool {
        let tail = self.tail_atomic().load(Ordering::Relaxed);
        self.write_slot(tail, data);
        self.tail_atomic().store(tail + 1, Ordering::Release);
        true
    }

    /// Write `data` into the ring slot for `tail` and stamp it with
    /// `seq = tail`.  The seq Release-store goes last so subscribers
    /// observing it via Acquire see the new len + payload.
    fn write_slot(&self, tail: u64, data: &[u8]) {
        let idx = (tail % self.capacity as u64) as usize;
        // SAFETY: idx < capacity (modulo); slot_offset + idx*stride <
        // slots_offset + capacity*stride <= mmap_size.  Slot base is
        // 8-byte aligned via `slot_stride()` padding, so the seq cast
        // is well-aligned.
        let slot = unsafe { self.base().add(self.slots_off + idx * self.stride()) };
        // SAFETY (write order):
        //   1. len at slot+8 (4 B; unaligned-safe via write_unaligned).
        //   2. payload at slot+12 (up to slot_payload_size bytes — the
        //      caller upholds `data.len() <= slot_payload_size`).
        //   3. seq at slot+0 with Release: subscribers' Acquire-load of
        //      seq sees the new len + payload.
        // Publisher is single (SPMC); no concurrent writer to this slot.
        unsafe {
            (slot.add(8) as *mut u32).write_unaligned(data.len() as u32);
            std::ptr::copy_nonoverlapping(data.as_ptr(), slot.add(12), data.len());
            (*(slot as *const AtomicU64)).store(tail, Ordering::Release);
        }
    }

    // ── Subscriber helpers ────────────────────────────────────────────────────

    /// Read the message at `local_cursor` for the subscriber at `cursor_idx`.
    ///
    /// On success: copies the payload into `out`, atomically advances the
    /// cursor slot in the ring, and returns `Some(local_cursor + 1)`.
    ///
    /// If `local_cursor` is behind the ring cursor (force-advanced by
    /// `publish_drop_oldest`), it skips forward and reads the latest available
    /// position instead, returning `Some(new_cursor)` (> `local_cursor + 1`).
    ///
    /// Returns `None` if no message is available.
    pub fn try_receive(
        &self,
        cursor_idx: usize,
        local_cursor: u64,
        out: &mut Vec<u8>,
    ) -> Option<u64> {
        let tail = self.tail_atomic().load(Ordering::Acquire);
        if local_cursor >= tail {
            return None;
        }

        // Seqlock read protocol — guards against torn reads under
        // `DropOldest` where the publisher may overwrite a slot mid-copy.
        const MAX_RETRIES: usize = 16;
        let mut effective = local_cursor;
        for _ in 0..MAX_RETRIES {
            let idx = (effective % self.capacity as u64) as usize;
            // SAFETY: same bounds + alignment argument as `write_slot`:
            // idx < capacity, slot within the mapped region, 8-byte aligned.
            let slot = unsafe { self.base().add(self.slots_off + idx * self.stride()) };
            // SAFETY: seq field is at slot+0, 8-byte aligned; this `&AtomicU64`
            // lives only for the iteration so it can't outlive the mmap.
            let seq_atomic = unsafe { &*(slot as *const AtomicU64) };

            let seq_before = seq_atomic.load(Ordering::Acquire);
            if seq_before > effective {
                // Publisher overwrote this slot with a newer message.
                // Skip forward to whatever's there now.
                effective = seq_before;
                continue;
            }
            if seq_before < effective {
                // Slot not yet written for our position.  Should be ruled
                // out by the tail check above; bail defensively.
                return None;
            }
            // SAFETY (read-after-seq-Acquire): paired with the publisher's
            // Release-store of `seq`; we therefore observe the len + payload
            // that was current at the seq_before time.  Mid-copy overwrite
            // by the publisher is detected by the seq_after re-check below.
            let len =
                unsafe { (slot.add(8) as *const u32).read_unaligned() as usize };
            if len > self.slot_payload_size as usize {
                // Torn read of `len` field — slot is being overwritten.
                continue;
            }
            out.clear();
            out.resize(len, 0);
            // SAFETY: len <= slot_payload_size (checked above); destination
            // Vec is grown to exactly len bytes; source is within the slot
            // payload region (slot+12 + len <= slot+12+slot_payload_size).
            unsafe {
                std::ptr::copy_nonoverlapping(slot.add(12), out.as_mut_ptr(), len);
            }
            let seq_after = seq_atomic.load(Ordering::Acquire);
            if seq_after != seq_before {
                // Slot was overwritten during the payload copy — retry,
                // possibly at the new seq if it leapt ahead.
                if seq_after > effective {
                    effective = seq_after;
                }
                continue;
            }

            let new_cursor = effective + 1;
            self.cursor_atomic(cursor_idx).store(new_cursor, Ordering::Release);
            return Some(new_cursor);
        }
        // Publisher is sustained-overwriting this slot faster than we can
        // copy — give up; the caller's wakeup loop will retry.
        None
    }

    pub fn current_tail(&self) -> u64 {
        self.tail_atomic().load(Ordering::Acquire)
    }

    /// Per-subscriber `(cursor_idx, lag)` for every claimed cursor, where
    /// `lag = tail - cursor`.  Used by `Bus::slow_subscribers` to identify
    /// laggards by stable index across subsequent calls.
    pub fn lags_with_idx(&self) -> Vec<(usize, u64)> {
        let tail = self.tail_atomic().load(Ordering::Acquire);
        (0..self.max_subscribers as usize)
            .filter_map(|i| {
                let c = self.cursor_atomic(i).load(Ordering::Acquire);
                if c == CURSOR_UNCLAIMED {
                    None
                } else {
                    Some((i, tail.saturating_sub(c)))
                }
            })
            .collect()
    }

    /// Snapshot of the ring's backpressure state.
    pub fn stats(&self) -> RingStats {
        let tail = self.tail_atomic().load(Ordering::Acquire);
        let mut lags = Vec::new();
        for i in 0..self.max_subscribers as usize {
            let c = self.cursor_atomic(i).load(Ordering::Acquire);
            if c != CURSOR_UNCLAIMED {
                lags.push(tail.saturating_sub(c));
            }
        }
        RingStats { tail, active_subscribers: lags.len(), lags }
    }
}

/// Snapshot of ring cursor state for monitoring.
#[derive(Debug, Clone)]
pub struct RingStats {
    /// Next slot the producer will write.
    pub tail: u64,
    /// Number of claimed (active) subscriber cursor slots.
    pub active_subscribers: usize,
    /// Per-subscriber lag in messages (tail − cursor). One entry per active
    /// subscriber; order is arbitrary.
    pub lags: Vec<u64>,
}
