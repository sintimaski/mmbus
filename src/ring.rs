use memmap2::MmapMut;
use std::cell::UnsafeCell;
use std::fs::OpenOptions;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};

// Magic = "mmbus" (5 bytes) packed into u64 high bits + version 1 in low byte.
pub(crate) const MAGIC: u64 = 0x6D6D_6275_7300_0001;
pub(crate) const HEADER_SIZE: usize = 64; // one cache line

// Header field offsets (bytes from mmap start).
const OFF_MAGIC: usize = 0;    // u64
const OFF_VERSION: usize = 8;  // u32
const OFF_CAPACITY: usize = 12; // u32  — number of slots
const OFF_SLOT_SIZE: usize = 16; // u32 — max payload bytes per slot
// 20..32: reserved
const OFF_HEAD: usize = 32; // AtomicU64 — consumer cursor (aligned to 8)
const OFF_TAIL: usize = 40; // AtomicU64 — producer cursor (aligned to 8)
// 48..64: padding to full cache line

// Slot layout inside the ring:
//   [0..4]  : u32 payload length (little-endian)
//   [4..4+slot_payload_size]: payload bytes

pub(crate) struct RingBuffer {
    // UnsafeCell enables write-through raw pointers from &self, which we need
    // because publisher and subscriber each hold their own MmapMut handle to
    // the same shared file. Atomic operations provide the actual synchronization.
    inner: UnsafeCell<MmapMut>,
    pub capacity: u32,
    pub slot_payload_size: u32,
}

unsafe impl Send for RingBuffer {}
unsafe impl Sync for RingBuffer {}

impl RingBuffer {
    pub fn create(path: &Path, capacity: u32, slot_payload_size: u32) -> std::io::Result<Self> {
        let stride = 4 + slot_payload_size as usize;
        let total_bytes = HEADER_SIZE + stride * capacity as usize;

        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(path)?;
        file.set_len(total_bytes as u64)?;

        let mut mmap = unsafe { MmapMut::map_mut(&file)? };

        let p = mmap.as_mut_ptr();
        unsafe {
            p.add(OFF_MAGIC).cast::<u64>().write_unaligned(MAGIC);
            p.add(OFF_VERSION).cast::<u32>().write_unaligned(1);
            p.add(OFF_CAPACITY).cast::<u32>().write_unaligned(capacity);
            p.add(OFF_SLOT_SIZE).cast::<u32>().write_unaligned(slot_payload_size);
            // head and tail are zero from the file allocation
        }

        Ok(Self {
            inner: UnsafeCell::new(mmap),
            capacity,
            slot_payload_size,
        })
    }

    pub fn open(path: &Path) -> std::io::Result<Self> {
        let file = OpenOptions::new().read(true).write(true).open(path)?;
        let mmap = unsafe { MmapMut::map_mut(&file)? };

        let p = mmap.as_ptr();
        let magic = unsafe { p.add(OFF_MAGIC).cast::<u64>().read_unaligned() };
        if magic != MAGIC {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "mmbus: invalid magic number",
            ));
        }
        let capacity = unsafe { p.add(OFF_CAPACITY).cast::<u32>().read_unaligned() };
        let slot_payload_size = unsafe { p.add(OFF_SLOT_SIZE).cast::<u32>().read_unaligned() };

        Ok(Self {
            inner: UnsafeCell::new(mmap),
            capacity,
            slot_payload_size,
        })
    }

    fn stride(&self) -> usize {
        4 + self.slot_payload_size as usize
    }

    // Raw mutable pointer into the mmap base. Safe to call from &self because
    // the UnsafeCell signals interior mutability; callers are responsible for
    // not aliasing the same bytes non-atomically from multiple threads.
    fn base(&self) -> *mut u8 {
        unsafe { (&mut *self.inner.get()).as_mut_ptr() }
    }

    fn tail_atomic(&self) -> &AtomicU64 {
        unsafe { &*(self.base().add(OFF_TAIL) as *const AtomicU64) }
    }

    fn head_atomic(&self) -> &AtomicU64 {
        unsafe { &*(self.base().add(OFF_HEAD) as *const AtomicU64) }
    }

    /// Write `data` into the next available slot. Returns false if the ring is full.
    ///
    /// Memory ordering: slot bytes are written before a Release store of tail,
    /// so a consumer doing an Acquire load of tail sees the full payload.
    pub fn try_publish(&self, data: &[u8]) -> bool {
        assert!(
            data.len() <= self.slot_payload_size as usize,
            "data len {} exceeds slot_payload_size {}",
            data.len(),
            self.slot_payload_size,
        );

        let tail = self.tail_atomic().load(Ordering::Relaxed);
        let head = self.head_atomic().load(Ordering::Acquire);

        if tail.wrapping_sub(head) >= self.capacity as u64 {
            return false; // ring full
        }

        let idx = (tail % self.capacity as u64) as usize;
        let slot = unsafe { self.base().add(HEADER_SIZE + idx * self.stride()) };

        unsafe {
            (slot as *mut u32).write_unaligned(data.len() as u32);
            std::ptr::copy_nonoverlapping(data.as_ptr(), slot.add(4), data.len());
        }

        // Release: ensures slot bytes are visible before tail is visible.
        self.tail_atomic().store(tail + 1, Ordering::Release);
        true
    }

    /// Read the message at `cursor` into `out`. Returns false if `cursor >= tail`
    /// (no message at this position yet). Caller must call `advance_head` after.
    pub fn try_receive(&self, cursor: u64, out: &mut Vec<u8>) -> bool {
        // Acquire: ensures we see the slot bytes written before the Release on tail.
        let tail = self.tail_atomic().load(Ordering::Acquire);
        if cursor >= tail {
            return false;
        }

        let idx = (cursor % self.capacity as u64) as usize;
        let slot = unsafe { self.base().add(HEADER_SIZE + idx * self.stride()) };
        let len = unsafe { (slot as *const u32).read_unaligned() as usize };

        out.clear();
        out.resize(len, 0);
        unsafe {
            std::ptr::copy_nonoverlapping(slot.add(4), out.as_mut_ptr(), len);
        }
        true
    }

    /// Advance the shared head pointer. Call after successfully receiving a message
    /// to inform the producer that the slot can be reused.
    pub fn advance_head(&self, new_head: u64) {
        self.head_atomic().store(new_head, Ordering::Release);
    }

    pub fn current_tail(&self) -> u64 {
        self.tail_atomic().load(Ordering::Acquire)
    }
}
