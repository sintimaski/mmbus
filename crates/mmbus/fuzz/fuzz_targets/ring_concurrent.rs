//! Concurrent fuzz target: one publisher thread + one subscriber thread
//! exercise the seqlock under sustained overwrite-mid-read pressure.
//!
//! Run via:  cargo +nightly fuzz run ring_concurrent -- -max_total_time=60
//!
//! Properties asserted on the subscriber:
//!   * cursor only ever advances (never goes backwards)
//!   * payload length is always within slot_payload_size
//!   * the message at every successful read is a valid u64 (we only
//!     publish u64 values 0..N — anything else means torn read OR UB)
//!
//! Bounded by `MAX_OPS` per iteration so libfuzzer keeps iterating.

#![no_main]

use libfuzzer_sys::fuzz_target;
use mmbus::RingBuffer;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread;

const CAPACITY: u32 = 16;
const SLOT_SIZE: u32 = 8;
const MAX_SUBS: u32 = 2;
const MAX_OPS: usize = 2_000;

fuzz_target!(|data: &[u8]| {
    if data.len() < 2 {
        return;
    }

    let tmp = tempfile::tempdir().expect("tempdir");
    let path = tmp.path().join("fuzz.mmap");
    let ring = Arc::new(
        RingBuffer::create(&path, CAPACITY, SLOT_SIZE, MAX_SUBS).expect("create"),
    );
    let cursor_idx = ring.claim_cursor(0).expect("claim");
    let stop = Arc::new(AtomicBool::new(false));

    // Publisher thread: bounded number of force-publishes derived from
    // the fuzz input.  Always writes u64 sequence numbers as payload.
    let pub_ring = ring.clone();
    let pub_stop = stop.clone();
    let n_writes = ((data[0] as usize) << 4 | (data[1] as usize)).min(MAX_OPS);
    let pub_handle = thread::spawn(move || {
        for i in 0..n_writes as u64 {
            if pub_stop.load(Ordering::Relaxed) {
                break;
            }
            pub_ring.publish_drop_oldest(&i.to_le_bytes());
        }
    });

    // Subscriber on this thread, racing the publisher.  Reads until either
    // the publisher is done AND the ring is empty, or we hit the op cap.
    let mut local_cursor = 0u64;
    let mut last_value: i128 = -1;
    let mut out = Vec::new();
    let mut consecutive_empty = 0;
    let mut ops = 0;

    while ops < MAX_OPS {
        ops += 1;
        match ring.try_receive(cursor_idx, local_cursor, &mut out) {
            Some(new_cursor) => {
                assert!(
                    new_cursor > local_cursor,
                    "cursor went backwards: {} -> {}",
                    local_cursor,
                    new_cursor,
                );
                local_cursor = new_cursor;
                consecutive_empty = 0;
                // Every payload is a u64 sequence number.  If we ever read
                // anything else (or wrong length), the seqlock failed.
                assert!(
                    out.len() == 8,
                    "payload len {} != 8 (slot torn read)",
                    out.len()
                );
                let value = u64::from_le_bytes(out.as_slice().try_into().unwrap());
                assert!(
                    (value as u128) < n_writes as u128,
                    "value {value} >= n_writes {n_writes} (corruption)",
                );
                assert!(
                    value as i128 > last_value,
                    "value went backwards: {} -> {} (DropOldest semantics: \
                     subscriber may skip but never reverse)",
                    last_value,
                    value,
                );
                last_value = value as i128;
            }
            None => {
                consecutive_empty += 1;
                if consecutive_empty > 100 {
                    // Ring's been empty for a while — publisher is probably
                    // done.  Stop racing.
                    break;
                }
                std::hint::spin_loop();
            }
        }
    }

    stop.store(true, Ordering::Relaxed);
    let _ = pub_handle.join();
    ring.release_cursor(cursor_idx);
});
