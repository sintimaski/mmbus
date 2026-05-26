//! Fuzz the low-level `RingBuffer` API: random sequences of publish, drop-
//! oldest publish, and try_receive across a small set of subscribers.
//!
//! Run via:  cargo +nightly fuzz run ring_publish_receive -- -max_total_time=60

#![no_main]

use libfuzzer_sys::fuzz_target;
use mmbus::RingBuffer;

const CAPACITY: u32 = 16;
const SLOT_SIZE: u32 = 32;
const MAX_SUBS: u32 = 4;

fuzz_target!(|data: &[u8]| {
    if data.is_empty() {
        return;
    }

    let tmp = tempfile::tempdir().expect("tempdir");
    let path = tmp.path().join("fuzz.mmap");
    let ring = RingBuffer::create(&path, CAPACITY, SLOT_SIZE, MAX_SUBS)
        .expect("RingBuffer::create");

    // First byte: how many subscribers to claim up front.
    let n_subs = (data[0] as u32 % (MAX_SUBS + 1)) as usize;
    let mut sub_idxs: Vec<usize> = Vec::with_capacity(n_subs);
    for _ in 0..n_subs {
        if let Some(idx) = ring.claim_cursor(0) {
            sub_idxs.push(idx);
        }
    }
    let mut local_cursors: Vec<u64> = vec![0; sub_idxs.len()];

    // Remaining bytes drive an op stream:
    //   op byte  -> op = op % 3
    //               0 = try_publish (len byte + payload)
    //               1 = publish_drop_oldest (len byte + payload)
    //               2 = try_receive on subscriber (sel byte chooses which)
    let mut i = 1;
    while i < data.len() {
        let op = data[i] % 3;
        i += 1;
        match op {
            0 | 1 => {
                if i >= data.len() {
                    break;
                }
                let want = data[i] as usize;
                i += 1;
                let len = want.min(SLOT_SIZE as usize).min(data.len() - i);
                let payload = &data[i..i + len];
                i += len;
                if op == 0 {
                    let _ = ring.try_publish(payload);
                } else {
                    let _ = ring.publish_drop_oldest(payload);
                }
            }
            2 => {
                if sub_idxs.is_empty() {
                    continue;
                }
                if i >= data.len() {
                    break;
                }
                let s = (data[i] as usize) % sub_idxs.len();
                i += 1;
                let mut out = Vec::new();
                if let Some(new_cur) =
                    ring.try_receive(sub_idxs[s], local_cursors[s], &mut out)
                {
                    // Property: cursor must advance monotonically.
                    assert!(
                        new_cur > local_cursors[s],
                        "cursor went backwards: {} -> {}",
                        local_cursors[s],
                        new_cur,
                    );
                    local_cursors[s] = new_cur;
                    // Property: payload length must be within slot size.
                    assert!(
                        out.len() <= SLOT_SIZE as usize,
                        "payload len {} exceeds slot size {}",
                        out.len(),
                        SLOT_SIZE,
                    );
                }
            }
            _ => unreachable!(),
        }
    }

    // Clean up: release every cursor we claimed.
    for idx in sub_idxs {
        ring.release_cursor(idx);
    }
});
