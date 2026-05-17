//! Bounded MPSC queue with drop-oldest-on-overflow semantics.
//!
//! `std::sync::mpsc::sync_channel` is *bounded* but blocks the sender
//! when full — we want the opposite: the publisher must never stall on
//! a slow peer, so the queue evicts its oldest entry to make room
//! instead.  This is the cross-machine analog of the local mmbus
//! `BackpressurePolicy::DropOldest`.
//!
//! Single-consumer (per peer's forwarder thread), multi-producer (one
//! subscriber thread per forward-enabled topic feeds the same queue).
//! `Sender::send` is O(1) under lock; `Receiver::recv_timeout` blocks
//! up to the requested duration on a condvar.
//!
//! Returns the number of *evicted* items so the caller can count
//! per-peer drops for monitoring.

use std::collections::VecDeque;
use std::sync::{Arc, Condvar, Mutex};
use std::time::{Duration, Instant};

struct Inner<T> {
    queue: VecDeque<T>,
    closed: bool,
}

struct Shared<T> {
    inner: Mutex<Inner<T>>,
    not_empty: Condvar,
    capacity: usize,
}

/// Producer handle.  `Clone` is cheap (just an Arc bump).
pub struct Sender<T> {
    shared: Arc<Shared<T>>,
}

impl<T> Clone for Sender<T> {
    fn clone(&self) -> Self {
        Self { shared: self.shared.clone() }
    }
}

impl<T> Sender<T> {
    /// Push `item` onto the queue.  If the queue is at capacity, evict
    /// the oldest entry first.  Returns the number of items dropped
    /// (0 or 1).
    pub fn send(&self, item: T) -> usize {
        let mut inner = self.shared.inner.lock().unwrap_or_else(|e| e.into_inner());
        if inner.closed {
            return 0;
        }
        let dropped = if inner.queue.len() >= self.shared.capacity {
            inner.queue.pop_front();
            1
        } else {
            0
        };
        inner.queue.push_back(item);
        // notify_one is enough — the consumer can drain the rest with
        // try_recv after waking.
        self.shared.not_empty.notify_one();
        dropped
    }
}

/// Consumer handle.  Single per queue; clone is not implemented to
/// keep the SPMC invariant clear.
pub struct Receiver<T> {
    shared: Arc<Shared<T>>,
}

impl<T> Receiver<T> {
    /// Block up to `timeout` for the next item.  Returns:
    /// * `Some(item)` — drained a message.
    /// * `None`       — timeout expired without an item, OR all
    ///   `Sender`s have been dropped AND the queue is empty
    ///   (i.e. clean shutdown).
    pub fn recv_timeout(&self, timeout: Duration) -> Option<T> {
        let deadline = Instant::now() + timeout;
        let mut inner = self.shared.inner.lock().unwrap_or_else(|e| e.into_inner());
        loop {
            if let Some(item) = inner.queue.pop_front() {
                return Some(item);
            }
            if inner.closed {
                return None;
            }
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return None;
            }
            let (guard, wait_res) = self
                .shared
                .not_empty
                .wait_timeout(inner, remaining)
                .unwrap_or_else(|e| e.into_inner());
            inner = guard;
            if wait_res.timed_out() && inner.queue.is_empty() {
                return None;
            }
        }
    }
}

impl<T> Drop for Sender<T> {
    fn drop(&mut self) {
        // Only flip `closed` when this is the last Sender; before that,
        // other senders are still active and the consumer should keep
        // waiting.  Sender count == strong_count - 1 (the receiver
        // also holds one).
        if Arc::strong_count(&self.shared) <= 2 {
            let mut inner = self.shared.inner.lock().unwrap_or_else(|e| e.into_inner());
            inner.closed = true;
            // Wake the consumer so it can observe `closed` and return.
            self.shared.not_empty.notify_all();
        }
    }
}

/// Build a (sender, receiver) pair with a hard cap of `capacity` items.
/// A `send` to a full queue evicts the oldest entry to make room.
///
/// Panics if `capacity == 0` (an immediately-full queue is never useful).
pub fn channel<T>(capacity: usize) -> (Sender<T>, Receiver<T>) {
    assert!(capacity > 0, "DropOldestQueue capacity must be > 0");
    let shared = Arc::new(Shared {
        inner: Mutex::new(Inner { queue: VecDeque::with_capacity(capacity), closed: false }),
        not_empty: Condvar::new(),
        capacity,
    });
    (Sender { shared: shared.clone() }, Receiver { shared })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;

    #[test]
    fn send_and_recv_roundtrip() {
        let (tx, rx) = channel::<u32>(8);
        assert_eq!(tx.send(1), 0);
        assert_eq!(tx.send(2), 0);
        assert_eq!(rx.recv_timeout(Duration::from_millis(10)), Some(1));
        assert_eq!(rx.recv_timeout(Duration::from_millis(10)), Some(2));
        assert_eq!(rx.recv_timeout(Duration::from_millis(10)), None);
    }

    #[test]
    fn drops_oldest_on_overflow() {
        let (tx, rx) = channel::<u32>(3);
        assert_eq!(tx.send(1), 0);
        assert_eq!(tx.send(2), 0);
        assert_eq!(tx.send(3), 0);
        // Capacity == 3 reached; next send evicts 1.
        assert_eq!(tx.send(4), 1);
        assert_eq!(tx.send(5), 1);
        // Queue should now hold 3, 4, 5 in order.
        assert_eq!(rx.recv_timeout(Duration::from_millis(10)), Some(3));
        assert_eq!(rx.recv_timeout(Duration::from_millis(10)), Some(4));
        assert_eq!(rx.recv_timeout(Duration::from_millis(10)), Some(5));
        assert_eq!(rx.recv_timeout(Duration::from_millis(10)), None);
    }

    #[test]
    fn receiver_unblocks_when_all_senders_drop() {
        let (tx, rx) = channel::<u32>(8);
        let tx2 = tx.clone();
        let h = thread::spawn(move || {
            // Block until the senders are dropped, then return.
            rx.recv_timeout(Duration::from_secs(5))
        });
        thread::sleep(Duration::from_millis(20));
        drop(tx);
        drop(tx2);
        let result = h.join().unwrap();
        assert_eq!(result, None, "recv must return None after all senders drop");
    }

    #[test]
    fn slow_consumer_sees_a_suffix_under_load() {
        // Under drop-oldest semantics with a deliberately-slow consumer,
        // some messages WILL be dropped — that's the whole point.  The
        // invariants we hold are: (1) the last item read is the most
        // recent one sent, and (2) the values we did see are strictly
        // monotonically increasing (drops are contiguous prefixes, not
        // gaps in the middle).
        let (tx, rx) = channel::<u32>(64);
        let n = 5_000u32;
        let producer = thread::spawn(move || {
            for i in 0..n {
                tx.send(i);
            }
        });
        producer.join().unwrap();
        // Producer is done; the queue has at most 64 of the most recent
        // values.  Drain.
        let mut got = Vec::new();
        while let Some(v) = rx.recv_timeout(Duration::from_millis(50)) {
            got.push(v);
        }
        assert!(got.len() <= 64, "queue retained at most capacity items");
        assert!(!got.is_empty(), "must retain something");
        assert_eq!(got.last(), Some(&(n - 1)), "most recent must always be present");
        for window in got.windows(2) {
            assert!(window[0] < window[1], "values must be strictly increasing");
        }
    }

    #[test]
    fn multi_producer_drains_to_empty() {
        let (tx, rx) = channel::<u32>(1024);
        let mut handles = Vec::new();
        for p in 0..4 {
            let tx = tx.clone();
            handles.push(thread::spawn(move || {
                for i in 0..100 {
                    tx.send(p * 100 + i);
                }
            }));
        }
        drop(tx); // close once producers are gone, the receiver will get None
        for h in handles {
            h.join().unwrap();
        }
        let mut count = 0;
        while rx.recv_timeout(Duration::from_millis(50)).is_some() {
            count += 1;
        }
        assert_eq!(count, 400);
    }

    #[test]
    #[should_panic(expected = "capacity must be > 0")]
    fn zero_capacity_panics() {
        let (_tx, _rx) = channel::<u32>(0);
    }
}
