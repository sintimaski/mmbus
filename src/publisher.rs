use crate::config::{BackpressurePolicy, BusConfig};
use crate::error::{Error, Result};
use crate::producer_lock::{acquire_producer_lock, ProducerLock};
use crate::ring::RingBuffer;
use crate::stats::TopicStats;
use crate::wal::Wal;
use std::fs;
use std::io;
use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};
use std::time::{Duration, Instant};

#[cfg(unix)]
use std::os::unix::io::AsRawFd;
#[cfg(unix)]
use std::os::unix::net::{UnixListener, UnixStream};

#[cfg(target_os = "linux")]
use std::os::fd::OwnedFd;

#[cfg(windows)]
use std::os::windows::io::OwnedHandle;
#[cfg(windows)]
use std::sync::atomic::AtomicBool;
#[cfg(windows)]
use std::sync::{mpsc, Arc};
#[cfg(windows)]
use std::thread;

/// Per-subscriber connection state held by the publisher.
struct Client {
    /// The subscriber's cursor-table slot index, received during the
    /// handshake.  Used to address this subscriber's wakeup flag so the
    /// publisher can coalesce: it fires a wakeup only when the flag is set
    /// (the subscriber announced it is about to sleep).
    cursor_idx: usize,

    /// On macOS, the wakeup channel — see [`Client::wake`].  On
    /// Linux, held only to keep the handshake socket alive (we
    /// wake via eventfd) so the peer detects publisher death via
    /// EOF on its end.  Hence the dead-code allow on Linux.
    #[cfg(unix)]
    #[cfg_attr(target_os = "linux", allow(dead_code))]
    sock: UnixStream,

    /// On Linux: the write-end of the subscriber's eventfd, received via
    /// SCM_RIGHTS. Used for low-overhead per-message wakeup.
    #[cfg(target_os = "linux")]
    efd: OwnedFd,

    /// On Windows: the (already-connected) handshake pipe instance —
    /// kept alive so the pipe stays open; we never read/write it after
    /// the handshake, but its closure signals peer death to the
    /// subscriber's `WaitForMultipleObjects`.
    #[cfg(windows)]
    _pipe: OwnedHandle,

    /// On Windows: a handle to the subscriber's semaphore, obtained by
    /// `DuplicateHandle` from the subscriber's process during the
    /// handshake.  Used for per-message wakeup via `ReleaseSemaphore`.
    #[cfg(windows)]
    sem: OwnedHandle,
}

impl Client {
    /// Send one wakeup signal. Returns false if the subscriber disconnected.
    fn wake(&self) -> bool {
        #[cfg(target_os = "linux")]
        {
            crate::waker::linux::eventfd_wake(self.efd.as_raw_fd())
        }
        #[cfg(target_os = "macos")]
        {
            send_wakeup_socket(&self.sock)
        }
        #[cfg(windows)]
        {
            use std::os::windows::io::AsRawHandle;
            crate::waker::windows::semaphore_wake(
                self.sem.as_raw_handle() as crate::waker::windows::RawWinHandle
            )
        }
    }
}

/// Low-level producer handle. Prefer [`crate::Bus::publish`] for most use-cases.
pub struct Publisher {
    ring: RingBuffer,
    #[cfg(unix)]
    listener: UnixListener,
    /// Windows: a background thread accepts named-pipe clients into this
    /// channel; `accept_clients` drains it non-blockingly on each
    /// `publish` call.  The thread holds `accept_stop` and exits when
    /// the Publisher drops (Drop wakes it by connecting a throwaway
    /// client to `accept_pipe_name`).
    /// Windows-only: wrapped in `Mutex` so `Publisher: Sync`
    /// (mpsc::Receiver is `!Sync`).  We're the sole consumer, so the
    /// Mutex is uncontended in practice — the cost is one
    /// `Mutex::lock` per `accept_clients` call, which itself runs
    /// at the publish cadence (typically µs scale, dominated by the
    /// ring write).  The publisher still owns the Receiver
    /// exclusively at runtime; the Sync impl is needed so PyO3's
    /// `allow_threads` (which takes `&self` and requires `T: Sync`)
    /// can accept a `&PyBus` containing this Publisher.
    #[cfg(windows)]
    accept_rx: std::sync::Mutex<mpsc::Receiver<io::Result<Client>>>,
    #[cfg(windows)]
    accept_stop: Arc<AtomicBool>,
    #[cfg(windows)]
    accept_thread: Option<thread::JoinHandle<()>>,
    #[cfg(windows)]
    accept_pipe_name: String,

    clients: Vec<Client>,
    backpressure: BackpressurePolicy,
    _lock: ProducerLock,
    /// Optional write-ahead log.  Present when `BusConfig::wal.enabled`
    /// was set at create-time.  On publish, the record is appended to
    /// the WAL *before* the ring write — a failed WAL append leaves
    /// the ring untouched (caller can retry).
    wal: Option<Wal>,
    /// Observability counters surfaced via [`TopicStats`].  Relaxed
    /// ordering — pure observability, ~1 ns hot-path cost.
    published_total: AtomicU64,
    full_rejected_total: AtomicU64,
    subscribers_dropped_total: AtomicU64,
    /// Wakeup syscalls actually fired (coalescing skips most under bursts).
    wakeups_sent_total: AtomicU64,
}

impl Publisher {
    /// Create a new named ring. Removes any stale socket from a previous run.
    pub fn create(name: &str, cfg: BusConfig) -> Result<Self> {
        let dir = cfg.base_dir.join(name);
        fs::create_dir_all(&dir)?;

        let ring_path = dir.join("ring.mmap");

        let lock = acquire_producer_lock(name, &dir)?;
        // create_or_reuse bumps the in-header `generation` if a compatible
        // file already exists (i.e. a prior publisher crashed) instead of
        // truncating it, which would SIGBUS any stale subscriber's mmap.
        let ring = RingBuffer::create_or_reuse(
            &ring_path,
            cfg.capacity,
            cfg.slot_size,
            cfg.max_subscribers,
        )?;

        // Open the WAL when enabled.  recover_truncate runs on every
        // segment as part of Wal::open so a power-loss-torn tail is
        // already dropped here.  When the WAL holds prior records, the
        // ring's tail (just reset to 0 by create_or_reuse) is bumped
        // forward to the WAL's next cursor so subscribers see a
        // monotonic cursor stream across publisher restarts.
        let wal = if cfg.wal.enabled {
            let w = Wal::open(&dir, cfg.wal.clone())?;
            let next = w.pending_cursor();
            if next > ring.current_tail() {
                ring.set_tail(next);
            }
            Some(w)
        } else {
            None
        };

        #[cfg(unix)]
        let listener = {
            let sock_path = dir.join("signal.sock");
            let _ = fs::remove_file(&sock_path);
            let l = UnixListener::bind(&sock_path)?;
            l.set_nonblocking(true)?;
            l
        };

        // Windows: spawn an accept thread that creates pipe instances,
        // blocks on ConnectNamedPipe, performs the handshake, and pushes
        // each finished Client into the channel.
        #[cfg(windows)]
        let (accept_rx, accept_stop, accept_thread, accept_pipe_name) =
            spawn_windows_accept_thread(name)?;

        tracing::info!(
            target: "mmbus::publisher",
            topic = name,
            ring_capacity = ring.capacity,
            slot_size = ring.slot_payload_size,
            wal_enabled = wal.is_some(),
            backpressure = ?cfg.backpressure,
            "publisher created",
        );
        Ok(Self {
            ring,
            #[cfg(unix)]
            listener,
            #[cfg(windows)]
            accept_rx: std::sync::Mutex::new(accept_rx),
            #[cfg(windows)]
            accept_stop,
            #[cfg(windows)]
            accept_thread: Some(accept_thread),
            #[cfg(windows)]
            accept_pipe_name,
            clients: Vec::new(),
            backpressure: cfg.backpressure,
            _lock: lock,
            wal,
            published_total: AtomicU64::new(0),
            full_rejected_total: AtomicU64::new(0),
            subscribers_dropped_total: AtomicU64::new(0),
            wakeups_sent_total: AtomicU64::new(0),
        })
    }

    /// Publish a message. Returns `Err(Error::Full)` if the ring is saturated
    /// and the backpressure policy is `Error`.
    ///
    /// When a WAL is enabled, the record is appended (and fsynced per the
    /// configured policy) BEFORE the ring write — a WAL append failure
    /// returns `Error::Wal` and the ring is not advanced.  Conversely a
    /// ring-full reject (`BackpressurePolicy::Error`) skips the WAL write
    /// entirely so the on-disk log never contains a record that no
    /// subscriber will ever observe via the live ring.
    pub fn publish(&mut self, data: &[u8]) -> Result<()> {
        if data.len() > self.ring.slot_payload_size as usize {
            return Err(Error::TooLarge {
                size: data.len(),
                max: self.ring.slot_payload_size as usize,
            });
        }

        self.accept_clients()?;

        if let Some(wal) = self.wal.as_ref() {
            // Pre-check ring capacity under Error backpressure so a
            // full ring doesn't leave a phantom WAL record that no
            // live subscriber will ever observe.  DropOldest never
            // rejects, so we always WAL-append + ring-publish.  This
            // check is intentionally INSIDE the WAL-enabled branch:
            // with WAL disabled, `ring.try_publish` does the same
            // is-full check and the publish path is byte-identical
            // to v0.1.0.
            if matches!(self.backpressure, BackpressurePolicy::Error)
                && self.ring.is_full()
            {
                return Err(Error::Full);
            }
            let cursor = self.ring.current_tail();
            // wal.current_ts() — cached (v2 Batched, ~2 ns atomic
            // load) or inline-computed (v0.1 + v2 None/Each, ~19 ns).
            let ts = wal.current_ts();
            wal.append(cursor, ts, data)?;
        }

        let published = match self.backpressure {
            BackpressurePolicy::Error => self.ring.try_publish(data),
            BackpressurePolicy::DropOldest => self.ring.publish_drop_oldest(data),
        };
        if !published {
            self.full_rejected_total
                .fetch_add(1, AtomicOrdering::Relaxed);
            return Err(Error::Full);
        }
        self.published_total.fetch_add(1, AtomicOrdering::Relaxed);

        self.broadcast_wakeup();

        Ok(())
    }

    /// Publish a batch of records and wake every subscriber ONCE at
    /// the end (one wakeup syscall per subscriber regardless of N).
    ///
    /// Returns the number of records successfully written.  Under
    /// `BackpressurePolicy::Error` a full ring stops the loop early
    /// — the caller compares the returned count to `items.len()` to
    /// know if any tail wasn't published.  Under
    /// `BackpressurePolicy::DropOldest` every item lands in the ring
    /// (possibly overwriting older messages), so the return value
    /// equals the input length.
    ///
    /// Why batched wakeups are safe:
    /// `Subscriber::receive_into` does a `try_receive` BEFORE calling
    /// `wait_wakeup`, so any subscriber sitting awake-and-draining
    /// reads all N records without needing per-message wakes.  A
    /// sleeping subscriber needs exactly ONE wake to start draining
    /// the burst.  The wake count never has to match the message
    /// count.
    pub fn publish_many<I, B>(&mut self, items: I) -> Result<usize>
    where
        I: IntoIterator<Item = B>,
        B: AsRef<[u8]>,
    {
        self.accept_clients()?;

        let slot_max = self.ring.slot_payload_size as usize;
        let wal_enabled = self.wal.is_some();
        let drop_oldest = matches!(self.backpressure, BackpressurePolicy::DropOldest);
        let mut count = 0usize;

        for item in items {
            let data = item.as_ref();
            if data.len() > slot_max {
                if count > 0 {
                    self.broadcast_wakeup();
                }
                return Err(Error::TooLarge { size: data.len(), max: slot_max });
            }

            if let Some(wal) = self.wal.as_ref() {
                if !drop_oldest && self.ring.is_full() {
                    break; // partial publish; caller sees `count < items.len()`
                }
                let cursor = self.ring.current_tail();
                let ts = wal.current_ts();
                if let Err(e) = wal.append(cursor, ts, data) {
                    if count > 0 {
                        self.broadcast_wakeup();
                    }
                    return Err(e.into());
                }
            }

            let ok = if drop_oldest {
                self.ring.publish_drop_oldest(data)
            } else {
                self.ring.try_publish(data)
            };
            if !ok {
                // Only Error policy hits here (DropOldest always succeeds);
                // the WAL pre-check above usually catches this, but a racing
                // subscriber cursor change can flip is_full between check
                // and try_publish.  Either way: partial publish.
                break;
            }
            count += 1;
            // Hint the loop has nothing to do with WAL when off — keeps
            // the no-WAL hot path tight.
            let _ = wal_enabled;
        }

        if count > 0 {
            self.published_total
                .fetch_add(count as u64, AtomicOrdering::Relaxed);
            self.broadcast_wakeup();
        }
        Ok(count)
    }

    /// Fire one wakeup per connected subscriber; drop any whose peer
    /// has closed.  Used by both `publish` and `publish_many`.
    fn broadcast_wakeup(&mut self) {
        if self.clients.is_empty() {
            return; // keeps the no-subscriber publish path free of the fence
        }
        // StoreLoad barrier between the ring's tail store (above, Release in
        // `write_slot`) and the per-subscriber flag loads below.  Pairs with
        // the subscriber's `set_wakeflag` + SeqCst fence in `wait_readable` /
        // `arm_wakeup`: in the SeqCst total order either the subscriber sees
        // our new tail (and doesn't sleep) or we see its flag (and wake it).
        std::sync::atomic::fence(AtomicOrdering::SeqCst);
        let mut i = 0;
        while i < self.clients.len() {
            let idx = self.clients[i].cursor_idx;

            // Reap a cleanly-disconnected subscriber directly: a released
            // (UNCLAIMED) cursor slot proves the subscriber's `Drop` ran, so
            // the client is gone regardless of the wakeup primitive.  We must
            // NOT rely on `wake()` failing here — on Linux an `eventfd` write
            // succeeds even when the reader has closed (the publisher holds
            // its own dup), so the wake never reports the death.  (macOS's
            // socket `send` does fail, but we want platform-uniform reaping.)
            if self.ring.cursor_is_unclaimed(idx) {
                self.clients.swap_remove(i);
                self.subscribers_dropped_total
                    .fetch_add(1, AtomicOrdering::Relaxed);
                tracing::info!(
                    target: "mmbus::publisher",
                    remaining_clients = self.clients.len(),
                    "subscriber dropped (cursor released; clean disconnect)",
                );
                continue; // swap_remove put a new client at i — re-examine it
            }

            // Live subscriber: wake only if it announced it is about to sleep
            // (flag set).  One that is actively draining (claimed cursor, no
            // flag) is skipped — it sees the new tail on its next ring read
            // without a syscall.  This is the coalescing win.
            if !self.ring.take_wakeflag(idx) {
                i += 1;
                continue;
            }
            self.wakeups_sent_total.fetch_add(1, AtomicOrdering::Relaxed);
            if self.clients[i].wake() {
                i += 1;
            } else {
                // Abrupt death (peer closed without releasing its cursor):
                // detected on macOS by the socket send failing.
                self.clients.swap_remove(i);
                self.subscribers_dropped_total
                    .fetch_add(1, AtomicOrdering::Relaxed);
                tracing::info!(
                    target: "mmbus::publisher",
                    remaining_clients = self.clients.len(),
                    "subscriber dropped (wake failed; peer closed)",
                );
            }
        }
    }

    /// Block until at least `min_count` subscribers have connected, or timeout.
    pub fn wait_for_subscribers(&mut self, min_count: usize, timeout: Duration) -> Result<()> {
        let deadline = Instant::now() + timeout;
        loop {
            self.accept_clients()?;
            if self.clients.len() >= min_count {
                return Ok(());
            }
            if Instant::now() >= deadline {
                return Err(Error::Timeout("waiting for subscribers".to_owned()));
            }
            std::thread::sleep(Duration::from_millis(1));
        }
    }

    /// Snapshot of ring and socket stats for this topic.
    pub fn stats(&self) -> TopicStats {
        TopicStats {
            ring: self.ring.stats(),
            connected_sockets: self.clients.len(),
            wal: self.wal.as_ref().map(|w| w.stats()),
            published_total: self.published_total.load(AtomicOrdering::Relaxed),
            full_rejected_total: self
                .full_rejected_total
                .load(AtomicOrdering::Relaxed),
            subscribers_dropped_total: self
                .subscribers_dropped_total
                .load(AtomicOrdering::Relaxed),
            wakeups_sent_total: self.wakeups_sent_total.load(AtomicOrdering::Relaxed),
        }
    }

    /// `(cursor_idx, lag)` pairs for active subscribers whose lag is
    /// `>= threshold` messages.  Empty Vec when nothing is lagging.
    pub fn slow_subscribers(&self, threshold: u64) -> Vec<(usize, u64)> {
        self.ring
            .lags_with_idx()
            .into_iter()
            .filter(|(_, lag)| *lag >= threshold)
            .collect()
    }

    pub fn slot_size(&self) -> u32 {
        self.ring.slot_payload_size
    }

    pub fn connected_subscribers(&self) -> usize {
        self.clients.len()
    }

    /// Drain pending new connections and promote them to clients.
    ///
    /// * Unix: non-blocking `accept()` loop on the UnixListener; Linux
    ///   also receives the subscriber's eventfd via SCM_RIGHTS here.
    /// * Windows: non-blocking drain of the accept-thread's channel.
    fn accept_clients(&mut self) -> Result<()> {
        #[cfg(unix)]
        loop {
            match self.listener.accept() {
                Ok((mut sock, _)) => {
                    sock.set_nonblocking(false)?;
                    suppress_sigpipe(&sock);

                    // Linux: the eventfd + cursor_idx arrive together via
                    // SCM_RIGHTS.  macOS: cursor_idx is the first 4 bytes
                    // written on the socket, ahead of the 1-byte wakeups.
                    #[cfg(target_os = "linux")]
                    let (efd, cursor_idx) = {
                        let (fd, idx) = crate::waker::linux::recv_fd(&sock)?;
                        (fd, idx as usize)
                    };
                    #[cfg(target_os = "macos")]
                    let cursor_idx = {
                        use std::io::Read;
                        let mut idx_bytes = [0u8; 4];
                        sock.read_exact(&mut idx_bytes)?;
                        u32::from_le_bytes(idx_bytes) as usize
                    };

                    self.clients.push(Client {
                        cursor_idx,
                        sock,
                        #[cfg(target_os = "linux")]
                        efd,
                    });
                }
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => break,
                Err(e) => return Err(e.into()),
            }
        }

        #[cfg(windows)]
        loop {
            // Mutex is uncontended — Publisher is single-owner.
            let rx = self.accept_rx.lock().unwrap_or_else(|e| e.into_inner());
            match rx.try_recv() {
                Ok(Ok(client)) => self.clients.push(client),
                Ok(Err(e)) => return Err(e.into()),
                Err(mpsc::TryRecvError::Empty) => break,
                Err(mpsc::TryRecvError::Disconnected) => break, // thread exited
            }
        }

        Ok(())
    }
}

#[cfg(windows)]
impl Drop for Publisher {
    fn drop(&mut self) {
        self.accept_stop.store(true, AtomicOrdering::Release);
        // Connect a transient pipe to unblock the accept thread's
        // `ConnectNamedPipe` call so it can observe the stop flag.
        // Best-effort — if it fails the thread will eventually exit when
        // the pipe is GC'd by the OS.
        let _ = crate::waker::windows::connect_pipe(&self.accept_pipe_name);
        if let Some(h) = self.accept_thread.take() {
            let _ = h.join();
        }
    }
}

// ── Platform helpers (publisher-side) ─────────────────────────────────────────

/// Set SO_NOSIGPIPE (macOS) so writing to a closed socket returns EPIPE
/// instead of raising SIGPIPE. On Linux MSG_NOSIGNAL is used per-send.
#[cfg(unix)]
fn suppress_sigpipe(_stream: &UnixStream) {
    #[cfg(target_os = "macos")]
    // SAFETY: setsockopt accepts a const-int option value via *const void;
    // we pass a stack-local int that lives for the call.  The fd comes
    // from a UnixStream we hold a reference to, so it's open.
    unsafe {
        let val: libc::c_int = 1;
        libc::setsockopt(
            _stream.as_raw_fd(),
            libc::SOL_SOCKET,
            libc::SO_NOSIGPIPE,
            &val as *const _ as *const libc::c_void,
            std::mem::size_of::<libc::c_int>() as libc::socklen_t,
        );
    }
}

/// Send the 1-byte wakeup signal over a Unix socket (macOS path).
#[cfg(target_os = "macos")]
fn send_wakeup_socket(stream: &UnixStream) -> bool {
    let byte: u8 = 0x01;
    // SAFETY: stream is a borrowed UnixStream (fd is open for the call);
    // &byte points to one stack byte; we tell libc::send length 1.
    unsafe {
        libc::send(stream.as_raw_fd(), &byte as *const u8 as *const libc::c_void, 1, 0) == 1
    }
}

#[cfg(windows)]
#[allow(clippy::type_complexity)]
fn spawn_windows_accept_thread(
    name: &str,
) -> Result<(
    mpsc::Receiver<io::Result<Client>>,
    Arc<AtomicBool>,
    thread::JoinHandle<()>,
    String,
)> {
    use std::os::windows::io::AsRawHandle;
    let pipe_name = crate::waker::windows::pipe_name(name);
    let (tx, rx) = mpsc::channel::<io::Result<Client>>();
    let stop = Arc::new(AtomicBool::new(false));
    let stop_clone = stop.clone();
    let pipe_name_clone = pipe_name.clone();
    let handle = thread::spawn(move || loop {
        if stop_clone.load(AtomicOrdering::Acquire) {
            break;
        }
        // Create a fresh pipe instance; ConnectNamedPipe blocks until a
        // client connects (or until our Drop wakes us by connecting a
        // throwaway pipe so we observe the stop flag).
        let pipe = match crate::waker::windows::create_pipe_instance(&pipe_name_clone) {
            Ok(h) => h,
            Err(e) => {
                let _ = tx.send(Err(e));
                break;
            }
        };
        if let Err(e) = crate::waker::windows::accept_pipe(
            pipe.as_raw_handle() as crate::waker::windows::RawWinHandle,
        ) {
            let _ = tx.send(Err(e));
            continue;
        }
        if stop_clone.load(AtomicOrdering::Acquire) {
            // Throwaway connection from Drop — discard pipe and exit.
            break;
        }
        // Read the subscriber's handshake + dup its semaphore + cursor_idx.
        let (sem, cursor_idx) = match crate::waker::windows::recv_handshake_and_dup(
            pipe.as_raw_handle() as crate::waker::windows::RawWinHandle,
        ) {
            Ok((h, idx)) => (h, idx as usize),
            Err(e) => {
                let _ = tx.send(Err(e));
                continue;
            }
        };
        if tx
            .send(Ok(Client { cursor_idx, _pipe: pipe, sem }))
            .is_err()
        {
            // Receiver dropped (Publisher gone) — exit.
            break;
        }
    });
    Ok((rx, stop, handle, pipe_name))
}
