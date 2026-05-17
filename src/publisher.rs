use crate::config::{BackpressurePolicy, BusConfig};
use crate::error::{Error, Result};
use crate::producer_lock::{acquire_producer_lock, ProducerLock};
use crate::ring::RingBuffer;
use crate::stats::TopicStats;
use std::fs;
use std::io;
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
use std::sync::atomic::{AtomicBool, Ordering as AtomicOrdering};
#[cfg(windows)]
use std::sync::{mpsc, Arc};
#[cfg(windows)]
use std::thread;

/// Per-subscriber connection state held by the publisher.
struct Client {
    #[cfg(unix)]
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
    #[cfg(windows)]
    accept_rx: mpsc::Receiver<io::Result<Client>>,
    #[cfg(windows)]
    accept_stop: Arc<AtomicBool>,
    #[cfg(windows)]
    accept_thread: Option<thread::JoinHandle<()>>,
    #[cfg(windows)]
    accept_pipe_name: String,

    clients: Vec<Client>,
    backpressure: BackpressurePolicy,
    _lock: ProducerLock,
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

        Ok(Self {
            ring,
            #[cfg(unix)]
            listener,
            #[cfg(windows)]
            accept_rx,
            #[cfg(windows)]
            accept_stop,
            #[cfg(windows)]
            accept_thread: Some(accept_thread),
            #[cfg(windows)]
            accept_pipe_name,
            clients: Vec::new(),
            backpressure: cfg.backpressure,
            _lock: lock,
        })
    }

    /// Publish a message. Returns `Err(Error::Full)` if the ring is saturated
    /// and the backpressure policy is `Error`.
    pub fn publish(&mut self, data: &[u8]) -> Result<()> {
        if data.len() > self.ring.slot_payload_size as usize {
            return Err(Error::TooLarge {
                size: data.len(),
                max: self.ring.slot_payload_size as usize,
            });
        }

        self.accept_clients()?;

        let published = match self.backpressure {
            BackpressurePolicy::Error => self.ring.try_publish(data),
            BackpressurePolicy::DropOldest => self.ring.publish_drop_oldest(data),
        };
        if !published {
            return Err(Error::Full);
        }

        // Broadcast one wakeup per connected subscriber; drop disconnected ones.
        let mut i = 0;
        while i < self.clients.len() {
            if self.clients[i].wake() {
                i += 1;
            } else {
                self.clients.swap_remove(i);
            }
        }

        Ok(())
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
        TopicStats { ring: self.ring.stats(), connected_sockets: self.clients.len() }
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
                Ok((sock, _)) => {
                    sock.set_nonblocking(false)?;
                    suppress_sigpipe(&sock);

                    #[cfg(target_os = "linux")]
                    let efd = crate::waker::linux::recv_fd(&sock)?;

                    self.clients.push(Client {
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
            match self.accept_rx.try_recv() {
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
        // Read the subscriber's handshake + dup its semaphore.
        let sem = match crate::waker::windows::recv_handshake_and_dup(
            pipe.as_raw_handle() as crate::waker::windows::RawWinHandle,
        ) {
            Ok(h) => h,
            Err(e) => {
                let _ = tx.send(Err(e));
                continue;
            }
        };
        if tx
            .send(Ok(Client { _pipe: pipe, sem }))
            .is_err()
        {
            // Receiver dropped (Publisher gone) — exit.
            break;
        }
    });
    Ok((rx, stop, handle, pipe_name))
}
