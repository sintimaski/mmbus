use crate::ring::{RingBuffer, RingStats};
use std::collections::{HashMap, HashSet};
use std::fs;
use std::io::{self, Read};
use std::os::unix::io::{AsRawFd, RawFd};
use std::os::unix::net::{UnixListener, UnixStream};
use std::path::{Path, PathBuf};
use std::sync::{LazyLock, Mutex};
use std::time::{Duration, Instant};
use thiserror::Error;

#[cfg(target_os = "linux")]
use std::os::fd::{FromRawFd, OwnedFd};

// ── Error ─────────────────────────────────────────────────────────────────────

#[derive(Debug, Error)]
pub enum Error {
    #[error("I/O error: {0}")]
    Io(#[from] io::Error),
    #[error("ring buffer full")]
    Full,
    #[error("message too large: {size} bytes, max is {max}")]
    TooLarge { size: usize, max: usize },
    #[error("connection timeout waiting for '{0}'")]
    Timeout(String),
    #[error("too many subscribers: limit is {0}")]
    TooManySubscribers(u32),
    #[error("a publisher is already running for topic '{0}'")]
    AlreadyPublishing(String),
}

pub type Result<T> = std::result::Result<T, Error>;

// ── BackpressurePolicy ────────────────────────────────────────────────────────

/// What the publisher does when the ring buffer is full.
#[derive(Clone, Debug, Default)]
pub enum BackpressurePolicy {
    /// Return `Err(Error::Full)` so the caller decides what to do.
    #[default]
    Error,
    /// Silently drop the oldest unread slot for the slowest subscriber and
    /// keep writing. The subscriber detects the skip on its next read.
    DropOldest,
}

// ── Producer lock ─────────────────────────────────────────────────────────────

// Tracks active publisher paths within this process. Required because BSD/macOS
// flock(2) semantics are per-process: all fds opened by the same process share
// one lock record, so a same-process duplicate would bypass flock alone.
static IN_PROCESS_LOCKS: LazyLock<Mutex<HashSet<PathBuf>>> =
    LazyLock::new(|| Mutex::new(HashSet::new()));

struct ProducerLock {
    path: PathBuf,
    _file: fs::File, // keeps the OS-level flock alive until dropped
}

impl Drop for ProducerLock {
    fn drop(&mut self) {
        // Remove in-process entry; _file drop releases the OS flock.
        let mut set = IN_PROCESS_LOCKS.lock().unwrap_or_else(|e| e.into_inner());
        set.remove(&self.path);
    }
}

fn acquire_producer_lock(name: &str, dir: &Path) -> Result<ProducerLock> {
    let path = dir.join("producer.lock");

    // In-process check must come first on macOS.
    {
        let mut set = IN_PROCESS_LOCKS.lock().unwrap_or_else(|e| e.into_inner());
        if !set.insert(path.clone()) {
            return Err(Error::AlreadyPublishing(name.to_owned()));
        }
    }

    let file = match fs::OpenOptions::new().create(true).write(true).open(&path) {
        Ok(f) => f,
        Err(e) => {
            IN_PROCESS_LOCKS.lock().unwrap_or_else(|e| e.into_inner()).remove(&path);
            return Err(Error::Io(e));
        }
    };

    // Cross-process exclusive advisory lock (non-blocking).
    if unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) } != 0 {
        IN_PROCESS_LOCKS.lock().unwrap_or_else(|e| e.into_inner()).remove(&path);
        return Err(Error::AlreadyPublishing(name.to_owned()));
    }

    Ok(ProducerLock { path, _file: file })
}

// ── Config ────────────────────────────────────────────────────────────────────

/// Configuration for a [`Bus`] or standalone [`Publisher`]/[`Subscriber`].
#[derive(Clone, Debug)]
pub struct BusConfig {
    /// Max payload bytes per message (default: 64 KiB).
    pub slot_size: u32,
    /// Ring buffer slot count (default: 256).
    pub capacity: u32,
    /// Root directory for bus files (default: `/tmp/mmbus`).
    pub base_dir: PathBuf,
    /// Maximum simultaneous subscribers per topic (default: 16).
    pub max_subscribers: u32,
    /// What to do when the ring is full (default: `BackpressurePolicy::Error`).
    pub backpressure: BackpressurePolicy,
}

impl Default for BusConfig {
    fn default() -> Self {
        Self {
            slot_size: 64 * 1024,
            capacity: 256,
            base_dir: PathBuf::from("/tmp/mmbus"),
            max_subscribers: 16,
            backpressure: BackpressurePolicy::Error,
        }
    }
}

// ── Bus ───────────────────────────────────────────────────────────────────────

/// Named pub-sub namespace. Topics are independent channels within the
/// namespace; each topic gets its own ring-buffer file on disk.
///
/// # Example
///
/// ```rust,no_run
/// use mmbus::Bus;
///
/// // Publisher process
/// let mut bus = Bus::new("my-app");
/// bus.publish("sensors", b"hello").unwrap();
///
/// // Subscriber process
/// let bus = Bus::new("my-app");
/// for msg in bus.subscribe("sensors").unwrap() {
///     println!("{:?}", msg.unwrap());
/// }
/// ```
pub struct Bus {
    name: String,
    config: BusConfig,
    publishers: HashMap<String, Publisher>,
}

impl Bus {
    /// Create or connect to a named bus with default config.
    pub fn new(name: impl Into<String>) -> Self {
        Self::with_config(name, BusConfig::default())
    }

    /// Create or connect to a named bus with custom config.
    pub fn with_config(name: impl Into<String>, config: BusConfig) -> Self {
        Self { name: name.into(), config, publishers: HashMap::new() }
    }

    /// Publish `data` to `topic`. Publisher is created on the first call and
    /// cached. Returns `Err(Error::Full)` when the ring is saturated.
    pub fn publish(&mut self, topic: &str, data: &[u8]) -> Result<()> {
        if !self.publishers.contains_key(topic) {
            let pub_ = Publisher::create(topic, self.topic_config(topic))?;
            self.publishers.insert(topic.to_owned(), pub_);
        }
        self.publishers.get_mut(topic).unwrap().publish(data)
    }

    /// Subscribe to `topic`, waiting up to 30 seconds for the publisher.
    pub fn subscribe(&self, topic: &str) -> Result<Subscription> {
        self.subscribe_timeout(topic, Duration::from_secs(30))
    }

    /// Subscribe to `topic` with a custom connection timeout.
    pub fn subscribe_timeout(&self, topic: &str, timeout: Duration) -> Result<Subscription> {
        let sub = Subscriber::connect(topic, &self.topic_config(topic), timeout)?;
        Ok(Subscription { sub })
    }

    /// Ensure the publisher for `topic` exists and block until at least `n`
    /// subscribers have connected, or until `timeout` expires.
    pub fn wait_for_subscribers(
        &mut self,
        topic: &str,
        n: usize,
        timeout: Duration,
    ) -> Result<()> {
        if !self.publishers.contains_key(topic) {
            let pub_ = Publisher::create(topic, self.topic_config(topic))?;
            self.publishers.insert(topic.to_owned(), pub_);
        }
        self.publishers.get_mut(topic).unwrap().wait_for_subscribers(n, timeout)
    }

    /// Snapshot of ring and socket stats for `topic`.
    /// Returns `None` if no publisher has been created for `topic` in this Bus.
    pub fn stats(&self, topic: &str) -> Option<TopicStats> {
        self.publishers.get(topic).map(|p| p.stats())
    }

    fn topic_config(&self, _topic: &str) -> BusConfig {
        BusConfig { base_dir: self.config.base_dir.join(&self.name), ..self.config.clone() }
    }
}

// ── Subscription ──────────────────────────────────────────────────────────────

/// A live subscription to a topic. Returned by [`Bus::subscribe`].
/// Implements `Iterator<Item = Result<Vec<u8>>>` for ergonomic loops.
pub struct Subscription {
    sub: Subscriber,
}

impl Subscription {
    /// Block until the next message arrives.
    pub fn recv(&mut self) -> Result<Vec<u8>> {
        self.sub.receive()
    }

    /// Block with a timeout. Returns `Ok(None)` if no message arrives in time.
    pub fn recv_timeout(&mut self, timeout: Duration) -> Result<Option<Vec<u8>>> {
        self.sub.receive_timeout(timeout)
    }

    /// Non-blocking poll. Returns `None` immediately if no message is ready.
    pub fn try_recv(&mut self) -> Option<Vec<u8>> {
        self.sub.try_receive()
    }

    /// How many messages this subscriber is behind the producer.
    pub fn lag(&self) -> u64 {
        self.sub.lag()
    }

    /// Current read cursor position.
    pub fn cursor(&self) -> u64 {
        self.sub.cursor()
    }

    /// The underlying wakeup file descriptor.
    ///
    /// On Linux this is the subscriber's `eventfd(2)`; on macOS it is the Unix
    /// domain socket.  Either way the fd becomes readable when a new message
    /// is available, so callers can pass it to `asyncio.get_event_loop().add_reader()`
    /// or any `epoll`/`kqueue`-based poller for truly non-blocking receive.
    pub fn fileno(&self) -> RawFd {
        self.sub.fileno()
    }
}

impl Iterator for Subscription {
    type Item = Result<Vec<u8>>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.sub.receive() {
            Ok(msg) => Some(Ok(msg)),
            Err(Error::Io(e))
                if e.kind() == io::ErrorKind::UnexpectedEof
                    || e.kind() == io::ErrorKind::ConnectionReset =>
            {
                None // publisher disconnected
            }
            Err(e) => Some(Err(e)),
        }
    }
}

// ── TopicStats ────────────────────────────────────────────────────────────────

/// Snapshot of a topic's ring-buffer and socket state.
#[derive(Debug, Clone)]
pub struct TopicStats {
    /// Ring stats (tail position, active subscriber cursors, per-cursor lags).
    pub ring: RingStats,
    /// Number of subscriber sockets currently accepted by the publisher.
    /// May lag slightly behind `ring.active_subscribers` (cursor is claimed
    /// before the socket handshake completes).
    pub connected_sockets: usize,
}

// ── Publisher ─────────────────────────────────────────────────────────────────

/// Per-subscriber connection state held by the publisher.
struct Client {
    sock: UnixStream,
    /// On Linux: the write-end of the subscriber's eventfd, received via
    /// SCM_RIGHTS.  Used for low-overhead per-message wakeup.
    #[cfg(target_os = "linux")]
    efd: OwnedFd,
}

impl Client {
    /// Send one wakeup signal to this subscriber.
    /// Returns false if the subscriber has disconnected.
    fn wake(&self) -> bool {
        #[cfg(target_os = "linux")]
        {
            crate::waker::linux::eventfd_wake(self.efd.as_raw_fd())
        }
        #[cfg(not(target_os = "linux"))]
        {
            send_wakeup_socket(&self.sock)
        }
    }
}

/// Low-level producer handle. Prefer [`Bus::publish`] for most use-cases.
pub struct Publisher {
    ring: RingBuffer,
    listener: UnixListener,
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
        let sock_path = dir.join("signal.sock");
        let _ = fs::remove_file(&sock_path);

        let lock = acquire_producer_lock(name, &dir)?;
        let ring =
            RingBuffer::create(&ring_path, cfg.capacity, cfg.slot_size, cfg.max_subscribers)?;
        let listener = UnixListener::bind(&sock_path)?;
        listener.set_nonblocking(true)?;

        Ok(Self {
            ring,
            listener,
            clients: Vec::new(),
            backpressure: cfg.backpressure,
            _lock: lock,
        })
    }

    /// Publish a message. Returns `Err(Error::Full)` if the ring is saturated
    /// and backpressure policy is `Error`.
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

    pub fn slot_size(&self) -> u32 {
        self.ring.slot_payload_size
    }

    pub fn connected_subscribers(&self) -> usize {
        self.clients.len()
    }

    /// Drain the non-blocking listener and promote new connections to clients.
    fn accept_clients(&mut self) -> Result<()> {
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
        Ok(())
    }
}

// ── Subscriber ────────────────────────────────────────────────────────────────

/// Low-level consumer handle. Prefer [`Bus::subscribe`] for most use-cases.
pub struct Subscriber {
    ring: RingBuffer,
    sock: UnixStream,
    /// On Linux: the subscriber-owned read-end of the eventfd.
    #[cfg(target_os = "linux")]
    efd: OwnedFd,
    cursor: u64,
    cursor_idx: usize,
}

impl Drop for Subscriber {
    fn drop(&mut self) {
        self.ring.release_cursor(self.cursor_idx);
    }
}

impl Subscriber {
    /// Connect to a named bus, retrying until `timeout` expires.
    ///
    /// The cursor is claimed in the ring *before* completing the socket
    /// handshake, so that by the time `Publisher::wait_for_subscribers` returns,
    /// the cursor is already visible to the producer's backpressure check.
    ///
    /// On Linux an `eventfd(2)` is created before connecting and its write-end
    /// is passed to the publisher via `SCM_RIGHTS` over the handshake socket.
    pub fn connect(name: &str, cfg: &BusConfig, timeout: Duration) -> Result<Self> {
        let dir = cfg.base_dir.join(name);
        let ring_path = dir.join("ring.mmap");
        let sock_path = dir.join("signal.sock");
        let deadline = Instant::now() + timeout;

        let ring = loop {
            match RingBuffer::open(&ring_path) {
                Ok(r) => break r,
                Err(_) if Instant::now() < deadline => {
                    std::thread::sleep(Duration::from_millis(5));
                }
                Err(_) => return Err(Error::Timeout(name.to_owned())),
            }
        };

        // Claim cursor before socket connect so the producer sees our position
        // as soon as it accepts the connection.
        let cursor = ring.current_tail();
        let cursor_idx = ring
            .claim_cursor(cursor)
            .ok_or_else(|| Error::TooManySubscribers(ring.max_subscribers))?;

        // On Linux: create the eventfd now so we can pass it during the
        // socket handshake.
        #[cfg(target_os = "linux")]
        let efd = crate::waker::linux::create_eventfd().map_err(|e| {
            ring.release_cursor(cursor_idx);
            Error::Io(e)
        })?;

        let sock = loop {
            match UnixStream::connect(&sock_path) {
                Ok(s) => break s,
                Err(_) if Instant::now() < deadline => {
                    std::thread::sleep(Duration::from_millis(5));
                }
                Err(_) => {
                    ring.release_cursor(cursor_idx);
                    return Err(Error::Timeout(name.to_owned()));
                }
            }
        };

        // Pass the eventfd write-end to the publisher so it can wake us.
        #[cfg(target_os = "linux")]
        if let Err(e) = crate::waker::linux::send_fd(&sock, efd.as_raw_fd()) {
            ring.release_cursor(cursor_idx);
            return Err(Error::Io(e));
        }

        Ok(Self {
            ring,
            sock,
            #[cfg(target_os = "linux")]
            efd,
            cursor,
            cursor_idx,
        })
    }

    /// Block until the next message arrives.
    pub fn receive(&mut self) -> Result<Vec<u8>> {
        let mut out = Vec::new();
        loop {
            self.wait_wakeup(-1)?; // -1 = wait forever
            if let Some(new_cursor) =
                self.ring.try_receive(self.cursor_idx, self.cursor, &mut out)
            {
                self.cursor = new_cursor;
                return Ok(out);
            }
            // Wakeup consumed but ring slot not yet visible (e.g. DropOldest
            // race).  Loop for the next wakeup.
        }
    }

    /// Block with a timeout. Returns `Ok(None)` if the timeout elapses.
    pub fn receive_timeout(&mut self, timeout: Duration) -> Result<Option<Vec<u8>>> {
        let timeout_ms = timeout.as_millis().min(i32::MAX as u128) as i32;
        let mut out = Vec::new();
        loop {
            match self.wait_wakeup(timeout_ms) {
                Ok(()) => {}
                Err(e)
                    if e.kind() == io::ErrorKind::WouldBlock
                        || e.kind() == io::ErrorKind::TimedOut =>
                {
                    return Ok(None);
                }
                Err(e) => return Err(e.into()),
            }
            if let Some(new_cursor) =
                self.ring.try_receive(self.cursor_idx, self.cursor, &mut out)
            {
                self.cursor = new_cursor;
                return Ok(Some(out));
            }
        }
    }

    /// Non-blocking poll. Returns `None` if no message is ready.
    pub fn try_receive(&mut self) -> Option<Vec<u8>> {
        let mut out = Vec::new();
        if let Some(new_cursor) = self.ring.try_receive(self.cursor_idx, self.cursor, &mut out) {
            self.cursor = new_cursor;
            Some(out)
        } else {
            None
        }
    }

    /// How many messages are currently ahead of this subscriber's read position.
    pub fn lag(&self) -> u64 {
        self.ring.current_tail().saturating_sub(self.cursor)
    }

    pub fn cursor(&self) -> u64 {
        self.cursor
    }

    /// The underlying wakeup fd: eventfd on Linux, Unix socket on macOS.
    /// Becomes readable when at least one message is available.
    pub fn fileno(&self) -> RawFd {
        #[cfg(target_os = "linux")]
        {
            self.efd.as_raw_fd()
        }
        #[cfg(not(target_os = "linux"))]
        {
            self.sock.as_raw_fd()
        }
    }

    // ── Internal wakeup helpers ───────────────────────────────────────────────

    /// Wait for one wakeup signal.  `timeout_ms = -1` blocks indefinitely.
    /// On Linux: `poll(2)` on eventfd + socket (disconnect detection).
    /// On macOS: `read_exact(1)` on the socket (with optional read timeout).
    fn wait_wakeup(&mut self, timeout_ms: i32) -> io::Result<()> {
        #[cfg(target_os = "linux")]
        {
            crate::waker::linux::poll_wakeup(
                self.efd.as_raw_fd(),
                self.sock.as_raw_fd(),
                timeout_ms,
            )?;
            // Drain the eventfd counter (reset to 0) so the next poll blocks.
            crate::waker::linux::eventfd_drain(self.efd.as_raw_fd())?;
            Ok(())
        }
        #[cfg(not(target_os = "linux"))]
        {
            if timeout_ms < 0 {
                let mut b = [0u8; 1];
                self.sock.read_exact(&mut b)
            } else {
                let t = Duration::from_millis(timeout_ms as u64);
                self.sock.set_read_timeout(Some(t))?;
                let mut b = [0u8; 1];
                let r = self.sock.read_exact(&mut b);
                let _ = self.sock.set_read_timeout(None);
                r
            }
        }
    }
}

// ── Platform helpers ──────────────────────────────────────────────────────────

/// Set SO_NOSIGPIPE (macOS) so writing to a closed socket returns EPIPE
/// instead of raising SIGPIPE. On Linux MSG_NOSIGNAL is used per-send.
fn suppress_sigpipe(_stream: &UnixStream) {
    #[cfg(target_os = "macos")]
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
#[cfg(not(target_os = "linux"))]
fn send_wakeup_socket(stream: &UnixStream) -> bool {
    let byte: u8 = 0x01;
    unsafe {
        libc::send(stream.as_raw_fd(), &byte as *const u8 as *const libc::c_void, 1, 0) == 1
    }
}
