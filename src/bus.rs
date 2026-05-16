use crate::ring::RingBuffer;
use std::collections::HashMap;
use std::fs;
use std::io::{self, Read, Write};
use std::os::unix::net::{UnixListener, UnixStream};
use std::path::PathBuf;
use std::time::{Duration, Instant};
use thiserror::Error;

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
}

pub type Result<T> = std::result::Result<T, Error>;

// ── Config ────────────────────────────────────────────────────────────────────

/// Configuration for a Bus or individual Publisher/Subscriber.
///
/// All fields have sane defaults; construct with `BusConfig::default()` and
/// override only what you need.
#[derive(Clone, Debug)]
pub struct BusConfig {
    /// Max payload bytes per message. Default: 64 KiB.
    pub slot_size: u32,
    /// Number of ring buffer slots. Default: 256.
    pub capacity: u32,
    /// Root directory for bus files. Default: `/tmp/mmbus`.
    pub base_dir: PathBuf,
}

impl Default for BusConfig {
    fn default() -> Self {
        Self {
            slot_size: 64 * 1024,
            capacity: 256,
            base_dir: PathBuf::from("/tmp/mmbus"),
        }
    }
}

// ── Bus ───────────────────────────────────────────────────────────────────────

/// The single entry point for mmbus.
///
/// A `Bus` is a named namespace. Topics are independent channels within the
/// namespace; each topic gets its own ring buffer file. Publishers and
/// subscribers within the same namespace find each other automatically.
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
        Self {
            name: name.into(),
            config,
            publishers: HashMap::new(),
        }
    }

    /// Publish `data` to `topic`.
    ///
    /// The publisher for this topic is created on the first call and cached.
    /// Returns `Err(Error::Full)` if the ring buffer is saturated.
    pub fn publish(&mut self, topic: &str, data: &[u8]) -> Result<()> {
        if !self.publishers.contains_key(topic) {
            let pub_ = Publisher::create(topic, self.topic_config(topic))?;
            self.publishers.insert(topic.to_owned(), pub_);
        }
        self.publishers.get_mut(topic).unwrap().publish(data)
    }

    /// Subscribe to `topic`, waiting up to 30 seconds for the publisher to start.
    pub fn subscribe(&self, topic: &str) -> Result<Subscription> {
        self.subscribe_timeout(topic, Duration::from_secs(30))
    }

    /// Subscribe to `topic` with a custom connection timeout.
    pub fn subscribe_timeout(&self, topic: &str, timeout: Duration) -> Result<Subscription> {
        let cfg = self.topic_config(topic);
        let sub = Subscriber::connect(topic, &cfg, timeout)?;
        Ok(Subscription { sub })
    }

    /// Ensure the publisher for `topic` exists and block until at least `n`
    /// subscribers have connected, or until `timeout` expires.
    ///
    /// Use this before the first `publish` call when you need to guarantee at
    /// least one subscriber is ready to receive. In Python/JS this maps to
    /// `bus.wait_for_subscribers("topic", n=1)`.
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
        self.publishers
            .get_mut(topic)
            .unwrap()
            .wait_for_subscribers(n, timeout)
    }

    fn topic_config(&self, _topic: &str) -> BusConfig {
        BusConfig {
            base_dir: self.config.base_dir.join(&self.name),
            ..self.config.clone()
        }
    }
}

// ── Subscription ──────────────────────────────────────────────────────────────

/// A live subscription to a topic.
///
/// Returned by [`Bus::subscribe`]. Implements `Iterator` for ergonomic loops.
/// For error handling, use [`Subscription::recv`] directly.
pub struct Subscription {
    sub: Subscriber,
}

impl Subscription {
    /// Block until the next message arrives.
    pub fn recv(&mut self) -> Result<Vec<u8>> {
        self.sub.receive()
    }

    /// Block with a timeout. Returns `Ok(None)` if the timeout elapses without
    /// a message; `Ok(Some(bytes))` on success.
    pub fn recv_timeout(&mut self, timeout: Duration) -> Result<Option<Vec<u8>>> {
        self.sub.receive_timeout(timeout)
    }

    /// Non-blocking poll. Returns `None` immediately if no message is ready.
    pub fn try_recv(&mut self) -> Option<Vec<u8>> {
        self.sub.try_receive()
    }
}

/// Iterates over incoming messages. `Item = Result<Vec<u8>>` so the caller
/// sees I/O errors as `Err(...)` rather than silent stops. Stops on disconnect.
impl Iterator for Subscription {
    type Item = Result<Vec<u8>>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.sub.receive() {
            Ok(msg) => Some(Ok(msg)),
            Err(Error::Io(e)) if e.kind() == io::ErrorKind::UnexpectedEof
                || e.kind() == io::ErrorKind::ConnectionReset =>
            {
                None // publisher disconnected cleanly
            }
            Err(e) => Some(Err(e)),
        }
    }
}

// ── Publisher ─────────────────────────────────────────────────────────────────

/// Low-level producer handle. Prefer [`Bus::publish`] unless you need direct
/// lifecycle control.
pub struct Publisher {
    ring: RingBuffer,
    listener: UnixListener,
    clients: Vec<UnixStream>,
}

impl Publisher {
    /// Create a new named bus. Removes any stale socket from a previous run.
    pub fn create(name: &str, cfg: BusConfig) -> Result<Self> {
        let dir = cfg.base_dir.join(name);
        fs::create_dir_all(&dir)?;

        let ring_path = dir.join("ring.mmap");
        let sock_path = dir.join("signal.sock");

        let _ = fs::remove_file(&sock_path);

        let ring = RingBuffer::create(&ring_path, cfg.capacity, cfg.slot_size)?;
        let listener = UnixListener::bind(&sock_path)?;
        listener.set_nonblocking(true)?;

        Ok(Self {
            ring,
            listener,
            clients: Vec::new(),
        })
    }

    /// Publish a message. Returns `Err(Error::Full)` if the ring is saturated.
    pub fn publish(&mut self, data: &[u8]) -> Result<()> {
        if data.len() > self.ring.slot_payload_size as usize {
            return Err(Error::TooLarge {
                size: data.len(),
                max: self.ring.slot_payload_size as usize,
            });
        }

        // Accept any pending new subscriber connections.
        loop {
            match self.listener.accept() {
                Ok((stream, _)) => {
                    stream.set_nonblocking(false)?;
                    self.clients.push(stream);
                }
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => break,
                Err(e) => return Err(e.into()),
            }
        }

        if !self.ring.try_publish(data) {
            return Err(Error::Full);
        }

        // Broadcast wakeup; drop disconnected subscribers.
        // TODO: set SO_NOSIGPIPE / MSG_NOSIGNAL to suppress SIGPIPE on disconnect.
        let wakeup = [0x01u8];
        let mut i = 0;
        while i < self.clients.len() {
            if self.clients[i].write_all(&wakeup).is_err() {
                self.clients.swap_remove(i);
            } else {
                i += 1;
            }
        }

        Ok(())
    }

    /// Block until at least `min_count` subscribers have connected.
    pub fn wait_for_subscribers(&mut self, min_count: usize, timeout: Duration) -> Result<()> {
        let deadline = Instant::now() + timeout;
        loop {
            loop {
                match self.listener.accept() {
                    Ok((stream, _)) => {
                        stream.set_nonblocking(false)?;
                        self.clients.push(stream);
                    }
                    Err(e) if e.kind() == io::ErrorKind::WouldBlock => break,
                    Err(e) => return Err(e.into()),
                }
            }
            if self.clients.len() >= min_count {
                return Ok(());
            }
            if Instant::now() >= deadline {
                return Err(Error::Timeout("waiting for subscribers".to_owned()));
            }
            std::thread::sleep(Duration::from_millis(1));
        }
    }

    pub fn slot_size(&self) -> u32 {
        self.ring.slot_payload_size
    }

    pub fn connected_subscribers(&self) -> usize {
        self.clients.len()
    }
}

// ── Subscriber ────────────────────────────────────────────────────────────────

/// Low-level consumer handle. Prefer [`Bus::subscribe`] unless you need direct
/// lifecycle control.
pub struct Subscriber {
    ring: RingBuffer,
    stream: UnixStream,
    cursor: u64,
}

impl Subscriber {
    /// Connect to a named bus, retrying until `timeout` expires.
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

        let stream = loop {
            match UnixStream::connect(&sock_path) {
                Ok(s) => break s,
                Err(_) if Instant::now() < deadline => {
                    std::thread::sleep(Duration::from_millis(5));
                }
                Err(_) => return Err(Error::Timeout(name.to_owned())),
            }
        };

        let cursor = ring.current_tail();
        ring.advance_head(cursor);

        Ok(Self { ring, stream, cursor })
    }

    /// Block until the next message arrives.
    pub fn receive(&mut self) -> Result<Vec<u8>> {
        let mut wakeup = [0u8; 1];
        let mut out = Vec::new();
        loop {
            self.stream.read_exact(&mut wakeup)?;
            if self.ring.try_receive(self.cursor, &mut out) {
                self.ring.advance_head(self.cursor + 1);
                self.cursor += 1;
                return Ok(out);
            }
        }
    }

    /// Block with a timeout. Returns `Ok(None)` if the timeout elapses.
    pub fn receive_timeout(&mut self, timeout: Duration) -> Result<Option<Vec<u8>>> {
        self.stream.set_read_timeout(Some(timeout))?;
        let result = self.receive();
        // Restore to blocking-indefinite before returning.
        let _ = self.stream.set_read_timeout(None);
        match result {
            Ok(msg) => Ok(Some(msg)),
            Err(Error::Io(e))
                if e.kind() == io::ErrorKind::TimedOut
                    || e.kind() == io::ErrorKind::WouldBlock =>
            {
                Ok(None)
            }
            Err(e) => Err(e),
        }
    }

    /// Non-blocking poll. Returns `None` if no message is ready.
    pub fn try_receive(&mut self) -> Option<Vec<u8>> {
        let mut out = Vec::new();
        if self.ring.try_receive(self.cursor, &mut out) {
            self.ring.advance_head(self.cursor + 1);
            self.cursor += 1;
            Some(out)
        } else {
            None
        }
    }

    pub fn cursor(&self) -> u64 {
        self.cursor
    }
}
