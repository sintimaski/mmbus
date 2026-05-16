use crate::ring::RingBuffer;
use std::fs;
use std::io::{self, Read, Write};
use std::os::unix::net::{UnixListener, UnixStream};
use std::path::PathBuf;
use std::time::{Duration, Instant};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("I/O error: {0}")]
    Io(#[from] io::Error),
    #[error("ring buffer full")]
    Full,
    #[error("message too large: {size} bytes, max is {max}")]
    TooLarge { size: usize, max: usize },
    #[error("connection timeout waiting for bus '{0}'")]
    Timeout(String),
}

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Clone, Debug)]
pub struct BusConfig {
    /// Max payload bytes per message.
    pub slot_size: u32,
    /// Number of ring buffer slots.
    pub capacity: u32,
    /// Directory under which bus files are created. Defaults to `/tmp/mmbus`.
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

/// The producer side of a named bus.
///
/// Creates the mmap ring buffer file and listens for subscriber connections.
/// On each `publish` call it first drains the accept queue (picking up any
/// new subscribers), writes to the ring, then sends a 1-byte wakeup to each
/// connected subscriber.
pub struct Publisher {
    ring: RingBuffer,
    listener: UnixListener,
    // Each connected subscriber stream; dropped on write failure (disconnected).
    clients: Vec<UnixStream>,
}

impl Publisher {
    /// Create a new named bus. Removes any stale socket from a previous run.
    pub fn create(name: &str, cfg: BusConfig) -> Result<Self> {
        let dir = cfg.base_dir.join(name);
        fs::create_dir_all(&dir)?;

        let ring_path = dir.join("ring.mmap");
        let sock_path = dir.join("signal.sock");

        // Remove stale socket if present (would cause `bind` to fail otherwise).
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

    /// Publish a message. Returns `Err(Error::Full)` if all ring slots are occupied.
    pub fn publish(&mut self, data: &[u8]) -> Result<()> {
        if data.len() > self.ring.slot_payload_size as usize {
            return Err(Error::TooLarge {
                size: data.len(),
                max: self.ring.slot_payload_size as usize,
            });
        }

        // Pick up any pending subscriber connections before writing.
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

        // Broadcast a 1-byte wakeup; evict any subscriber that has disconnected.
        // Note: on Unix, a broken pipe raises SIGPIPE. Production code should set
        // SO_NOSIGPIPE / MSG_NOSIGNAL; for the POC both ends stay alive.
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

    /// Block until at least `min_count` subscribers have connected, or `timeout` expires.
    pub fn wait_for_subscribers(&mut self, min_count: usize, timeout: Duration) -> Result<()> {
        let deadline = Instant::now() + timeout;
        loop {
            // Drain the accept queue.
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

/// The consumer side of a named bus.
///
/// Connects to an existing publisher, initializing its cursor at the current
/// tail (receives only messages published after `connect`).
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

        // Cursor starts at current tail so we only receive future messages.
        // Advance head to match so the producer's available-space calculation
        // isn't confused by a head pointer that's far behind the current tail.
        let cursor = ring.current_tail();
        ring.advance_head(cursor);

        Ok(Self { ring, stream, cursor })
    }

    /// Block until the next message arrives, then return its bytes.
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
            // Spurious wakeup — shouldn't occur in SPSC but guard defensively.
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
