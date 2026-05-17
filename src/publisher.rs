use crate::config::{BackpressurePolicy, BusConfig};
use crate::error::{Error, Result};
use crate::producer_lock::{acquire_producer_lock, ProducerLock};
use crate::ring::RingBuffer;
use crate::stats::TopicStats;
use std::fs;
use std::io;
use std::os::unix::io::AsRawFd;
use std::os::unix::net::{UnixListener, UnixStream};
use std::time::{Duration, Instant};

#[cfg(target_os = "linux")]
use std::os::fd::OwnedFd;

/// Per-subscriber connection state held by the publisher.
struct Client {
    sock: UnixStream,

    /// On Linux: the write-end of the subscriber's eventfd, received via
    /// SCM_RIGHTS. Used for low-overhead per-message wakeup.
    #[cfg(target_os = "linux")]
    efd: OwnedFd,
}

impl Client {
    /// Send one wakeup signal. Returns false if the subscriber disconnected.
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

/// Low-level producer handle. Prefer [`crate::Bus::publish`] for most use-cases.
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

    pub fn slot_size(&self) -> u32 {
        self.ring.slot_payload_size
    }

    pub fn connected_subscribers(&self) -> usize {
        self.clients.len()
    }

    /// Drain the non-blocking listener and promote new connections to clients.
    /// On Linux this also receives the subscriber's eventfd via SCM_RIGHTS.
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

// ── Platform helpers (publisher-side) ─────────────────────────────────────────

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
