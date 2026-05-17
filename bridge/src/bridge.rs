//! The runtime that ties config + mmbus + TCP forwarders together.
//!
//! Threading model (B1, single peer; B3 will generalise to N):
//!
//! ```text
//!   mmbus topic T  ──[Subscription::recv]──►  subscriber thread
//!                                              encodes Frame
//!                                              fan-outs cloned bytes
//!                                                ├──► peer-1 forwarder thread ──► TCP
//!                                                └──► peer-N forwarder thread ──► TCP
//! ```
//!
//! Each forwarder owns its `TcpStream`, reconnects with backoff on
//! disconnect, and drops messages while disconnected.  Each subscriber
//! owns the local mmbus `Subscription` and serialises one `Frame` per
//! published message into bytes, then `clone()`s the bytes once per
//! peer for delivery.  No per-message allocation beyond that.

use crate::config::BridgeConfig;
use crate::frame::Frame;
use mmbus::{Bus, BusConfig};
use std::io::Write;
use std::net::TcpStream;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{mpsc, Arc};
use std::thread::{self, JoinHandle};
use std::time::Duration;

/// One running bridge process.  Returned by [`Bridge::start`]; drop
/// (or call [`Bridge::shutdown`]) to stop and join all threads.
pub struct Bridge {
    /// `origin_id` this bridge stamps into outbound frames (loop prevention).
    pub origin_id: u64,

    shutdown: Arc<AtomicBool>,

    /// Subscriber threads (one per configured forward-enabled topic).
    sub_threads: Vec<JoinHandle<()>>,

    /// Forwarder threads (one per configured peer).
    fwd_threads: Vec<JoinHandle<()>>,

    /// Senders for the bridge to fan a frame's encoded bytes out to all
    /// peer forwarders.  Kept here to hand a fresh clone to each new
    /// subscriber thread (in B3 we may rotate this list under config
    /// reload; for B1 it's set-once-at-start).
    peer_tx: Vec<mpsc::Sender<Vec<u8>>>,
}

impl Bridge {
    /// Spin up subscriber + forwarder threads.  Returns once threads
    /// are running (does NOT block on traffic).
    pub fn start(cfg: &BridgeConfig) -> Result<Self, BridgeError> {
        let origin_id = cfg.origin_id.unwrap_or_else(random_origin_id);
        let shutdown = Arc::new(AtomicBool::new(false));

        // Build the mmbus handle (one shared Bus per process — Bus itself
        // is internally Sync for the parts we touch).
        let bus = build_bus(cfg);

        // One outbound channel per peer.
        let mut peer_tx = Vec::new();
        let mut fwd_threads = Vec::new();
        for peer in &cfg.peers {
            let (tx, rx) = mpsc::channel::<Vec<u8>>();
            peer_tx.push(tx);
            let endpoint = peer.endpoint.clone();
            let name = peer.name.clone();
            let shutdown_clone = shutdown.clone();
            let hello = {
                let mut buf = Vec::new();
                Frame::peer_hello(origin_id).encode(&mut buf);
                buf
            };
            fwd_threads.push(thread::spawn(move || {
                forwarder_main(name, endpoint, hello, rx, shutdown_clone);
            }));
        }

        // One subscriber per topic with `forward = true`.
        let mut sub_threads = Vec::new();
        for topic in &cfg.topics {
            if !topic.forward {
                continue;
            }
            let topic_name = topic.name.clone();
            let bus_clone = bus.clone();
            let shutdown_clone = shutdown.clone();
            let peer_tx_clone = peer_tx.clone();
            let seq = Arc::new(AtomicU64::new(0));
            sub_threads.push(thread::spawn(move || {
                subscriber_main(
                    bus_clone,
                    topic_name,
                    origin_id,
                    seq,
                    peer_tx_clone,
                    shutdown_clone,
                );
            }));
        }

        Ok(Self { origin_id, shutdown, sub_threads, fwd_threads, peer_tx })
    }

    /// Signal all threads to stop, then join them.  Idempotent — calling
    /// twice (or after `drop`) is harmless because shutdown is just a flag.
    pub fn shutdown(mut self) {
        self.shutdown_inner();
    }

    fn shutdown_inner(&mut self) {
        self.shutdown.store(true, Ordering::Release);
        // Dropping all senders signals the forwarders that no more
        // frames are coming (their rx will return Disconnected).
        self.peer_tx.clear();
        for h in self.sub_threads.drain(..) {
            let _ = h.join();
        }
        for h in self.fwd_threads.drain(..) {
            let _ = h.join();
        }
    }
}

impl Drop for Bridge {
    fn drop(&mut self) {
        // If shutdown() wasn't called, do it on Drop so threads don't
        // outlive us.
        self.shutdown_inner();
    }
}

#[derive(Debug, thiserror::Error)]
pub enum BridgeError {
    #[error("mmbus error: {0}")]
    Mmbus(#[from] mmbus::Error),
}

// ── Implementation details ────────────────────────────────────────────────────

fn build_bus(cfg: &BridgeConfig) -> Arc<Bus> {
    let mut bcfg = BusConfig::default();
    if let Some(dir) = &cfg.base_dir {
        bcfg.base_dir = dir.clone();
    }
    Arc::new(Bus::with_config(cfg.bus.clone(), bcfg))
}

/// Subscriber thread body: read from the local mmbus topic, encode each
/// message as a `Msg` frame, and fan out the bytes to all peer
/// forwarders.  Exits when the subscription returns EOF or `shutdown`
/// is set.
fn subscriber_main(
    bus: Arc<Bus>,
    topic: String,
    origin_id: u64,
    seq: Arc<AtomicU64>,
    peer_tx: Vec<mpsc::Sender<Vec<u8>>>,
    shutdown: Arc<AtomicBool>,
) {
    // Tight-loop reconnect: if the local publisher dies, wait for it to
    // come back rather than tearing the bridge down.
    while !shutdown.load(Ordering::Acquire) {
        let mut sub = match bus.subscribe_timeout(&topic, Duration::from_millis(500)) {
            Ok(s) => s,
            Err(_) => {
                // Publisher not up yet; wait a beat and retry.
                thread::sleep(Duration::from_millis(50));
                continue;
            }
        };
        loop {
            if shutdown.load(Ordering::Acquire) {
                return;
            }
            // recv_timeout so we periodically observe the shutdown flag
            // even when the topic is quiet.
            match sub.recv_timeout(Duration::from_millis(200)) {
                Ok(Some(payload)) => {
                    let s = seq.fetch_add(1, Ordering::Relaxed);
                    let frame = Frame::msg(origin_id, s, topic.as_bytes().to_vec(), payload);
                    let mut buf = Vec::with_capacity(frame.encoded_len());
                    frame.encode(&mut buf);
                    // Fan out — clone once per peer.  A peer that's
                    // disconnected/slow drops here when its rx is gone.
                    for tx in &peer_tx {
                        let _ = tx.send(buf.clone());
                    }
                }
                Ok(None) => continue, // timeout — re-check shutdown + recv again
                Err(_) => break,       // EOF or other — reconnect outer loop
            }
        }
    }
}

/// Forwarder thread body: maintain a TCP connection to one peer; on
/// connect, send `PeerHello`; then forward every frame from `rx` to
/// the wire.  Reconnects with exponential backoff (capped at ~1 s).
fn forwarder_main(
    peer_name: String,
    endpoint: String,
    hello_bytes: Vec<u8>,
    rx: mpsc::Receiver<Vec<u8>>,
    shutdown: Arc<AtomicBool>,
) {
    let mut backoff = Duration::from_millis(50);
    let max_backoff = Duration::from_secs(1);

    while !shutdown.load(Ordering::Acquire) {
        let stream = match TcpStream::connect_timeout(
            &match endpoint.parse() {
                Ok(addr) => addr,
                Err(e) => {
                    eprintln!(
                        "bridge: peer {peer_name:?} has unparseable endpoint {endpoint:?}: {e}"
                    );
                    return;
                }
            },
            Duration::from_secs(2),
        ) {
            Ok(s) => s,
            Err(e) => {
                // Connection failed; wait and retry.
                eprintln!("bridge: peer {peer_name:?} connect failed ({e}); retrying in {backoff:?}");
                sleep_interruptible(backoff, &shutdown);
                backoff = (backoff * 2).min(max_backoff);
                continue;
            }
        };
        backoff = Duration::from_millis(50); // reset on success
        eprintln!("bridge: peer {peer_name:?} connected at {endpoint}");

        if let Err(e) = run_connection(&peer_name, stream, &hello_bytes, &rx, &shutdown) {
            eprintln!("bridge: peer {peer_name:?} disconnected: {e}");
        }
    }
}

/// One connected-session of the forwarder.  Sends the hello, then
/// pumps frames from `rx` to the wire until either the channel closes
/// (sender dropped, bridge shutting down) or a write fails.
fn run_connection(
    peer_name: &str,
    mut stream: TcpStream,
    hello_bytes: &[u8],
    rx: &mpsc::Receiver<Vec<u8>>,
    shutdown: &AtomicBool,
) -> std::io::Result<()> {
    // Reasonable defaults; v1 doesn't expose tuning knobs.
    stream.set_nodelay(true)?;
    stream.set_write_timeout(Some(Duration::from_secs(5)))?;

    stream.write_all(hello_bytes)?;

    loop {
        if shutdown.load(Ordering::Acquire) {
            return Ok(());
        }
        // recv_timeout: lets us periodically observe the shutdown flag
        // even when no frames are queued.
        match rx.recv_timeout(Duration::from_millis(200)) {
            Ok(bytes) => stream.write_all(&bytes)?,
            Err(mpsc::RecvTimeoutError::Timeout) => continue,
            Err(mpsc::RecvTimeoutError::Disconnected) => {
                eprintln!(
                    "bridge: peer {peer_name:?} channel closed; closing TCP cleanly"
                );
                return Ok(());
            }
        }
    }
}

/// Sleep that wakes up early if `shutdown` is set during the wait —
/// avoids leaving threads sleeping for seconds after the bridge stops.
fn sleep_interruptible(total: Duration, shutdown: &AtomicBool) {
    let step = Duration::from_millis(50);
    let mut left = total;
    while left > Duration::ZERO {
        if shutdown.load(Ordering::Acquire) {
            return;
        }
        let chunk = left.min(step);
        thread::sleep(chunk);
        left = left.saturating_sub(chunk);
    }
}

/// Non-cryptographic 64-bit ID derived from clock + pid.  Collision risk
/// is negligible at realistic mesh sizes (< 10^6 bridges).
fn random_origin_id() -> u64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0);
    let pid = std::process::id() as u64;
    // Splittable-mix bit-mix so close-in-time IDs don't share high bits.
    let mut z = nanos ^ (pid.rotate_left(32));
    z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    z ^ (z >> 31)
}
