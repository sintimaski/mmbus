//! The runtime that ties config + mmbus + TCP transports together.
//!
//! Threading model (B2 — forward + receive, single-peer per direction;
//! B3 generalises to N-peer mesh):
//!
//! ```text
//!   forward path:
//!     mmbus topic T  ──[Subscription::recv]──►  subscriber thread
//!                                                encodes Frame
//!                                                fan-outs cloned bytes
//!                                                  ├──► peer-1 forwarder ──► TCP out
//!                                                  └──► peer-N forwarder ──► TCP out
//!
//!   receive path (only spawned when config.listen is set):
//!                            ┌──► reader thread ──┐
//!     TCP in ──► listener ──►│         …          │──► publisher thread ──► Bus::publish
//!                            └──► reader thread ──┘
//! ```
//!
//! Each forwarder owns its `TcpStream`, reconnects with backoff on
//! disconnect, and drops messages while disconnected.  Each subscriber
//! owns the local mmbus `Subscription` and serialises one `Frame` per
//! published message into bytes, then `clone()`s the bytes once per
//! peer for delivery.  No per-message allocation beyond that.
//!
//! On the receive side, the single publisher thread is the only one
//! that holds an `&mut Bus` for publish (acquire_producer_lock is
//! per-process; sharing the publish duty across threads would deadlock
//! the second one with `AlreadyPublishing`).  Reader threads funnel
//! `(topic, payload)` pairs to it via an mpsc channel.

use crate::config::{BridgeConfig, TransportKind};
use crate::frame::{decode, parse_peer_hello, DecodeError, Frame, FrameType};
use crate::queue;
use mmbus::{Bus, BusConfig};
use std::collections::HashSet;
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{mpsc, Arc};
use std::thread::{self, JoinHandle};
use std::time::Duration;

/// One running bridge process.  Returned by [`Bridge::start`]; drop
/// (or call [`Bridge::shutdown`]) to stop and join all threads.
pub struct Bridge {
    /// `origin_id` this bridge stamps into outbound frames (loop prevention).
    pub origin_id: u64,

    /// Address the listener bound to, if any.  When [`BridgeConfig::listen`]
    /// is `"0.0.0.0:0"` this is the resolved ephemeral port — tests use it
    /// to find the bridge.
    pub listen_addr: Option<std::net::SocketAddr>,

    /// QUIC listener address resolved at bind time (mirrors `listen_addr`
    /// for QUIC).  `None` when no QUIC listener is configured.
    #[cfg(feature = "quic")]
    pub quic_listen_addr: Option<std::net::SocketAddr>,

    /// SHA-256 fingerprint of the bridge's self-signed QUIC cert,
    /// surfaced for operator logs and tests that need to copy it into
    /// a peer's `peer_cert_fingerprint` config.  `None` when QUIC is
    /// not enabled.
    #[cfg(feature = "quic")]
    pub quic_fingerprint: Option<String>,

    shutdown: Arc<AtomicBool>,

    /// Subscriber threads (one per configured forward-enabled topic).
    sub_threads: Vec<JoinHandle<()>>,

    /// Forwarder threads (one per configured peer).
    fwd_threads: Vec<JoinHandle<()>>,

    /// Listener thread, plus one reader per accepted connection.  All
    /// joined on shutdown.
    rx_threads: Vec<JoinHandle<()>>,

    /// The single publisher thread that owns the receive-side mmbus
    /// publisher.  Joined on shutdown.
    publish_thread: Option<JoinHandle<()>>,

    /// Senders for the bridge to fan a frame's encoded bytes out to all
    /// peer forwarders.  Drop-oldest semantics — a slow/disconnected
    /// peer cannot stall the publisher; its buffer fills up to
    /// `config.peer_buffer_max` then evicts the oldest entry on each
    /// new send.
    peer_tx: Vec<queue::Sender<Vec<u8>>>,

    /// Sender for the receive path; cloned to each reader thread so
    /// they can forward decoded Msg frames to the publisher.  `None`
    /// when no listener is configured.
    publish_tx: Option<mpsc::Sender<(String, Vec<u8>)>>,

    /// Threads bridging the sync `queue::Receiver<Vec<u8>>` of each
    /// QUIC peer into the tokio runtime's mpsc.  One per QUIC peer.
    #[cfg(feature = "quic")]
    quic_bridge_threads: Vec<JoinHandle<()>>,

    /// Dedicated tokio runtime owning every QUIC connection.  Kept
    /// here so `Drop` can shut it down cleanly.  `None` when QUIC is
    /// not in use (no QUIC peers and no `listen_quic`).
    #[cfg(feature = "quic")]
    quic_runtime: Option<Arc<tokio::runtime::Runtime>>,
}

impl Bridge {
    /// Spin up subscriber + forwarder threads.  Returns once threads
    /// are running (does NOT block on traffic).
    pub fn start(cfg: &BridgeConfig) -> Result<Self, BridgeError> {
        // Fail fast if the config asks for QUIC features the build
        // can't deliver.  Better an explicit startup error than a
        // mysterious "peer never connects" later.
        #[cfg(not(feature = "quic"))]
        {
            if cfg.listen_quic.is_some() {
                return Err(BridgeError::QuicNotCompiled {
                    reason: "listen_quic is set",
                });
            }
            if cfg.peers.iter().any(|p| p.transport.is_quic()) {
                return Err(BridgeError::QuicNotCompiled {
                    reason: "one or more peers use transport = \"quic\"",
                });
            }
        }

        let origin_id = cfg.origin_id.unwrap_or_else(random_origin_id);
        let shutdown = Arc::new(AtomicBool::new(false));

        // Build the mmbus handle (one shared Bus per process — Bus itself
        // is internally Sync for the parts we touch).
        let bus = build_bus(cfg);

        // Spin up the QUIC runtime + identity if any peer needs it.
        // The runtime stays parked when no QUIC traffic is in flight;
        // the worker threads are bounded so this is cheap when unused.
        #[cfg(feature = "quic")]
        let (quic_runtime, quic_identity) = build_quic_runtime_and_identity(cfg)?;

        // One outbound channel per peer.  Drop-oldest bounded queue —
        // slow peers cannot stall the publisher.  Each forwarder
        // sends a PeerHello stamped with that peer's PSK so the
        // receiving bridge can authenticate the connection.  TCP peers
        // get a sync forwarder thread; QUIC peers each get a sync→async
        // bridge thread that drains the queue into a tokio mpsc which
        // the QUIC runtime then writes onto a quinn bidirectional
        // stream.
        let mut peer_tx = Vec::new();
        let mut fwd_threads = Vec::new();
        #[cfg(feature = "quic")]
        let mut quic_bridge_threads: Vec<JoinHandle<()>> = Vec::new();
        for peer in &cfg.peers {
            let (tx, rx) = queue::channel::<Vec<u8>>(cfg.peer_buffer_max);
            peer_tx.push(tx);
            match peer.transport {
                TransportKind::Tcp => {
                    let endpoint = peer.endpoint.clone();
                    let name = peer.name.clone();
                    let shutdown_clone = shutdown.clone();
                    let hello = {
                        let mut buf = Vec::new();
                        Frame::peer_hello_with_psk(
                            origin_id,
                            peer.preshared_key.as_bytes(),
                        )
                        .encode(&mut buf);
                        buf
                    };
                    fwd_threads.push(thread::spawn(move || {
                        forwarder_main(name, endpoint, hello, rx, shutdown_clone);
                    }));
                }
                TransportKind::Quic => {
                    #[cfg(feature = "quic")]
                    {
                        let _ = &quic_identity; // suppress unused warning when only outbound peers exist
                        let rt = quic_runtime
                            .as_ref()
                            .expect("quic_runtime must exist if any quic peer is configured");
                        let (tok_tx, tok_rx) =
                            tokio::sync::mpsc::channel::<Vec<u8>>(cfg.peer_buffer_max.max(1));
                        let bridge_handle =
                            crate::quic::spawn_queue_bridge(rx, tok_tx, shutdown.clone());
                        quic_bridge_threads.push(bridge_handle);

                        let hello = {
                            let mut buf = Vec::new();
                            Frame::peer_hello_with_psk(
                                origin_id,
                                peer.preshared_key.as_bytes(),
                            )
                            .encode(&mut buf);
                            buf
                        };
                        let name = peer.name.clone();
                        let endpoint = peer.endpoint.clone();
                        let pinned_fp = peer
                            .peer_cert_fingerprint
                            .clone()
                            .expect("validated at config parse time");
                        let server_name = "mmbus-bridge".to_owned();
                        let shutdown_clone = shutdown.clone();
                        rt.spawn(async move {
                            crate::quic::outbound_main(
                                name,
                                endpoint,
                                server_name,
                                pinned_fp,
                                hello,
                                tok_rx,
                                shutdown_clone,
                            )
                            .await;
                        });
                    }
                    #[cfg(not(feature = "quic"))]
                    {
                        // Unreachable under the startup guard above,
                        // but the compiler still wants this arm to do
                        // *something* with `rx` so the borrow
                        // disappears.
                        let _ = rx;
                        unreachable!("QuicNotCompiled guard should have fired");
                    }
                }
            }
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

        // Receive side — only when at least one listener is configured.
        // A single publisher thread owns the &mut Bus (acquire_producer_lock
        // is per-process, so we centralise all incoming traffic into one
        // mmbus publisher).  Both the TCP and QUIC listeners feed it via
        // the same publish channel.
        #[allow(unused_mut)]
        let mut listen_addr: Option<std::net::SocketAddr> = None;
        #[cfg(feature = "quic")]
        let mut quic_listen_addr: Option<std::net::SocketAddr> = None;

        let want_tcp_listen = cfg.listen.is_some();
        #[cfg(feature = "quic")]
        let want_quic_listen = cfg.listen_quic.is_some();
        #[cfg(not(feature = "quic"))]
        let want_quic_listen = false;

        let (rx_threads, publish_thread, publish_tx) =
            if want_tcp_listen || want_quic_listen {
                let receive_topics: HashSet<String> = cfg
                    .topics
                    .iter()
                    .filter(|t| t.receive)
                    .map(|t| t.name.clone())
                    .collect();
                // Authentication set: any peer presenting one of these
                // PSKs in their PeerHello is accepted.  Built from
                // cfg.peers — symmetric meshes typically share the
                // same PSK between A→B and B→A entries.
                let accepted_psks: HashSet<Vec<u8>> = cfg
                    .peers
                    .iter()
                    .map(|p| p.preshared_key.as_bytes().to_vec())
                    .collect();
                let (ptx, prx) = mpsc::channel::<(String, Vec<u8>)>();
                let publish_shutdown = shutdown.clone();
                let publisher_bus_cfg = bus_config(cfg);
                let publisher_bus_name = cfg.bus.clone();
                let publish_handle = thread::spawn(move || {
                    publisher_main(
                        publisher_bus_name,
                        publisher_bus_cfg,
                        prx,
                        publish_shutdown,
                    );
                });

                let receive_topics_arc = Arc::new(receive_topics);
                let accepted_psks_arc = Arc::new(accepted_psks);

                let mut rx_threads: Vec<JoinHandle<()>> = Vec::new();

                // TCP listener (optional)
                if let Some(listen_str) = &cfg.listen {
                    let listener = TcpListener::bind(listen_str.as_str())
                        .map_err(BridgeError::Listen)?;
                    listener
                        .set_nonblocking(true)
                        .map_err(BridgeError::Listen)?;
                    let resolved = listener
                        .local_addr()
                        .map_err(BridgeError::Listen)?;
                    listen_addr = Some(resolved);

                    let listener_shutdown = shutdown.clone();
                    let listener_ptx = ptx.clone();
                    let topics = receive_topics_arc.clone();
                    let psks = accepted_psks_arc.clone();
                    let listen_handle = thread::spawn(move || {
                        listener_main(
                            listener,
                            origin_id,
                            topics,
                            psks,
                            listener_ptx,
                            listener_shutdown,
                        );
                    });
                    rx_threads.push(listen_handle);
                }

                // QUIC listener (optional)
                #[cfg(feature = "quic")]
                if let Some(listen_str) = &cfg.listen_quic {
                    let bind_addr: std::net::SocketAddr = listen_str.parse().map_err(|e| {
                        BridgeError::QuicSetup(format!(
                            "listen_quic={listen_str:?}: {e}"
                        ))
                    })?;
                    let id = quic_identity.as_ref().ok_or_else(|| {
                        BridgeError::QuicSetup(
                            "identity missing for QUIC listener".to_owned(),
                        )
                    })?;
                    let server_config = crate::quic::server_config_from_identity(id)
                        .map_err(quic_err_to_bridge)?;
                    let rt = quic_runtime.as_ref().expect("runtime must exist if QUIC requested");
                    let (bound_tx, bound_rx) =
                        tokio::sync::oneshot::channel::<std::net::SocketAddr>();
                    let topics = receive_topics_arc.clone();
                    let psks = accepted_psks_arc.clone();
                    let ptx_clone = ptx.clone();
                    let sd = shutdown.clone();
                    rt.spawn(async move {
                        crate::quic::listener_main(
                            bind_addr,
                            server_config,
                            origin_id,
                            topics,
                            psks,
                            ptx_clone,
                            sd,
                            bound_tx,
                        )
                        .await;
                    });
                    // Wait for the listener to actually bind before
                    // returning to the caller — tests rely on
                    // quic_listen_addr being populated right after
                    // Bridge::start.
                    quic_listen_addr = rt.block_on(async move {
                        tokio::time::timeout(Duration::from_secs(5), bound_rx)
                            .await
                            .ok()
                            .and_then(|r| r.ok())
                    });
                }

                (rx_threads, Some(publish_handle), Some(ptx))
            } else {
                (Vec::new(), None, None)
            };

        Ok(Self {
            origin_id,
            listen_addr,
            #[cfg(feature = "quic")]
            quic_listen_addr,
            #[cfg(feature = "quic")]
            quic_fingerprint: quic_identity.as_ref().map(|i| i.fingerprint.clone()),
            shutdown,
            sub_threads,
            fwd_threads,
            rx_threads,
            publish_thread,
            peer_tx,
            publish_tx,
            #[cfg(feature = "quic")]
            quic_bridge_threads,
            #[cfg(feature = "quic")]
            quic_runtime,
        })
    }

    /// Signal all threads to stop, then join them.  Idempotent — calling
    /// twice (or after `drop`) is harmless because shutdown is just a flag.
    pub fn shutdown(mut self) {
        self.shutdown_inner();
    }

    fn shutdown_inner(&mut self) {
        self.shutdown.store(true, Ordering::Release);
        // Dropping all senders signals the forwarders / publisher that
        // no more frames are coming (their rx will return Disconnected
        // on the next recv).
        self.peer_tx.clear();
        self.publish_tx = None;
        for h in self.sub_threads.drain(..) {
            let _ = h.join();
        }
        for h in self.fwd_threads.drain(..) {
            let _ = h.join();
        }
        for h in self.rx_threads.drain(..) {
            let _ = h.join();
        }
        if let Some(h) = self.publish_thread.take() {
            let _ = h.join();
        }
        #[cfg(feature = "quic")]
        {
            for h in self.quic_bridge_threads.drain(..) {
                let _ = h.join();
            }
            // Drop the Arc to the runtime — when refcount hits 0 the
            // runtime drops + halts any spawned tasks.  We use the
            // Arc form so spawn() calls from the quic outbound code
            // can keep the runtime alive for their own use.
            if let Some(rt) = self.quic_runtime.take() {
                // shutdown_background returns immediately and shuts
                // down the runtime in the background.  Good enough
                // for a bridge that's exiting.
                if let Ok(rt) = Arc::try_unwrap(rt) {
                    rt.shutdown_background();
                }
            }
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

    #[error("failed to bind listen socket: {0}")]
    Listen(std::io::Error),

    /// Config asks for QUIC behaviour but this binary was built
    /// without `--features quic`.  Rebuild with the feature on, or
    /// remove the QUIC config entries.
    #[error(
        "QUIC requested in config ({reason}) but this build lacks `--features quic`; \
         rebuild with `cargo build --features quic`"
    )]
    QuicNotCompiled { reason: &'static str },

    /// QUIC setup (cert gen/load, runtime build, endpoint bind)
    /// failed at startup.
    #[error("QUIC setup failed: {0}")]
    QuicSetup(String),
}

// ── Implementation details ────────────────────────────────────────────────────

fn build_bus(cfg: &BridgeConfig) -> Arc<Bus> {
    Arc::new(Bus::with_config(cfg.bus.clone(), bus_config(cfg)))
}

fn bus_config(cfg: &BridgeConfig) -> BusConfig {
    let mut bcfg = BusConfig::default();
    if let Some(dir) = &cfg.base_dir {
        bcfg.base_dir = dir.clone();
    }
    bcfg
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
    peer_tx: Vec<queue::Sender<Vec<u8>>>,
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
                    // Fan out — clone once per peer.  Drop-oldest at
                    // the queue means a slow/disconnected peer evicts
                    // its oldest queued frame instead of stalling the
                    // publisher; the return value of `send` is the
                    // count of evicted frames (currently unused, but
                    // available for per-peer drop metrics).
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
    rx: queue::Receiver<Vec<u8>>,
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
    rx: &queue::Receiver<Vec<u8>>,
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
        // even when no frames are queued.  `None` means either timeout
        // OR all senders dropped — we distinguish by re-checking the
        // shutdown flag at the top of the loop.
        match rx.recv_timeout(Duration::from_millis(200)) {
            Some(bytes) => stream.write_all(&bytes)?,
            None => {
                if shutdown.load(Ordering::Acquire) {
                    eprintln!(
                        "bridge: peer {peer_name:?} channel closed; closing TCP cleanly"
                    );
                    return Ok(());
                }
                // Plain timeout — re-loop.
                continue;
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

/// Listener thread body: accept incoming peer connections; each accept
/// spawns one reader thread.  Exits when shutdown is set; joins all
/// reader threads on exit so the parent join is sufficient to wait
/// for the whole receive-side fan-in to drain.
fn listener_main(
    listener: TcpListener,
    our_origin_id: u64,
    receive_topics: Arc<HashSet<String>>,
    accepted_psks: Arc<HashSet<Vec<u8>>>,
    publish_tx: mpsc::Sender<(String, Vec<u8>)>,
    shutdown: Arc<AtomicBool>,
) {
    let mut readers: Vec<JoinHandle<()>> = Vec::new();
    while !shutdown.load(Ordering::Acquire) {
        match listener.accept() {
            Ok((stream, addr)) => {
                eprintln!("bridge: accepted peer from {addr}");
                if let Err(e) = stream.set_nonblocking(false) {
                    eprintln!("bridge: set_nonblocking failed: {e}");
                    continue;
                }
                if let Err(e) = stream
                    .set_read_timeout(Some(Duration::from_millis(200)))
                {
                    eprintln!("bridge: set_read_timeout failed: {e}");
                    continue;
                }
                let topics = receive_topics.clone();
                let psks = accepted_psks.clone();
                let tx = publish_tx.clone();
                let sd = shutdown.clone();
                readers.push(thread::spawn(move || {
                    reader_main(stream, our_origin_id, topics, psks, tx, sd);
                }));
            }
            Err(e)
                if e.kind() == std::io::ErrorKind::WouldBlock
                    || e.kind() == std::io::ErrorKind::TimedOut =>
            {
                thread::sleep(Duration::from_millis(50));
            }
            Err(e) => {
                eprintln!("bridge: accept failed: {e}; stopping listener");
                break;
            }
        }
    }
    // Join all reader threads so a caller awaiting `bridge.shutdown()`
    // is guaranteed they've all drained.
    for h in readers {
        let _ = h.join();
    }
}

/// Per-accepted-connection reader: validate the PeerHello, then decode
/// the frame stream, drop our own (loop prevention), and forward Msg
/// frames whose topic is in the receive set to the publisher.
///
/// Wire-level state machine:
///   1. Read until we have a full `PeerHello`.
///   2. Validate its PSK against `accepted_psks`; close on mismatch
///      or if the first frame isn't a PeerHello.
///   3. Loop over subsequent frames as before.
fn reader_main(
    mut stream: TcpStream,
    our_origin_id: u64,
    receive_topics: Arc<HashSet<String>>,
    accepted_psks: Arc<HashSet<Vec<u8>>>,
    publish_tx: mpsc::Sender<(String, Vec<u8>)>,
    shutdown: Arc<AtomicBool>,
) {
    let mut buf: Vec<u8> = Vec::with_capacity(8 * 1024);
    let mut tmp = [0u8; 4 * 1024];
    let mut authenticated = false;

    while !shutdown.load(Ordering::Acquire) {
        // Refill from the socket; the 200 ms read timeout makes this
        // loop check the shutdown flag periodically even when the peer
        // is quiet.
        match stream.read(&mut tmp) {
            Ok(0) => {
                eprintln!("bridge: peer closed cleanly");
                return;
            }
            Ok(n) => buf.extend_from_slice(&tmp[..n]),
            Err(e)
                if e.kind() == std::io::ErrorKind::WouldBlock
                    || e.kind() == std::io::ErrorKind::TimedOut =>
            {
                continue;
            }
            Err(e) => {
                eprintln!("bridge: peer read failed: {e}");
                return;
            }
        }

        // Decode every complete frame currently in buf.
        loop {
            match decode(&buf) {
                Ok((frame, n)) => {
                    if !authenticated {
                        // First frame MUST be PeerHello with a matching PSK.
                        if frame.frame_type != FrameType::PeerHello {
                            eprintln!(
                                "bridge: first frame must be PeerHello (got {:?}); closing",
                                frame.frame_type
                            );
                            return;
                        }
                        let hello = match parse_peer_hello(&frame.payload) {
                            Ok(h) => h,
                            Err(e) => {
                                eprintln!("bridge: malformed PeerHello: {e}; closing");
                                return;
                            }
                        };
                        if !accepted_psks.contains(hello.psk) {
                            eprintln!(
                                "bridge: PSK mismatch from origin_id={} ({} byte PSK); closing",
                                hello.origin_id,
                                hello.psk.len()
                            );
                            return;
                        }
                        eprintln!(
                            "bridge: authenticated peer origin_id={}",
                            hello.origin_id
                        );
                        authenticated = true;
                        buf.drain(..n);
                        continue;
                    }
                    handle_frame(&frame, our_origin_id, &receive_topics, &publish_tx);
                    buf.drain(..n);
                }
                Err(DecodeError::Incomplete { .. }) => break, // wait for more bytes
                Err(e) => {
                    eprintln!("bridge: protocol error from peer: {e}; closing");
                    return;
                }
            }
        }
    }
}

fn handle_frame(
    frame: &Frame,
    our_origin_id: u64,
    receive_topics: &HashSet<String>,
    publish_tx: &mpsc::Sender<(String, Vec<u8>)>,
) {
    // Loop prevention: never re-publish a frame we originated.
    if frame.origin_id == our_origin_id {
        return;
    }
    match frame.frame_type {
        FrameType::Msg => {
            // Reject non-UTF-8 topics (mmbus topic names are strings).
            let topic = match std::str::from_utf8(&frame.topic) {
                Ok(t) => t,
                Err(_) => {
                    eprintln!("bridge: dropping Msg with non-UTF-8 topic");
                    return;
                }
            };
            if receive_topics.contains(topic) {
                // Clone the payload to hand ownership to the publisher
                // thread; `frame` itself is borrowed.
                let _ = publish_tx
                    .send((topic.to_string(), frame.payload.clone()));
            }
        }
        FrameType::PeerHello => {
            // A second PeerHello mid-stream is unexpected (we only
            // process the first one at auth time).  Ignore for
            // forwards-compat in case a future protocol re-handshakes.
        }
        FrameType::Ping => {
            // B2 doesn't yet respond; pings are silently absorbed.
        }
        FrameType::TopicSubscribe => {
            // Reserved: will let peers register interest in additional
            // topics beyond the bridge's configured forward set.
            // Ignored today.
        }
    }
}

/// Publisher thread body: owns the mut Bus + drains the publish
/// channel, calling `Bus::publish` for each (topic, payload).
fn publisher_main(
    bus_name: String,
    bus_cfg: BusConfig,
    rx: mpsc::Receiver<(String, Vec<u8>)>,
    shutdown: Arc<AtomicBool>,
) {
    let mut bus = Bus::with_config(bus_name, bus_cfg);
    while !shutdown.load(Ordering::Acquire) {
        match rx.recv_timeout(Duration::from_millis(200)) {
            Ok((topic, payload)) => {
                if let Err(e) = bus.publish(&topic, &payload) {
                    eprintln!(
                        "bridge: republish failed (topic={topic:?}, {} bytes): {e}",
                        payload.len()
                    );
                }
            }
            Err(mpsc::RecvTimeoutError::Timeout) => continue,
            Err(mpsc::RecvTimeoutError::Disconnected) => return,
        }
    }
}

/// Build the QUIC runtime + identity if any peer/listener needs them.
/// Returns `(None, None)` when the bridge has no QUIC traffic to handle.
#[cfg(feature = "quic")]
fn build_quic_runtime_and_identity(
    cfg: &BridgeConfig,
) -> Result<(Option<Arc<tokio::runtime::Runtime>>, Option<crate::quic::Identity>), BridgeError>
{
    let any_quic_peer = cfg.peers.iter().any(|p| p.transport.is_quic());
    let want_listen = cfg.listen_quic.is_some();
    if !any_quic_peer && !want_listen {
        return Ok((None, None));
    }
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(cfg.quic_worker_threads)
        .enable_all()
        .thread_name("mmbus-bridge-quic")
        .build()
        .map_err(BridgeError::Listen)?;
    let id = if want_listen {
        let base = cfg
            .base_dir
            .clone()
            .unwrap_or_else(std::env::temp_dir);
        let cert_path = cfg
            .quic_cert_path
            .clone()
            .unwrap_or_else(|| base.join("bridge.cert.der"));
        let key_path = cfg
            .quic_key_path
            .clone()
            .unwrap_or_else(|| base.join("bridge.key.der"));
        Some(
            crate::quic::gen_or_load_identity(&cert_path, &key_path)
                .map_err(quic_err_to_bridge)?,
        )
    } else {
        None
    };
    Ok((Some(Arc::new(rt)), id))
}

#[cfg(feature = "quic")]
fn quic_err_to_bridge(e: crate::quic::QuicError) -> BridgeError {
    BridgeError::QuicSetup(format!("{e}"))
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
