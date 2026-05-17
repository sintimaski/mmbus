//! `mmbus-bridge` — cross-machine relay for mmbus topics.
//!
//! See `docs/rfc-multi-machine.md` in the parent crate for the design.
//! This crate is intentionally a separate workspace member so its
//! network + crypto dependencies don't bleed into the core mmbus
//! library's build matrix.
//!
//! Stage B0 (current): config parsing + wire-frame codec, no I/O.
//! Stage B1: local subscribe + TCP forward to one peer.
//! Stage B2: receive from peer + dedupe by origin_id + republish locally.
//! Stage B3: N-peer mesh + per-peer bounded buffers.
//! Stage B4: QUIC (quinn) transport behind a feature flag.
//! Stage B5: Python helper + systemd unit.

pub mod bridge;
pub mod config;
pub mod frame;
pub mod queue;

#[cfg(feature = "quic")]
pub mod quic;

pub use bridge::{Bridge, BridgeError};
pub use config::{BridgeConfig, ConfigError, PeerConfig, TopicConfig, TransportKind};
pub use frame::{DecodeError, Frame, FrameType, FRAME_VERSION, HEADER_LEN};
