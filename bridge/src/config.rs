//! TOML configuration for `mmbus-bridge`.
//!
//! ```toml
//! bus = "my-app"
//! base_dir = "/tmp/mmbus"          # optional; mmbus default if omitted
//! origin_id = 1234567890123456789  # optional; random per-launch if omitted
//!
//! [[topics]]
//! name = "events"
//! forward = true
//! receive = true
//!
//! [[peers]]
//! name = "machine-b"
//! endpoint = "machine-b.internal:4443"
//! preshared_key = "..."
//! ```

use serde::Deserialize;
use std::path::{Path, PathBuf};
use thiserror::Error;

#[derive(Debug, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct BridgeConfig {
    /// `Bus` name to attach to locally.
    pub bus: String,

    /// Override `BusConfig.base_dir`.  When `None`, the bridge uses the
    /// crate-default (`/tmp/mmbus` on Unix, `%LOCALAPPDATA%\mmbus` on
    /// Windows).
    #[serde(default)]
    pub base_dir: Option<PathBuf>,

    /// 64-bit identifier this bridge stamps into outbound frames'
    /// `origin_id` field.  When `None`, the binary generates a random
    /// one at startup.  Use a stable explicit value if you want
    /// loop-prevention dedup to survive a bridge restart.
    #[serde(default)]
    pub origin_id: Option<u64>,

    /// Address to bind for incoming peer connections (e.g.
    /// `"0.0.0.0:4443"`).  When `None`, the bridge runs forward-only:
    /// it dials configured peers but doesn't accept any.  Set this on
    /// any bridge that should receive from at least one other peer.
    #[serde(default)]
    pub listen: Option<String>,

    /// Per-peer outbound buffer (in message count).  A slow or
    /// disconnected peer accumulates up to this many encoded frames
    /// before the bridge starts dropping the oldest entry on each
    /// new send.  Default: 4096.  Set lower for memory-constrained
    /// deployments; set higher for bursty workloads where you'd
    /// rather pay memory than drop.
    #[serde(default = "default_peer_buffer_max")]
    pub peer_buffer_max: usize,

    /// Topics this bridge forwards out / accepts in.  Order is
    /// preserved; duplicate names are not deduplicated (the bridge
    /// loops over the list and a duplicate is wasted work, not a bug).
    #[serde(default)]
    pub topics: Vec<TopicConfig>,

    /// Peers in the mesh.  Empty list = receive-only operation
    /// (the bridge could still bind a listen socket; behaviour for that
    /// is the binary's call, not the config's).
    #[serde(default)]
    pub peers: Vec<PeerConfig>,
}

#[derive(Debug, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct TopicConfig {
    pub name: String,
    /// Forward locally-published messages on this topic to all peers
    /// (default: `true`).
    #[serde(default = "default_true")]
    pub forward: bool,
    /// Republish messages received from peers on this topic locally
    /// (default: `true`).
    #[serde(default = "default_true")]
    pub receive: bool,
}

#[derive(Debug, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct PeerConfig {
    /// Operator-friendly label for logs and the peer-hello message.
    /// Not used as an identity — `origin_id` does that.
    pub name: String,
    /// `host:port` to dial.  No URI scheme today (TCP only); QUIC + TLS
    /// in B4 will introduce a scheme prefix.
    pub endpoint: String,
    /// Pre-shared key the bridge sends in `peer-hello`.  Symmetric +
    /// out-of-band-distributed in v1; no PKI, no rotation.
    pub preshared_key: String,
}

fn default_true() -> bool {
    true
}

fn default_peer_buffer_max() -> usize {
    4096
}

#[derive(Debug, Error)]
pub enum ConfigError {
    #[error("could not read config file: {0}")]
    Io(#[from] std::io::Error),

    #[error("invalid TOML in config file: {0}")]
    Toml(#[from] toml::de::Error),

    #[error("bus name must not be empty")]
    EmptyBus,

    #[error("peer endpoint must contain a host:port (got {0:?})")]
    BadEndpoint(String),

    #[error("two peers share the name {0:?} — names must be unique")]
    DuplicatePeerName(String),
}

impl BridgeConfig {
    /// Parse + semantically validate a TOML file from disk.
    pub fn from_path(path: &Path) -> Result<Self, ConfigError> {
        let text = std::fs::read_to_string(path)?;
        Self::from_str(&text)
    }

    /// Parse + semantically validate a TOML config from a string.
    ///
    /// Named `from_str` to match the convention set by `toml::from_str`
    /// / `serde_json::from_str`, even though that collides with the
    /// `std::str::FromStr` trait method.  Implementing `FromStr` itself
    /// would force callers to write `text.parse::<BridgeConfig>()`
    /// which is harder to discover.
    #[allow(clippy::should_implement_trait)]
    pub fn from_str(text: &str) -> Result<Self, ConfigError> {
        let cfg: BridgeConfig = toml::from_str(text)?;
        cfg.validate()?;
        Ok(cfg)
    }

    fn validate(&self) -> Result<(), ConfigError> {
        if self.bus.trim().is_empty() {
            return Err(ConfigError::EmptyBus);
        }
        for peer in &self.peers {
            if !peer.endpoint.contains(':') {
                return Err(ConfigError::BadEndpoint(peer.endpoint.clone()));
            }
        }
        let mut seen = std::collections::HashSet::new();
        for peer in &self.peers {
            if !seen.insert(peer.name.as_str()) {
                return Err(ConfigError::DuplicatePeerName(peer.name.clone()));
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_minimal_config() {
        let cfg = BridgeConfig::from_str(r#"bus = "demo""#).unwrap();
        assert_eq!(cfg.bus, "demo");
        assert!(cfg.topics.is_empty());
        assert!(cfg.peers.is_empty());
        assert_eq!(cfg.base_dir, None);
        assert_eq!(cfg.origin_id, None);
    }

    #[test]
    fn parses_listen_field() {
        let cfg = BridgeConfig::from_str(
            r#"
                bus = "demo"
                listen = "0.0.0.0:4443"
            "#,
        )
        .unwrap();
        assert_eq!(cfg.listen.as_deref(), Some("0.0.0.0:4443"));
    }

    #[test]
    fn peer_buffer_max_defaults_and_overrides() {
        let default = BridgeConfig::from_str(r#"bus = "demo""#).unwrap();
        assert_eq!(default.peer_buffer_max, 4096);
        let overridden = BridgeConfig::from_str(
            r#"
                bus = "demo"
                peer_buffer_max = 32
            "#,
        )
        .unwrap();
        assert_eq!(overridden.peer_buffer_max, 32);
    }

    #[test]
    fn parses_full_config() {
        let text = r#"
            bus = "my-app"
            base_dir = "/var/lib/mmbus"
            origin_id = 1234567890123456789

            [[topics]]
            name = "events"

            [[topics]]
            name = "alerts"
            forward = false
            receive = true

            [[peers]]
            name = "machine-b"
            endpoint = "machine-b.internal:4443"
            preshared_key = "hunter2"
        "#;
        let cfg = BridgeConfig::from_str(text).unwrap();
        assert_eq!(cfg.bus, "my-app");
        assert_eq!(cfg.base_dir, Some(PathBuf::from("/var/lib/mmbus")));
        assert_eq!(cfg.origin_id, Some(1234567890123456789));
        assert_eq!(cfg.topics.len(), 2);
        assert!(cfg.topics[0].forward && cfg.topics[0].receive);
        assert!(!cfg.topics[1].forward);
        assert_eq!(cfg.peers.len(), 1);
        assert_eq!(cfg.peers[0].endpoint, "machine-b.internal:4443");
    }

    #[test]
    fn rejects_empty_bus_name() {
        match BridgeConfig::from_str(r#"bus = """#) {
            Err(ConfigError::EmptyBus) => (),
            other => panic!("expected EmptyBus, got {other:?}"),
        }
    }

    #[test]
    fn rejects_endpoint_without_port() {
        let text = r#"
            bus = "demo"
            [[peers]]
            name = "p1"
            endpoint = "hostnameonly"
            preshared_key = "k"
        "#;
        match BridgeConfig::from_str(text) {
            Err(ConfigError::BadEndpoint(s)) => assert_eq!(s, "hostnameonly"),
            other => panic!("expected BadEndpoint, got {other:?}"),
        }
    }

    #[test]
    fn rejects_duplicate_peer_names() {
        let text = r#"
            bus = "demo"
            [[peers]]
            name = "p"
            endpoint = "a:1"
            preshared_key = "k1"
            [[peers]]
            name = "p"
            endpoint = "b:1"
            preshared_key = "k2"
        "#;
        match BridgeConfig::from_str(text) {
            Err(ConfigError::DuplicatePeerName(s)) => assert_eq!(s, "p"),
            other => panic!("expected DuplicatePeerName, got {other:?}"),
        }
    }

    #[test]
    fn rejects_unknown_fields() {
        // deny_unknown_fields keeps typos from silently being ignored.
        let text = r#"
            bus = "demo"
            bsu = "demo"
        "#;
        let err = BridgeConfig::from_str(text).unwrap_err();
        assert!(matches!(err, ConfigError::Toml(_)), "expected Toml parse error, got {err:?}");
    }

    #[test]
    fn from_path_round_trips_through_disk() {
        use std::io::Write;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("bridge.toml");
        let text = r#"bus = "from-disk""#;
        let mut f = std::fs::File::create(&path).unwrap();
        f.write_all(text.as_bytes()).unwrap();
        drop(f);

        let cfg = BridgeConfig::from_path(&path).unwrap();
        assert_eq!(cfg.bus, "from-disk");
    }
}
