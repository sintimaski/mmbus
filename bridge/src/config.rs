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

/// Per-peer transport selector.  Used as a TOML enum (lowercase
/// strings) via serde rename_all.
#[derive(Debug, Deserialize, Default, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum TransportKind {
    /// Plain TCP — default; works without any Cargo features enabled.
    #[default]
    Tcp,
    /// QUIC + TLS 1.3 with self-signed peer-pinned certs.  Requires
    /// the bridge to be built with `--features quic`.
    Quic,
}

impl TransportKind {
    pub fn is_tcp(self) -> bool {
        matches!(self, TransportKind::Tcp)
    }
    pub fn is_quic(self) -> bool {
        matches!(self, TransportKind::Quic)
    }
}

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

    /// QUIC listen address (separate from TCP `listen`).  When `None`,
    /// the bridge doesn't accept incoming QUIC connections.  Requires
    /// `--features quic` to actually serve traffic; a config that
    /// sets this without the feature is rejected at startup.
    #[serde(default)]
    pub listen_quic: Option<String>,

    /// Path to the bridge's self-signed QUIC certificate (PEM-encoded).
    /// Created at first start if missing.  Default: `bridge.cert.pem`
    /// next to `base_dir`.
    #[serde(default)]
    pub quic_cert_path: Option<PathBuf>,

    /// Path to the bridge's QUIC private key (PEM-encoded).
    /// Created at first start if missing; chmod 600.  Default:
    /// `bridge.key.pem` next to `base_dir`.
    #[serde(default)]
    pub quic_key_path: Option<PathBuf>,

    /// Worker threads for the bridge's QUIC tokio runtime.  Default 2.
    /// Most workloads don't need to tune this; bump only if you're
    /// driving high QUIC traffic and see the runtime saturating.
    #[serde(default = "default_quic_worker_threads")]
    pub quic_worker_threads: usize,

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
    /// Wire transport for this peer.  Default: TCP.
    #[serde(default)]
    pub transport: TransportKind,
    /// QUIC peer cert fingerprint (`sha256:HEX...`).  Required when
    /// `transport = "quic"`; ignored for TCP.  The bridge pins the
    /// remote's cert to this value (SSH-known_hosts-style trust).
    #[serde(default)]
    pub peer_cert_fingerprint: Option<String>,
}

fn default_true() -> bool {
    true
}

fn default_peer_buffer_max() -> usize {
    4096
}

fn default_quic_worker_threads() -> usize {
    2
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

    #[error(
        "peer {0:?} uses transport=\"quic\" but has no peer_cert_fingerprint — pin the remote's cert"
    )]
    QuicPeerMissingFingerprint(String),

    #[error("peer_cert_fingerprint on {peer:?} must look like \"sha256:HEX\" — got {got:?}")]
    BadCertFingerprint { peer: String, got: String },

    #[error("quic_worker_threads must be >= 1")]
    QuicWorkerThreadsZero,
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
            if peer.transport.is_quic() {
                let fp = peer
                    .peer_cert_fingerprint
                    .as_deref()
                    .ok_or_else(|| ConfigError::QuicPeerMissingFingerprint(peer.name.clone()))?;
                validate_cert_fingerprint(&peer.name, fp)?;
            }
        }
        let mut seen = std::collections::HashSet::new();
        for peer in &self.peers {
            if !seen.insert(peer.name.as_str()) {
                return Err(ConfigError::DuplicatePeerName(peer.name.clone()));
            }
        }
        if self.quic_worker_threads == 0 {
            return Err(ConfigError::QuicWorkerThreadsZero);
        }
        Ok(())
    }
}

/// Loose validation: the fingerprint must start with `sha256:` and have
/// at least one hex byte.  We don't decode here — `transport::quic`
/// will reject a bad-on-decode value at startup.
fn validate_cert_fingerprint(peer: &str, fp: &str) -> Result<(), ConfigError> {
    let rest = match fp.strip_prefix("sha256:") {
        Some(r) => r,
        None => {
            return Err(ConfigError::BadCertFingerprint {
                peer: peer.to_owned(),
                got: fp.to_owned(),
            });
        }
    };
    if rest.is_empty() || !rest.chars().all(|c| c.is_ascii_hexdigit()) {
        return Err(ConfigError::BadCertFingerprint {
            peer: peer.to_owned(),
            got: fp.to_owned(),
        });
    }
    Ok(())
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
    fn quic_peer_requires_fingerprint() {
        let text = r#"
            bus = "demo"
            [[peers]]
            name = "p"
            endpoint = "h:1"
            preshared_key = "k"
            transport = "quic"
        "#;
        match BridgeConfig::from_str(text) {
            Err(ConfigError::QuicPeerMissingFingerprint(s)) => assert_eq!(s, "p"),
            other => panic!("expected QuicPeerMissingFingerprint, got {other:?}"),
        }
    }

    #[test]
    fn quic_peer_with_good_fingerprint_parses() {
        let text = r#"
            bus = "demo"
            [[peers]]
            name = "p"
            endpoint = "h:1"
            preshared_key = "k"
            transport = "quic"
            peer_cert_fingerprint = "sha256:DEADBEEF"
        "#;
        let cfg = BridgeConfig::from_str(text).unwrap();
        assert_eq!(cfg.peers[0].transport, TransportKind::Quic);
        assert_eq!(
            cfg.peers[0].peer_cert_fingerprint.as_deref(),
            Some("sha256:DEADBEEF")
        );
    }

    #[test]
    fn quic_peer_with_bad_fingerprint_rejected() {
        for fp in ["sha256:XYZ", "md5:abc", "no-prefix", "sha256:"] {
            let text = format!(
                r#"
                    bus = "demo"
                    [[peers]]
                    name = "p"
                    endpoint = "h:1"
                    preshared_key = "k"
                    transport = "quic"
                    peer_cert_fingerprint = "{fp}"
                "#,
            );
            match BridgeConfig::from_str(&text) {
                Err(ConfigError::BadCertFingerprint { peer, got }) => {
                    assert_eq!(peer, "p");
                    assert_eq!(got, fp);
                }
                other => panic!("fp={fp:?} expected BadCertFingerprint, got {other:?}"),
            }
        }
    }

    #[test]
    fn quic_listen_and_paths_parse() {
        let text = r#"
            bus = "demo"
            listen_quic = "0.0.0.0:4443"
            quic_cert_path = "/etc/mmbus/cert.pem"
            quic_key_path = "/etc/mmbus/key.pem"
            quic_worker_threads = 4
        "#;
        let cfg = BridgeConfig::from_str(text).unwrap();
        assert_eq!(cfg.listen_quic.as_deref(), Some("0.0.0.0:4443"));
        assert_eq!(
            cfg.quic_cert_path.as_deref(),
            Some(Path::new("/etc/mmbus/cert.pem"))
        );
        assert_eq!(
            cfg.quic_key_path.as_deref(),
            Some(Path::new("/etc/mmbus/key.pem"))
        );
        assert_eq!(cfg.quic_worker_threads, 4);
    }

    #[test]
    fn quic_worker_threads_zero_rejected() {
        let text = r#"
            bus = "demo"
            quic_worker_threads = 0
        "#;
        match BridgeConfig::from_str(text) {
            Err(ConfigError::QuicWorkerThreadsZero) => (),
            other => panic!("expected QuicWorkerThreadsZero, got {other:?}"),
        }
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
