//! QUIC transport for `mmbus-bridge` (feature `quic`).
//!
//! Stage B4b-2 ships the outbound half: cert generation / loading,
//! a quinn client endpoint, a rustls verifier that pins the peer's
//! cert by SHA-256 fingerprint (SSH known_hosts pattern), and an
//! async forwarder that pumps a `queue::Receiver<Vec<u8>>` over a
//! single bidirectional QUIC stream.
//!
//! The accept-side listener lands in B4b-3 — it reuses the cert /
//! verifier / runtime plumbing built here.

use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer};
use rustls::DigitallySignedStruct;

use crate::queue;

/// ALPN identifier for the bridge's QUIC streams.  Peers that don't
/// advertise this ALPN are rejected during the TLS handshake.
const ALPN_BRIDGE: &[u8] = b"mmbus-bridge/1";

/// Self-signed identity (cert + private key + SHA-256 fingerprint of
/// the DER-encoded cert).  Constructed once at bridge startup.
#[derive(Debug, Clone)]
pub struct Identity {
    pub cert_der: Vec<u8>,
    pub key_der: Vec<u8>,
    pub fingerprint: String,
}

impl Identity {
    /// Generate a fresh self-signed identity for the bridge.  CN is
    /// `mmbus-bridge`; SAN includes `localhost` so loopback test
    /// configs work without extra params.
    pub fn generate() -> Result<Self, QuicError> {
        let key_pair = rcgen::KeyPair::generate().map_err(QuicError::Rcgen)?;
        let mut params = rcgen::CertificateParams::new(vec![
            "mmbus-bridge".to_owned(),
            "localhost".to_owned(),
        ])
        .map_err(QuicError::Rcgen)?;
        params.distinguished_name = rcgen::DistinguishedName::new();
        params
            .distinguished_name
            .push(rcgen::DnType::CommonName, "mmbus-bridge");
        let cert = params.self_signed(&key_pair).map_err(QuicError::Rcgen)?;
        let cert_der = cert.der().to_vec();
        let key_der = key_pair.serialize_der();
        let fingerprint = fingerprint_der(&cert_der);
        Ok(Self { cert_der, key_der, fingerprint })
    }

    /// Load identity from on-disk DER files.  Returns the
    /// recomputed fingerprint alongside the loaded bytes.
    pub fn load(cert_path: &Path, key_path: &Path) -> Result<Self, QuicError> {
        let cert_der = std::fs::read(cert_path)
            .map_err(|e| QuicError::IdentityIo { path: cert_path.to_owned(), source: e })?;
        let key_der = std::fs::read(key_path)
            .map_err(|e| QuicError::IdentityIo { path: key_path.to_owned(), source: e })?;
        let fingerprint = fingerprint_der(&cert_der);
        Ok(Self { cert_der, key_der, fingerprint })
    }

    /// Persist identity to disk (DER format).  Key file is chmod 600
    /// on Unix; on Windows the default ACL applies.
    pub fn save(&self, cert_path: &Path, key_path: &Path) -> Result<(), QuicError> {
        std::fs::write(cert_path, &self.cert_der)
            .map_err(|e| QuicError::IdentityIo { path: cert_path.to_owned(), source: e })?;
        std::fs::write(key_path, &self.key_der)
            .map_err(|e| QuicError::IdentityIo { path: key_path.to_owned(), source: e })?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mut perms = std::fs::metadata(key_path)
                .map_err(|e| QuicError::IdentityIo { path: key_path.to_owned(), source: e })?
                .permissions();
            perms.set_mode(0o600);
            std::fs::set_permissions(key_path, perms)
                .map_err(|e| QuicError::IdentityIo { path: key_path.to_owned(), source: e })?;
        }
        Ok(())
    }
}

/// Load (or generate + persist) the bridge's QUIC identity.
pub fn gen_or_load_identity(
    cert_path: &Path,
    key_path: &Path,
) -> Result<Identity, QuicError> {
    if cert_path.exists() && key_path.exists() {
        Identity::load(cert_path, key_path)
    } else {
        if let Some(parent) = cert_path.parent() {
            std::fs::create_dir_all(parent).map_err(|e| QuicError::IdentityIo {
                path: parent.to_owned(),
                source: e,
            })?;
        }
        let id = Identity::generate()?;
        id.save(cert_path, key_path)?;
        Ok(id)
    }
}

/// Compute `"sha256:HEX..."` for a DER-encoded cert.  Hex is
/// uppercase, matches the config's loose validator.  Uses ring
/// (already in the tree via rustls) for the digest.
pub fn fingerprint_der(der: &[u8]) -> String {
    use ring::digest;
    use std::fmt::Write;
    let digest = digest::digest(&digest::SHA256, der);
    let mut s = String::with_capacity("sha256:".len() + 64);
    s.push_str("sha256:");
    for b in digest.as_ref() {
        let _ = write!(s, "{:02X}", b);
    }
    s
}

// ── rustls verifier: pin one cert by SHA-256 fingerprint ──────────────────────

/// Server-cert verifier that accepts exactly one cert (matched by
/// SHA-256 fingerprint).  Skips name + chain checks — the only
/// trust anchor IS this fingerprint.
#[derive(Debug)]
pub struct PinnedFingerprintVerifier {
    pub expected_fp: String,
    /// Supported signature schemes.  Cached at construction time.
    pub schemes: Vec<rustls::SignatureScheme>,
}

impl PinnedFingerprintVerifier {
    pub fn new(expected_fp: String) -> Self {
        Self {
            expected_fp,
            schemes: rustls::crypto::ring::default_provider()
                .signature_verification_algorithms
                .supported_schemes(),
        }
    }
}

impl rustls::client::danger::ServerCertVerifier for PinnedFingerprintVerifier {
    fn verify_server_cert(
        &self,
        end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name: &rustls::pki_types::ServerName<'_>,
        _ocsp_response: &[u8],
        _now: rustls::pki_types::UnixTime,
    ) -> Result<rustls::client::danger::ServerCertVerified, rustls::Error> {
        let actual = fingerprint_der(end_entity.as_ref());
        // Constant-time compare on the hex string is overkill (the
        // fingerprint isn't a secret) but matches good hygiene.
        if !ct_eq(actual.as_bytes(), self.expected_fp.as_bytes()) {
            return Err(rustls::Error::General(format!(
                "QUIC cert pin mismatch: expected {}, got {}",
                self.expected_fp, actual
            )));
        }
        Ok(rustls::client::danger::ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        _message: &[u8],
        _cert: &CertificateDer<'_>,
        _dss: &DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        // TLS 1.2 isn't enabled for our endpoints; this should never
        // be called, but the trait requires an impl.
        Err(rustls::Error::PeerIncompatible(
            rustls::PeerIncompatible::Tls12NotOfferedOrEnabled,
        ))
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls13_signature(
            message,
            cert,
            dss,
            &rustls::crypto::ring::default_provider().signature_verification_algorithms,
        )
    }

    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
        self.schemes.clone()
    }
}

fn ct_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let mut diff = 0u8;
    for (x, y) in a.iter().zip(b.iter()) {
        diff |= x ^ y;
    }
    diff == 0
}

// ── quinn configs ─────────────────────────────────────────────────────────────

/// Build a quinn ClientConfig that pins the remote cert by
/// fingerprint, requires TLS 1.3, and advertises the bridge ALPN.
pub fn client_config_pinning(
    expected_fp: String,
) -> Result<quinn::ClientConfig, QuicError> {
    let crypto = rustls::ClientConfig::builder_with_provider(Arc::new(
        rustls::crypto::ring::default_provider(),
    ))
    .with_protocol_versions(&[&rustls::version::TLS13])
    .map_err(QuicError::Rustls)?
    .dangerous()
    .with_custom_certificate_verifier(Arc::new(PinnedFingerprintVerifier::new(expected_fp)))
    .with_no_client_auth();
    let mut crypto = crypto;
    crypto.alpn_protocols = vec![ALPN_BRIDGE.to_vec()];
    let cfg = quinn::ClientConfig::new(Arc::new(
        quinn::crypto::rustls::QuicClientConfig::try_from(crypto)
            .map_err(|e| QuicError::Quinn(format!("client config: {e}")))?,
    ));
    Ok(cfg)
}

/// Build a quinn ServerConfig from a self-signed identity.  No client
/// auth — PSK on the first frame is what authenticates the peer.
pub fn server_config_from_identity(
    id: &Identity,
) -> Result<quinn::ServerConfig, QuicError> {
    let cert = CertificateDer::from(id.cert_der.clone());
    let key = PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(id.key_der.clone()));
    let mut crypto = rustls::ServerConfig::builder_with_provider(Arc::new(
        rustls::crypto::ring::default_provider(),
    ))
    .with_protocol_versions(&[&rustls::version::TLS13])
    .map_err(QuicError::Rustls)?
    .with_no_client_auth()
    .with_single_cert(vec![cert], key)
    .map_err(QuicError::Rustls)?;
    crypto.alpn_protocols = vec![ALPN_BRIDGE.to_vec()];
    let cfg = quinn::ServerConfig::with_crypto(Arc::new(
        quinn::crypto::rustls::QuicServerConfig::try_from(crypto)
            .map_err(|e| QuicError::Quinn(format!("server config: {e}")))?,
    ));
    Ok(cfg)
}

// ── outbound forwarder ────────────────────────────────────────────────────────

/// Async forwarder: connect to `endpoint` with cert-pinning + PSK in
/// the hello, then pump `rx` into a single bidirectional QUIC stream.
/// Reconnects with exponential backoff on disconnect.
pub async fn outbound_main(
    peer_name: String,
    endpoint_str: String,
    server_name: String,
    pinned_fp: String,
    hello_bytes: Vec<u8>,
    mut rx: tokio::sync::mpsc::Receiver<Vec<u8>>,
    shutdown: Arc<AtomicBool>,
) {
    let mut backoff = Duration::from_millis(50);
    let max_backoff = Duration::from_secs(1);

    while !shutdown.load(Ordering::Acquire) {
        let client_config = match client_config_pinning(pinned_fp.clone()) {
            Ok(c) => c,
            Err(e) => {
                eprintln!(
                    "bridge: QUIC peer {peer_name:?} could not build client config: {e}; aborting forwarder"
                );
                return;
            }
        };
        let socket_addr: SocketAddr = match endpoint_str.parse() {
            Ok(a) => a,
            Err(e) => {
                eprintln!(
                    "bridge: QUIC peer {peer_name:?} has unparseable endpoint {endpoint_str:?}: {e}"
                );
                return;
            }
        };

        let bind_addr: SocketAddr = if socket_addr.is_ipv6() {
            "[::]:0".parse().unwrap()
        } else {
            "0.0.0.0:0".parse().unwrap()
        };
        let mut endpoint = match quinn::Endpoint::client(bind_addr) {
            Ok(e) => e,
            Err(e) => {
                eprintln!(
                    "bridge: QUIC peer {peer_name:?} could not bind client endpoint: {e}; retrying in {backoff:?}"
                );
                sleep_interruptible(backoff, &shutdown).await;
                backoff = (backoff * 2).min(max_backoff);
                continue;
            }
        };
        endpoint.set_default_client_config(client_config);

        let conn = match endpoint.connect(socket_addr, &server_name) {
            Ok(c) => match c.await {
                Ok(c) => c,
                Err(e) => {
                    eprintln!(
                        "bridge: QUIC peer {peer_name:?} connect failed ({e}); retrying in {backoff:?}"
                    );
                    sleep_interruptible(backoff, &shutdown).await;
                    backoff = (backoff * 2).min(max_backoff);
                    continue;
                }
            },
            Err(e) => {
                eprintln!(
                    "bridge: QUIC peer {peer_name:?} connect spawn failed ({e}); aborting forwarder"
                );
                return;
            }
        };
        backoff = Duration::from_millis(50);
        eprintln!("bridge: QUIC peer {peer_name:?} connected at {endpoint_str}");

        if let Err(e) =
            run_quic_connection(&peer_name, conn, &hello_bytes, &mut rx, &shutdown).await
        {
            eprintln!("bridge: QUIC peer {peer_name:?} disconnected: {e}");
        }
    }
}

async fn run_quic_connection(
    peer_name: &str,
    conn: quinn::Connection,
    hello_bytes: &[u8],
    rx: &mut tokio::sync::mpsc::Receiver<Vec<u8>>,
    shutdown: &AtomicBool,
) -> Result<(), QuicError> {
    let (mut send, _recv) = conn
        .open_bi()
        .await
        .map_err(|e| QuicError::Quinn(format!("open_bi: {e}")))?;

    send.write_all(hello_bytes)
        .await
        .map_err(|e| QuicError::Quinn(format!("write hello: {e}")))?;

    loop {
        if shutdown.load(Ordering::Acquire) {
            let _ = send.finish();
            return Ok(());
        }
        match tokio::time::timeout(Duration::from_millis(200), rx.recv()).await {
            Ok(Some(bytes)) => {
                send.write_all(&bytes)
                    .await
                    .map_err(|e| QuicError::Quinn(format!("write frame: {e}")))?;
            }
            Ok(None) => {
                // Channel closed — shutdown.
                eprintln!("bridge: QUIC peer {peer_name:?} channel closed; finishing stream");
                let _ = send.finish();
                return Ok(());
            }
            Err(_elapsed) => {
                // Timeout — re-check shutdown.
            }
        }
    }
}

async fn sleep_interruptible(total: Duration, shutdown: &AtomicBool) {
    let step = Duration::from_millis(50);
    let mut left = total;
    while left > Duration::ZERO {
        if shutdown.load(Ordering::Acquire) {
            return;
        }
        let chunk = left.min(step);
        tokio::time::sleep(chunk).await;
        left = left.saturating_sub(chunk);
    }
}

// ── sync↔async bridge ────────────────────────────────────────────────────────

/// Drain a sync `queue::Receiver<Vec<u8>>` into a tokio mpsc.  Runs
/// on a dedicated std::thread so the tokio runtime never sees
/// blocking ops.  Returns when shutdown is set or the queue closes.
pub fn spawn_queue_bridge(
    queue_rx: queue::Receiver<Vec<u8>>,
    tokio_tx: tokio::sync::mpsc::Sender<Vec<u8>>,
    shutdown: Arc<AtomicBool>,
) -> std::thread::JoinHandle<()> {
    std::thread::spawn(move || {
        while !shutdown.load(Ordering::Acquire) {
            match queue_rx.recv_timeout(Duration::from_millis(200)) {
                Some(bytes) => {
                    if tokio_tx.blocking_send(bytes).is_err() {
                        // tokio side closed — exit.
                        return;
                    }
                }
                None => {
                    if shutdown.load(Ordering::Acquire) {
                        return;
                    }
                    // Timeout — re-loop.
                }
            }
        }
    })
}

// ── errors ────────────────────────────────────────────────────────────────────

#[derive(Debug, thiserror::Error)]
pub enum QuicError {
    #[error("rcgen: {0}")]
    Rcgen(#[from] rcgen::Error),

    #[error("rustls: {0}")]
    Rustls(rustls::Error),

    #[error("quinn: {0}")]
    Quinn(String),

    #[error("identity I/O at {path:?}: {source}")]
    IdentityIo { path: PathBuf, source: std::io::Error },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fingerprint_format() {
        let fp = fingerprint_der(b"hello");
        assert!(fp.starts_with("sha256:"));
        let hex = fp.strip_prefix("sha256:").unwrap();
        assert_eq!(hex.len(), 64, "SHA-256 hex = 32 bytes = 64 hex chars");
        assert!(hex.chars().all(|c| c.is_ascii_hexdigit()));
    }

    #[test]
    fn identity_round_trip_through_disk() {
        let tmp = tempfile::tempdir().unwrap();
        let cert = tmp.path().join("test.cert.der");
        let key = tmp.path().join("test.key.der");

        // gen_or_load creates + writes on first call.
        let a = gen_or_load_identity(&cert, &key).expect("generate");
        assert!(cert.exists());
        assert!(key.exists());

        // Second call reads the same files; fingerprint must match.
        let b = gen_or_load_identity(&cert, &key).expect("reload");
        assert_eq!(a.fingerprint, b.fingerprint);
        assert_eq!(a.cert_der, b.cert_der);
        assert_eq!(a.key_der, b.key_der);
    }

    #[test]
    fn pinned_verifier_accepts_matching_fingerprint() {
        let id = Identity::generate().unwrap();
        let v = PinnedFingerprintVerifier::new(id.fingerprint.clone());
        let cert = CertificateDer::from(id.cert_der.clone());
        let name = rustls::pki_types::ServerName::try_from("mmbus-bridge").unwrap();
        let result = rustls::client::danger::ServerCertVerifier::verify_server_cert(
            &v,
            &cert,
            &[],
            &name,
            &[],
            rustls::pki_types::UnixTime::now(),
        );
        assert!(result.is_ok(), "matching fingerprint must verify");
    }

    #[test]
    fn pinned_verifier_rejects_other_fingerprint() {
        let id_a = Identity::generate().unwrap();
        let id_b = Identity::generate().unwrap();
        assert_ne!(id_a.fingerprint, id_b.fingerprint);
        // Verifier pinned to A's fp; presented with B's cert → reject.
        let v = PinnedFingerprintVerifier::new(id_a.fingerprint.clone());
        let cert = CertificateDer::from(id_b.cert_der.clone());
        let name = rustls::pki_types::ServerName::try_from("mmbus-bridge").unwrap();
        let result = rustls::client::danger::ServerCertVerifier::verify_server_cert(
            &v,
            &cert,
            &[],
            &name,
            &[],
            rustls::pki_types::UnixTime::now(),
        );
        assert!(result.is_err(), "fingerprint mismatch must reject");
    }
}
