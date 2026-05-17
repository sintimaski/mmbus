//! Wire-frame codec for mmbus-bridge.
//!
//! ```text
//! struct Frame {
//!     u32  version       // = FRAME_VERSION (1)
//!     u32  frame_type    // FrameType discriminant
//!     u64  origin_id     // 64-bit random per bridge — loop prevention
//!     u64  origin_seq    // monotonic per (origin_id, topic) — gap detection
//!     u32  topic_len
//!     [u8] topic_bytes
//!     u32  payload_len
//!     [u8] payload_bytes
//! }
//! ```
//!
//! All integers are little-endian.  The encoder appends to a caller-owned
//! `Vec<u8>` (so a single buffer can frame many messages without
//! allocation); the decoder accepts a `&[u8]` and reports bytes consumed
//! so callers can buffer a partial frame and try again later.

use thiserror::Error;

/// Wire-format version. Bumped on any backwards-incompatible change to
/// the [`Frame`] layout.
pub const FRAME_VERSION: u32 = 1;

/// Fixed-size header bytes: version + type + origin_id + origin_seq.
/// Variable-length tails (topic_len + topic, payload_len + payload) come
/// after the header.
pub const HEADER_LEN: usize = 4 + 4 + 8 + 8;

/// Minimum on-the-wire size of a frame: header + two empty length-prefixed
/// fields (topic_len + payload_len, both 4 bytes).
pub const MIN_FRAME_LEN: usize = HEADER_LEN + 4 + 4;

/// Hard cap on encoded frame size.  Mainly protects the decoder from a
/// hostile peer sending an absurd length prefix; legitimate mmbus
/// topics and payloads are bounded by the configured ring slot size
/// (typically 64 KiB) so 16 MiB is plenty of headroom while staying
/// well below `usize::MAX` on 32-bit platforms.
pub const MAX_FRAME_LEN: usize = 16 * 1024 * 1024;

/// Discriminant for [`Frame::frame_type`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u32)]
pub enum FrameType {
    /// Normal message: `topic_bytes` + `payload_bytes` both populated.
    Msg = 0,
    /// Keepalive probe.  Both length-prefixed tails are empty.
    Ping = 1,
    /// Topic-subscribe request.  `topic_bytes` names the desired topic;
    /// `payload_bytes` is empty.  Used by a peer to register interest in
    /// a topic this bridge doesn't forward by default.
    TopicSubscribe = 2,
    /// Handshake on connect.  `topic_bytes` is empty;
    /// `payload_bytes` carries the implementation-defined hello message
    /// (currently: 8-byte origin_id only, but kept extensible).
    PeerHello = 3,
}

impl FrameType {
    fn from_u32(n: u32) -> Result<Self, DecodeError> {
        match n {
            0 => Ok(FrameType::Msg),
            1 => Ok(FrameType::Ping),
            2 => Ok(FrameType::TopicSubscribe),
            3 => Ok(FrameType::PeerHello),
            other => Err(DecodeError::UnknownFrameType(other)),
        }
    }
}

/// One on-the-wire bridge frame.  Owns its topic + payload buffers so
/// the codec hands ownership in and out without lifetime contortions.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Frame {
    pub frame_type: FrameType,
    /// Random 64-bit identifier of the bridge that originated this
    /// message.  Used to drop loops (a bridge ignores frames whose
    /// `origin_id` matches its own).
    pub origin_id: u64,
    /// Monotonic counter per `(origin_id, topic)`.  Receivers can use
    /// gaps to detect drops if a WAL is present.
    pub origin_seq: u64,
    pub topic: Vec<u8>,
    pub payload: Vec<u8>,
}

impl Frame {
    /// Construct a `Msg` frame.
    pub fn msg(origin_id: u64, origin_seq: u64, topic: Vec<u8>, payload: Vec<u8>) -> Self {
        Self { frame_type: FrameType::Msg, origin_id, origin_seq, topic, payload }
    }

    /// Construct a `PeerHello` frame whose payload is the origin_id
    /// of the sender (forward-compatible with longer hello messages).
    pub fn peer_hello(origin_id: u64) -> Self {
        Self {
            frame_type: FrameType::PeerHello,
            origin_id,
            origin_seq: 0,
            topic: Vec::new(),
            payload: origin_id.to_le_bytes().to_vec(),
        }
    }

    /// Construct a `Ping` frame.  All variable fields empty.
    pub fn ping(origin_id: u64) -> Self {
        Self {
            frame_type: FrameType::Ping,
            origin_id,
            origin_seq: 0,
            topic: Vec::new(),
            payload: Vec::new(),
        }
    }

    /// On-the-wire size (`HEADER_LEN + 4 + topic.len() + 4 + payload.len()`).
    pub fn encoded_len(&self) -> usize {
        HEADER_LEN + 4 + self.topic.len() + 4 + self.payload.len()
    }

    /// Append the encoded frame to `out`.
    pub fn encode(&self, out: &mut Vec<u8>) {
        out.extend_from_slice(&FRAME_VERSION.to_le_bytes());
        out.extend_from_slice(&(self.frame_type as u32).to_le_bytes());
        out.extend_from_slice(&self.origin_id.to_le_bytes());
        out.extend_from_slice(&self.origin_seq.to_le_bytes());
        out.extend_from_slice(&(self.topic.len() as u32).to_le_bytes());
        out.extend_from_slice(&self.topic);
        out.extend_from_slice(&(self.payload.len() as u32).to_le_bytes());
        out.extend_from_slice(&self.payload);
    }
}

/// Errors the decoder can surface.  `Incomplete` is the soft error a
/// streaming reader should treat as "buffer more data and retry";
/// everything else is a protocol violation that should drop the
/// connection.
#[derive(Debug, Error)]
pub enum DecodeError {
    /// `buf` is shorter than required; need at least `needed` bytes
    /// total (so the caller can size its read).
    #[error("incomplete frame: have {have} bytes, need at least {needed}")]
    Incomplete { have: usize, needed: usize },

    /// Wire version field doesn't match [`FRAME_VERSION`].
    #[error("unsupported frame version {0} (this build speaks {FRAME_VERSION})")]
    UnsupportedVersion(u32),

    /// `frame_type` discriminant isn't a known [`FrameType`].
    #[error("unknown frame_type {0}")]
    UnknownFrameType(u32),

    /// Encoded length exceeds [`MAX_FRAME_LEN`] — likely a corrupt or
    /// hostile peer.
    #[error("frame size {0} exceeds MAX_FRAME_LEN ({})", MAX_FRAME_LEN)]
    TooLarge(usize),
}

/// Decode one frame from the start of `buf`.  On success returns the
/// parsed frame and the number of bytes consumed.  On
/// `DecodeError::Incomplete` the caller should buffer more data and
/// retry; `needed` is the *minimum* additional bytes required to make
/// further progress.
pub fn decode(buf: &[u8]) -> Result<(Frame, usize), DecodeError> {
    if buf.len() < HEADER_LEN + 4 {
        return Err(DecodeError::Incomplete { have: buf.len(), needed: HEADER_LEN + 4 });
    }
    let version = u32::from_le_bytes(buf[0..4].try_into().unwrap());
    if version != FRAME_VERSION {
        return Err(DecodeError::UnsupportedVersion(version));
    }
    let frame_type = FrameType::from_u32(u32::from_le_bytes(buf[4..8].try_into().unwrap()))?;
    let origin_id = u64::from_le_bytes(buf[8..16].try_into().unwrap());
    let origin_seq = u64::from_le_bytes(buf[16..24].try_into().unwrap());

    let topic_len = u32::from_le_bytes(buf[24..28].try_into().unwrap()) as usize;
    let after_topic = HEADER_LEN + 4 + topic_len;
    let needed_for_payload_len = after_topic + 4;
    if topic_len > MAX_FRAME_LEN {
        return Err(DecodeError::TooLarge(topic_len));
    }
    if buf.len() < needed_for_payload_len {
        return Err(DecodeError::Incomplete {
            have: buf.len(),
            needed: needed_for_payload_len,
        });
    }
    let payload_len =
        u32::from_le_bytes(buf[after_topic..after_topic + 4].try_into().unwrap()) as usize;
    if payload_len > MAX_FRAME_LEN {
        return Err(DecodeError::TooLarge(payload_len));
    }
    let total_len = after_topic + 4 + payload_len;
    if total_len > MAX_FRAME_LEN {
        return Err(DecodeError::TooLarge(total_len));
    }
    if buf.len() < total_len {
        return Err(DecodeError::Incomplete { have: buf.len(), needed: total_len });
    }

    let topic = buf[HEADER_LEN + 4..after_topic].to_vec();
    let payload = buf[after_topic + 4..total_len].to_vec();

    Ok((
        Frame { frame_type, origin_id, origin_seq, topic, payload },
        total_len,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn roundtrip(f: &Frame) {
        let mut buf = Vec::new();
        f.encode(&mut buf);
        assert_eq!(buf.len(), f.encoded_len(), "encoded_len() must match");
        let (got, n) = decode(&buf).expect("decode");
        assert_eq!(n, buf.len(), "decode must consume the whole encoded frame");
        assert_eq!(&got, f);
    }

    #[test]
    fn roundtrip_msg() {
        roundtrip(&Frame::msg(0xDEAD_BEEF, 42, b"events".to_vec(), b"hello".to_vec()));
    }

    #[test]
    fn roundtrip_msg_empty_topic_and_payload() {
        roundtrip(&Frame::msg(0, 0, Vec::new(), Vec::new()));
    }

    #[test]
    fn roundtrip_ping() {
        roundtrip(&Frame::ping(0x1234_5678));
    }

    #[test]
    fn roundtrip_peer_hello() {
        roundtrip(&Frame::peer_hello(0xCAFE_F00D_DEAD_BEEF));
    }

    #[test]
    fn roundtrip_topic_subscribe() {
        roundtrip(&Frame {
            frame_type: FrameType::TopicSubscribe,
            origin_id: 7,
            origin_seq: 0,
            topic: b"sensor.temperature".to_vec(),
            payload: Vec::new(),
        });
    }

    #[test]
    fn roundtrip_two_frames_in_one_buffer() {
        // Decoder must report bytes consumed accurately so a streaming
        // reader can advance and decode the next frame.
        let a = Frame::msg(1, 10, b"t".to_vec(), b"aaa".to_vec());
        let b = Frame::msg(1, 11, b"t".to_vec(), b"bbb".to_vec());
        let mut buf = Vec::new();
        a.encode(&mut buf);
        b.encode(&mut buf);

        let (got_a, n_a) = decode(&buf).unwrap();
        assert_eq!(got_a, a);
        let (got_b, n_b) = decode(&buf[n_a..]).unwrap();
        assert_eq!(got_b, b);
        assert_eq!(n_a + n_b, buf.len());
    }

    #[test]
    fn incomplete_at_header() {
        let f = Frame::msg(1, 1, b"t".to_vec(), b"x".to_vec());
        let mut buf = Vec::new();
        f.encode(&mut buf);
        // Truncate before reaching the first length prefix.
        match decode(&buf[..10]) {
            Err(DecodeError::Incomplete { have: 10, needed }) => {
                assert_eq!(needed, HEADER_LEN + 4);
            }
            other => panic!("expected Incomplete, got {other:?}"),
        }
    }

    #[test]
    fn incomplete_between_topic_and_payload_len() {
        let f = Frame::msg(1, 1, b"long-topic-name".to_vec(), b"x".to_vec());
        let mut buf = Vec::new();
        f.encode(&mut buf);
        let truncated = &buf[..HEADER_LEN + 4 + 5]; // mid-topic
        match decode(truncated) {
            Err(DecodeError::Incomplete { needed, .. }) => {
                assert!(needed > truncated.len(), "needed must exceed have");
            }
            other => panic!("expected Incomplete, got {other:?}"),
        }
    }

    #[test]
    fn incomplete_mid_payload() {
        let f = Frame::msg(1, 1, b"t".to_vec(), b"xxxxxxxxxx".to_vec());
        let mut buf = Vec::new();
        f.encode(&mut buf);
        // Drop the last byte of payload.
        match decode(&buf[..buf.len() - 1]) {
            Err(DecodeError::Incomplete { have, needed }) => {
                assert_eq!(have, buf.len() - 1);
                assert_eq!(needed, buf.len());
            }
            other => panic!("expected Incomplete, got {other:?}"),
        }
    }

    #[test]
    fn rejects_unsupported_version() {
        let mut buf = Vec::new();
        Frame::ping(1).encode(&mut buf);
        // Mangle the version field.
        buf[0..4].copy_from_slice(&999u32.to_le_bytes());
        match decode(&buf) {
            Err(DecodeError::UnsupportedVersion(999)) => (),
            other => panic!("expected UnsupportedVersion(999), got {other:?}"),
        }
    }

    #[test]
    fn rejects_unknown_frame_type() {
        let mut buf = Vec::new();
        Frame::ping(1).encode(&mut buf);
        buf[4..8].copy_from_slice(&42u32.to_le_bytes());
        match decode(&buf) {
            Err(DecodeError::UnknownFrameType(42)) => (),
            other => panic!("expected UnknownFrameType(42), got {other:?}"),
        }
    }

    #[test]
    fn rejects_length_prefix_above_cap() {
        let mut buf = Vec::new();
        Frame::ping(1).encode(&mut buf);
        // Plant a topic_len that exceeds MAX_FRAME_LEN.
        buf[24..28].copy_from_slice(&(MAX_FRAME_LEN as u32 + 1).to_le_bytes());
        match decode(&buf) {
            Err(DecodeError::TooLarge(_)) => (),
            other => panic!("expected TooLarge, got {other:?}"),
        }
    }

    #[test]
    fn min_frame_len_constant_matches_empty_encoding() {
        let f = Frame::ping(0);
        let mut buf = Vec::new();
        f.encode(&mut buf);
        assert_eq!(buf.len(), MIN_FRAME_LEN);
    }
}
