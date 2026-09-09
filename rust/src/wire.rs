//! TeleQuick agent-transport wire protocol — byte-identical to the Python,
//! TypeScript and Go packages and the engine.
//!
//! * MEDIA — WebTransport datagrams: `version(1) type(1) call_id(2,BE) seq(2,BE) | payload`
//! * CONTROL — one bidi stream of newline-delimited JSON envelopes `{"op":...}`
//!
//! Pure (no I/O) so the framing is unit-tested without a socket.

use serde_json::{Map, Value};

pub const WIRE_VERSION: u8 = 1;
pub const DG_AUDIO: u8 = 1;
const HEADER_LEN: usize = 6;

/// One media frame on the wire.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Datagram {
    pub call_id: u16,
    pub seq: u16,
    pub payload: Vec<u8>,
    pub kind: u8,
    pub version: u8,
}

#[derive(Debug, thiserror::Error)]
pub enum WireError {
    #[error("short datagram: {0} bytes")]
    Short(usize),
    #[error("unsupported wire version {0}")]
    Version(u8),
}

pub fn make_datagram(call_id: u16, seq: u16, payload: Vec<u8>) -> Datagram {
    Datagram { call_id, seq, payload, kind: DG_AUDIO, version: WIRE_VERSION }
}

pub fn encode_datagram(dg: &Datagram) -> Vec<u8> {
    let mut out = Vec::with_capacity(HEADER_LEN + dg.payload.len());
    out.push(dg.version);
    out.push(dg.kind);
    out.extend_from_slice(&dg.call_id.to_be_bytes());
    out.extend_from_slice(&dg.seq.to_be_bytes());
    out.extend_from_slice(&dg.payload);
    out
}

pub fn decode_datagram(buf: &[u8]) -> Result<Datagram, WireError> {
    if buf.len() < HEADER_LEN {
        return Err(WireError::Short(buf.len()));
    }
    if buf[0] != WIRE_VERSION {
        return Err(WireError::Version(buf[0]));
    }
    Ok(Datagram {
        version: buf[0],
        kind: buf[1],
        call_id: u16::from_be_bytes([buf[2], buf[3]]),
        seq: u16::from_be_bytes([buf[4], buf[5]]),
        payload: buf[HEADER_LEN..].to_vec(),
    })
}

/// One decoded control envelope.
pub type CtrlMsg = Map<String, Value>;

/// The envelope's `op`, or "" when absent.
pub fn ctrl_op(m: &CtrlMsg) -> &str {
    m.get("op").and_then(Value::as_str).unwrap_or("")
}

/// One newline-terminated control envelope; `op` is serialized first
/// (matches the other SDKs), then `fields`.
pub fn encode_ctrl(op: &str, fields: Map<String, Value>) -> Vec<u8> {
    let mut s = format!("{{\"op\":{}", serde_json::to_string(op).expect("string"));
    if fields.is_empty() {
        s.push('}');
    } else {
        let body = serde_json::to_string(&Value::Object(fields)).expect("object");
        s.push(',');
        s.push_str(&body[1..]); // drop the leading '{'; body already ends with '}'
    }
    s.push('\n');
    s.into_bytes()
}

/// Parse whole envelopes from a growing byte buffer; returns them plus the
/// unconsumed remainder (a partial trailing line to prepend to the next read).
/// Robust to envelopes split across QUIC STREAM frames; malformed lines skip.
pub fn decode_ctrl_stream(buf: &[u8]) -> (Vec<CtrlMsg>, Vec<u8>) {
    let mut msgs = Vec::new();
    let mut rest = buf;
    while let Some(nl) = rest.iter().position(|&b| b == b'\n') {
        let line = &rest[..nl];
        rest = &rest[nl + 1..];
        let line = std::str::from_utf8(line).map(str::trim).unwrap_or("");
        if line.is_empty() {
            continue;
        }
        if let Ok(Value::Object(m)) = serde_json::from_str::<Value>(line) {
            msgs.push(m);
        }
    }
    (msgs, rest.to_vec())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn datagram_round_trip_big_endian() {
        let enc = encode_datagram(&make_datagram(0x1234, 0xabcd, vec![1, 2, 3, 4, 5]));
        assert_eq!(&enc[..6], &[1, 1, 0x12, 0x34, 0xab, 0xcd]);
        let dg = decode_datagram(&enc).unwrap();
        assert_eq!((dg.call_id, dg.seq, dg.payload.as_slice()), (0x1234, 0xabcd, &[1, 2, 3, 4, 5][..]));
        assert!(decode_datagram(&[1, 1, 0]).is_err());
        assert!(decode_datagram(&[9, 1, 0, 0, 0, 0]).is_err());
    }

    #[test]
    fn ctrl_op_first_and_split_stream() {
        let mut f = Map::new();
        f.insert("call_id".into(), 7.into());
        f.insert("caller".into(), "+1".into());
        let b = encode_ctrl("job_assign", f);
        assert!(b.starts_with(b"{\"op\":\"job_assign\","));
        assert!(b.ends_with(b"}\n"));
        assert_eq!(encode_ctrl("ping", Map::new()), b"{\"op\":\"ping\"}\n");
        let (m1, rem) = decode_ctrl_stream(&b[..10]);
        assert!(m1.is_empty() && rem.len() == 10);
        let mut joined = rem;
        joined.extend_from_slice(&b[10..]);
        let (m2, rem2) = decode_ctrl_stream(&joined);
        assert_eq!(m2.len(), 1);
        assert_eq!(ctrl_op(&m2[0]), "job_assign");
        assert_eq!(m2[0]["call_id"], 7);
        assert!(rem2.is_empty());
    }
}
