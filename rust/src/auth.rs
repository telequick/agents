//! Agent-transport auth — wire-identical to the engine's HMAC scheme.
//!
//! ```text
//! sign string = "<socket_id>:<channel>[:<channel_data>]"
//! auth token  = "<app_key>:<hmac_sha256_hex(secret, sign_string)>"
//! ```
//! The connection auth is bound to a server-issued nonce (`socket_id`) so a
//! captured signature can't be replayed. Media channel = "media"
//! (channel_data = agent name). Your media app key/secret are server-side
//! credentials.

use hmac::{Hmac, Mac};
use sha2::Sha256;
use subtle::ConstantTimeEq;

pub const MEDIA_CHANNEL: &str = "media";
pub const ERR_APP_KEY_NOT_FOUND: u32 = 4001;
pub const ERR_AUTH_FAILED: u32 = 4009;

/// HMAC-SHA256 -> lowercase hex, byte-identical to the engine's AWS-LC
/// implementation and the Python/TS/Go packages.
pub fn hmac_sha256_hex(key: &str, msg: &str) -> String {
    let mut mac = Hmac::<Sha256>::new_from_slice(key.as_bytes()).expect("hmac accepts any key length");
    mac.update(msg.as_bytes());
    hex::encode(mac.finalize().into_bytes())
}

pub fn sign_string(socket_id: &str, channel: &str, channel_data: &str) -> String {
    if channel_data.is_empty() {
        format!("{socket_id}:{channel}")
    } else {
        format!("{socket_id}:{channel}:{channel_data}")
    }
}

/// Constant-time string compare (matches the engine's `ct_equal`).
pub fn ct_equal(a: &str, b: &str) -> bool {
    a.len() == b.len() && a.as_bytes().ct_eq(b.as_bytes()).into()
}

/// The token "<app_key>:<hmac>" the agent presents on connect.
pub fn make_auth(app_key: &str, app_secret: &str, socket_id: &str, channel: &str, channel_data: &str) -> String {
    format!("{app_key}:{}", hmac_sha256_hex(app_secret, &sign_string(socket_id, channel, channel_data)))
}

pub fn media_auth(app_key: &str, app_secret: &str, socket_id: &str, agent_name: &str) -> String {
    make_auth(app_key, app_secret, socket_id, MEDIA_CHANNEL, agent_name)
}

/// Engine-side check, mirrored for tests and defensive use.
pub fn verify_auth(app_key: &str, app_secret: &str, socket_id: &str, channel: &str, channel_data: &str, presented: &str) -> bool {
    let (key, sig) = presented.split_once(':').unwrap_or((presented, ""));
    if !ct_equal(key, app_key) {
        return false;
    }
    ct_equal(sig, &hmac_sha256_hex(app_secret, &sign_string(socket_id, channel, channel_data)))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// python3 -c "import hmac,hashlib; print(hmac.new(b'k', b'msg', hashlib.sha256).hexdigest())"
    const KNOWN: &str = "bf1a0c1242929b6464a6c0a9ac6298a67e09bd1cd4ef1f182ce0141691fc17a0";

    #[test]
    fn hmac_parity_with_python_and_engine() {
        assert_eq!(hmac_sha256_hex("k", "msg"), KNOWN);
    }

    #[test]
    fn sign_and_verify() {
        assert_eq!(sign_string("n", "media", ""), "n:media");
        assert_eq!(sign_string("n", "media", "bot"), "n:media:bot");
        let tok = media_auth("mk_test", "secret123", "nonce-abc", "salesbot");
        assert!(verify_auth("mk_test", "secret123", "nonce-abc", "media", "salesbot", &tok));
        assert!(!verify_auth("mk_test", "WRONG", "nonce-abc", "media", "salesbot", &tok));
        assert!(!verify_auth("mk_other", "secret123", "nonce-abc", "media", "salesbot", &tok));
    }
}
