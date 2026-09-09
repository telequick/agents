package agents

import "testing"

// Byte-parity with the Python package and the engine's AWS-LC HMAC:
//   python3 -c "import hmac,hashlib; print(hmac.new(b'k', b'msg', hashlib.sha256).hexdigest())"
const knownHmac = "bf1a0c1242929b6464a6c0a9ac6298a67e09bd1cd4ef1f182ce0141691fc17a0"

func TestHmacParity(t *testing.T) {
	if got := HmacSha256Hex("k", "msg"); got != knownHmac {
		t.Fatalf("hmac=%s want %s", got, knownHmac)
	}
}

func TestSignStringAndVerify(t *testing.T) {
	if s := SignString("n", "media", ""); s != "n:media" {
		t.Fatalf("sign=%q", s)
	}
	if s := SignString("n", "media", "bot"); s != "n:media:bot" {
		t.Fatalf("sign=%q", s)
	}
	tok := MediaAuth("mk_test", "secret123", "nonce-abc", "salesbot")
	if !VerifyAuth("mk_test", "secret123", "nonce-abc", "media", "salesbot", tok) {
		t.Fatal("valid token rejected")
	}
	if VerifyAuth("mk_test", "WRONG", "nonce-abc", "media", "salesbot", tok) {
		t.Fatal("wrong secret accepted")
	}
	if VerifyAuth("mk_other", "secret123", "nonce-abc", "media", "salesbot", tok) {
		t.Fatal("wrong key accepted")
	}
}
