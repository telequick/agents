package agents

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"strings"
)

// Agent-transport auth — wire-identical to the engine's HMAC scheme:
//
//	sign string = "<socket_id>:<channel>[:<channel_data>]"
//	auth token  = "<app_key>:<hmac_sha256_hex(secret, sign_string)>"
//
// The connection auth is bound to a server-issued nonce (socket_id) so a
// captured signature can't be replayed. Media channel = "media" (channel_data
// = agent name). Your media app key/secret are server-side credentials.
const (
	MediaChannel       = "media"
	ErrAppKeyNotFound  = 4001
	ErrAuthFailed      = 4009
)

// HmacSha256Hex is HMAC-SHA256 -> lowercase hex, byte-identical to the
// engine's AWS-LC implementation and the Python/TS packages.
func HmacSha256Hex(key, msg string) string {
	h := hmac.New(sha256.New, []byte(key))
	h.Write([]byte(msg))
	return hex.EncodeToString(h.Sum(nil))
}

// SignString builds "<socket_id>:<channel>[:<channel_data>]".
func SignString(socketID, channel, channelData string) string {
	if channelData == "" {
		return socketID + ":" + channel
	}
	return socketID + ":" + channel + ":" + channelData
}

// CtEqual is a constant-time string compare (matches the engine's ct_equal).
func CtEqual(a, b string) bool { return hmac.Equal([]byte(a), []byte(b)) }

// MakeAuth returns the token "<app_key>:<hmac>" the agent presents on connect.
func MakeAuth(appKey, appSecret, socketID, channel, channelData string) string {
	return appKey + ":" + HmacSha256Hex(appSecret, SignString(socketID, channel, channelData))
}

// MediaAuth is MakeAuth for the media channel (channel_data = agent name).
func MediaAuth(appKey, appSecret, socketID, agentName string) string {
	return MakeAuth(appKey, appSecret, socketID, MediaChannel, agentName)
}

// VerifyAuth mirrors the engine-side check, for tests and defensive use.
func VerifyAuth(appKey, appSecret, socketID, channel, channelData, presented string) bool {
	key, sig, _ := strings.Cut(presented, ":")
	if !CtEqual(key, appKey) {
		return false
	}
	return CtEqual(sig, HmacSha256Hex(appSecret, SignString(socketID, channel, channelData)))
}
