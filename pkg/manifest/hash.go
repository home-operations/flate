package manifest

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	jsonv2 "encoding/json/v2"
)

// SHA256Hex returns the hex-encoded SHA-256 identity of buffered bytes.
func SHA256Hex(data []byte) string {
	sum := sha256.Sum256(data)
	var encoded [sha256.Size * 2]byte
	hex.Encode(encoded[:], sum[:])
	return string(encoded[:])
}

// Fingerprint hashes structured content with stable JSON framing. Cache owners
// MUST bypass lookup and publication when marshaling returns an empty key.
func Fingerprint(payload any) string {
	h := sha256.New()
	if err := jsonv2.MarshalWrite(h, payload, json.DefaultOptionsV1()); err != nil {
		return ""
	}
	var sum [sha256.Size]byte
	var encoded [sha256.Size * 2]byte
	hex.Encode(encoded[:], h.Sum(sum[:0]))
	return string(encoded[:])
}
