package manifest

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	jsonv2 "encoding/json/v2"
	"hash"
	"sync"
)

type fingerprintState struct {
	hash    hash.Hash
	sum     [sha256.Size]byte
	encoded [sha256.Size * 2]byte
}

var fingerprintStates = sync.Pool{New: func() any { return &fingerprintState{hash: sha256.New()} }}

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
	state := fingerprintStates.Get().(*fingerprintState) //nolint:errcheck // The pool contains only fingerprint states.
	defer fingerprintStates.Put(state)
	state.hash.Reset()
	if err := jsonv2.MarshalWrite(state.hash, payload, json.DefaultOptionsV1()); err != nil {
		return ""
	}
	hex.Encode(state.encoded[:], state.hash.Sum(state.sum[:0]))
	return string(state.encoded[:])
}
