package source

import (
	"crypto/rand"
	"encoding/hex"
)

// MutableCacheKey returns a one-shot cache key for mutable source refs.
// Mutable refs must refresh instead of serving a stale slot, but a failed
// refresh must also leave any previously committed artifact intact.
func MutableCacheKey(base string) string {
	var nonce [16]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		panic("crypto/rand unavailable: " + err.Error())
	}
	return base + "#mutable:" + hex.EncodeToString(nonce[:])
}
