package source

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"os"
	"sync/atomic"
	"time"
)

var mutableCacheFallbackSeq atomic.Uint64

// MutableCacheKey returns a one-shot cache key for mutable source refs.
// Mutable refs must refresh instead of serving a stale slot, but a failed
// refresh must also leave any previously committed artifact intact.
func MutableCacheKey(base string) string {
	var nonce [16]byte
	if _, err := rand.Read(nonce[:]); err == nil {
		return base + "#mutable:" + hex.EncodeToString(nonce[:])
	}
	return fmt.Sprintf("%s#mutable:%d:%d:%d",
		base, os.Getpid(), time.Now().UnixNano(), mutableCacheFallbackSeq.Add(1))
}
