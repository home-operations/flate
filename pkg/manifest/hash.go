package manifest

import (
	"crypto/sha256"
	"encoding/hex"
)

// SHA256Hex returns the hex-encoded SHA-256 digest of data. It is the
// one-shot full-buffer digest the controllers' render-input
// fingerprints use (kustomizationFingerprint / helmReleaseFingerprint)
// to dedup re-renders whose effective inputs are byte-identical.
//
// This is deliberately NOT for streaming/incremental hashing (e.g. a
// file-tree walk that feeds bytes into a running sha256.Hash) — those
// callers keep their own hasher.
func SHA256Hex(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}
