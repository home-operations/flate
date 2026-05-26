package oci

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"regexp"
	"strings"

	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source/atomic"
)

// cachedDigestFile is the slot-relative path where flate records the
// resolved digest of an OCIRepository pull. Used to re-verify on cache
// hit when spec.verify is configured.
const cachedDigestFile = ".flate-digest"

// verifyMarkerFile records the cosign verify-policy fingerprint that
// the slot's cached digest was last validated against. When the
// current spec.verify hashes to the same value, the cache-hit path
// can skip the re-verify roundtrip — restoring flate's offline
// promise for sources with verify configured. A missing or
// mismatched marker forces re-verify (covering policy changes,
// pre-marker slots, and tampered caches).
const verifyMarkerFile = ".flate-verified"

// digestRE matches a well-formed OCI content digest:
// "<algorithm>:<hex>" where the hex side is at least 32 chars (sha256
// truncated, the OCI spec minimum). Catches torn writes where the
// previous run died mid-WriteFile and left a partial digest string —
// rather than passing the partial to cosign and getting a misleading
// "signature not found" error, treat it as a missing marker.
var digestRE = regexp.MustCompile(`^[a-z0-9]+:[a-fA-F0-9]{32,}$`)

// writeCachedDigest persists digest atomically via atomic.WriteFile.
// Without atomicity, a crash mid-write could leave a partial digest
// string that would later mis-read on cache hit and trigger a
// misleading cosign failure on the next reconcile.
func writeCachedDigest(slot, digest string) error {
	return atomic.WriteFile(filepath.Join(slot, cachedDigestFile), []byte(digest), 0o600, true)
}

// readCachedDigest returns the cached digest only when it parses as a
// well-formed OCI content digest. Empty + malformed both return "" so
// the caller's "no marker" branch handles the recovery uniformly.
func readCachedDigest(slot string) string {
	b, err := os.ReadFile(filepath.Join(slot, cachedDigestFile)) //nolint:gosec // slot is fetcher-owned cache path
	if err != nil {
		return ""
	}
	s := strings.TrimSpace(string(b))
	if !digestRE.MatchString(s) {
		return ""
	}
	return s
}

// verifyFingerprint hashes the verify spec into a short identifier
// stored next to the cached digest. Any meaningful change to the
// verify policy (provider, MatchOIDCIdentity, SecretRef) produces a
// different fingerprint and forces re-verify. JSON-marshal the spec
// for a deterministic representation — the upstream Verify struct
// has stable JSON tags.
func verifyFingerprint(v *manifest.OCIRepositoryVerify) string {
	if v == nil {
		return ""
	}
	b, err := json.Marshal(v)
	if err != nil {
		// Marshal-of-typed-struct shouldn't fail; if it does we
		// fingerprint as empty which forces re-verify (safe default).
		return ""
	}
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:16])
}

// writeVerifyMarker persists the verify-policy fingerprint to the
// slot atomically. atomic.WriteFile handles the temp-file + fsync +
// rename dance so a crash mid-write can't leave a partial fingerprint
// that would falsely match.
func writeVerifyMarker(slot, fingerprint string) error {
	return atomic.WriteFile(filepath.Join(slot, verifyMarkerFile), []byte(fingerprint), 0o600, true)
}

// readVerifyMarker returns the cached fingerprint or "" when the
// marker is missing / unreadable. Empty matches the fingerprint of a
// nil Verify; a different non-empty value forces re-verify.
func readVerifyMarker(slot string) string {
	b, err := os.ReadFile(filepath.Join(slot, verifyMarkerFile)) //nolint:gosec // slot is fetcher-owned cache path
	if err != nil {
		return ""
	}
	return strings.TrimSpace(string(b))
}
