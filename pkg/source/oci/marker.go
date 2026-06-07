package oci

import (
	"regexp"
	"time"

	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source"
)

// An OCI slot records its resolved digest and verify-policy fingerprint in the
// slot's source.SlotMeta sidecar (one .flate-meta.json per slot). The digest is
// written as the final step of a successful pull; the verify fingerprint is
// written (read-modify-write, preserving the digest) only after a successful
// verification, so meta.Verified == want always implies the cached content was
// validated under that exact policy — the cache-hit verify-skip can never skip
// an unvalidated slot. Both writers run under the slot lock (cache.Slot
// serializes per key), so the read-modify-write has a single writer.

// digestRE matches a well-formed OCI content digest ("<algorithm>:<hex>", hex
// >= 32 chars). A digest that doesn't match is treated as a missing marker so
// a hand-modified or legacy cache rebuilds rather than feeding a bad digest to
// cosign.
var digestRE = regexp.MustCompile(`^[a-z0-9]+:[a-fA-F0-9]{32,}$`)

// writeCachedDigest records digest in the slot's meta sidecar, preserving any
// existing verify fingerprint.
func writeCachedDigest(slot, digest string) error {
	m, _ := source.ReadSlotMeta(slot)
	m.Digest = digest
	return source.WriteSlotMeta(slot, m)
}

// readCachedDigest returns the slot's recorded digest only when it is
// well-formed; "" otherwise (missing sidecar, legacy slot, or malformed digest).
func readCachedDigest(slot string) string {
	m, ok := source.ReadSlotMeta(slot)
	if !ok || !digestRE.MatchString(m.Digest) {
		return ""
	}
	return m.Digest
}

// cachedDigestFresh returns the recorded digest only when the sidecar was
// written within maxAge and the digest is well-formed.
func cachedDigestFresh(slot string, maxAge time.Duration) (string, bool) {
	m, ok := source.ReadSlotMetaFresh(slot, maxAge)
	if !ok || !digestRE.MatchString(m.Digest) {
		return "", false
	}
	return m.Digest, true
}

// verifyFingerprint hashes the verify spec into a short identifier stored in
// the sidecar. Any meaningful change to the verify policy (provider,
// MatchOIDCIdentity, SecretRef) produces a different fingerprint and forces
// re-verify. JSON-marshal the spec for a deterministic representation.
func verifyFingerprint(v *manifest.OCIRepositoryVerify) string {
	if v == nil {
		return ""
	}
	h, err := source.CacheKeyHash(v, 16)
	if err != nil {
		// Marshal-of-typed-struct shouldn't fail; if it does we fingerprint as
		// empty, which forces re-verify (the safe default).
		return ""
	}
	return h
}

// writeVerifyMarker records the verify-policy fingerprint in the slot's meta
// sidecar, preserving the digest. Called only after a successful verification.
func writeVerifyMarker(slot, fingerprint string) error {
	m, _ := source.ReadSlotMeta(slot)
	m.Verified = fingerprint
	return source.WriteSlotMeta(slot, m)
}

// readVerifyMarker returns the slot's recorded verify fingerprint, or "" when
// the sidecar is absent (forcing re-verify).
func readVerifyMarker(slot string) string {
	m, _ := source.ReadSlotMeta(slot)
	return m.Verified
}
