package oci

import (
	"regexp"
	"time"

	"github.com/home-operations/flate/pkg/source"
)

// An OCI slot records its resolved digest in the slot's source.SlotMeta sidecar
// (one .flate-meta.json per slot), written as the final step of a successful
// pull. The write runs under the slot lock (cache.Slot serializes per key).

// digestRE matches a well-formed OCI content digest ("<algorithm>:<hex>", hex
// >= 32 chars). A digest that doesn't match is treated as a missing marker so
// a hand-modified or legacy cache rebuilds rather than trusting a bad digest.
var digestRE = regexp.MustCompile(`^[a-z0-9]+:[a-fA-F0-9]{32,}$`)

// writeCachedDigest records digest in the slot's meta sidecar, preserving the
// slot's other recorded facts.
func writeCachedDigest(slot, digest string) error {
	return source.UpdateSlotMeta(slot, func(m *source.SlotMeta) { m.Digest = digest })
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
