package source

import (
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/home-operations/flate/pkg/source/atomic"
)

// SlotMetaFile is the single structured sidecar a fetched cache slot carries,
// replacing the family of per-fact .flate-* marker files (the git revision, the
// OCI digest, the verify-policy fingerprint). It is `.flate-`-prefixed so it
// never collides with a user file in the materialized source tree, and JSON so
// a new fact is a new field rather than a new file.
const SlotMetaFile = ".flate-meta.json"

// SlotMeta is the content of SlotMetaFile: the metadata a cache-hit needs to
// validate a slot and decide whether work (a refetch, a re-verify) can be
// skipped. Fields are populated by whichever fetcher owns the slot — git sets
// Revision, OCI sets Digest (+ Verified when spec.verify is configured).
type SlotMeta struct {
	// Revision is the resolved git commit SHA (GitRepository slots).
	Revision string `json:"revision,omitempty"`
	// Digest is the resolved OCI content digest (OCIRepository slots).
	Digest string `json:"digest,omitempty"`
	// Verified is the verify-policy fingerprint the slot's content was last
	// validated against; empty when verify is unconfigured. A mismatch on the
	// next reconcile forces re-verify.
	Verified string `json:"verified,omitempty"`

	// ChartVersion, ChartDigest, ChartURL record a HelmRepository HTTP chart
	// resolution (the concrete version, bare-hex tarball sha256, and absolute
	// .tgz URL the repo's index.yaml resolved a chart name+version to). They
	// live on a dedicated "helm-resolve:" slot so a warm run skips the live
	// index.yaml fetch within spec.interval. Named distinctly from the OCI
	// Digest so a slot never aliases the two. ChartDigest may be empty for a
	// digest-less (mutable) index entry.
	ChartVersion string `json:"chartVersion,omitempty"`
	ChartDigest  string `json:"chartDigest,omitempty"`
	ChartURL     string `json:"chartURL,omitempty"`
}

// WriteSlotMeta persists meta into slotDir atomically (temp file + fsync +
// rename + dir sync), so a crash mid-write can never leave a torn sidecar that
// a later run misreads as a live marker.
func WriteSlotMeta(slotDir string, meta SlotMeta) error {
	b, err := json.Marshal(meta)
	if err != nil {
		return err
	}
	return atomic.WriteFile(filepath.Join(slotDir, SlotMetaFile), b, 0o600, true)
}

// UpdateSlotMeta read-modify-writes the slot's sidecar: read the current
// SlotMeta (preserving fields the caller leaves alone), apply mutate, write it
// back. Marker writers run under the slot lock (cache.Slot serializes per key),
// so the RMW has a single writer. Shared by the per-kind fetchers (oci digest +
// verify fingerprint, helmchart chart resolution, git revision) so each records
// its facts without clobbering the others.
func UpdateSlotMeta(slotDir string, mutate func(*SlotMeta)) error {
	m, _ := ReadSlotMeta(slotDir)
	mutate(&m)
	return WriteSlotMeta(slotDir, m)
}

// ReadSlotMeta returns the slot's sidecar. A missing, unreadable, or unparseable
// sidecar yields ok=false — all three mean "no usable marker", so the caller
// rebuilds. Atomic writes guarantee the file is either absent or complete, so a
// parse failure indicates a pre-meta.json slot or external tampering, not a torn
// write.
func ReadSlotMeta(slotDir string) (meta SlotMeta, ok bool) {
	b, err := os.ReadFile(filepath.Join(slotDir, SlotMetaFile)) //nolint:gosec // slotDir is fetcher-owned cache path
	if err != nil {
		return SlotMeta{}, false
	}
	if err := json.Unmarshal(b, &meta); err != nil {
		return SlotMeta{}, false
	}
	return meta, true
}

// ReadSlotMetaFresh returns the sidecar only when it exists and was written
// within maxAge — the freshness gate a mutable ref (branch/tag/semver/HEAD)
// uses to skip a network refetch within its reconcile interval. A single open
// (fstat for mtime, then read) avoids a stat/read TOCTOU on a fetcher-written
// file. maxAge <= 0 disables the gate (always stale).
func ReadSlotMetaFresh(slotDir string, maxAge time.Duration) (meta SlotMeta, ok bool) {
	if maxAge <= 0 {
		return SlotMeta{}, false
	}
	f, err := os.Open(filepath.Join(slotDir, SlotMetaFile)) //nolint:gosec // slotDir is fetcher-owned cache path
	if err != nil {
		return SlotMeta{}, false
	}
	defer func() { _ = f.Close() }()
	info, err := f.Stat()
	if err != nil || time.Since(info.ModTime()) > maxAge {
		return SlotMeta{}, false
	}
	b, err := io.ReadAll(f)
	if err != nil {
		return SlotMeta{}, false
	}
	if err := json.Unmarshal(b, &meta); err != nil {
		return SlotMeta{}, false
	}
	return meta, true
}
