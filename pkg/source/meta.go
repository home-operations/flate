package source

import (
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/home-operations/flate/pkg/source/atomic"
)

// SlotMetaFile names the Git source slot's atomic metadata sidecar.
const SlotMetaFile = ".flate-meta.json"

// SlotMeta records the resolved Git commit for a source slot.
type SlotMeta struct {
	// Revision is the resolved Git commit SHA.
	Revision string `json:"revision,omitempty"`
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

// UpdateSlotMeta updates a sidecar under the caller-held Git slot lock.
func UpdateSlotMeta(slotDir string, mutate func(*SlotMeta)) error {
	m, _ := ReadSlotMeta(slotDir)
	mutate(&m)
	return WriteSlotMeta(slotDir, m)
}

// ReadSlotMeta returns usable Git metadata. Missing, unreadable or invalid
// sidecars MUST be treated as cache misses.
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
