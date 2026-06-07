package git

import (
	"time"

	"github.com/go-git/go-git/v5"

	"github.com/home-operations/flate/pkg/source"
)

// The resolved commit SHA of a fetched slot is recorded in the slot's
// source.SlotMeta sidecar (one .flate-meta.json per slot). It is written AFTER
// the caller's ApplyIgnore wipes .git/, so a cache-hit check can validate the
// slot without re-running git.PlainOpen on a tree whose .git/ is gone.

// writeCachedRevision records rev in the slot's meta sidecar (atomic).
func writeCachedRevision(slot, rev string) error {
	return source.WriteSlotMeta(slot, source.SlotMeta{Revision: rev})
}

// readCachedRevision returns the slot's recorded commit SHA, or "" when the
// sidecar is absent/unreadable (a pre-meta.json slot — rebuilt on miss).
func readCachedRevision(slot string) string {
	m, ok := source.ReadSlotMeta(slot)
	if !ok {
		return ""
	}
	return m.Revision
}

// cachedRevisionFresh returns the recorded SHA only when the sidecar was
// written within maxAge — the freshness gate a mutable ref uses to skip a
// network refetch within its reconcile interval.
func cachedRevisionFresh(slot string, maxAge time.Duration) (string, bool) {
	m, ok := source.ReadSlotMetaFresh(slot, maxAge)
	if !ok || m.Revision == "" {
		return "", false
	}
	return m.Revision, true
}

// readResolvedRevision returns the current commit SHA at the worktree.
// Best-effort: returns empty string on any failure. Used post-clone
// (before .git/ is wiped by ApplyIgnore) to capture the resolved SHA
// for the artifact + the cached-revision marker.
func readResolvedRevision(slot string) (string, error) {
	repo, err := git.PlainOpen(slot)
	if err != nil {
		return "", err
	}
	h, err := repo.Head()
	if err != nil {
		return "", err
	}
	return h.Hash().String(), nil
}
