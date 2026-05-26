package git

import (
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/go-git/go-git/v5"

	"github.com/home-operations/flate/pkg/source/atomic"
)

// cachedRevisionFile holds the resolved commit SHA of a fetched slot.
// Written post-clone, BEFORE the caller's ApplyIgnore wipes .git/, so
// future cache-hit checks can validate the slot without re-running
// git.PlainOpen.
const cachedRevisionFile = ".flate-git-revision"

// writeCachedRevision persists rev atomically — matches the OCI
// marker's durability shape (atomic.WriteFile with syncDir). A crash
// mid-write would otherwise leave a partial SHA that the next
// reconcile reads back via TrimSpace, surfacing as a torn-but-non-
// empty marker that the cache-hit gate treats as live.
func writeCachedRevision(slot, rev string) error {
	return atomic.WriteFile(filepath.Join(slot, cachedRevisionFile), []byte(rev), 0o600, true)
}

func readCachedRevision(slot string) string {
	b, err := os.ReadFile(filepath.Join(slot, cachedRevisionFile)) //nolint:gosec // slot is fetcher-owned cache path
	if err != nil {
		return ""
	}
	return strings.TrimSpace(string(b))
}

func cachedRevisionFresh(slot string, maxAge time.Duration) (string, bool) {
	if maxAge <= 0 {
		return "", false
	}
	path := filepath.Join(slot, cachedRevisionFile)
	info, err := os.Stat(path)
	if err != nil || time.Since(info.ModTime()) > maxAge {
		return "", false
	}
	b, err := os.ReadFile(path) //nolint:gosec // slot is fetcher-owned cache path
	if err != nil {
		return "", false
	}
	rev := strings.TrimSpace(string(b))
	return rev, rev != ""
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
