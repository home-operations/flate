package git

import (
	"os"
	"path/filepath"
	"strings"

	"github.com/go-git/go-git/v5"
)

// cachedRevisionFile holds the resolved commit SHA of a fetched slot.
// Written post-clone, BEFORE the caller's ApplyIgnore wipes .git/, so
// future cache-hit checks can validate the slot without re-running
// git.PlainOpen.
const cachedRevisionFile = ".flate-git-revision"

func writeCachedRevision(slot, rev string) error {
	return os.WriteFile(filepath.Join(slot, cachedRevisionFile), []byte(rev), 0o600)
}

func readCachedRevision(slot string) string {
	b, err := os.ReadFile(filepath.Join(slot, cachedRevisionFile)) //nolint:gosec // slot is fetcher-owned cache path
	if err != nil {
		return ""
	}
	return strings.TrimSpace(string(b))
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
