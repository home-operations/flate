package blob

import (
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/home-operations/flate/pkg/source/atomic"
	"github.com/home-operations/flate/pkg/source/cacheroot"
)

// Refs is a tiny on-disk key→digest lookup table that sits beside the
// CAS blob store. It exists so callers that resolve artifacts by some
// mutable identity tuple (e.g. (repo, chart, version) for a helm
// tarball, or (URL, ref, authID) for a source CR) can persist the
// "this identity currently points at this content" mapping without
// stat-walking the blob store on every lookup.
//
// Each entry is one tiny file at <dir>/<urlEscape(key)> containing
// the hex digest. The choice of one-file-per-key keeps writes atomic
// (os.Rename), avoids parsing a single index file under contention,
// and survives partial writes — a corrupted entry just looks like a
// cache miss.
type Refs struct {
	dir string
	mu  sync.Mutex
}

// NewRefs constructs a Refs table for one category under the supplied
// Layout. category names a stable subdirectory under <root>/refs/
// (e.g. "chart-tarballs") that GC and introspection tooling share with
// the writer. The directory is created lazily on first Put.
func NewRefs(layout cacheroot.Layout, category string) *Refs {
	return &Refs{dir: layout.RefsCategory(category)}
}

// Get reads the digest stored under key, or returns ("", false) when
// the key is unknown. Treats partial or empty entries as misses so a
// torn write doesn't surface as a sentinel.
func (r *Refs) Get(key string) (string, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	path, err := r.pathFor(key)
	if err != nil {
		return "", false
	}
	b, err := os.ReadFile(path) //nolint:gosec // path is built from dir + escaped key
	if err != nil {
		return "", false
	}
	digest := strings.TrimSpace(string(b))
	if digest == "" {
		return "", false
	}
	return digest, true
}

// Put records (key → digest) durably via atomic.WriteFile.
// Concurrent writers to the same key serialize on the Refs mutex;
// different keys proceed in parallel. Overwriting an existing key is
// supported (an upstream tag re-resolved to a new digest) — the
// rename atomically replaces the file.
//
// syncDir=false: refs files are cheap to rebuild on the next
// reconcile (they're just digest lookups), so the fsync barrier is
// not worth the per-render I/O cost.
func (r *Refs) Put(key, digest string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if err := os.MkdirAll(r.dir, 0o750); err != nil {
		return fmt.Errorf("refs dir: %w", err)
	}
	final, err := r.pathFor(key)
	if err != nil {
		return err
	}
	return atomic.WriteFile(final, []byte(digest), 0o600, false)
}

// pathFor URL-escapes key into a single-segment filename. Refuses keys
// that would escape the refs dir after escape — defense in depth; the
// escape itself never produces "..", but a future encoding bug
// shouldn't open path traversal.
func (r *Refs) pathFor(key string) (string, error) {
	safe := url.PathEscape(key)
	if strings.ContainsAny(safe, "/\\") || safe == "" || safe == "." || safe == ".." {
		return "", fmt.Errorf("refs: refusing escaped key %q", safe)
	}
	return filepath.Join(r.dir, safe), nil
}
