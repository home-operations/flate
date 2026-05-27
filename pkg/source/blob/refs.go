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
//
// An in-memory cache (sync.Map) sits in front of the disk reads so
// the hot path — a render with N HelmReleases against the same repo
// — doesn't pay a syscall per lookup. The cache is populated by
// every successful Get/Put and invalidated by Put.
type Refs struct {
	dir string
	mu  sync.Mutex // serializes disk-writing Puts
	mem sync.Map   // map[string]string — key → digest
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
//
// Consults the in-memory cache first; a hit avoids the disk read
// entirely. A miss falls through to ReadFile and populates the cache
// via LoadOrStore — if a concurrent Put landed a newer digest into
// mem between our ReadFile (which may have observed the old contents)
// and our cache-fill, the Put's value wins. Without LoadOrStore, this
// Get could overwrite the Put's NEW with the disk-read OLD and poison
// the cache for the rest of the run.
func (r *Refs) Get(key string) (string, bool) {
	if v, ok := r.mem.Load(key); ok {
		digest, _ := v.(string)
		return digest, digest != ""
	}
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
	if v, loaded := r.mem.LoadOrStore(key, digest); loaded {
		if existing, _ := v.(string); existing != "" {
			return existing, true
		}
	}
	return digest, true
}

// Put records (key → digest) durably via atomic.WriteFile.
// Concurrent writers to the same key serialize on the Refs mutex;
// different keys proceed in parallel. Overwriting an existing key is
// supported (an upstream tag re-resolved to a new digest) — the
// rename atomically replaces the file.
//
// Takes the package-level GC shared lock for the duration of the
// write so a concurrent gc.Sweep can't observe a not-yet-written ref
// during mark and then purge its blob during sweep (see gclock.go).
//
// syncDir=false on the underlying atomic write: refs files are cheap
// to rebuild on the next reconcile, so the fsync barrier is not worth
// the per-render I/O cost.
func (r *Refs) Put(key, digest string) error {
	// MkdirAll is idempotent and safe to call without the mutex; moving
	// it outside shrinks the critical section around the atomic rename.
	if err := os.MkdirAll(r.dir, 0o750); err != nil {
		return fmt.Errorf("refs dir: %w", err)
	}
	final, err := r.pathFor(key)
	if err != nil {
		return err
	}
	unlockGC := rLockGC()
	defer unlockGC()
	r.mu.Lock()
	defer r.mu.Unlock()
	if err := atomic.WriteFile(final, []byte(digest), 0o600, false); err != nil {
		return err
	}
	r.mem.Store(key, digest)
	return nil
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
