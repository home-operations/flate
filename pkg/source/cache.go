package source

import (
	"cmp"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
)

// Cache manages a content-addressed on-disk directory for fetched
// sources. Each (url, ref) tuple gets its own slot, so multiple revisions
// of the same upstream coexist without clobbering one another.
//
// The cache is safe for concurrent use. A per-slot mutex serializes the
// full fetch-write-read lifecycle on a single slot — two distinct
// source CRs with the same (url, ref) hash to the same slot, and
// without per-slot locking one would observe the other mid-write
// (e.g. read an empty marker, call Reset, wipe the in-progress clone).
// Different slots proceed in parallel.
type Cache struct {
	root string
	mu   sync.Mutex // guards locks
	// locks holds a sync.Mutex per slot path. Lazily created; never
	// reaped — the slot count is bounded by user-declared sources.
	locks map[string]*sync.Mutex
}

// NewCache constructs a Cache rooted at dir. If dir is empty, a
// flate-cache subdirectory under os.TempDir() is used.
func NewCache(dir string) *Cache {
	return &Cache{root: cmp.Or(dir, filepath.Join(os.TempDir(), "flate-cache"))}
}

// slotMu returns the per-slot mutex for path, creating it on first
// access. Caller must NOT hold c.mu.
func (c *Cache) slotMu(path string) *sync.Mutex {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.locks == nil {
		c.locks = make(map[string]*sync.Mutex)
	}
	m, ok := c.locks[path]
	if !ok {
		m = &sync.Mutex{}
		c.locks[path] = m
	}
	return m
}

// Slot returns the path under which (url, ref) should be cached, the
// per-slot release function the caller MUST defer, and an exists flag
// indicating the directory was already populated.
//
// Holding the returned release across the full fetch-and-publish
// lifecycle serializes other fetchers (different CRs, same (url, ref))
// against the in-flight one — see Cache type doc for the race motivation.
func (c *Cache) Slot(url, ref string) (path string, exists bool, release func(), err error) {
	slug := slugifyRepo(url)
	h := sha256.Sum256([]byte(url + "@" + ref))
	hash := hex.EncodeToString(h[:])[:16]
	path = filepath.Join(c.root, slug, hash)

	m := c.slotMu(path)
	m.Lock()
	release = m.Unlock

	info, statErr := os.Stat(path)
	switch {
	case statErr == nil && info.IsDir():
		// Non-empty directory counts as populated. We use the presence
		// of any entry as the indicator so a bare `mkdir` from a prior
		// aborted run doesn't masquerade as a hit.
		f, ferr := os.Open(path) //nolint:gosec // path is a cache slot under our cache root
		if ferr == nil {
			entries, _ := f.Readdirnames(1)
			_ = f.Close()
			exists = len(entries) > 0
		}
		return path, exists, release, nil
	case os.IsNotExist(statErr):
		if mkErr := os.MkdirAll(path, 0o750); mkErr != nil {
			release()
			return "", false, nil, mkErr
		}
		return path, false, release, nil
	default:
		release()
		return "", false, nil, fmt.Errorf("cache slot stat: %w", statErr)
	}
}

// Reset removes a previously allocated slot. Called when a fetch fails
// so retries start clean. The caller is expected to hold the slot
// release (i.e. Reset is called from within the Slot-Acquire critical
// section). This function does NOT take the per-slot lock — doing so
// would self-deadlock — but it does serialize against new Slot
// acquisitions via the absence of a sibling holder.
func (c *Cache) Reset(path string) error {
	if path == "" {
		return nil
	}
	return os.RemoveAll(path)
}

// nonAlnum collapses non-alphanumeric (plus `.-_`) runs into a single
// dash so the resulting slug is fs-safe.
var nonAlnum = regexp.MustCompile(`[^a-zA-Z0-9._-]+`)

// maxSlugLen caps slug length so cache paths stay below typical
// filesystem name limits and remain greppable.
const maxSlugLen = 50

// slugifyRepo reduces a URL to a short, filesystem-safe identifier. It
// preserves the last path segment so cache directories are recognizable
// when poking around manually.
func slugifyRepo(url string) string {
	url = strings.TrimSuffix(url, ".git")
	if idx := strings.LastIndexAny(url, "/:"); idx >= 0 && idx < len(url)-1 {
		url = url[idx+1:]
	}
	url = nonAlnum.ReplaceAllString(url, "-")
	url = strings.Trim(url, "-_.")
	if len(url) > maxSlugLen {
		url = url[:maxSlugLen]
	}
	return cmp.Or(url, "repo")
}
