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
// The cache is safe for concurrent use; an internal mutex serializes
// allocation so two reconcilers can't race on the same slot path.
type Cache struct {
	root string
	mu   sync.Mutex
}

// NewCache constructs a Cache rooted at dir. If dir is empty, a
// flate-cache subdirectory under os.TempDir() is used.
func NewCache(dir string) *Cache {
	return &Cache{root: cmp.Or(dir, filepath.Join(os.TempDir(), "flate-cache"))}
}

// Root returns the cache root directory.
func (c *Cache) Root() string { return c.root }

// Slot returns the path under which (url, ref) should be cached. The
// returned directory is created if it does not already exist. The
// returned exists flag is true when the directory was already populated.
func (c *Cache) Slot(url, ref string) (path string, exists bool, err error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	slug := slugifyRepo(url)
	h := sha256.Sum256([]byte(url + "@" + ref))
	hash := hex.EncodeToString(h[:])[:16]
	path = filepath.Join(c.root, slug, hash)

	info, statErr := os.Stat(path)
	switch {
	case statErr == nil && info.IsDir():
		// Non-empty directory counts as populated. We use the presence
		// of any entry as the indicator so a bare `mkdir` from a prior
		// aborted run doesn't masquerade as a hit.
		f, err := os.Open(path) //nolint:gosec // path is a cache slot under our cache root
		if err == nil {
			defer func() { _ = f.Close() }()
			entries, _ := f.Readdirnames(1)
			exists = len(entries) > 0
		}
		return path, exists, nil
	case os.IsNotExist(statErr):
		return path, false, os.MkdirAll(path, 0o750)
	default:
		return "", false, fmt.Errorf("cache slot stat: %w", statErr)
	}
}

// Reset removes a previously allocated slot. Called when a fetch fails so
// retries start clean.
//
// Acquires the cache mutex for the same reason Slot does: two fetchers
// for the same (url, ref) hash to the same slot, and a Reset overlapping
// with another fetcher's mid-clone could partially delete the
// in-progress directory. Holding c.mu here serializes Reset against any
// concurrent Slot allocation.
func (c *Cache) Reset(path string) error {
	if path == "" {
		return nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()
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
