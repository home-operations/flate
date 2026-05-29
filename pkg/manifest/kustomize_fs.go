package manifest

import (
	"os"
	"path/filepath"
	"sync"

	yaml "go.yaml.in/yaml/v4"
)

// ReadKustomizeComponents returns the top-level `components:` field of
// the kustomization file at base (resolved relative to repoRoot).
// Returns nil when the file is missing / unreadable / malformed —
// pure best-effort, as the caller's claim graph is built from the
// union of declared sources and on-disk reads.
//
// Lives here so loader's parent index and change's ownership index
// share the same on-disk reader. Previously each module had its own
// copy; behavior must agree across them or change attribution and
// loader discovery silently disagree on which files belong to which
// Flux Kustomization.
func ReadKustomizeComponents(repoRoot, base string) []string {
	for _, name := range KustomizeBuilderFilenames {
		data, err := os.ReadFile(filepath.Join(repoRoot, base, name)) //nolint:gosec // path composed from known cluster layout
		if err != nil {
			continue
		}
		var doc struct {
			Components []string `yaml:"components"`
		}
		if err := yaml.Unmarshal(data, &doc); err != nil {
			continue
		}
		return doc.Components
	}
	return nil
}

// componentCacheKey identifies one (repoRoot, base) tuple. Both halves
// are included so a single cache can serve consumers that walk the
// same tree from different roots (rare today, but the diff flow
// constructs two orchestrators against sibling checkouts and a
// future shared cache would collide on `base` alone).
type componentCacheKey struct{ repoRoot, base string }

// ComponentCache memoizes ReadKustomizeComponents(repoRoot, base)
// results across multiple consumers in a single Bootstrap. Both
// loader.KSPathPrefixes (called from discovery and the orchestrator's
// orphan/finalize paths) and change.buildOwnership (called once per
// Filter.resolve) walk the same KS list and re-read every
// kustomization.yaml to fold its `components:` entries into the
// claim graph. Without sharing, the file is read 2×N times per
// Bootstrap (N = unique spec.path); a single cache turns that into
// 1×N. Live for one Bootstrap — instantiate fresh per orchestrator
// so on-disk edits between runs (test harnesses, `flate watch`)
// aren't masked.
//
// RWMutex-guarded because the loader and Filter resolve can read
// concurrently if a future caller drives them in parallel; today
// they're serial within Bootstrap but the lock is cheap and the
// contract is hard to revisit later.
type ComponentCache struct {
	mu    sync.RWMutex
	cache map[componentCacheKey][]string
}

// NewComponentCache returns an empty cache. nil is also a valid
// receiver for Get — it falls through to ReadKustomizeComponents with
// no memoization, which lets call sites that don't want caching pass
// nil rather than special-casing the constructor.
func NewComponentCache() *ComponentCache {
	return &ComponentCache{cache: make(map[componentCacheKey][]string)}
}

// Get returns the cached components slice for (repoRoot, base),
// computing and caching it via ReadKustomizeComponents on miss.
// nil-receiver short-circuits to the uncached read so callers that
// hold an optional cache don't need to nil-check around every Get.
//
// The returned slice is the cache's own storage — callers MUST treat
// it as read-only. The cache never mutates entries after insert, so
// concurrent readers see a stable slice.
func (c *ComponentCache) Get(repoRoot, base string) []string {
	if c == nil {
		return ReadKustomizeComponents(repoRoot, base)
	}
	key := componentCacheKey{repoRoot: repoRoot, base: base}
	c.mu.RLock()
	if v, ok := c.cache[key]; ok {
		c.mu.RUnlock()
		return v
	}
	c.mu.RUnlock()
	// Compute outside the lock — ReadKustomizeComponents does an
	// os.ReadFile + yaml.Unmarshal, both of which can block. Holding
	// the write lock across those would serialize every concurrent
	// reader for the duration of a disk read.
	v := ReadKustomizeComponents(repoRoot, base)
	c.mu.Lock()
	// Re-check after acquiring the write lock: another goroutine may
	// have raced ahead and inserted the same key while we were
	// reading from disk. Reuse their slice so the cache holds one
	// canonical entry per key.
	if existing, ok := c.cache[key]; ok {
		c.mu.Unlock()
		return existing
	}
	c.cache[key] = v
	c.mu.Unlock()
	return v
}
