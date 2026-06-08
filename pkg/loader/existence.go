package loader

import (
	"maps"
	"sync"

	"github.com/home-operations/flate/pkg/manifest"
)

// ExistenceIndex records "object with this id was parsed from this
// file" without committing it to the store. Populated by the loader
// under DiscoveryOnly: HelmReleases, sources, ConfigMaps, Secrets,
// and other reconcilable kinds skip AddObject and land here instead.
//
// Three consumers:
//
//   - depwait's missing-dep fallback. A KS that names a CM via
//     substituteFrom blocks on existence; if the CM lives in a sibling
//     KS's spec.path the file is in the index even though the store
//     doesn't have it yet. Lazy-loading from the indexed path unblocks
//     the depwait without forcing the rendered version to materialize
//     first — which is impossible for self-rendering patterns like
//     bjw-s where the parent KS's substituteFrom CM is rendered by
//     that same parent KS.
//
//   - orchestrator orphan promotion. After discovery, any index entry
//     whose file path is not under any KS's spec.path is promoted to
//     the store via AddObject. Standalone CRs (e.g. a loose HR file
//     at repo root with no enclosing KS) keep working through
//     `flate build`.
//
//   - change.Detect sees the file path through the loader's
//     SourceFiles map regardless of whether the object reached the
//     store. The index doesn't duplicate that bookkeeping.
//
// Thread-safe: the loader walks files serially from one goroutine
// today, but lazy-promotion from depwait can be invoked from
// reconcile-worker goroutines. Cheap RW mutex keeps the contract
// clear if either side grows concurrency later.
type ExistenceIndex struct {
	mu      sync.RWMutex
	entries map[manifest.NamedResource]string
}

// NewExistenceIndex returns an empty index ready for Record / Get.
func NewExistenceIndex() *ExistenceIndex {
	return &ExistenceIndex{entries: map[manifest.NamedResource]string{}}
}

// Record associates id with the absolute file path it was parsed
// from. Subsequent Record calls for the same id overwrite — the
// loader's PreferExisting branch handles ordering across multiple
// scan roots before reaching here.
func (i *ExistenceIndex) Record(id manifest.NamedResource, absPath string) {
	if i == nil {
		return
	}
	i.mu.Lock()
	defer i.mu.Unlock()
	i.entries[id] = absPath
}

// Get returns the indexed file path for id and whether the index
// has an entry. The returned path is absolute (whatever Record was
// called with).
func (i *ExistenceIndex) Get(id manifest.NamedResource) (string, bool) {
	if i == nil {
		return "", false
	}
	i.mu.RLock()
	defer i.mu.RUnlock()
	path, ok := i.entries[id]
	return path, ok
}

// All yields a snapshot of every recorded {id, path}. The returned
// map is a fresh copy so callers can iterate without holding the
// lock or worrying about concurrent Record.
func (i *ExistenceIndex) All() map[manifest.NamedResource]string {
	if i == nil {
		return nil
	}
	i.mu.RLock()
	defer i.mu.RUnlock()
	return maps.Clone(i.entries)
}
