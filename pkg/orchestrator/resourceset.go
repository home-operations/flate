package orchestrator

import (
	"cmp"
	"slices"
	"sync"

	"github.com/home-operations/flate/pkg/manifest"
)

// rsRawSink collects the non-Flux (RawObject) children every ResourceSet
// emits during the run, so the orchestrator can group them under each
// RS's structural-parent Kustomization for `flate build` output — the
// same output the deleted post-run expansion pass produced, now fed
// in-DAG by the RS controller as it renders.
//
// The RS controller calls Record concurrently (one goroutine per RS
// reconcile), so the collection is mutex-guarded. Dedup is deferred to
// commit: rather than first-wins-by-arrival (which would make the winner
// of a cross-RS DedupKey collision depend on goroutine scheduling), every
// entry is recorded with its owning RS id and the global first-wins dedup
// runs at commit time in sorted RS order — replicating the deleted pass's
// deterministic "sorted store order, first-wins" semantics.
type rsRawSink struct {
	mu      sync.Mutex
	entries []rsRawEntry
}

type rsRawEntry struct {
	owner    manifest.NamedResource // the ResourceSet that emitted doc
	parentKS manifest.NamedResource // doc's grouping parent
	key      string                 // resourceset.DedupKey(doc)
	doc      map[string]any
}

// Record appends one RawObject child. owner is the emitting RS, key its
// DedupKey; both feed the deterministic commit-time dedup.
func (s *rsRawSink) Record(owner, parentKS manifest.NamedResource, key string, doc map[string]any) {
	s.mu.Lock()
	s.entries = append(s.entries, rsRawEntry{owner: owner, parentKS: parentKS, key: key, doc: doc})
	s.mu.Unlock()
}

// commit resolves the collected entries into the parent-keyed extension
// map. Entries are sorted by (owner, parentKS, key) so the first RS to
// claim a DedupKey wins deterministically regardless of emit order; a
// name-grouped RS rendering the same child from each namespace variant
// collapses to one doc, matching the deleted post-run pass's output.
func (s *rsRawSink) commit() map[manifest.NamedResource][]map[string]any {
	s.mu.Lock()
	ordered := slices.Clone(s.entries)
	s.mu.Unlock()
	slices.SortFunc(ordered, func(a, b rsRawEntry) int {
		return cmp.Or(
			a.owner.Compare(b.owner),
			a.parentKS.Compare(b.parentKS),
			cmp.Compare(a.key, b.key),
		)
	})
	seen := map[string]struct{}{}
	out := map[manifest.NamedResource][]map[string]any{}
	for _, e := range ordered {
		if e.key == "" {
			continue
		}
		if _, dup := seen[e.key]; dup {
			continue
		}
		seen[e.key] = struct{}{}
		out[e.parentKS] = append(out[e.parentKS], e.doc)
	}
	return out
}
