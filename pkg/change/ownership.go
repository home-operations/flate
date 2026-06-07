package change

import (
	"strings"

	"github.com/home-operations/flate/pkg/manifest"
)

// ksClaim records a single (Flux Kustomization, path) tuple. A KS may
// claim multiple paths — its own spec.path plus every spec.components
// entry — and several KSes may claim the same path (a shared
// component).
type ksClaim struct {
	id   manifest.NamedResource
	path string // repo-relative, slash-separated, with trailing "/"
}

// ownershipIndex maps changed files to the Flux Kustomizations that
// would re-render in response. Built once per filter resolution.
//
// Lookups memoize their (ownersOf, ancestorsOf) results per file —
// the Filter.resolve BFS calls these functions O(keep) times over
// O(distinct-files) input strings, so without caching the BFS walks
// the full claims slice once per visit and runs at O(K²). The cache
// drops it to O(distinct-files × claims) once + O(1) per BFS step.
//
// Caches are unsynchronized: Filter.resolve is serial within one
// orchestrator construction and the index is never shared across
// resolutions. Each resolve builds a fresh index.
type ownershipIndex struct {
	claims         []ksClaim
	ownersCache    map[string][]manifest.NamedResource
	ancestorsCache map[string][]manifest.NamedResource
}

// buildOwnership records every Flux KS's spec.path, spec.components,
// and any `components:` referenced from the kustomization.yaml at
// spec.path. Sorted longest-prefix first so lookup returns the most
// specific claimant. Kustomization-file reads are memoized via the
// shared *manifest.ComponentCache (when supplied) so KSes sharing a
// spec.path don't re-stat the same disk — AND so the same reads done
// by loader.KSPathPrefixes during discovery are served from cache
// here. nil cache falls back to a per-call local map; behavior is
// identical, just without cross-call sharing.
func buildOwnership(objs ObjectLister, repoRoot string, cache *manifest.ComponentCache) ownershipIndex {
	objList := objs.ListObjects(manifest.KindKustomization)
	kss := make([]*manifest.Kustomization, 0, len(objList))
	for _, obj := range objList {
		if ks, ok := obj.(*manifest.Kustomization); ok {
			kss = append(kss, ks)
		}
	}
	// Construction is single-sourced in manifest.BuildKSClaims so it can't
	// drift from loader's parent index (the two are documented must-agree
	// invariants). This side keeps only the multi-owner ownersOf/ancestorsOf
	// lookup semantics below. (BuildKSClaims gates on-disk component reads on
	// repoRoot != "" — a no-op here since resolve always passes a real root.)
	mclaims := manifest.BuildKSClaims(kss, repoRoot, cache)
	claims := make([]ksClaim, len(mclaims))
	for i, c := range mclaims {
		claims[i] = ksClaim{id: c.ID, path: c.Prefix}
	}
	return ownershipIndex{
		claims:         claims,
		ownersCache:    make(map[string][]manifest.NamedResource),
		ancestorsCache: make(map[string][]manifest.NamedResource),
	}
}

// ownersOf returns every KS that claims the longest-matching prefix
// of file. Multiple KSes are possible when a shared component is in
// play. Results are memoized — see ownershipIndex doc.
func (idx ownershipIndex) ownersOf(file string) []manifest.NamedResource {
	if file == "" {
		return nil
	}
	if cached, ok := idx.ownersCache[file]; ok {
		return cached
	}
	prefixed := file + "/" // so prefix matching catches whole-segment boundaries
	var bestLen int
	var owners []manifest.NamedResource
	for _, c := range idx.claims {
		if len(c.path) < bestLen {
			break // sorted longest-first; nothing shorter can beat
		}
		if !strings.HasPrefix(prefixed, c.path) {
			continue
		}
		if len(c.path) > bestLen {
			bestLen = len(c.path)
			owners = owners[:0]
		}
		owners = append(owners, c.id)
	}
	idx.ownersCache[file] = owners
	return owners
}

// ancestorsOf returns every Kustomization whose claim is a strict
// prefix of file but shorter than the longest match (so excluding
// what ownersOf would return). Used by Filter.resolve to keep
// parent/meta Kustomizations in scope under changed-only mode —
// their render injects spec.patches and postBuild.substituteFrom
// onto children, and skipping them produces undefined-${VAR}
// failures when the leaf renders. See #58. Results are memoized —
// see ownershipIndex doc.
func (idx ownershipIndex) ancestorsOf(file string) []manifest.NamedResource {
	if file == "" {
		return nil
	}
	if cached, ok := idx.ancestorsCache[file]; ok {
		return cached
	}
	prefixed := file + "/"
	var bestLen int
	var ancestors []manifest.NamedResource
	for _, c := range idx.claims {
		if !strings.HasPrefix(prefixed, c.path) {
			continue
		}
		if bestLen == 0 {
			bestLen = len(c.path) // first (longest) match — skip
			continue
		}
		if len(c.path) < bestLen {
			ancestors = append(ancestors, c.id)
		}
	}
	idx.ancestorsCache[file] = ancestors
	return ancestors
}
