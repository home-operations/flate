package change

import (
	"iter"
	"strings"

	"github.com/home-operations/flate/pkg/manifest"
)

// ownershipIndex maps changed files to the Flux Kustomizations that
// would re-render in response. Built once per filter resolution.
//
// byPrefix groups every claim ID under its slash-terminated prefix
// string. A file is owned/anchored by exactly the claims whose prefix
// is a whole-segment prefix of file+"/", so a lookup enumerates the
// O(depth) directory prefixes of the file and probes byPrefix for each
// — O(depth) map hits instead of an O(claims) linear HasPrefix scan
// (the resolve BFS does this once per distinct file, so the linear
// scan dominated CPU on large repos). Within a group the IDs keep
// claims-slice order (longest-prefix-first build order), so results
// match the previous full-scan order exactly.
//
// Lookups still memoize their (ownersOf, ancestorsOf) results per file
// — the Filter.resolve BFS calls these O(keep) times over
// O(distinct-files) inputs, and the cache turns repeat visits into an
// O(1) map hit.
//
// Caches are unsynchronized: Filter.resolve is serial within one
// orchestrator construction and the index is never shared across
// resolutions. Each resolve builds a fresh index.
type ownershipIndex struct {
	byPrefix       map[string][]manifest.NamedResource
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
	// Group IDs by their slash-terminated prefix. mclaims is sorted
	// longest-prefix-first, so iterating in order keeps each group's IDs
	// in the same relative order the old full-claims scan produced. The
	// index is bounded by the claim count (one map entry per distinct
	// prefix) and lives only for the duration of one resolve.
	byPrefix := make(map[string][]manifest.NamedResource, len(mclaims))
	for _, c := range mclaims {
		byPrefix[c.Prefix] = append(byPrefix[c.Prefix], c.ID)
	}
	return ownershipIndex{
		byPrefix:       byPrefix,
		ownersCache:    make(map[string][]manifest.NamedResource),
		ancestorsCache: make(map[string][]manifest.NamedResource),
	}
}

// memoize returns cache[file], computing and storing it via compute on
// a miss. The empty-file short-circuit is shared by both lookups: an
// empty path has no owner or ancestor.
func memoize(cache map[string][]manifest.NamedResource, file string, compute func() []manifest.NamedResource) []manifest.NamedResource {
	if file == "" {
		return nil
	}
	if cached, ok := cache[file]; ok {
		return cached
	}
	result := compute()
	cache[file] = result
	return result
}

// matchingPrefixes yields the claim group at each slash-terminated,
// whole-segment prefix of file+"/" that has at least one claim, from
// longest to shortest. These are exactly the c.path values
// strings.HasPrefix(file+"/", c.path) would have matched in the old full
// scan: c.path ends in "/", so a match means c.path is file+"/" truncated
// at some "/" boundary. Probing each directory prefix in byPrefix replaces
// the O(claims) HasPrefix loop with O(depth) map hits. Prefix substrings
// share file's backing array, so no per-probe allocation.
func (idx ownershipIndex) matchingPrefixes(file string) iter.Seq[[]manifest.NamedResource] {
	return func(yield func(ids []manifest.NamedResource) bool) {
		prefixed := file + "/"
		// Candidate prefixes are prefixed[:k] for every k where prefixed[k-1]
		// is '/', longest first. The full prefixed (k==len, a claim on file's
		// own directory) is the first candidate; each earlier "/" truncation
		// follows.
		for end := len(prefixed); end > 0; {
			if ids, ok := idx.byPrefix[prefixed[:end]]; ok {
				if !yield(ids) {
					return
				}
			}
			i := strings.LastIndexByte(prefixed[:end-1], '/')
			if i < 0 {
				break
			}
			end = i + 1
		}
	}
}

// ownersOf returns every KS that claims the longest-matching prefix
// of file. Multiple KSes are possible when a shared component is in
// play. Results are memoized — see ownershipIndex doc.
func (idx ownershipIndex) ownersOf(file string) []manifest.NamedResource {
	return memoize(idx.ownersCache, file, func() []manifest.NamedResource {
		for ids := range idx.matchingPrefixes(file) {
			return ids // longest match first — take it and stop
		}
		return nil
	})
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
	return memoize(idx.ancestorsCache, file, func() []manifest.NamedResource {
		var ancestors []manifest.NamedResource
		first := true
		for ids := range idx.matchingPrefixes(file) {
			if first {
				first = false // longest match — skip (it's what ownersOf returns)
				continue
			}
			ancestors = append(ancestors, ids...)
		}
		return ancestors
	})
}
