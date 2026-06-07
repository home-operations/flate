package loader

import (
	"path/filepath"
	"strings"

	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

// NormalizePrefix turns a Kustomization spec.path into a slash-
// terminated repo-relative prefix suitable for HasPrefix matching.
// Applies filepath.ToSlash first so Windows-style spec.path values
// (rare, but possible since the Flux CRD doesn't constrain it)
// normalize to the same shape as loader.SourceFiles entries.
func NormalizePrefix(p string) string {
	p = filepath.ToSlash(p)
	p = strings.TrimPrefix(p, "./")
	return strings.TrimSuffix(p, "/") + "/"
}

// KSPathPrefix pairs a Kustomization id with one of its
// slash-terminated, repo-relative claimed-path prefixes. A KS may
// produce multiple prefixes — one for spec.path plus one per
// spec.components entry, plus any on-disk `components:` referenced
// from the kustomization.yaml living at spec.path. Sharing the same
// ID across multiple prefixes is intentional: parent-index lookup
// returns the longest-matching entry, and a child file inside a
// component directory is correctly attributed to the parent that
// includes that component.
type KSPathPrefix struct {
	ID     manifest.NamedResource
	Prefix string
}

// KSPathPrefixesWithCache returns one or more entries per loaded
// Kustomization with a non-empty spec.path. Each KS contributes:
//
//  1. Its spec.path (always).
//  2. Each spec.components entry (when present, resolved against
//     spec.path).
//  3. Each entry from `components:` declared in the kustomization.yaml
//     at spec.path (when readable from repoRoot; missing or
//     malformed files are silently skipped — pure best-effort, the
//     spec.path entry is enough to keep the index sound).
//
// Entries are sorted by prefix length descending so the first
// HasPrefix match on a given file is the deepest claimant — a child
// file under a parent's component dir wins over the parent's
// spec.path. Previously this function only emitted (1); the new
// (2)+(3) bring loader's parent index in line with change/ownership's
// already-richer attribution, eliminating the false-orphan class
// where a child KS lives inside a parent's component subtree.
//
// repoRoot is the filesystem root the kustomization-file reads
// resolve relative to. Pass "" to skip on-disk component lookup
// entirely (only spec.path + spec.components are recorded).
//
// A shared component cache is threaded in via cache. The orchestrator
// instantiates one cache per Bootstrap and passes it to every consumer
// (discovery's orphan promotion, BuildParentIndexForKindWithCache, the
// orchestrator's finalize detectOrphans, change.buildOwnership) so the
// kustomization.yaml at each spec.path is read once per Bootstrap
// instead of once per consumer. Pass nil to fall back to a per-call
// cache so a single call doesn't re-read the same kustomization.yaml
// across KSes that share a spec.path.
func KSPathPrefixesWithCache(s *store.Store, repoRoot string, cache *manifest.ComponentCache) []KSPathPrefix {
	// Thin adapter over the canonical builder in pkg/manifest — the claim
	// construction (spec.path + spec.components + on-disk components:, the
	// reject/clean resolver, longest-first sort) is single-sourced there so
	// it can't drift from change.buildOwnership. This side keeps only the
	// KSPathPrefix shape and the LongestParent lookup semantics.
	claims := manifest.BuildKSClaims(store.ListAs[*manifest.Kustomization](s, manifest.KindKustomization), repoRoot, cache)
	out := make([]KSPathPrefix, len(claims))
	for i, c := range claims {
		out[i] = KSPathPrefix{ID: c.ID, Prefix: c.Prefix}
	}
	return out
}

// LongestParent returns the deepest KS whose spec.path covers file
// (slash-normalized repo-relative path), excluding self. The second
// return reports whether a parent was found. prefixes is expected to
// be the sorted output of KSPathPrefixesWithCache.
func LongestParent(prefixes []KSPathPrefix, file string, self manifest.NamedResource) (manifest.NamedResource, bool) {
	slashFile := filepath.ToSlash(file)
	for _, p := range prefixes {
		if p.ID == self {
			continue
		}
		if strings.HasPrefix(slashFile, p.Prefix) {
			return p.ID, true
		}
	}
	return manifest.NamedResource{}, false
}

// longestStrictParent is LongestParent restricted to a *strict*
// ancestor: besides skipping self, it skips any candidate whose prefix
// the child itself claims (selfPrefixes). Two Kustomizations pointing
// at the same spec.path directory each claim that directory, so each
// would otherwise match the other as a parent — a mutual edge that
// becomes a depwait deadlock in collectDeps (both wait on the other to
// reach Ready, then both time out). Peers sharing a directory don't
// render each other, so neither is the other's parent. selfPrefixes is
// the set of prefixes the child contributes to KSPathPrefixesWithCache; nil/empty
// (e.g. a HelmRelease child, which contributes none) reduces to
// LongestParent.
func longestStrictParent(prefixes []KSPathPrefix, file string, self manifest.NamedResource, selfPrefixes map[string]struct{}) (manifest.NamedResource, bool) {
	slashFile := filepath.ToSlash(file)
	for _, p := range prefixes {
		if p.ID == self {
			continue
		}
		if _, claimed := selfPrefixes[p.Prefix]; claimed {
			continue
		}
		if strings.HasPrefix(slashFile, p.Prefix) {
			return p.ID, true
		}
	}
	return manifest.NamedResource{}, false
}

// BuildParentIndexForKindWithCache maps each childKind resource to its
// enclosing Flux Kustomization — the KS whose spec.path or component
// directory is the deepest strict ancestor of the child's source
// file. Excludes self-matches.
//
// Real Flux's reconcile chain enforces this naturally: a parent
// Kustomization renders and applies its children, then the
// downstream controller reconciles each. flate's controllers fire
// on AddObject and would otherwise race the parent's render — the
// child controllers use this index to gate reconcile on the
// parent's Ready, so any parent-render-time spec mutations
// (`replacements:` injecting spec.targetNamespace, `patches:`
// rewriting HelmRelease driftDetection) are visible to the child's
// first reconcile. Without the gate the file-loaded child renders
// once with stale spec, the parent re-emits a mutated copy, and the
// child renders again — twice the helm template / kustomize build
// work for one logical resource.
//
// sourceFiles is the orchestrator's NamedResource → repo-relative
// source-file map; entries without a recorded file are skipped.
//
// childKind=KindKustomization for the KS→KS parent map; pass
// KindHelmRelease for the HR→KS map. The orchestrator builds both
// (see discovery.Run → mergeParents).
//
// repoRoot is the filesystem root used to read each KS's
// kustomization.yaml when folding `components:` into the prefix set;
// pass the orchestrator's --path. An empty repoRoot means "no on-disk
// component lookup", which still gives a correct (just slightly
// less-precise) index built from spec.path + spec.components alone.
//
// A shared *manifest.ComponentCache is threaded into the
// KSPathPrefixesWithCache call via cache. Used by discovery so the
// KS-parent-map build and the HR-parent-map build share component-file
// reads across the two passes — without sharing, each invocation walks
// the same KS list and re-reads every kustomization.yaml's
// `components:` independently. Pass nil to fall back to a per-call
// cache.
func BuildParentIndexForKindWithCache(s *store.Store, repoRoot string, sourceFiles map[manifest.NamedResource]string, childKind string, cache *manifest.ComponentCache) map[manifest.NamedResource]manifest.NamedResource {
	return BuildParentIndexFromPrefixes(KSPathPrefixesWithCache(s, repoRoot, cache), s, sourceFiles, childKind)
}

// BuildParentIndexFromPrefixes is BuildParentIndexForKindWithCache with
// the KS path-prefix list passed in precomputed. discovery.Run derives
// the prefixes once (an O(KS) walk + sort + component reads) and reuses
// them for the KS-parent index, the HR-parent index, AND orphan
// promotion — three consumers that previously each rebuilt the identical
// list. Standalone callers use BuildParentIndexForKindWithCache, which
// computes the prefixes for a single use.
func BuildParentIndexFromPrefixes(prefixes []KSPathPrefix, s *store.Store, sourceFiles map[manifest.NamedResource]string, childKind string) map[manifest.NamedResource]manifest.NamedResource {
	// Group each id's own claimed prefixes so a peer KS claiming the same
	// spec.path directory isn't mistaken for an enclosing parent (which
	// would mutually deadlock the pair through collectDeps). Children
	// that contribute no prefixes (HelmReleases) get an empty set, so
	// their attribution is unchanged.
	ownPrefixes := make(map[manifest.NamedResource]map[string]struct{})
	for _, p := range prefixes {
		set := ownPrefixes[p.ID]
		if set == nil {
			set = make(map[string]struct{})
			ownPrefixes[p.ID] = set
		}
		set[p.Prefix] = struct{}{}
	}
	out := map[manifest.NamedResource]manifest.NamedResource{}
	for _, obj := range s.ListObjects(childKind) {
		id := obj.Named()
		file, ok := sourceFiles[id]
		if !ok {
			continue
		}
		if parent, ok := longestStrictParent(prefixes, file, id, ownPrefixes[id]); ok {
			out[id] = parent
		}
	}
	return out
}
