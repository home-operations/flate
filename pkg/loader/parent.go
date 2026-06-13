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
	return manifest.NormalizeClaimBase(p) + "/"
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

// ExternalSourcedKSIDs returns the ids of Kustomizations whose sourceRef points
// at a genuine EXTERNAL source rather than the working tree, so callers can keep
// their spec.path from being resolved against the local checkout.
//
// flate renders the local tree. A KS sourced from the bootstrap/self source (or
// a bare sourceRef, which anchors on the seeded BootstrapSourceID) renders that
// local tree, so its spec.path legitimately claims local prefixes. But a KS
// sourced from an in-tree GitRepository/OCIRepository CR that was NOT aliased to
// the working tree — a private side-repo synced into the cluster, e.g.
// `shelly-fleet` (ssh://…/shelly-fleet.git) with `path: ./kubernetes` — renders
// THAT repo's tree, which flate can't see. Letting such a KS claim local
// prefixes makes it the false structural parent/producer of everything beneath a
// wide spec.path like ./kubernetes, so when its source soft-skips (missing auth
// Secret) the skip cascades to the entire cluster (179 passed / 209 blocked on
// lunevans/talos-cluster, issue #752).
//
// A source is "local" — excluded from the result — when: the sourceRef is bare
// (→ BootstrapSourceID), the id IS BootstrapSourceID, the source carries a
// working-tree artifact (aliased by discovery, LocalPath == repoRoot), or no
// source object exists yet (a missing sourceRef that bootstrap aliasing
// resolves). Only an in-tree, non-bootstrap, non-aliased source CR is external.
func ExternalSourcedKSIDs(s *store.Store, repoRoot string) map[manifest.NamedResource]struct{} {
	out := map[manifest.NamedResource]struct{}{}
	for _, ks := range store.ListAs[*manifest.Kustomization](s, manifest.KindKustomization) {
		if ks.Path == "" || ks.SourceName == "" {
			continue
		}
		src := manifest.NamedResource{Kind: ks.SourceKind, Namespace: ks.SourceNamespace, Name: ks.SourceName}
		if src == manifest.BootstrapSourceID {
			continue
		}
		if art, ok := s.GetArtifact(src).(*store.SourceArtifact); ok && art.LocalPath == repoRoot {
			continue
		}
		if s.GetObject(src) == nil {
			continue
		}
		out[ks.Named()] = struct{}{}
	}
	return out
}

// KSPathPrefixesLocalOnly is KSPathPrefixesWithCache with external-sourced
// Kustomizations' claims dropped (see ExternalSourcedKSIDs). Use this for the
// parent index, orphan promotion, and any other local-tree path attribution so
// an external-sourced KS never becomes the structural parent of local resources.
func KSPathPrefixesLocalOnly(s *store.Store, repoRoot string, cache *manifest.ComponentCache) []KSPathPrefix {
	all := KSPathPrefixesWithCache(s, repoRoot, cache)
	external := ExternalSourcedKSIDs(s, repoRoot)
	if len(external) == 0 {
		return all
	}
	out := make([]KSPathPrefix, 0, len(all))
	for _, p := range all {
		if _, skip := external[p.ID]; !skip {
			out = append(out, p)
		}
	}
	return out
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
// spec.path. Emitting (2)+(3) keeps loader's parent index in line with
// change/ownership's attribution, eliminating the false-orphan class
// where a child KS lives inside a parent's component subtree.
//
// repoRoot is the filesystem root the kustomization-file reads
// resolve relative to. Pass "" to skip on-disk component lookup
// entirely (only spec.path + spec.components are recorded).
//
// A shared component cache is threaded in via cache. The orchestrator
// instantiates one cache per Bootstrap and passes it to every consumer
// (discovery's orphan promotion, the orchestrator's finalize detectOrphans,
// change.buildOwnership) so the
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
	// A nil selfPrefixes set skips no candidate as a peer, which is exactly
	// "deepest ancestor excluding self" — see longestStrictParent.
	return longestStrictParent(prefixes, file, self, nil)
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

// BuildParentIndexFromPrefixes maps each childKind resource to its enclosing
// Flux Kustomization — the KS whose spec.path or component directory is the
// deepest strict ancestor of the child's source file. Excludes self-matches.
// The KS path-prefix list is passed in precomputed: discovery.Run derives the
// prefixes once (an O(KS) walk + sort + component reads) and reuses them for
// the KS-parent index, the HR-parent index, AND orphan promotion — three
// consumers that previously each rebuilt the identical list.
//
// Real Flux's reconcile chain enforces this parent→child ordering naturally: a
// parent Kustomization renders and applies its children, then the downstream
// controller reconciles each. flate's controllers fire on AddObject and would
// otherwise race the parent's render — the child controllers use this index to
// gate reconcile on the parent's Ready, so parent-render-time spec mutations
// (`replacements:` injecting spec.targetNamespace, `patches:` rewriting
// HelmRelease driftDetection) are visible to the child's first reconcile.
//
// sourceFiles is the orchestrator's NamedResource → repo-relative source-file
// map; entries without a recorded file are skipped. childKind=KindKustomization
// for the KS→KS parent map; pass KindHelmRelease for the HR→KS map.
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
