package loader

import (
	"cmp"
	"path/filepath"
	"slices"
	"strings"

	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

// KSPathPrefix pairs a Kustomization id with its slash-terminated,
// repo-relative spec.path prefix. Returned by KSPathPrefixes for use
// in parent-index construction and orphan classification.
type KSPathPrefix struct {
	ID     manifest.NamedResource
	Prefix string
}

// KSPathPrefixes returns one entry per loaded Kustomization with a
// non-empty spec.path, sorted by prefix length descending so the
// first HasPrefix match on a given file is the most-specific
// structural parent. The descending sort drops parent lookup from
// O(K²) to O(K · depth) in the typical case.
func KSPathPrefixes(s *store.Store) []KSPathPrefix {
	var out []KSPathPrefix
	for _, ks := range store.ListAs[*manifest.Kustomization](s, manifest.KindKustomization) {
		if ks.Path == "" {
			continue
		}
		out = append(out, KSPathPrefix{ID: ks.Named(), Prefix: normalizePrefix(ks.Path)})
	}
	slices.SortFunc(out, func(a, b KSPathPrefix) int {
		return cmp.Compare(len(b.Prefix), len(a.Prefix))
	})
	return out
}

// LongestParent returns the deepest KS whose spec.path covers file
// (slash-normalized repo-relative path), excluding self. The second
// return reports whether a parent was found. prefixes is expected to
// be the sorted output of KSPathPrefixes.
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

// BuildParentIndex maps each Flux Kustomization to its structural
// parent — the Flux Kustomization whose spec.path is the deepest
// strict ancestor of the child's source file. Excludes self-matches.
//
// Real Flux's reconcile chain enforces this naturally: a parent
// Kustomization renders and applies its child Kustomizations, then
// the kustomize-controller reconciles each child. flate's controllers
// fire on AddObject and would otherwise race the parent's render —
// the controller uses this index to wait for the parent's Ready
// before reconciling, so any parent-render-time spec mutations
// (e.g. `replacements:` injecting spec.targetNamespace) are visible
// to the child's first reconcile.
//
// sourceFiles is the orchestrator's NamedResource → repo-relative
// source-file map; entries without a recorded file are skipped.
func BuildParentIndex(s *store.Store, sourceFiles map[manifest.NamedResource]string) map[manifest.NamedResource]manifest.NamedResource {
	return buildParentIndexForKind(s, sourceFiles, manifest.KindKustomization)
}

// BuildParentIndexForKind extends BuildParentIndex to map any kind to
// its enclosing Flux Kustomization. The HR controller uses this to
// gate reconcile on the parent KS — without that gate, the file-
// loaded HR renders once with stale spec, the parent KS applies
// `replacements:` / `patches:` and re-emits a mutated HR, the HR
// controller renders again with the canonical spec, and helm runs
// twice for one logical resource. Same parent-prefix logic as
// BuildParentIndex, just iterating the requested child kind.
func BuildParentIndexForKind(s *store.Store, sourceFiles map[manifest.NamedResource]string, childKind string) map[manifest.NamedResource]manifest.NamedResource {
	return buildParentIndexForKind(s, sourceFiles, childKind)
}

func buildParentIndexForKind(s *store.Store, sourceFiles map[manifest.NamedResource]string, childKind string) map[manifest.NamedResource]manifest.NamedResource {
	prefixes := KSPathPrefixes(s)
	out := map[manifest.NamedResource]manifest.NamedResource{}
	for _, obj := range s.ListObjects(childKind) {
		id := obj.Named()
		file, ok := sourceFiles[id]
		if !ok {
			continue
		}
		if parent, ok := LongestParent(prefixes, file, id); ok {
			out[id] = parent
		}
	}
	return out
}
