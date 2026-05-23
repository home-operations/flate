package loader

import (
	"cmp"
	"path/filepath"
	"slices"
	"strings"

	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

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
	type owner struct {
		prefix string
		id     manifest.NamedResource
	}
	var owners []owner
	for _, obj := range s.ListObjects(manifest.KindKustomization) {
		ks, ok := obj.(*manifest.Kustomization)
		if !ok || ks.Path == "" {
			continue
		}
		owners = append(owners, owner{prefix: normalizePrefix(ks.Path), id: ks.Named()})
	}
	// Sort by prefix length descending so the longest-prefix parent
	// (the most specific structural owner) is the first match in the
	// inner loop; we can short-circuit on first hit. Drops the worst
	// case from O(K²) to O(K · depth) and the typical case to O(K).
	slices.SortFunc(owners, func(a, b owner) int {
		return cmp.Compare(len(b.prefix), len(a.prefix))
	})
	out := map[manifest.NamedResource]manifest.NamedResource{}
	for _, obj := range s.ListObjects(manifest.KindKustomization) {
		ks, ok := obj.(*manifest.Kustomization)
		if !ok {
			continue
		}
		id := ks.Named()
		file, ok := sourceFiles[id]
		if !ok {
			continue
		}
		slashFile := filepath.ToSlash(file)
		for _, o := range owners {
			if o.id == id {
				continue
			}
			if strings.HasPrefix(slashFile, o.prefix) {
				out[id] = o.id
				break
			}
		}
	}
	return out
}
