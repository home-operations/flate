package loader

import (
	"cmp"
	"path"
	"path/filepath"
	"slices"
	"strings"

	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

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

// KSPathPrefixes returns one or more entries per loaded Kustomization
// with a non-empty spec.path. Each KS contributes:
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
func KSPathPrefixes(s *store.Store, repoRoot string) []KSPathPrefix {
	var out []KSPathPrefix
	componentCache := make(map[string][]string)
	for _, ks := range store.ListAs[*manifest.Kustomization](s, manifest.KindKustomization) {
		if ks.Path == "" {
			continue
		}
		id := ks.Named()
		base := NormalizePrefix(ks.Path)
		out = append(out, KSPathPrefix{ID: id, Prefix: base})
		addComponent := func(ref string) {
			if ref == "" || strings.Contains(ref, "://") || filepath.IsAbs(ref) {
				return
			}
			resolved := path.Clean(path.Join(strings.TrimSuffix(base, "/"), ref))
			if resolved == "." || strings.HasPrefix(resolved, "..") {
				return
			}
			out = append(out, KSPathPrefix{ID: id, Prefix: resolved + "/"})
		}
		for _, comp := range ks.Components {
			addComponent(comp)
		}
		if repoRoot != "" {
			baseTrimmed := strings.TrimSuffix(base, "/")
			comps, ok := componentCache[baseTrimmed]
			if !ok {
				comps = manifest.ReadKustomizeComponents(repoRoot, baseTrimmed)
				componentCache[baseTrimmed] = comps
			}
			for _, comp := range comps {
				addComponent(comp)
			}
		}
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

// BuildParentIndexForKind maps each childKind resource to its
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
func BuildParentIndexForKind(s *store.Store, repoRoot string, sourceFiles map[manifest.NamedResource]string, childKind string) map[manifest.NamedResource]manifest.NamedResource {
	prefixes := KSPathPrefixes(s, repoRoot)
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
