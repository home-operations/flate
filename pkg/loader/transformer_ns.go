package loader

import (
	"os"
	"path"
	"path/filepath"
	"slices"
	"strings"

	yaml "go.yaml.in/yaml/v4"

	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

// transformerScanMaxDepth bounds the recursive walk that confirms a
// `transformers:` reference resolves to a builtin NamespaceTransformer.
// Real repos nest one or two levels (overlay → transformers/ → shared
// transformer dir); the cap keeps a pathological resources: cycle from
// running away even though visited already breaks true cycles.
const transformerScanMaxDepth = 6

// StampTransformerTargetNamespaces fills empty spec.targetNamespace on
// file-loaded Flux Kustomizations from a builtin NamespaceTransformer
// that an enclosing kustomize overlay applies.
//
// flatops-style repos keep resources namespace-less "for DRYness" and
// inject the namespace via a shared NamespaceTransformer that sets
// spec.targetNamespace on every Kustomization (issue #528). That
// injection only happens when the overlay is kustomize-built — and when
// the overlay is rendered by a further-up, itself render-emitted parent,
// the injection lands *after* flate has already fired the leaf KS's
// first reconcile. The leaf then renders its children with an empty
// targetNamespace, producing an empty-namespace copy of each HelmRelease
// that lingers in the store and later fails to render (no namespace to
// resolve its chart's HelmRepository against).
//
// Resolving the namespace here, at load time, lets the leaf KS render
// into the right namespace on its first pass — the load-time analog of
// the same NamespaceTransformer kustomize would apply, mirroring how
// ApplyNamespaceInheritanceWithRefs front-runs kustomize-controller's
// apply-time namespace defaulting.
//
// Only spec.targetNamespace is set (never metadata.namespace), so the
// KS's store id stays stable. The value reaches rendered children via
// ks.Contents (RenderFlux feeds it to kustomize) and reaches
// store-resident namespace-less resources under spec.path via
// ApplyNamespaceInheritanceWithRefs's existing projection, which now sees
// a populated targetNamespace.
//
// Runs before ApplyNamespaceInheritanceWithRefs (see discovery.applyNamespaces).
func StampTransformerTargetNamespaces(s *store.Store, sourceFiles map[manifest.NamedResource]string, repoRoot string) {
	if len(sourceFiles) == 0 {
		return
	}
	// Per-directory resolution cache: many KSes share the same overlay
	// ancestry, and resolution re-reads kustomization.yaml files off
	// disk. Live only for this pass.
	cache := map[string]nsResolution{}
	for _, ks := range store.ListAs[*manifest.Kustomization](s, manifest.KindKustomization) {
		if ks.TargetNamespace != "" {
			continue
		}
		file, ok := sourceFiles[ks.Named()]
		if !ok {
			continue
		}
		ns := resolveOverlayTransformerNamespace(repoRoot, file, cache)
		if ns == "" {
			continue
		}
		// Store immutability contract: clone before mutating, then
		// AddObject the copy. The id is unchanged (targetNamespace isn't
		// part of NamedResource), so this updates in place.
		updated := ks.Clone()
		updated.SetTargetNamespace(ns)
		s.AddObject(updated)
	}
}

// nsResolution memoizes a per-directory transformer-namespace lookup.
type nsResolution struct {
	ns string
	ok bool
}

// resolveOverlayTransformerNamespace walks up the ancestor directories
// of a Flux KS's source file and returns the namespace injected by the
// nearest enclosing overlay's NamespaceTransformer. Deepest enclosing
// overlay wins (first hit walking up), matching kustomize's "closest
// overlay" semantics.
func resolveOverlayTransformerNamespace(repoRoot, file string, cache map[string]nsResolution) string {
	dir := path.Dir(filepath.ToSlash(file))
	for dir != "." && dir != "/" && dir != "" {
		res, ok := cache[dir]
		if !ok {
			res = resolveTransformerTargetNamespace(repoRoot, dir)
			cache[dir] = res
		}
		if res.ok {
			return res.ns
		}
		dir = path.Dir(dir)
	}
	return ""
}

// resolveTransformerTargetNamespace inspects the kustomization.yaml at
// overlayDir. When it applies a `transformers:` entry whose subtree both
// carries a `namespace:` directive and resolves to a builtin
// NamespaceTransformer that sets Kustomization spec/targetNamespace, the
// directive value is the namespace that transformer injects.
func resolveTransformerTargetNamespace(repoRoot, overlayDir string) nsResolution {
	d, ok := readKustomizeDirectives(repoRoot, overlayDir)
	if !ok || len(d.Transformers) == 0 {
		return nsResolution{}
	}
	for _, ref := range d.Transformers {
		transformerDir, ok := resolveRef(overlayDir, ref)
		if !ok {
			continue
		}
		td, ok := readKustomizeDirectives(repoRoot, transformerDir)
		if !ok || td.Namespace == "" {
			continue
		}
		if !subtreeHasNamespaceTransformer(repoRoot, transformerDir, map[string]struct{}{}, 0) {
			continue
		}
		return nsResolution{ns: td.Namespace, ok: true}
	}
	return nsResolution{}
}

// subtreeHasNamespaceTransformer reports whether the kustomize subtree
// rooted at dir declares (directly or via resources:/transformers:
// references) a builtin NamespaceTransformer that targets Kustomization
// spec/targetNamespace. Bounded by transformerScanMaxDepth and a
// visited set so malformed reference cycles can't loop.
func subtreeHasNamespaceTransformer(repoRoot, dir string, visited map[string]struct{}, depth int) bool {
	if depth > transformerScanMaxDepth {
		return false
	}
	if _, seen := visited[dir]; seen {
		return false
	}
	visited[dir] = struct{}{}
	d, ok := readKustomizeDirectives(repoRoot, dir)
	if !ok {
		return false
	}
	// A ref is either a file (the transformer manifest itself) or a
	// directory with its own kustomization.yaml — try both.
	return slices.ContainsFunc(slices.Concat(d.Resources, d.Transformers), func(ref string) bool {
		target, ok := resolveRef(dir, ref)
		if !ok {
			return false
		}
		return fileHasNamespaceTransformer(repoRoot, target) ||
			subtreeHasNamespaceTransformer(repoRoot, target, visited, depth+1)
	})
}

// fileHasNamespaceTransformer reads target as a manifest file and
// reports whether any document in it is a qualifying NamespaceTransformer.
// Returns false when target is a directory or unreadable.
func fileHasNamespaceTransformer(repoRoot, target string) bool {
	data, err := os.ReadFile(filepath.Join(repoRoot, target)) //nolint:gosec // path composed from known cluster layout
	if err != nil {
		return false
	}
	docs, err := manifest.SplitDocs(data)
	if err != nil {
		return false
	}
	return slices.ContainsFunc(docs, docIsNamespaceTransformerForKustomization)
}

// docIsNamespaceTransformerForKustomization reports whether doc is a
// builtin NamespaceTransformer with a fieldSpec that writes
// spec/targetNamespace on Kustomizations — the precise shape that
// injects a Flux KS's targetNamespace.
func docIsNamespaceTransformerForKustomization(doc map[string]any) bool {
	if manifest.DocKind(doc) != "NamespaceTransformer" {
		return false
	}
	specs, _ := doc["fieldSpecs"].([]any)
	return slices.ContainsFunc(specs, func(raw any) bool {
		fs, ok := raw.(map[string]any)
		if !ok {
			return false
		}
		kind, _ := fs["kind"].(string)
		p, _ := fs["path"].(string)
		return kind == manifest.KindKustomization && p == "spec/targetNamespace"
	})
}

// resolveRef cleans a kustomize resource/transformer reference relative
// to baseDir, rejecting remote (scheme://) and absolute refs and any
// path that escapes above the repo (leading ".."). The boolean reports
// whether the ref is a usable in-repo relative path.
func resolveRef(baseDir, ref string) (string, bool) {
	if ref == "" || strings.Contains(ref, "://") || filepath.IsAbs(ref) {
		return "", false
	}
	resolved := path.Clean(path.Join(baseDir, filepath.ToSlash(ref)))
	if resolved == "." || strings.HasPrefix(resolved, "..") {
		return "", false
	}
	return resolved, true
}

// kustomizeDirectives is the subset of a kustomization.yaml that
// transformer-namespace resolution and self-production attribution read.
type kustomizeDirectives struct {
	Namespace    string   `yaml:"namespace"`
	Resources    []string `yaml:"resources"`
	Transformers []string `yaml:"transformers"`
	Components   []string `yaml:"components"`
}

// readKustomizeDirectives reads the kustomization file in dir (resolved
// under repoRoot) and returns its namespace/resources/transformers/
// components fields. ok is false when no kustomization file is present or
// it can't be parsed — pure best-effort, same contract as
// readKustomizeNamespace.
func readKustomizeDirectives(repoRoot, dir string) (kustomizeDirectives, bool) {
	for _, name := range manifest.KustomizeBuilderFilenames {
		data, err := os.ReadFile(filepath.Join(repoRoot, dir, name)) //nolint:gosec // path composed from known cluster layout
		if err != nil {
			continue
		}
		var d kustomizeDirectives
		if err := yaml.Unmarshal(data, &d); err != nil {
			continue
		}
		return d, true
	}
	return kustomizeDirectives{}, false
}
