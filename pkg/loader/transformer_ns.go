package loader

import (
	"bytes"
	"errors"
	"io"
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
// file-loaded Flux Kustomizations from the namespace an enclosing kustomize
// overlay injects — via EITHER a builtin NamespaceTransformer (flatops) or a
// `replacements:` rule copying a Namespace's name into spec.targetNamespace
// (home-operations). (Name kept for git-history continuity though it now
// covers both patterns; see resolveInjectedTargetNamespace.)
//
// flatops-style repos keep resources namespace-less "for DRYness" and
// inject the namespace via a shared NamespaceTransformer that sets
// spec.targetNamespace on every Kustomization (issue #528). home-operations
// repos do the same with an overlay `namespace:` directive plus a
// `replacements:` rule (kubernetes/components/replacements/ks.yaml). That
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
		ns := resolveOverlayInjectedNamespace(repoRoot, file, cache)
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

// resolveOverlayInjectedNamespace walks up the ancestor directories of a
// Flux KS's source file and returns the namespace injected by the nearest
// enclosing overlay — via either a NamespaceTransformer or a
// targetNamespace-injecting `replacements:` rule. Deepest enclosing overlay
// wins (first hit walking up), matching kustomize's "closest overlay"
// semantics.
func resolveOverlayInjectedNamespace(repoRoot, file string, cache map[string]nsResolution) string {
	dir := path.Dir(filepath.ToSlash(file))
	for dir != "." && dir != "/" && dir != "" {
		res, ok := cache[dir]
		if !ok {
			res = resolveInjectedTargetNamespace(repoRoot, dir)
			cache[dir] = res
		}
		if res.ok {
			return res.ns
		}
		dir = path.Dir(dir)
	}
	return ""
}

// nsInjectionStrategies are the supported overlay-namespace detectors, tried in
// order: a builtin NamespaceTransformer (flatops), then a `replacements:` rule
// (home-operations). Each reports the namespace the overlay at overlayDir
// injects onto Flux Kustomization spec.targetNamespace, or the zero nsResolution
// when it doesn't apply. Adding a third pattern is one entry here.
var nsInjectionStrategies = []func(repoRoot, overlayDir string) nsResolution{
	transformerInjectedNamespace,
	replacementInjectedNamespace,
}

// resolveInjectedTargetNamespace returns the namespace the overlay at overlayDir
// injects onto Flux Kustomizations via the first matching strategy. Strategies
// key on the same overlay dir, so the caller's per-dir cache and
// deepest-overlay-wins walk are unaffected.
func resolveInjectedTargetNamespace(repoRoot, overlayDir string) nsResolution {
	for _, strategy := range nsInjectionStrategies {
		if r := strategy(repoRoot, overlayDir); r.ok {
			return r
		}
	}
	return nsResolution{}
}

// transformerInjectedNamespace inspects the kustomization.yaml at overlayDir.
// When it applies a `transformers:` entry whose subtree both carries a
// `namespace:` directive and resolves to a builtin NamespaceTransformer that
// sets Kustomization spec/targetNamespace, the directive value is the namespace
// that transformer injects.
func transformerInjectedNamespace(repoRoot, overlayDir string) nsResolution {
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
		if subtreeHasNamespaceTransformer(repoRoot, transformerDir, map[string]struct{}{}, 0) {
			return nsResolution{ns: td.Namespace, ok: true}
		}
	}
	return nsResolution{}
}

// replacementInjectedNamespace inspects the kustomization.yaml at overlayDir.
// When it carries BOTH a `namespace:` directive AND a `replacements:` rule that
// copies a Namespace's metadata.name into Kustomization spec.targetNamespace,
// the directive value is the namespace that replacement injects — because the
// same `namespace:` directive renames the source Namespace to that value before
// the copy runs. Requiring the directive is conservative: an overlay with the
// rule but no directive (the Namespace would keep a literal component name) is
// left unstamped rather than guessed, and a bare `namespace:` directive with no
// targetNamespace rule is owned by metadata.namespace inheritance, not us.
func replacementInjectedNamespace(repoRoot, overlayDir string) nsResolution {
	d, ok := readKustomizeDirectives(repoRoot, overlayDir)
	if !ok || d.Namespace == "" || len(d.Replacements) == 0 {
		return nsResolution{}
	}
	for _, r := range d.Replacements {
		if r.injectsKustomizationTargetNamespace(repoRoot, overlayDir) {
			return nsResolution{ns: d.Namespace, ok: true}
		}
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

// fileHasNamespaceTransformer reads target as a (possibly multi-document)
// manifest file and reports whether any document is a qualifying
// NamespaceTransformer. Returns false when target is a directory, unreadable, or
// any document is malformed (all-or-nothing, matching a whole-file parse).
func fileHasNamespaceTransformer(repoRoot, target string) bool {
	data, err := os.ReadFile(filepath.Join(repoRoot, target)) //nolint:gosec // path composed from known cluster layout
	if err != nil {
		return false
	}
	// Decode every document up front and bail on any malformed one, matching
	// the prior whole-file SplitDocs parse (all-or-nothing) exactly.
	dec := yaml.NewDecoder(bytes.NewReader(data))
	var docs []namespaceTransformerDoc
	for {
		var doc namespaceTransformerDoc
		err := dec.Decode(&doc)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return false
		}
		docs = append(docs, doc)
	}
	return slices.ContainsFunc(docs, namespaceTransformerDoc.injectsKustomizationTargetNamespace)
}

// namespaceTransformerDoc is the subset of a builtin NamespaceTransformer that
// targetNamespace detection reads: a fieldSpec writing spec/targetNamespace on
// Kustomizations is what injects a Flux KS's targetNamespace.
type namespaceTransformerDoc struct {
	Kind       string                 `yaml:"kind"`
	FieldSpecs []transformerFieldSpec `yaml:"fieldSpecs"`
}

// transformerFieldSpec is one `fieldSpecs:` entry: the kind it applies to and
// the object path it writes.
type transformerFieldSpec struct {
	Kind string `yaml:"kind"`
	Path string `yaml:"path"`
}

// injectsKustomizationTargetNamespace reports whether doc is a builtin
// NamespaceTransformer with a fieldSpec that writes spec/targetNamespace on
// Kustomizations — the precise shape that injects a Flux KS's targetNamespace.
func (d namespaceTransformerDoc) injectsKustomizationTargetNamespace() bool {
	if d.Kind != "NamespaceTransformer" {
		return false
	}
	return slices.ContainsFunc(d.FieldSpecs, func(fs transformerFieldSpec) bool {
		return fs.Kind == manifest.KindKustomization && fs.Path == "spec/targetNamespace"
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
	Namespace    string                 `yaml:"namespace"`
	Resources    []string               `yaml:"resources"`
	Transformers []string               `yaml:"transformers"`
	Components   []string               `yaml:"components"`
	Replacements []replacementDirective `yaml:"replacements"`
}

// replacementDirective captures a single `replacements:` entry. kustomize
// allows two shapes: an external file ref (`{path: <file>}`, where the file is a
// YAML list of replacement objects) or an inline replacement (`{source,
// targets}`). Both decode into this one struct: the file form leaves
// Source/Targets nil, the inline form leaves Path empty.
type replacementDirective struct {
	Path    string              `yaml:"path"`
	Source  *replacementSource  `yaml:"source"`
	Targets []replacementTarget `yaml:"targets"`
}

// replacementSource / replacementTarget / replacementSelect model only the
// fields targetNamespace detection reads. A replacement field path may be given
// singular (`fieldPath`) or plural (`fieldPaths`); both are accepted on either
// side. Selector keys beyond kind/group (version, name, namespace) are
// intentionally ignored — same as the prior map-based check.
type replacementSource struct {
	Kind       string   `yaml:"kind"`
	FieldPath  string   `yaml:"fieldPath"`
	FieldPaths []string `yaml:"fieldPaths"`
}

type replacementTarget struct {
	Select     replacementSelect `yaml:"select"`
	FieldPath  string            `yaml:"fieldPath"`
	FieldPaths []string          `yaml:"fieldPaths"`
}

type replacementSelect struct {
	Kind  string `yaml:"kind"`
	Group string `yaml:"group"`
}

// injectsKustomizationTargetNamespace reports whether r is a
// Namespace-name → Kustomization.spec.targetNamespace replacement, resolving the
// external `{path:}` form (a YAML list of replacement objects) against
// overlayDir.
func (r replacementDirective) injectsKustomizationTargetNamespace(repoRoot, overlayDir string) bool {
	if r.Source != nil {
		return r.copiesNamespaceNameToKustomizationTargetNamespace()
	}
	if r.Path == "" {
		return false
	}
	ref, ok := resolveRef(overlayDir, r.Path)
	if !ok {
		return false
	}
	data, err := os.ReadFile(filepath.Join(repoRoot, ref)) //nolint:gosec // path composed from known cluster layout
	if err != nil {
		return false
	}
	var rules []replacementDirective
	if err := yaml.Unmarshal(data, &rules); err != nil {
		return false
	}
	return slices.ContainsFunc(rules, replacementDirective.copiesNamespaceNameToKustomizationTargetNamespace)
}

// copiesNamespaceNameToKustomizationTargetNamespace reports whether r's inline
// source/targets copy a Namespace's metadata.name into the spec.targetNamespace
// of Flux Kustomizations — the precise shape
// kubernetes/components/replacements/ks.yaml uses.
func (r replacementDirective) copiesNamespaceNameToKustomizationTargetNamespace() bool {
	if r.Source == nil || r.Source.Kind != "Namespace" ||
		!fieldMatches(r.Source.FieldPath, r.Source.FieldPaths, "metadata.name") {
		return false
	}
	return slices.ContainsFunc(r.Targets, func(t replacementTarget) bool {
		return t.Select.Kind == manifest.KindKustomization &&
			t.Select.Group == manifest.FluxKustomizeDomain &&
			fieldMatches(t.FieldPath, t.FieldPaths, "spec.targetNamespace")
	})
}

// fieldMatches reports whether a replacement field path equals want in either
// the singular (`fieldPath`) or plural (`fieldPaths`) form. Accepting both on
// either side costs nothing and tolerates hand-written variants.
func fieldMatches(single string, plural []string, want string) bool {
	return single == want || slices.Contains(plural, want)
}

// readKustomizeDirectives reads the kustomization file in dir (resolved
// under repoRoot) and returns its namespace/resources/transformers/
// components/replacements fields. ok is false when no kustomization file is
// present or it can't be parsed — pure best-effort, same contract as
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
