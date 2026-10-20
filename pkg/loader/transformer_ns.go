package loader

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"io/fs"
	"path"
	"path/filepath"
	"slices"

	yaml "go.yaml.in/yaml/v4"
	"sigs.k8s.io/kustomize/api/filters/namespace"
	"sigs.k8s.io/kustomize/api/types"
	sigsyaml "sigs.k8s.io/yaml"

	"github.com/home-operations/flate/internal/kustpath"
	"github.com/home-operations/flate/pkg/manifest"
)

// transformerScanMaxDepth bounds the recursive walk that confirms a
// `transformers:` reference resolves to a builtin NamespaceTransformer.
// Real repos nest one or two levels (overlay → transformers/ → shared
// transformer dir); the cap keeps a pathological resources: cycle from
// running away even though visited already breaks true cycles.
const transformerScanMaxDepth = 6

func (l *Loader) predictNamespaces(ks *manifest.Kustomization, repoRoot string, record manifest.DiscoveryRecord, cache map[string]nsResolution) (*manifest.Kustomization, string) {
	file := record.File
	for root := filepath.Dir(record.Path); record.Path != "" && root != filepath.Dir(root); root = filepath.Dir(root) {
		if l.documents.views[root] != nil {
			repoRoot = root
			rel, _ := filepath.Rel(root, record.Path)
			file = filepath.ToSlash(rel)
			break
		}
	}
	res := l.documents.resolveOverlayInjectedNamespace(repoRoot, file, cache)
	forced := ""
	if res.transformer {
		// The builtin namespace filter also sets metadata.namespace.
		forced = res.ns
	}
	if res.ns == "" || ks.TargetNamespace != "" {
		return ks, forced
	}
	updated := ks.Clone()
	updated.SetTargetNamespace(res.ns)
	return updated, forced
}

// nsResolution memoizes a per-directory transformer-namespace lookup.
type nsResolution struct {
	ns          string
	ok          bool
	transformer bool
}

// resolveOverlayInjectedNamespace walks up the ancestor directories of a
// Flux KS's source file and returns the namespace injected by the nearest
// enclosing overlay — via either a NamespaceTransformer or a
// targetNamespace-injecting `replacements:` rule. Deepest enclosing overlay
// wins (first hit walking up), matching kustomize's "closest overlay"
// semantics.
func (reader documentReader) resolveOverlayInjectedNamespace(repoRoot, file string, cache map[string]nsResolution) nsResolution {
	dir := path.Dir(filepath.ToSlash(file))
	for dir != "." && dir != "/" && dir != "" {
		key := filepath.Join(repoRoot, filepath.FromSlash(dir))
		res, ok := cache[key]
		if !ok {
			res = reader.resolveInjectedTargetNamespace(repoRoot, dir)
			cache[key] = res
		}
		if res.ok {
			return res
		}
		dir = path.Dir(dir)
	}
	return nsResolution{}
}

func (reader documentReader) resolveInjectedTargetNamespace(repoRoot, overlayDir string) nsResolution {
	if r := reader.transformerInjectedNamespace(repoRoot, overlayDir); r.ok {
		return r
	}
	return reader.replacementInjectedNamespace(repoRoot, overlayDir)
}

// transformerInjectedNamespace inspects the kustomization.yaml at overlayDir.
// When it applies a `transformers:` entry whose subtree both carries a
// `namespace:` directive and resolves to a builtin NamespaceTransformer that
// sets Kustomization spec/targetNamespace, the directive value is the namespace
// that transformer injects.
func (reader documentReader) transformerInjectedNamespace(repoRoot, overlayDir string) nsResolution {
	d, _ := reader.read(filepath.Join(repoRoot, overlayDir))
	if d == nil || len(d.Transformers) == 0 {
		return nsResolution{}
	}
	for _, ref := range d.Transformers {
		transformerDir, ok := kustpath.Resolve(overlayDir, ref, kustpath.Transformer)
		if !ok {
			continue
		}
		td, _ := reader.read(filepath.Join(repoRoot, transformerDir))
		if td == nil || td.Namespace == "" {
			continue
		}
		if reader.subtreeHasNamespaceTransformer(repoRoot, transformerDir, map[string]struct{}{}, 0) {
			return nsResolution{ns: td.Namespace, ok: true, transformer: true}
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
func (reader documentReader) replacementInjectedNamespace(repoRoot, overlayDir string) nsResolution {
	d, _ := reader.read(filepath.Join(repoRoot, overlayDir))
	if d == nil || d.Namespace == "" || len(d.Replacements) == 0 {
		return nsResolution{}
	}
	for _, r := range d.Replacements {
		if replacementInjectsTargetNamespace(r, reader, repoRoot, overlayDir) {
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
func (reader documentReader) subtreeHasNamespaceTransformer(repoRoot, dir string, visited map[string]struct{}, depth int) bool {
	if depth > transformerScanMaxDepth {
		return false
	}
	if _, seen := visited[dir]; seen {
		return false
	}
	visited[dir] = struct{}{}
	d, _ := reader.read(filepath.Join(repoRoot, dir))
	if d == nil {
		return false
	}
	// A ref is either a file (the transformer manifest itself) or a
	// directory with its own kustomization.yaml — try both.
	return slices.ContainsFunc(slices.Concat(d.Resources, d.Transformers), func(ref string) bool {
		target, ok := kustpath.Resolve(dir, ref, kustpath.Transformer)
		if !ok {
			return false
		}
		return reader.fileHasNamespaceTransformer(repoRoot, target) ||
			reader.subtreeHasNamespaceTransformer(repoRoot, target, visited, depth+1)
	})
}

// fileHasNamespaceTransformer reads target as a (possibly multi-document)
// manifest file and reports whether any document is a qualifying
// NamespaceTransformer. Returns false when target is a directory, unreadable, or
// any document is malformed (all-or-nothing, matching a whole-file parse).
func (reader documentReader) fileHasNamespaceTransformer(repoRoot, target string) bool {
	input, name := reader.input(filepath.Join(repoRoot, target))
	data, err := fs.ReadFile(input, name)
	if err != nil {
		return false
	}
	// Every document MUST decode successfully before any prediction is accepted.
	dec := yaml.NewDecoder(bytes.NewReader(data))
	found := false
	for {
		var raw map[string]any
		err := dec.Decode(&raw)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return false
		}
		data, err := json.Marshal(raw)
		var doc namespaceTransformerDoc
		if err != nil || json.Unmarshal(data, &doc) != nil {
			return false
		}
		if doc.Kind == "NamespaceTransformer" {
			switch doc.SetRoleBindingSubjects {
			case namespace.SubjectModeUnspecified, namespace.DefaultSubjectsOnly,
				namespace.AllServiceAccountSubjects, namespace.NoSubjects:
			default:
				return false
			}
		}
		found = doc.injectsKustomizationTargetNamespace() || found
	}
	return found
}

// NamespaceTransformerPlugin is upstream-internal; only its public field specs
// and options needed to prove unconditional target creation are decoded here.
type namespaceTransformerDoc struct {
	Kind                   string                           `json:"kind"`
	FieldSpecs             []types.FieldSpec                `json:"fieldSpecs"`
	UnsetOnly              bool                             `json:"unsetOnly"`
	SetRoleBindingSubjects namespace.RoleBindingSubjectMode `json:"setRoleBindingSubjects"`
}

func (d namespaceTransformerDoc) injectsKustomizationTargetNamespace() bool {
	if d.Kind != "NamespaceTransformer" || d.UnsetOnly {
		return false
	}
	return slices.ContainsFunc(d.FieldSpecs, func(fs types.FieldSpec) bool {
		return fs.Kind == manifest.KindKustomization && fs.Group == manifest.FluxKustomizeDomain &&
			(fs.Version == "" || fs.Version == "v1") && fs.Path == "spec/targetNamespace" && fs.CreateIfNotPresent
	})
}

func replacementInjectsTargetNamespace(r types.ReplacementField, reader documentReader, repoRoot, overlayDir string) bool {
	if r.Path == "" {
		return copiesNamespaceNameToTargetNamespace(r.Replacement)
	}
	if r.Source != nil || len(r.Targets) != 0 || r.SourceValue != nil {
		return false
	}
	ref, ok := kustpath.Resolve(overlayDir, r.Path, kustpath.Transformer)
	if !ok {
		return false
	}
	input, name := reader.input(filepath.Join(repoRoot, ref))
	data, err := fs.ReadFile(input, name)
	if err != nil {
		return false
	}
	var rules []types.Replacement
	if sigsyaml.Unmarshal(data, &rules) != nil {
		return false
	}
	return slices.ContainsFunc(rules, copiesNamespaceNameToTargetNamespace)
}

// Prediction requires an unrestricted selector and field creation. Existing
// explicit targetNamespace values remain authoritative; other shapes are left
// to the actual render, including no-create rules and external map definitions.
func copiesNamespaceNameToTargetNamespace(r types.Replacement) bool {
	if r.Source == nil || r.SourceValue != nil || r.Source.Kind != "Namespace" ||
		r.Source.Group != "" || (r.Source.Version != "" && r.Source.Version != "v1") ||
		r.Source.Name != "" || r.Source.Namespace != "" || r.Source.Options != nil ||
		(r.Source.FieldPath != "" && r.Source.FieldPath != types.DefaultReplacementFieldPath) {
		return false
	}
	for _, t := range r.Targets {
		if t == nil || t.Select == nil || len(t.Reject) != 0 ||
			t.Select.Kind != manifest.KindKustomization || t.Select.Group != manifest.FluxKustomizeDomain ||
			(t.Select.Version != "" && t.Select.Version != "v1") || t.Select.Name != "" || t.Select.Namespace != "" ||
			t.Select.LabelSelector != "" || t.Select.AnnotationSelector != "" ||
			!slices.Contains(t.FieldPaths, "spec.targetNamespace") ||
			t.Options == nil || *t.Options != (types.FieldOptions{Create: true}) {
			return false
		}
	}
	return len(r.Targets) != 0
}
