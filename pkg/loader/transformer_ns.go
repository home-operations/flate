package loader

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"io/fs"
	"path"
	"path/filepath"
	"slices"
	"strings"

	yaml "go.yaml.in/yaml/v4"
	"sigs.k8s.io/kustomize/api/filters/namespace"
	"sigs.k8s.io/kustomize/api/types"
	sigsyaml "sigs.k8s.io/yaml"

	"github.com/home-operations/flate/internal/kustomizeinput"
	"github.com/home-operations/flate/pkg/manifest"
)

// transformerScanMaxDepth bounds the recursive walk that confirms a
// `transformers:` reference resolves to a builtin NamespaceTransformer.
// Real repos nest one or two levels (overlay → transformers/ → shared
// transformer dir); the cap keeps a pathological resources: cycle from
// running away even though visited already breaks true cycles.
const transformerScanMaxDepth = 6

type namespacePrediction struct {
	object *manifest.Kustomization
	forced string
}

func (l *Loader) predictNamespaces(
	ks *manifest.Kustomization,
	repoRoot string,
	inputRecord *manifest.DiscoveryRecord,
	cache map[string]nsResolution,
) namespacePrediction {
	record := *inputRecord
	file := record.File
	for root := filepath.Dir(record.Path); record.Path != "" && root != filepath.Dir(root); root = filepath.Dir(root) {
		if l.documents.views[root] != nil {
			repoRoot = root
			rel, _ := filepath.Rel(root, record.Path)
			file = filepath.ToSlash(rel)
			break
		}
	}
	res := new(l.documents).resolveOverlayInjectedNamespace(repoRoot, file, cache)
	forced := ""
	if res.transformer {
		// The builtin namespace filter also sets metadata.namespace.
		forced = res.ns
	}
	if res.ns == "" || ks.TargetNamespace != "" {
		return namespacePrediction{object: ks, forced: forced}
	}
	updated := ks.Clone()
	updated.TargetNamespace = res.ns
	return namespacePrediction{object: updated, forced: forced}
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
func (r *documentReader) resolveOverlayInjectedNamespace(
	repoRoot, file string,
	cache map[string]nsResolution,
) nsResolution {
	dr := *r
	for dir := path.Dir(filepath.ToSlash(file)); dir != "." && dir != "/" && dir != ""; dir = path.Dir(dir) {
		key := filepath.Join(repoRoot, filepath.FromSlash(dir))
		if dr.cache[key].body == nil {
			continue
		}
		res, ok := cache[key]
		if !ok {
			res = new(dr).resolveInjectedTargetNamespace(repoRoot, dir)
			cache[key] = res
		}
		if res.ok {
			return res
		}
	}
	return nsResolution{}
}

func (r *documentReader) resolveInjectedTargetNamespace(repoRoot, overlayDir string) nsResolution {
	dr := *r
	if r := new(dr).transformerInjectedNamespace(repoRoot, overlayDir, nil); r.ok {
		return r
	}
	return new(dr).replacementInjectedNamespace(repoRoot, overlayDir)
}

// transformerInjectedNamespace inspects the kustomization.yaml at overlayDir.
// When it applies a `transformers:` entry whose subtree both carries a
// `namespace:` directive and resolves to a builtin NamespaceTransformer that
// sets Kustomization spec/targetNamespace, the directive value is the namespace
// that transformer injects.
func (r *documentReader) transformerInjectedNamespace(repoRoot, overlayDir string, inputs *[]walkInput) nsResolution {
	dr := *r
	spec := dr.read(filepath.Join(repoRoot, overlayDir)).body
	for ref := range kustomizeinput.Kustomization(spec) {
		if ref.Role != kustomizeinput.Configuration || !strings.HasPrefix(ref.Field, ".transformers") || ref.Inline {
			continue
		}
		transformerDir, ok := kustomizeinput.Local(repoRoot, overlayDir, ref.Path)
		if !ok {
			continue
		}
		if inputs != nil {
			*inputs = append(*inputs, walkInput{path: filepath.Join(repoRoot, transformerDir)})
		}
		td := dr.read(filepath.Join(repoRoot, transformerDir)).body
		if td == nil || td.Namespace == "" {
			continue
		}
		if new(dr).subtreeHasNamespaceTransformer(repoRoot, transformerDir, map[string]struct{}{}, inputs) {
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
func (r *documentReader) replacementInjectedNamespace(repoRoot, overlayDir string) nsResolution {
	dr := *r
	d := dr.read(filepath.Join(repoRoot, overlayDir)).body
	if d == nil || d.Namespace == "" || len(d.Replacements) == 0 {
		return nsResolution{}
	}
	for _, r := range d.Replacements {
		if replacementInjectsTargetNamespace(r, new(dr), repoRoot, overlayDir) {
			return nsResolution{ns: d.Namespace, ok: true}
		}
	}
	return nsResolution{}
}

func (r *documentReader) subtreeHasNamespaceTransformer(
	repoRoot, dir string,
	visited map[string]struct{},
	inputs *[]walkInput,
) bool {
	dr := *r
	w := traversal{documents: dr, policy: traversalProof, directories: visited, scopeInputs: inputs}
	return !w.walk(context.TODO(), graphVisit{root: repoRoot, dir: dir}, nil)
}

// fileHasNamespaceTransformer reads target as a (possibly multi-document)
// manifest file and reports whether any document is a qualifying
// NamespaceTransformer. Returns false when target is a directory, unreadable, or
// any document is malformed (all-or-nothing, matching a whole-file parse).
func (r *documentReader) fileHasNamespaceTransformer(repoRoot, target string) bool {
	dr := *r
	inputFile := new(dr).input(filepath.Join(repoRoot, target))
	data, err := fs.ReadFile(inputFile.fs, inputFile.name)
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

func replacementInjectsTargetNamespace(
	r types.ReplacementField,
	inputReader *documentReader,
	repoRoot, overlayDir string,
) bool {
	dr := *inputReader
	if r.Path == "" {
		return copiesNamespaceNameToTargetNamespace(r.Replacement)
	}
	if r.Source != nil || len(r.Targets) != 0 || r.SourceValue != nil {
		return false
	}
	var ref string
	spec := dr.read(filepath.Join(repoRoot, overlayDir)).body
	for input := range kustomizeinput.Kustomization(spec) {
		if !strings.HasPrefix(input.Field, ".replacements.*") || input.Path != r.Path {
			continue
		}
		var ok bool
		ref, ok = kustomizeinput.Local(repoRoot, overlayDir, input.Path)
		if !ok {
			return false
		}
		break
	}
	if ref == "" {
		return false
	}
	inputFile2 := new(dr).input(filepath.Join(repoRoot, ref))
	data, err := fs.ReadFile(inputFile2.fs, inputFile2.name)
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
