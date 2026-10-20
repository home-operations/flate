// Package kustomizeinput classifies Kustomize's direct file and configuration inputs.
package kustomizeinput

import (
	"iter"
	"path/filepath"
	"strconv"
	"strings"

	"sigs.k8s.io/kustomize/api/provider"
	"sigs.k8s.io/kustomize/api/resource"
	"sigs.k8s.io/kustomize/api/types"
	"sigs.k8s.io/kustomize/kyaml/yaml"

	"github.com/home-operations/flate/internal/kustpath"
)

// Role distinguishes terminal inputs from accumulated resources and configurations.
type Role uint8

const (
	// Resource is an accumulated resource file or directory.
	Resource Role = iota
	// Component is an accumulated component directory.
	Component
	// Configuration is a plugin configuration file, directory or inline resource.
	Configuration
	// File is a terminal input, never a resource to admit.
	File
)

// Reference retains an operand's immutable classification and its rewrite location.
type Reference struct {
	Node      *yaml.Node
	Path      string
	Field     string
	Role      Role
	Inline    bool
	Resources []*resource.Resource
	Indices   [2]int
	key       string
}

// Rewrite updates a private render input, retaining any generator key.
func (r Reference) Rewrite(value string) {
	if r.key != "" {
		value = r.key + "=" + value
	}
	r.Node.Value, r.Node.Tag, r.Node.Style = value, "!!str", 0
}

// Bind attaches an input to a private render tree so rewrites retain comments.
func (r *Reference) Bind(doc *yaml.Node) {
	node := doc
	index := 0
	for part := range strings.SplitSeq(strings.TrimPrefix(r.Field, "."), ".") {
		if part == "*" {
			if node == nil || node.Kind != yaml.SequenceNode || r.Indices[index] >= len(node.Content) {
				return
			}
			node = node.Content[r.Indices[index]]
			index++
		} else {
			node = MappingValue(node, part)
		}
	}
	r.Node = node
}

// FieldPath returns the indexed selector for a diagnostic.
func (r Reference) FieldPath() string {
	label := r.Field
	for _, index := range r.Indices {
		before, after, indexed := strings.Cut(label, ".*")
		if !indexed {
			break
		}
		label = before + "[" + strconv.Itoa(index) + "]" + after
	}
	return label
}

type operandKind uint8

const (
	plain operandKind = iota
	generator
	strategic
)

type field struct {
	path string
	role Role
	kind operandKind
}

var generatorFields = []field{{".files.*", File, generator}, {".envs.*", File, plain}}

// Kustomization returns direct inputs without mutating the typed document.
//
//nolint:staticcheck // Deprecated Kustomize fields still contribute read dependencies.
func Kustomization(k *types.Kustomization) iter.Seq[Reference] {
	return func(yield func(Reference) bool) {
		if k == nil {
			return
		}
		add := func(value string, f field, indices [2]int) bool {
			if r, ok := inputReference(Reference{Path: value, Field: f.path, Role: f.role, Indices: indices}, f.kind); ok {
				return yield(r)
			}
			return true
		}
		list := func(values []string, f field) bool {
			for i := range values {
				if !add(values[i], f, [2]int{i}) {
					return false
				}
			}
			return true
		}
		if !list(k.Resources, field{".resources.*", Resource, plain}) {
			return
		}
		if !list(k.Bases, field{".bases.*", Resource, plain}) {
			return
		}
		if !list(k.Components, field{".components.*", Component, plain}) {
			return
		}
		for i := range k.Patches {
			if !add(k.Patches[i].Path, field{".patches.*.path", File, plain}, [2]int{i}) {
				return
			}
		}
		for i := range k.PatchesJson6902 {
			if !add(k.PatchesJson6902[i].Path, field{".patchesJson6902.*.path", File, plain}, [2]int{i}) {
				return
			}
		}
		for i := range k.PatchesStrategicMerge {
			if !add(string(k.PatchesStrategicMerge[i]), field{".patchesStrategicMerge.*", File, strategic}, [2]int{i}) {
				return
			}
		}
		for i := range k.Replacements {
			if !add(k.Replacements[i].Path, field{".replacements.*.path", File, plain}, [2]int{i}) {
				return
			}
		}
		if !list(k.Configurations, field{".configurations.*", File, plain}) {
			return
		}
		if !list(k.Crds, field{".crds.*", File, plain}) {
			return
		}
		if !add(k.OpenAPI["path"], field{".openapi.path", File, plain}, [2]int{}) {
			return
		}
		generatorInputs := func(g *types.KvPairSources, inventory [3]field, index int) bool {
			for i := range g.FileSources {
				if !add(g.FileSources[i], inventory[0], [2]int{index, i}) {
					return false
				}
			}
			for i := range g.EnvSources {
				if !add(g.EnvSources[i], inventory[1], [2]int{index, i}) {
					return false
				}
			}
			return add(g.EnvSource, inventory[2], [2]int{index})
		}
		for i := range k.ConfigMapGenerator {
			if !generatorInputs(&k.ConfigMapGenerator[i].KvPairSources, [3]field{{".configMapGenerator.*.files.*", File, generator}, {".configMapGenerator.*.envs.*", File, plain}, {".configMapGenerator.*.env", File, plain}}, i) {
				return
			}
		}
		for i := range k.SecretGenerator {
			if !generatorInputs(&k.SecretGenerator[i].KvPairSources, [3]field{{".secretGenerator.*.files.*", File, generator}, {".secretGenerator.*.envs.*", File, plain}, {".secretGenerator.*.env", File, plain}}, i) {
				return
			}
		}
		if !list(k.Generators, field{".generators.*", Configuration, plain}) {
			return
		}
		if !list(k.Transformers, field{".transformers.*", Configuration, plain}) {
			return
		}
		if !list(k.Validators, field{".validators.*", Configuration, plain}) {
			return
		}
	}
}

// Plugin returns builtin configuration operands and whether the kind is known.
func Plugin(doc *yaml.Node) ([]Reference, bool) {
	api, kind := MappingValue(doc, "apiVersion"), MappingValue(doc, "kind")
	if api == nil || api.Value != "builtin" || kind == nil {
		return nil, false
	}
	var inventory []field
	switch kind.Value {
	case "ConfigMapGenerator", "SecretGenerator":
		inventory = generatorFields
	case "PatchTransformer", "PatchJson6902Transformer":
		inventory = []field{{".path", File, plain}}
	case "PatchStrategicMergeTransformer":
		inventory = []field{{".paths.*", File, strategic}}
	case "ReplacementTransformer":
		inventory = []field{{".replacements.*.path", File, plain}}
	case "ValueAddTransformer":
		inventory = []field{{".targetFilePath", File, plain}}
	case "IAMPolicyGenerator", "AnnotationsTransformer", "HashTransformer", "ImageTagTransformer", "LabelTransformer",
		"NamespaceTransformer", "PrefixSuffixTransformer", "PrefixTransformer", "SuffixTransformer", "ReplicaCountTransformer":
	default:
		return nil, false
	}
	return fields(doc, inventory), true
}

func fields(doc *yaml.Node, inventory []field) []Reference {
	var refs []Reference
	for _, f := range inventory {
		path := strings.TrimPrefix(f.path, ".")
		name, rest, indexed := strings.Cut(path, ".*")
		node := MappingValue(doc, name)
		if node == nil {
			continue
		}
		if !indexed {
			if r, ok := inputReference(Reference{Node: node, Path: node.Value, Field: f.path, Role: f.role}, f.kind); ok {
				refs = append(refs, r)
			}
			continue
		}
		if node.Kind != yaml.SequenceNode {
			continue
		}
		for i, child := range node.Content {
			if rest != "" {
				child = MappingValue(child, strings.TrimPrefix(rest, "."))
			}
			if child != nil {
				if r, ok := inputReference(Reference{Node: child, Path: child.Value, Field: f.path, Role: f.role, Indices: [2]int{i}}, f.kind); ok {
					refs = append(refs, r)
				}
			}
		}
	}
	return refs
}

func inputReference(r Reference, kind operandKind) (Reference, bool) {
	if r.Path == "" || r.Node != nil && r.Node.Kind != yaml.ScalarNode {
		return r, false
	}
	switch kind {
	case generator:
		if strings.Count(r.Path, "=") > 1 {
			return r, false // Upstream owns invalid file-source diagnostics.
		}
		key, value, explicit := strings.Cut(r.Path, "=")
		if explicit {
			if key == "" || value == "" {
				return r, false
			}
			r.key, r.Path = key, value
		} else {
			r.key = filepath.Base(r.Path)
		}
	case strategic:
		if _, err := Resources([]byte(r.Path)); err == nil {
			return r, false
		}
	}
	if r.Role == Configuration {
		resources, err := Resources([]byte(r.Path))
		r.Inline = err == nil
		r.Resources = resources
	}
	return r, true
}

// Resources uses the same inline resource grammar as Kustomize's plugin loader.
func Resources(body []byte) ([]*resource.Resource, error) {
	return provider.NewDefaultDepProvider().GetResourceFactory().SliceFromBytes(body)
}

// MappingValue follows upstream's case-insensitive JSON field precedence.
func MappingValue(node *yaml.Node, key string) *yaml.Node {
	if node == nil {
		return nil
	}
	if node.Kind == yaml.DocumentNode && len(node.Content) != 0 {
		node = node.Content[0]
	}
	if node.Kind == yaml.AliasNode {
		return MappingValue(node.Alias, key)
	}
	var found *yaml.Node
	matched := ""
	hasMerge := false
	if node.Kind == yaml.MappingNode {
		for i := 0; i+1 < len(node.Content); i += 2 {
			name, value := node.Content[i], node.Content[i+1]
			hasMerge = hasMerge || name.Tag == "!!merge"
			if strings.EqualFold(name.Value, key) && name.Value >= matched {
				matched, found = name.Value, value
			}
		}
	}
	if found == nil && hasMerge {
		for i := 0; i+1 < len(node.Content); i += 2 {
			name, value := node.Content[i], node.Content[i+1]
			if name.Tag != "!!merge" {
				continue
			}
			if value.Kind == yaml.SequenceNode {
				for _, merged := range value.Content {
					if found = MappingValue(merged, key); found != nil {
						break
					}
				}
			} else {
				found = MappingValue(value, key)
			}
			if found != nil {
				break
			}
		}
	}
	if found != nil && found.Kind == yaml.AliasNode {
		return found.Alias
	}
	return found
}

// Local resolves an input inside its source and returns source-relative coordinates.
func Local(root, dir, raw string) (string, bool) {
	resolved, ok := kustpath.Resolve(filepath.Join(root, dir), raw, kustpath.Dependency)
	if !ok {
		return "", false
	}
	rel, err := filepath.Rel(root, resolved)
	if err != nil || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return "", false
	}
	return filepath.ToSlash(rel), true
}

// PreservesOperands proves the narrow metadata-only configuration wrapper case.
//
//nolint:staticcheck // Deprecated Kustomize transforms still affect operand uncertainty.
func PreservesOperands(k *types.Kustomization) bool {
	if len(k.Patches) != 0 || len(k.PatchesJson6902) != 0 || len(k.PatchesStrategicMerge) != 0 ||
		len(k.Replacements) != 0 || len(k.Configurations) != 0 || len(k.Crds) != 0 ||
		len(k.Vars) != 0 || len(k.Images) != 0 || len(k.ImageTags) != 0 || len(k.Replicas) != 0 {
		return false
	}
	for _, label := range k.Labels {
		for _, spec := range label.FieldSpecs {
			if !strings.HasPrefix(spec.Path, "metadata/") {
				return false
			}
		}
	}
	return true
}

// MetadataOnly reports whether a plugin can only change configuration metadata.
func MetadataOnly(doc *yaml.Node) bool {
	api, kind := MappingValue(doc, "apiVersion"), MappingValue(doc, "kind")
	if api == nil || api.Value != "builtin" || kind == nil {
		return false
	}
	switch kind.Value {
	case "NamespaceTransformer", "AnnotationsTransformer", "LabelTransformer", "PrefixSuffixTransformer", "PrefixTransformer", "SuffixTransformer":
	default:
		return false
	}
	return metadataFields(MappingValue(doc, "fieldSpecs"))
}

func metadataFields(specs *yaml.Node) bool {
	if specs != nil {
		for _, spec := range specs.Content {
			p := MappingValue(spec, "path")
			if p == nil || !strings.HasPrefix(p.Value, "metadata/") {
				return false
			}
		}
	}
	return true
}
