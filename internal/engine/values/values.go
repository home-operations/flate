package values

import (
	"fmt"
	"maps"
	"regexp"
	"slices"
	"sort"
	"strings"
	"sync"

	"helm.sh/helm/v4/pkg/chart/v2/loader"
	"helm.sh/helm/v4/pkg/strvals"
	"sigs.k8s.io/yaml"

	"github.com/home-operations/flate/internal/engine/store"
	"github.com/home-operations/flate/pkg/manifest"
)

// varsubRegex matches the upstream kustomize-controller var-name
// validation from fluxcd/pkg/kustomize/kustomize_varsub.go: identifiers
// composed of an alpha/underscore lead and alnum/underscore tail. A
// ConfigMap key like `my-var` (dash) is invalid; upstream rejects the
// whole postBuild rather than silently substituting nothing.
var varsubRegex = regexp.MustCompile(`^[_a-zA-Z][_a-zA-Z0-9]*$`)

// provider MUST preserve selected evidence and pre-supply data with each lookup.
type provider interface {
	Lookup(manifest.NamedResource) store.ReferenceFacts
}

type missingPolicy interface {
	AllowsKind(string) bool
}

type dataReader interface {
	ReferenceFacts(manifest.NamedResource, *manifest.DiscoveryIndex, manifest.NamedResource) store.ReferenceFacts
}

// StoreProvider retains one execution's captured values and availability evidence.
type StoreProvider struct {
	s dataReader

	owner  manifest.NamedResource
	inputs []capturedInput
}

// NewStoreProvider returns one execution's captured values and availability evidence.
func NewStoreProvider(s dataReader) *StoreProvider {
	p := &StoreProvider{s: s}
	if owner, ok := s.(interface{ ID() manifest.NamedResource }); ok {
		p.owner = owner.ID()
	}
	return p
}

type capturedInput struct {
	id    manifest.NamedResource
	input store.ReferenceFacts
}

// Lookup captures a values object and its materializers once for expansion and diagnostics.
func (p *StoreProvider) Lookup(id manifest.NamedResource) store.ReferenceFacts {
	i := sort.Search(len(p.inputs), func(i int) bool { return p.inputs[i].id.Name >= id.Name })
	for i < len(p.inputs) && p.inputs[i].id.Name == id.Name {
		if p.inputs[i].id == id {
			return p.inputs[i].input
		}
		i++
	}
	input := p.s.ReferenceFacts(id, nil, p.owner)
	p.inputs = slices.Insert(p.inputs, i, capturedInput{id: id, input: input})
	return input
}

// DeepMergeInto merges override into dst using Helm's merge semantics.
// Callers MUST own dst. Nested inputs remain read-only; non-colliding
// maps and lists may be borrowed by reference.
func DeepMergeInto(dst, override map[string]any) map[string]any {
	if dst == nil {
		dst = map[string]any{}
	}
	for k, v := range override {
		if existing, ok := dst[k]; ok {
			ebm, eok := existing.(map[string]any)
			vbm, vok := v.(map[string]any)
			if eok && vok {
				dst[k] = loader.MergeMaps(ebm, vbm)
				continue
			}
		}
		dst[k] = v
	}
	return dst
}

// Cache memoizes valuesFrom YAML by content across HelmReleases.
// Stored maps are immutable; callers must clone before mutation or merge
// with copy-on-write. The zero value is ready for concurrent use.
// A nil *Cache disables memoization.
type Cache struct {
	m sync.Map // map[string]map[string]any
}

func (c *Cache) lookup(key string) (map[string]any, bool) {
	if c == nil {
		return nil, false
	}
	v, ok := c.m.Load(key)
	if !ok {
		return nil, false
	}
	return v.(map[string]any), true //nolint:errcheck // Cache.put publishes only parsed maps under this private cache key.
}

// put stores an immutable parsed map. A nil receiver disables memoization.
func (c *Cache) put(key string, parsed map[string]any) {
	if c != nil {
		c.m.Store(key, parsed)
	}
}

// ExpandValueReferences MUST consume original authored positions before filtering references.
func ExpandValueReferences(hr *manifest.HelmRelease, inputs []Result, cache *Cache) error {
	if hr == nil || len(hr.ValuesFrom) == 0 {
		return nil
	}
	if err := validateResults(hr, inputs, "valuesFrom"); err != nil {
		return err
	}
	ex := valueExpansion{inline: hr.Values}
	filter := len(inputs) != len(hr.ValuesFrom)
	for i := range inputs {
		filter = filter || inputs[i].effect == Omitted
		ex.targets = ex.targets || inputs[i].effect == Used && hr.ValuesFrom[inputs[i].ref.Position].TargetPath != ""
	}
	if ex.targets {
		ex.inline = manifest.DeepCopyMap(ex.inline)
	}
	kept := hr.ValuesFrom
	if filter {
		kept = make([]manifest.ValuesReference, 0, len(inputs))
	}
	for i := range inputs {
		result := &inputs[i]
		if result.effect == Omitted {
			continue
		}
		ref := hr.ValuesFrom[result.ref.Position]
		if err := ex.expand(&ref, result.value, cache); err != nil {
			return err
		}
		if filter {
			kept = append(kept, ref)
		}
	}
	ex.values = DeepMergeInto(ex.values, ex.inline)
	hr.Values, hr.ValuesFrom = ex.values, kept
	return nil
}

func validateResults(obj manifest.BaseManifest, inputs []Result, use string) error {
	position := 0
	for ref := range manifest.References(obj, manifest.ReferenceRelations) {
		if ref.Use != use || ref.Name == "" {
			continue
		}
		if position == len(inputs) {
			return manifest.NewError(
				manifest.ErrInvalidValuesReference,
				nil,
				"reference requires a settled assessment in authored order",
			)
		}
		result := &inputs[position]
		if err := result.Err(); err != nil {
			return err
		}
		matchesReference := result.ref.NamedResource == ref.NamedResource && result.ref.Use == use &&
			result.ref.Position == ref.Position
		if !matchesReference || result.effect != Used && result.effect != Omitted {
			return manifest.NewError(
				manifest.ErrInvalidValuesReference,
				nil,
				"reference requires a settled assessment in authored order",
			)
		}
		position++
	}
	if position != len(inputs) {
		return manifest.NewError(manifest.ErrInvalidValuesReference, nil, "unexpected reference assessment")
	}
	return nil
}

func (c *Cache) document(ref manifest.ValuesReference, found string) (map[string]any, error) {
	parsed, ok := c.lookup(found)
	if !ok {
		var decoded map[string]any
		if err := yaml.Unmarshal([]byte(found), &decoded); err != nil {
			return nil, manifest.NewError(
				manifest.ErrInput,
				fmt.Errorf("expected '%s' values key %q to be valid YAML: %w", ref.Name, ref.GetValuesKey(), err),
				"",
			)
		}
		if decoded == nil {
			return nil, nil
		}
		parsed = decoded
		c.put(found, parsed)
	}
	return parsed, nil
}

// Matching quotes MUST force strings instead of Helm's ordinary type coercion.
func replaceValueAtPath(values map[string]any, path, value string) error {
	var err error
	if unquoted, ok := stripMatchingQuotes(value); ok {
		err = strvals.ParseIntoString(path+"="+unquoted, values)
	} else {
		err = strvals.ParseInto(path+"="+value, values)
	}
	if err != nil {
		return manifest.NewError(manifest.ErrInput, fmt.Errorf("targetPath %q: %w", path, err), "")
	}
	return nil
}

// ParseIntoString preserves escaped path separators; ParseLiteralInto does not.
func replaceLiteralValueAtPath(values map[string]any, path, value string) error {
	// Backslash must be escaped first so the escapes added for the other
	// metacharacters aren't themselves re-escaped.
	escaper := strings.NewReplacer(
		`\`, `\\`,
		`,`, `\,`,
		`[`, `\[`,
		`]`, `\]`,
		`{`, `\{`,
		`}`, `\}`,
	)
	if err := strvals.ParseIntoString(path+"="+escaper.Replace(value), values); err != nil {
		return manifest.NewError(manifest.ErrInput, fmt.Errorf("targetPath %q: %w", path, err), "")
	}
	return nil
}

// A single quote MUST remain a scalar; stripping it would produce an invalid slice.
func stripMatchingQuotes(value string) (string, bool) {
	if len(value) >= matchingQuoteWidth {
		if q := value[0]; (q == '\'' || q == '"') && value[len(value)-1] == q {
			return value[1 : len(value)-1], true
		}
	}
	return value, false
}

// ExpandPostBuildSubstituteReference MUST return captured failures before mutating the declaration.
func ExpandPostBuildSubstituteReference(ks *manifest.Kustomization, inputs []Result) error {
	if ks == nil || ks.PostBuild == nil {
		return nil
	}
	if len(ks.PostBuild.SubstituteFrom) > 0 && ks.Namespace == "" {
		return manifest.NewError(
			manifest.ErrInvalidSubstituteReference,
			nil,
			"Kustomization with substituteFrom has no namespace",
		)
	}
	if err := validateResults(ks, inputs, substitutionUse); err != nil {
		return err
	}
	capacity := len(ks.PostBuild.Substitute)
	for i := range inputs {
		if inputs[i].effect == Used {
			capacity += len(inputs[i].variables)
		}
	}
	variables := make(map[string]string, capacity)
	for i := range inputs {
		result := &inputs[i]
		if result.effect == Used {
			maps.Copy(variables, result.variables)
		}
	}
	maps.Copy(variables, ks.PostBuild.Substitute)
	if err := validateSubstitutions(variables); err != nil {
		return err
	}
	ks.PostBuild.Substitute = variables
	return nil
}

func validateSubstitutions(values map[string]string) error {
	// Flux rejects invalid variable names; sort diagnostics for determinism.
	var invalid []string
	for name := range values {
		if !varsubRegex.MatchString(name) {
			invalid = append(invalid, name)
		}
	}
	if len(invalid) > 0 {
		slices.Sort(invalid)
		noun := "variable"
		if len(invalid) > 1 {
			noun = "variables"
		}
		return manifest.NewError(
			manifest.ErrInvalidSubstituteReference,
			fmt.Errorf("substitute var name(s) %v invalid (must match %s)", invalid, varsubRegex.String()),
			"invalid substitute "+noun+" "+strings.Join(invalid, ", "),
		)
	}
	return nil
}

func setValueReference(values map[string]any, ref *manifest.ValuesReference, found string) error {
	if ref.Literal {
		return replaceLiteralValueAtPath(values, ref.TargetPath, found)
	}
	return replaceValueAtPath(values, ref.TargetPath, found)
}

type valueExpansion struct {
	values  map[string]any
	inline  map[string]any
	targets bool
}

func (ex *valueExpansion) expand(ref *manifest.ValuesReference, found string, cache *Cache) error {
	if ref.TargetPath != "" {
		ex.values = DeepMergeInto(ex.values, ex.inline)
		return setValueReference(ex.values, ref, found)
	}
	parsed, err := cache.document(*ref, found)
	if err != nil {
		return err
	}
	if ex.targets && cache != nil {
		// A cache miss also publishes an immutable document before target writes.
		parsed = manifest.DeepCopyMap(parsed)
	}
	if ex.targets && ex.values == nil {
		ex.values = parsed
	} else {
		ex.values = DeepMergeInto(ex.values, parsed)
	}
	return nil
}

const (
	matchingQuoteWidth = 2
)
