package values

import (
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"regexp"
	"slices"
	"sort"
	"strings"
	"sync"

	"helm.sh/helm/v4/pkg/chart/v2/loader"
	"helm.sh/helm/v4/pkg/strvals"
	"sigs.k8s.io/yaml"

	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

// varsubRegex matches the upstream kustomize-controller var-name
// validation from fluxcd/pkg/kustomize/kustomize_varsub.go: identifiers
// composed of an alpha/underscore lead and alnum/underscore tail. A
// ConfigMap key like `my-var` (dash) is invalid; upstream rejects the
// whole postBuild rather than silently substituting nothing.
var varsubRegex = regexp.MustCompile(`^[_a-zA-Z][_a-zA-Z0-9]*$`)

// Provider exposes lazy ConfigMap/Secret data by canonical identity.
type Provider interface {
	Data(manifest.NamedResource) manifest.Data
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

// Data returns the captured data for id.
//
//nolint:ireturn // The values contract supports multiple data implementations.
func (p *StoreProvider) Data(id manifest.NamedResource) manifest.Data { return p.Lookup(id).Data }

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

// ExpandValueReferences resolves all spec.valuesFrom references on hr,
// merges them with hr.Values (inline values take precedence per Helm
// semantics), and writes the result back to hr.Values.
//
// Honors ValuesReference.Optional: missing resources or values keys on
// Optional=true refs are skipped silently; Optional=false refs fail.
// Matches Flux helm-controller chartutil semantics.
//
// Hard errors from the lookup itself — unsupported kind, malformed
// binaryData — always bubble up; they are unrelated to whether the ref
// is optional.
//
// cache may be nil — tests and embedders without an orchestrator pass
// nil and pay the (small) per-ref yaml.Unmarshal cost. Orchestrators
// supply a Cache constructed at startup so refs shared across HRs
// (a common pattern for platform-wide values CMs) parse exactly once.
func ExpandValueReferences(
	hr *manifest.HelmRelease,
	provider Provider,
	cache *Cache,
) error {
	if hr == nil || len(hr.ValuesFrom) == 0 {
		return nil
	}

	ex := valueExpansion{values: map[string]any{}, owned: true}
	for edge := range manifest.References(hr, manifest.ReferenceRelations) {
		if edge.Use != "valuesFrom" {
			continue
		}
		ref := hr.ValuesFrom[edge.Position]
		if err := ex.expand(&ref, &edge, provider, cache); err != nil {
			return err
		}
	}
	DeepMergeInto(ex.values, hr.Values)
	hr.Values = ex.values
	return nil
}

// lookupValueRef reads only the requested key, preserving unrelated malformed siblings.
func lookupValueRef(ref manifest.ValuesReference, namespace string, p Provider) (string, error) {
	data, err := resourceData(ref.Kind, namespace, ref.Name, p)
	if err != nil {
		return "", err
	}
	if data == nil {
		return "", &missingValueRefError{kind: ref.Kind, namespace: namespace, name: ref.Name}
	}
	key := ref.GetValuesKey()
	valueResult, err := data.Value(key)
	value, ok := valueResult.Value, valueResult.Found
	if err != nil {
		return "", err
	}
	if !ok {
		// Empty bags retain the same missing-resource disposition as absent objects.
		if !data.HasKeys() {
			return "", &missingValueRefError{kind: ref.Kind, namespace: namespace, name: ref.Name}
		}
		return "", &missingValueRefError{kind: ref.Kind, namespace: namespace, name: ref.Name, key: key}
	}
	return value, nil
}

//nolint:ireturn // The values contract supports multiple data implementations.
func resourceData(kind, namespace, name string, p Provider) (manifest.Data, error) {
	switch kind {
	case manifest.KindSecret, manifest.KindConfigMap:
	default:
		return nil, manifest.NewError(manifest.ErrInvalidValuesReference, nil, fmt.Sprintf("unsupported kind %s", kind))
	}
	return p.Data(manifest.NamedResource{Kind: kind, Namespace: namespace, Name: name}), nil
}

type missingValueRefError struct {
	kind      string
	namespace string
	name      string
	key       string
}

func (e *missingValueRefError) Error() string {
	if e.key != "" {
		return fmt.Sprintf("valuesFrom %s %s/%s key %q not found",
			e.kind, e.namespace, e.name, e.key)
	}
	return fmt.Sprintf("valuesFrom %s %s/%s not found", e.kind, e.namespace, e.name)
}

func (e *missingValueRefError) Unwrap() error {
	if e.key != "" {
		return manifest.ErrInvalidValuesReference
	}
	return manifest.ErrObjectNotFound
}

func (c *Cache) updateHelmReleaseValues(
	ref manifest.ValuesReference,
	found string,
	values map[string]any,
) error {
	parsed, ok := c.lookup(found)
	if !ok {
		var decoded map[string]any
		if err := yaml.Unmarshal([]byte(found), &decoded); err != nil {
			return manifest.NewError(
				manifest.ErrInput,
				fmt.Errorf("expected '%s' values key %q to be valid YAML: %w", ref.Name, ref.GetValuesKey(), err),
				"",
			)
		}
		if decoded == nil {
			return nil
		}
		parsed = decoded
		c.put(found, parsed)
	}
	DeepMergeInto(values, parsed)
	return nil
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

// ExpandPostBuildSubstituteReference resolves referenced and authored inline substitutions.
func ExpandPostBuildSubstituteReference(ks *manifest.Kustomization, p Provider) error {
	if ks == nil || ks.PostBuild == nil || len(ks.PostBuild.SubstituteFrom) == 0 && len(ks.PostBuild.Substitute) == 0 {
		return nil
	}
	if len(ks.PostBuild.SubstituteFrom) > 0 && ks.Namespace == "" {
		return manifest.NewError(
			manifest.ErrInvalidSubstituteReference,
			nil,
			"Kustomization with substituteFrom has no namespace",
		)
	}

	// Flux gives inline substitutions precedence over references.
	values := map[string]string{}
	if err := readSubstitutions(ks, p, values); err != nil {
		return err
	}
	maps.Copy(values, ks.PostBuild.Substitute)

	if err := validateSubstitutions(values); err != nil {
		return err
	}
	ks.PostBuild.Substitute = values
	return nil
}

func mergeSubstitutions(values map[string]string, data manifest.Data) error {
	var failure struct {
		key string
		err error
	}
	data.Keys()(func(k string) bool {
		valueResult, err := data.Value(k)
		v := valueResult.Value
		if err != nil {
			if failure.err == nil || k < failure.key {
				failure.err, failure.key = err, k
			}
			return true
		}
		// Flux strips newlines before substituting into single-line YAML fields.
		values[k] = strings.ReplaceAll(v, "\n", "")
		return true
	})
	return failure.err
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
	values map[string]any
	owned  bool
}

func (ex *valueExpansion) expand(
	ref *manifest.ValuesReference,
	edge *manifest.Reference,
	provider Provider,
	cache *Cache,
) error {
	found, err := lookupValueRef(*ref, edge.Namespace, provider)
	if err != nil {
		if _, ok := errors.AsType[*missingValueRefError](err); ok && ref.Optional {
			return nil
		}
		return err
	}
	if ref.TargetPath != "" {
		if !ex.owned {
			// Cached nested ex.values may be borrowed; strvals mutates them in place.
			ex.values = manifest.DeepCopyMap(ex.values)
			ex.owned = true
		}
		err = setValueReference(ex.values, ref, found)
	} else {
		err = cache.updateHelmReleaseValues(*ref, found, ex.values)
		ex.owned = false
	}
	if err != nil {
		return err
	}
	return nil
}

func readSubstitutions(ks *manifest.Kustomization, p Provider, values map[string]string) error {
	for _, ref := range ks.PostBuild.SubstituteFrom {
		data, err := resourceData(ref.Kind, ks.Namespace, ref.Name, p)
		if err != nil {
			if errors.Is(err, manifest.ErrInvalidValuesReference) {
				slog.Debug("values: unsupported SubstituteReference kind",
					"id", manifest.DiagnosticFor(ks.Named(), ks).String(), "kind", ref.Kind)
				continue
			}
			return err
		}
		if data == nil || !data.HasKeys() {
			if !ref.Optional && ref.Kind != manifest.KindSecret {
				slog.Debug("values: SubstituteReference not found",
					"id", manifest.DiagnosticFor(ks.Named(), ks).String(), "ref", ref.Name)
			}
			continue
		}
		if err := mergeSubstitutions(values, data); err != nil {
			return err
		}
	}

	return nil
}

const (
	matchingQuoteWidth = 2
)
