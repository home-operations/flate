package values

import (
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"regexp"
	"slices"
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

// ValidSubstitutionName reports whether name follows Flux's variable-name rule.
func ValidSubstitutionName(name string) bool { return varsubRegex.MatchString(name) }

// Provider exposes lazy ConfigMap/Secret data by canonical identity.
type Provider interface {
	Data(manifest.NamedResource) manifest.Data
}

type dataReader interface {
	GetObject(manifest.NamedResource) manifest.BaseManifest
	ReferenceFacts(manifest.NamedResource, *manifest.DiscoveryIndex) store.ReferenceFacts
}

// Input retains data and the evidence explaining its offline availability.
type Input struct {
	Data          manifest.Data
	Materializers []manifest.NamedResource
	Generated     bool
	Offline       bool
	Private       bool
}

// NewStoreProvider returns one execution's captured values and availability evidence.
func NewStoreProvider(s dataReader, wipeSecrets bool, supplied map[manifest.NamedResource]manifest.BaseManifest) *StoreProvider {
	p := &StoreProvider{s: s, wipeSecrets: wipeSecrets, supplied: supplied}
	if owner, ok := s.(interface{ ID() manifest.NamedResource }); ok {
		p.owner = owner.ID()
	}
	return p
}

// StoreProvider retains one execution's captured values and availability evidence.
type StoreProvider struct {
	s           dataReader
	wipeSecrets bool
	owner       manifest.NamedResource
	supplied    map[manifest.NamedResource]manifest.BaseManifest
	inputs      map[manifest.NamedResource]Input
}

// Data returns the captured data for id.
func (p *StoreProvider) Data(id manifest.NamedResource) manifest.Data { return p.Lookup(id).Data }

// Lookup captures a values object and its materializers once for expansion and diagnostics.
func (p *StoreProvider) Lookup(id manifest.NamedResource) Input {
	if input, ok := p.inputs[id]; ok {
		return input
	}
	data, supplied := p.supplied[id].(manifest.Data)
	input := Input{Data: data, Private: supplied}
	if !supplied {
		facts := p.s.ReferenceFacts(id, nil)
		self, independent := false, false
		var declarations []manifest.NamedResource
		for _, production := range facts.Productions {
			self = self || production.Owner == p.owner && p.owner != (manifest.NamedResource{})
			if production.Owner != (manifest.NamedResource{}) && production.Owner != p.owner && !slices.Contains(input.Materializers, production.Owner) {
				input.Materializers = append(input.Materializers, production.Owner)
			}
			if production.Declaration == (manifest.NamedResource{}) {
				input.Offline = input.Offline || production.Owner != (manifest.NamedResource{}) || production.Path != ""
				independent = independent || production.Owner == (manifest.NamedResource{}) && production.Path != ""
			} else {
				input.Generated = true
				declarations = append(declarations, production.Declaration)
			}
		}
		input.Private = self
		if p.owner == (manifest.NamedResource{}) || facts.Accepted || independent || len(input.Materializers) == 0 {
			input.Data = facts.Data
		}
		pending := false
		if reads, ok := p.s.(interface {
			UnsettledProducers(manifest.NamedResource) []manifest.NamedResource
		}); ok && input.Data == nil && input.Generated {
			pending = len(reads.UnsettledProducers(id)) != 0
		}
		if input.Data == nil && p.wipeSecrets && !input.Offline && !pending {
			for _, declarationID := range declarations {
				declaration := p.s.GetObject(declarationID)
				if declaration == nil {
					continue
				}
				for _, target := range manifest.ProducerTargets(declaration, id) {
					if target.NamedResource != id || len(target.DeclaredKeys) == 0 {
						continue
					}
					data := make(map[string]any, len(target.DeclaredKeys))
					for _, key := range target.DeclaredKeys {
						data[key] = manifest.ValuePlaceholder(key)
					}
					input.Data = &manifest.Secret{Name: id.Name, Namespace: id.Namespace, StringData: data}
					break
				}
				if input.Data != nil {
					break
				}
			}
		}
		if input.Data != nil {
			input.Materializers = nil
		}
	}
	if p.inputs == nil {
		p.inputs = make(map[manifest.NamedResource]Input)
	}
	p.inputs[id] = input
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

// NewCache returns an empty *Cache ready for use. Construct one per
// orchestrator run and pass to ExpandValueReferences.
func NewCache() *Cache { return &Cache{} }

func (c *Cache) lookup(key string) (map[string]any, bool) {
	if c == nil {
		return nil, false
	}
	v, ok := c.m.Load(key)
	if !ok {
		return nil, false
	}
	return v.(map[string]any), true
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
// The returned identities name required wiped values files omitted during expansion.
//
// cache may be nil — tests and embedders without an orchestrator pass
// nil and pay the (small) per-ref yaml.Unmarshal cost. Orchestrators
// supply a Cache constructed at startup so refs shared across HRs
// (a common pattern for platform-wide values CMs) parse exactly once.
func ExpandValueReferences(hr *manifest.HelmRelease, provider Provider, cache *Cache) ([]manifest.NamedResource, error) {
	if hr == nil || len(hr.ValuesFrom) == 0 {
		return nil, nil
	}
	var omitted []manifest.NamedResource
	values := map[string]any{}
	owned := true
	for edge := range manifest.References(hr) {
		if edge.Use != "valuesFrom" {
			continue
		}
		ref := hr.ValuesFrom[edge.Position]
		found, err := lookupValueRef(ref, edge.Namespace, provider)
		if err != nil {
			if _, ok := errors.AsType[*missingValueRefError](err); ok && ref.Optional {
				continue
			}
			return omitted, fmt.Errorf("building HelmRelease %s: %w", hr.Named().NamespacedName(), err)
		}
		if ref.TargetPath != "" {
			if !owned {
				// Cached nested values may be borrowed; strvals mutates them in place.
				values = manifest.DeepCopyMap(values)
				owned = true
			}
			if ref.Literal {
				err = replaceLiteralValueAtPath(values, ref.TargetPath, found)
			} else {
				err = replaceValueAtPath(values, ref.TargetPath, found)
			}
		} else {
			var unavailable bool
			unavailable, err = cache.updateHelmReleaseValues(ref, found, values)
			if unavailable && !ref.Optional {
				omitted = append(omitted, edge.NamedResource)
			}
			owned = false
		}
		if err != nil {
			return omitted, fmt.Errorf("building HelmRelease %s: %w", hr.Named().NamespacedName(), err)
		}
	}
	DeepMergeInto(values, hr.Values)
	hr.Values = values
	return omitted, nil
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
	value, ok, err := data.Value(key)
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

func resourceData(kind, namespace, name string, p Provider) (manifest.Data, error) {
	switch kind {
	case manifest.KindSecret, manifest.KindConfigMap:
	default:
		return nil, fmt.Errorf("%w: unsupported kind %s", manifest.ErrInvalidValuesReference, kind)
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

func (c *Cache) updateHelmReleaseValues(ref manifest.ValuesReference, found string, values map[string]any) (bool, error) {
	parsed, ok := c.lookup(found)
	if !ok {
		var decoded map[string]any
		if err := yaml.Unmarshal([]byte(found), &decoded); err != nil {
			// Wiped scalar placeholders MUST act as empty values files.
			if manifest.HasValuePlaceholder(found) {
				return true, nil
			}
			return false, fmt.Errorf("%w: expected '%s' values key %q to be valid YAML: %w", manifest.ErrInput, ref.Name, ref.GetValuesKey(), err)
		}
		if decoded == nil {
			return false, nil
		}
		parsed = decoded
		c.put(found, parsed)
	}
	DeepMergeInto(values, parsed)
	return false, nil
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
		return fmt.Errorf("%w: targetPath %q: %w", manifest.ErrInput, path, err)
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
		return fmt.Errorf("%w: targetPath %q: %w", manifest.ErrInput, path, err)
	}
	return nil
}

// A single quote MUST remain a scalar; stripping it would produce an invalid slice.
func stripMatchingQuotes(value string) (string, bool) {
	if len(value) >= 2 {
		if q := value[0]; (q == '\'' || q == '"') && value[len(value)-1] == q {
			return value[1 : len(value)-1], true
		}
	}
	return value, false
}

// ExpandPostBuildSubstituteReference resolves substituteFrom references
// against the provider and updates ks.PostBuild.Substitute. Missing
// references are logged (Secrets silently) and the
// substitution proceeds with what's available.
func ExpandPostBuildSubstituteReference(ks *manifest.Kustomization, p Provider) error {
	return ExpandPostBuildSubstituteReferenceWithSubstitutions(ks, p, nil)
}

// ExpandPostBuildSubstituteReferenceWithSubstitutions applies overlay after
// referenced and inline values. An overlay never enables absent postBuild.
func ExpandPostBuildSubstituteReferenceWithSubstitutions(ks *manifest.Kustomization, p Provider, overlay map[string]string) error {
	if ks == nil || ks.PostBuild == nil || len(ks.PostBuild.SubstituteFrom) == 0 && len(ks.PostBuild.Substitute) == 0 && len(overlay) == 0 {
		return nil
	}
	if len(ks.PostBuild.SubstituteFrom) > 0 && ks.Namespace == "" {
		return fmt.Errorf("%w: Kustomization with substituteFrom has no namespace", manifest.ErrInvalidSubstituteReference)
	}

	// Flux gives inline substitutions precedence over references.
	values := map[string]string{}
	for _, ref := range ks.PostBuild.SubstituteFrom {
		data, err := resourceData(ref.Kind, ks.Namespace, ref.Name, p)
		if err != nil {
			if errors.Is(err, manifest.ErrInvalidValuesReference) {
				slog.Debug("values: unsupported SubstituteReference kind",
					"id", ks.Named().String(), "kind", ref.Kind)
				continue
			}
			return err
		}
		if data == nil || !data.HasKeys() {
			if !ref.Optional && ref.Kind != manifest.KindSecret {
				slog.Debug("values: SubstituteReference not found",
					"id", ks.Named().String(), "ref", ref.Name)
			}
			continue
		}
		var failure struct {
			key string
			err error
		}
		data.Keys()(func(k string) bool {
			v, _, err := data.Value(k)
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
		if failure.err != nil {
			return failure.err
		}
	}
	maps.Copy(values, ks.PostBuild.Substitute)
	maps.Copy(values, overlay)

	// Flux rejects invalid variable names; sort diagnostics for determinism.
	var invalid []string
	for name := range values {
		if !varsubRegex.MatchString(name) {
			invalid = append(invalid, name)
		}
	}
	if len(invalid) > 0 {
		slices.Sort(invalid)
		return fmt.Errorf("%w: substitute var name(s) %v invalid (must match %s)",
			manifest.ErrInvalidSubstituteReference, invalid, varsubRegex.String())
	}
	ks.PostBuild.Substitute = values
	return nil
}
