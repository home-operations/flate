package values

import (
	"encoding/base64"
	"fmt"
	"log/slog"
	"maps"
	"strings"

	"sigs.k8s.io/yaml"

	"github.com/home-operations/flate/pkg/manifest"
)

// Provider exposes the ConfigMap/Secret lookups needed for value
// reference expansion. The controllers implement it against the central
// store (see NewStoreProvider); tests use SliceProvider.
type Provider interface {
	ConfigMap(namespace, name string) *manifest.ConfigMap
	Secret(namespace, name string) *manifest.Secret
}

// SliceProvider implements Provider from in-memory slices.
type SliceProvider struct {
	ConfigMaps []*manifest.ConfigMap
	Secrets    []*manifest.Secret
}

// ConfigMap finds a ConfigMap by namespace+name.
func (p *SliceProvider) ConfigMap(namespace, name string) *manifest.ConfigMap {
	for _, c := range p.ConfigMaps {
		if c.Name == name && c.Namespace == namespace {
			return c
		}
	}
	return nil
}

// Secret finds a Secret by namespace+name.
func (p *SliceProvider) Secret(namespace, name string) *manifest.Secret {
	for _, s := range p.Secrets {
		if s.Name == name && s.Namespace == namespace {
			return s
		}
	}
	return nil
}

// ObjectLister is the minimal Store surface needed for value lookups.
// It is satisfied by *store.Store and by any test-only fake.
type ObjectLister interface {
	GetByName(kind, namespace, name string) manifest.BaseManifest
}

// NewStoreProvider returns a Provider backed by an ObjectLister (the
// central Store). It replaces the per-controller storeProvider types.
func NewStoreProvider(l ObjectLister) Provider { return &storeProvider{l: l} }

type storeProvider struct{ l ObjectLister }

func (p *storeProvider) ConfigMap(namespace, name string) *manifest.ConfigMap {
	c, _ := p.l.GetByName(manifest.KindConfigMap, namespace, name).(*manifest.ConfigMap)
	return c
}

func (p *storeProvider) Secret(namespace, name string) *manifest.Secret {
	s, _ := p.l.GetByName(manifest.KindSecret, namespace, name).(*manifest.Secret)
	return s
}

// DeepMerge returns a new map with override's keys merged into base.
// Nested maps recurse; lists and scalars from override fully replace
// values from base — matching Helm's merge semantics.
func DeepMerge(base, override map[string]any) map[string]any {
	out := make(map[string]any, len(base)+len(override))
	maps.Copy(out, base)
	for k, v := range override {
		if existing, ok := out[k]; ok {
			ebm, eok := existing.(map[string]any)
			vbm, vok := v.(map[string]any)
			if eok && vok {
				out[k] = DeepMerge(ebm, vbm)
				continue
			}
		}
		out[k] = v
	}
	return out
}

// ExpandValueReferences resolves all spec.valuesFrom references on hr,
// merges them with hr.Values (inline values take precedence per Helm
// semantics), and writes the result back to hr.Values.
//
// Per-reference resolution failures are logged and skipped, matching
// upstream behavior. Hard errors (kind unknown, missing key in present
// resource) bubble up.
func ExpandValueReferences(hr *manifest.HelmRelease, provider Provider) error {
	if hr == nil || len(hr.ValuesFrom) == 0 {
		return nil
	}
	values := map[string]any{}
	for _, ref := range hr.ValuesFrom {
		found, err := lookupValueRef(ref, hr.Namespace, provider)
		if err != nil {
			slog.Debug("values: skipped ValuesReference",
				"id", hr.Named().String(), "ref", ref.Name, "err", err)
			continue
		}
		if found == "" {
			continue
		}
		merged, err := updateHelmReleaseValues(ref, found, values)
		if err != nil {
			return fmt.Errorf("building HelmRelease %s: %w", hr.NamespacedName(), err)
		}
		values = merged
	}
	if len(hr.Values) > 0 {
		values = DeepMerge(values, hr.Values)
	}
	hr.Values = values
	return nil
}

// lookupValueRef returns the raw string value referenced by ref, or an
// empty string when the referenced object is missing and the ref is
// optional. Missing-but-optional refs with a target_path produce a
// placeholder so the downstream YAML still parses.
func lookupValueRef(ref manifest.ValuesReference, namespace string, p Provider) (string, error) {
	var data map[string]string
	var err error
	switch ref.Kind {
	case manifest.KindSecret:
		s := p.Secret(namespace, ref.Name)
		if s != nil {
			data, err = decodeBag(s.StringData, s.Data)
		}
	case manifest.KindConfigMap:
		c := p.ConfigMap(namespace, ref.Name)
		if c != nil {
			data, err = decodeBag(c.Data, c.BinaryData)
		}
	default:
		return "", fmt.Errorf("%w: unsupported valuesFrom kind %s", manifest.ErrInvalidValuesReference, ref.Kind)
	}
	if err != nil {
		return "", err
	}
	if data == nil {
		// Missing reference.
		if ref.TargetPath != "" {
			return fmt.Sprintf(manifest.ValuePlaceholderTemplate, ref.Name), nil
		}
		return "", nil
	}

	key := ref.EffectiveValuesKey()
	val, ok := data[key]
	if !ok {
		return "", fmt.Errorf("%w: key %q not found in %s/%s",
			manifest.ErrInvalidValuesReference, key, namespace, ref.Name)
	}
	return val, nil
}

// decodeBag normalizes ConfigMap/Secret data so callers see a single
// map[string]string. binaryData values are base64-decoded.
func decodeBag(stringData, binaryData map[string]any) (map[string]string, error) {
	if len(stringData) == 0 && len(binaryData) == 0 {
		return nil, nil
	}
	out := make(map[string]string, len(stringData)+len(binaryData))
	for k, v := range stringData {
		s, ok := v.(string)
		if !ok {
			s = fmt.Sprint(v)
		}
		out[k] = s
	}
	for k, v := range binaryData {
		s, ok := v.(string)
		if !ok {
			s = fmt.Sprint(v)
		}
		decoded, err := base64.StdEncoding.DecodeString(s)
		if err != nil {
			return nil, fmt.Errorf("decode binaryData[%s]: %w", k, err)
		}
		out[k] = string(decoded)
	}
	return out, nil
}

// updateHelmReleaseValues writes found into values using ref.TargetPath
// when set; otherwise the found YAML is parsed and deep-merged.
func updateHelmReleaseValues(ref manifest.ValuesReference, found string, values map[string]any) (map[string]any, error) {
	if ref.TargetPath != "" {
		parts := splitDottedPath(ref.TargetPath)
		inner := values
		for _, p := range parts[:len(parts)-1] {
			next, ok := inner[p].(map[string]any)
			if !ok {
				if _, alreadyHas := inner[p]; alreadyHas {
					return nil, fmt.Errorf("expected '%s' at %q to be a map", ref.Name, ref.TargetPath)
				}
				next = map[string]any{}
				inner[p] = next
			}
			inner = next
		}
		inner[parts[len(parts)-1]] = found
		return values, nil
	}

	var parsed map[string]any
	if err := yaml.Unmarshal([]byte(found), &parsed); err != nil {
		return nil, fmt.Errorf("expected '%s' values to be valid YAML: %w", ref.Name, err)
	}
	if parsed == nil {
		parsed = map[string]any{}
	}
	return DeepMerge(values, parsed), nil
}

// splitDottedPath splits a target_path on unescaped dots. Backslash
// escapes a literal dot in a key name.
func splitDottedPath(s string) []string {
	var out []string
	var cur strings.Builder
	for i := 0; i < len(s); i++ {
		switch s[i] {
		case '\\':
			if i+1 < len(s) {
				cur.WriteByte(s[i+1])
				i++
				continue
			}
		case '.':
			out = append(out, cur.String())
			cur.Reset()
		default:
			cur.WriteByte(s[i])
		}
	}
	out = append(out, cur.String())
	return out
}

// ExpandPostBuildSubstituteReference resolves substituteFrom references
// against the provider and updates ks.PostBuildSubstitute (and its raw
// Contents). Missing references are logged (Secrets silently) and the
// substitution proceeds with what's available.
func ExpandPostBuildSubstituteReference(ks *manifest.Kustomization, p Provider) error {
	if ks == nil || len(ks.PostBuildSubstituteFrom) == 0 {
		return nil
	}
	if ks.Namespace == "" {
		return fmt.Errorf("%w: Kustomization with substituteFrom has no namespace", manifest.ErrInvalidSubstituteReference)
	}

	values := maps.Clone(ks.PostBuildSubstitute)
	if values == nil {
		values = map[string]any{}
	}
	for _, ref := range ks.PostBuildSubstituteFrom {
		var data map[string]string
		var err error
		switch ref.Kind {
		case manifest.KindSecret:
			s := p.Secret(ks.Namespace, ref.Name)
			if s != nil {
				data, err = decodeBag(s.StringData, s.Data)
			}
		case manifest.KindConfigMap:
			c := p.ConfigMap(ks.Namespace, ref.Name)
			if c != nil {
				data, err = decodeBag(c.Data, c.BinaryData)
			}
		default:
			slog.Debug("values: unsupported SubstituteReference kind",
				"id", ks.Named().String(), "kind", ref.Kind)
			continue
		}
		if err != nil {
			return err
		}
		if data == nil {
			if !ref.Optional && ref.Kind != manifest.KindSecret {
				slog.Debug("values: SubstituteReference not found",
					"id", ks.Named().String(), "ref", ref.Name)
			}
			continue
		}
		for k, v := range data {
			values[k] = v
		}
	}
	ks.UpdatePostBuildSubstitutions(values)
	return nil
}

// VarsMap returns the substitution variables for use with
// kustomize.Substitute. Non-string values are stringified.
func VarsMap(in map[string]any) map[string]string {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]string, len(in))
	for k, v := range in {
		switch tv := v.(type) {
		case string:
			out[k] = tv
		default:
			out[k] = fmt.Sprint(v)
		}
	}
	return out
}
