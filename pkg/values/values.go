package values

import (
	"encoding/base64"
	"fmt"
	"log/slog"
	"maps"
	"regexp"
	"slices"
	"strings"

	"helm.sh/helm/v4/pkg/strvals"
	"sigs.k8s.io/yaml"

	"github.com/home-operations/flate/pkg/manifest"
)

// varsubRegex matches the upstream kustomize-controller var-name
// validation from fluxcd/pkg/kustomize/kustomize_varsub.go: identifiers
// composed of an alpha/underscore lead and alnum/underscore tail. A
// ConfigMap key like `my-var` (dash) is invalid; upstream rejects the
// whole postBuild rather than silently substituting nothing.
var varsubRegex = regexp.MustCompile(`^[_a-zA-Z][_a-zA-Z0-9]*$`)

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
// values from base — matching Helm's merge semantics. Both inputs
// are read-only.
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

// DeepMergeInto merges override's keys into dst in place. Same merge
// semantics as DeepMerge but mutates dst instead of allocating a new
// map. Callers MUST own dst and any sub-maps reachable from it —
// passing a map that shares sub-trees with another reachable
// reference will corrupt that reference. Designed for hot paths
// (ExpandValueReferences's per-ref loop) where the caller is
// building up a fresh map across N refs and the N-1 intermediate
// allocations DeepMerge would do are wasted.
//
// Sub-maps coming from override are inserted by reference (not
// cloned) when no existing key collides — same as DeepMerge.
// Returns dst for fluent-style use.
func DeepMergeInto(dst, override map[string]any) map[string]any {
	if dst == nil {
		dst = map[string]any{}
	}
	for k, v := range override {
		if existing, ok := dst[k]; ok {
			ebm, eok := existing.(map[string]any)
			vbm, vok := v.(map[string]any)
			if eok && vok {
				dst[k] = DeepMergeInto(ebm, vbm)
				continue
			}
		}
		dst[k] = v
	}
	return dst
}

// ExpandValueReferences resolves all spec.valuesFrom references on hr,
// merges them with hr.Values (inline values take precedence per Helm
// semantics), and writes the result back to hr.Values.
//
// Honors ValuesReference.Optional: missing references on Optional=true
// refs are skipped silently; missing references on Optional=false (the
// default) return ErrObjectNotFound so the orchestrator can surface a
// real failure or honor --allow-missing-secrets. Matches Flux's helm-
// controller, which fails the reconcile on a missing non-optional
// valuesFrom but tolerates a missing optional one.
//
// Hard errors from the lookup itself — unsupported kind, missing key
// in a present resource, malformed binaryData — always bubble up; they
// are unrelated to whether the ref is optional.
func ExpandValueReferences(hr *manifest.HelmRelease, provider Provider) error {
	if hr == nil || len(hr.ValuesFrom) == 0 {
		return nil
	}
	values := map[string]any{}
	for _, ref := range hr.ValuesFrom {
		found, err := lookupValueRef(ref, hr.Namespace, provider)
		if err != nil {
			return fmt.Errorf("building HelmRelease %s: %w", hr.Named().NamespacedName(), err)
		}
		if found == "" {
			// Resource missing. lookupValueRef only returns "" when
			// the underlying CM/Secret was absent AND no TargetPath
			// short-circuit produced a placeholder; that's the only
			// case where Optional applies.
			if !ref.Optional {
				return fmt.Errorf("%w: HelmRelease %s: valuesFrom %s %s/%s not found",
					manifest.ErrObjectNotFound, hr.Named().NamespacedName(), ref.Kind, hr.Namespace, ref.Name)
			}
			continue
		}
		merged, err := updateHelmReleaseValues(ref, found, values)
		if err != nil {
			return fmt.Errorf("building HelmRelease %s: %w", hr.Named().NamespacedName(), err)
		}
		values = merged
	}
	if len(hr.Values) > 0 {
		// hr.Values is the inline-values map decoded from the HR
		// manifest. The Prepare path clones hr before calling here
		// (helm.Prepare in pkg/helm), so mutating hr.Values's
		// sub-tree is safe; reaching values as the dst would
		// however share sub-references back into hr.Values, so
		// build the inline layer ON TOP of our owned values map.
		values = DeepMergeInto(values, hr.Values)
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
			// valuesFrom reads only ConfigMap.data; upstream
			// fluxcd/pkg/chartutil/values.go ChartValuesFromReferences
			// pulls from typedRes.Data[ref.GetValuesKey()] and never
			// touches BinaryData. Pass nil so a ConfigMap carrying
			// binaryData doesn't quietly leak base64-decoded entries
			// into hr.Values — flate would render with keys real Flux
			// never sees.
			data, err = decodeBag(c.Data, nil)
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

	key := ref.GetValuesKey()
	val, ok := data[key]
	if !ok {
		return "", fmt.Errorf("%w: key %q not found in %s/%s",
			manifest.ErrInvalidValuesReference, key, namespace, ref.Name)
	}
	return val, nil
}

// decodeBag normalizes ConfigMap/Secret data so callers see a single
// map[string]string. binaryData values are base64-decoded. Non-string
// shapes (a Secret.Data field decoded as []byte, a number leaf the
// parser produced from an unquoted YAML scalar) get explicit handling
// — the previous fmt.Sprint fallback silently rendered Go's Stringer
// output, which for []byte is "[107 58]" rather than the intended
// "k:" — silently breaking values-reference resolution.
func decodeBag(stringData, binaryData map[string]any) (map[string]string, error) {
	if len(stringData) == 0 && len(binaryData) == 0 {
		return nil, nil
	}
	out := make(map[string]string, len(stringData)+len(binaryData))
	for k, v := range stringData {
		s, err := bagValueAsString(v)
		if err != nil {
			return nil, fmt.Errorf("stringData[%s]: %w", k, err)
		}
		out[k] = s
	}
	for k, v := range binaryData {
		s, err := bagValueAsString(v)
		if err != nil {
			return nil, fmt.Errorf("binaryData[%s]: %w", k, err)
		}
		decoded, err := base64.StdEncoding.DecodeString(s)
		if err != nil {
			return nil, fmt.Errorf("decode binaryData[%s]: %w", k, err)
		}
		out[k] = string(decoded)
	}
	return out, nil
}

// bagValueAsString coerces a single ConfigMap.Data / Secret.Data value
// into the string form helm values / postBuild substitution consume.
// Distinguishes the JSON/YAML shapes the decoder actually produces
// (string, []byte after future schema corrections, JSON-numeric
// scalars) from the "garbage value" case which now returns an error
// instead of silently rendering "[107 58]"-style Stringer output.
func bagValueAsString(v any) (string, error) {
	switch t := v.(type) {
	case string:
		return t, nil
	case []byte:
		// Hypothetical today (the YAML decoder lands strings), but
		// the spec-correct shape for Secret.Data is []byte and a
		// future schema fix could land us here.
		return string(t), nil
	case nil:
		return "", nil
	case bool, int, int32, int64, uint, uint32, uint64, float32, float64:
		// JSON scalar shapes — render via fmt.Sprint, deterministic
		// for these types.
		return fmt.Sprint(t), nil
	default:
		return "", fmt.Errorf("unsupported value type %T", v)
	}
}

// updateHelmReleaseValues writes found into values using ref.TargetPath
// when set; otherwise the found YAML is parsed and deep-merged.
//
// When targetPath is set, write through Helm's strvals parser
// (path=value form). This matches upstream Flux's
// chartutil.ChartValuesFromReferences (which calls strvals.ParseInto)
// and gives the correct type coercion: "3" → int 3, "true" → bool,
// "null" → nil. Single/double-quoted values force string-coercion
// (strvals.ParseIntoString). A naive `inner[k] = found` left every
// targetPath value as a literal string, which broke chart-schema
// validation against `replicaCount: integer` and similar.
//
// Mutates values in place — ExpandValueReferences owns the map and
// chaining N refs through DeepMerge previously paid for N-1 wasted
// full-tree clones (the O(N²) shape the audit flagged).
func updateHelmReleaseValues(ref manifest.ValuesReference, found string, values map[string]any) (map[string]any, error) {
	if ref.TargetPath != "" {
		return replaceValueAtPath(values, ref.TargetPath, found)
	}

	// Wiped Secret values surface here as the literal placeholder
	// string. yaml.Unmarshal of a scalar string into a map errors out
	// — treat as empty so a wiped values-file (common pattern: kustomize
	// `secretGenerator` wrapping a SOPS-encrypted values.yaml) doesn't
	// block the whole HR render.
	if manifest.IsValuePlaceholder(found) {
		return values, nil
	}
	var parsed map[string]any
	if err := yaml.Unmarshal([]byte(found), &parsed); err != nil {
		return nil, fmt.Errorf("expected '%s' values to be valid YAML: %w", ref.Name, err)
	}
	if parsed == nil {
		return values, nil
	}
	return DeepMergeInto(values, parsed), nil
}

// replaceValueAtPath writes value into values at path using Helm's
// strvals parser. Matches upstream Flux's chartutil.ReplacePathValue:
// single/double-quoted values use ParseIntoString (forced string);
// bare values use ParseInto (type-coerced: "3" → int, "true" → bool,
// "null" → nil). Strvals also handles list indices (foo.bar[0]) and
// escaped dot keys (foo\\.bar).
func replaceValueAtPath(values map[string]any, path, value string) (map[string]any, error) {
	const (
		singleQuote = "'"
		doubleQuote = `"`
	)
	var err error
	// The len >= 2 guard is load-bearing: a single-char "'" or `"`
	// has prefix == suffix == the same byte, which would pass the
	// boolean test below and then trip a `value[1:0]` slice — runtime
	// panic. Untrusted Secret data shouldn't be able to crash the
	// renderer, so reject the degenerate case explicitly.
	if len(value) >= 2 &&
		((strings.HasPrefix(value, singleQuote) && strings.HasSuffix(value, singleQuote)) ||
			(strings.HasPrefix(value, doubleQuote) && strings.HasSuffix(value, doubleQuote))) {
		stripped := value[1 : len(value)-1]
		err = strvals.ParseIntoString(path+"="+stripped, values)
	} else {
		err = strvals.ParseInto(path+"="+value, values)
	}
	if err != nil {
		return nil, fmt.Errorf("targetPath %q: %w", path, err)
	}
	return values, nil
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

	// Upstream kustomize-controller's LoadVariables merges substituteFrom
	// refs first, then OVERWRITES with inline spec.postBuild.substitute —
	// inline values win on key collision. flate previously inverted this
	// (seeded from inline, then substituteFrom overwrote). Match upstream:
	// substituteFrom first into a fresh map, then layer inline on top.
	values := map[string]any{}
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
				// substituteFrom only reads cm.Data — upstream
				// kustomize-controller does NOT expose binaryData as
				// substitution vars. Pass nil to skip that branch.
				data, err = decodeBag(c.Data, nil)
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
			// Match upstream kustomize-controller: every substituteFrom
			// var value has \n stripped (multi-line ConfigMap entries
			// would otherwise break inline substitution into single-line
			// YAML fields like `image:`). See
			// fluxcd/pkg/kustomize/kustomize_varsub.go.
			values[k] = strings.ReplaceAll(v, "\n", "")
		}
	}
	// Layer inline spec.postBuild.substitute on top — inline wins on
	// key collision per upstream LoadVariables order.
	maps.Copy(values, ks.PostBuildSubstitute)

	// Reject invalid var names — matches upstream fluxcd/pkg/kustomize
	// varSubstitution which fails the whole postBuild on any name that
	// doesn't match `^[_[:alpha:]][_[:alpha:][:digit:]]*$`. Without this
	// check flate would render with the invalid keys silently dropped
	// while real Flux would fail the Kustomization — divergent output.
	// Collect every invalid name and report them sorted so the error
	// message is deterministic across runs. Map iteration is
	// randomized in Go; the previous "return first invalid name"
	// surfaced a different bad name on each run when multiple were
	// present, making bisection and CI noise unnecessarily painful.
	var invalid []string
	for name := range values {
		if !varsubRegex.MatchString(name) {
			invalid = append(invalid, name)
		}
	}
	if len(invalid) > 0 {
		slices.Sort(invalid)
		return fmt.Errorf("%w: substituteFrom var name(s) %v invalid (must match %s)",
			manifest.ErrInvalidSubstituteReference, invalid, varsubRegex.String())
	}
	ks.UpdatePostBuildSubstitutions(values)
	return nil
}

// VarsMap returns the substitution variables for use with
// kustomize.Substitute. Non-string values are stringified. Newline
// characters are stripped from every value — upstream kustomize-
// controller does the same so multi-line entries can't break inline
// substitution into single-line YAML fields.
func VarsMap(in map[string]any) map[string]string {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]string, len(in))
	for k, v := range in {
		var s string
		switch tv := v.(type) {
		case string:
			s = tv
		default:
			s = fmt.Sprint(v)
		}
		out[k] = strings.ReplaceAll(s, "\n", "")
	}
	return out
}
