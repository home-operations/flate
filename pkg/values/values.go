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

// Provider exposes the ConfigMap/Secret lookups needed for value
// reference expansion. The controllers implement it against the central
// store (see NewStoreProvider); tests use SliceProvider.
type Provider interface {
	ConfigMap(namespace, name string) *manifest.ConfigMap
	Secret(namespace, name string) *manifest.Secret
}

// NewStoreProvider returns a Provider backed by the central Store.
func NewStoreProvider(s *store.Store) Provider { return &storeProvider{s: s} }

type storeProvider struct{ s *store.Store }

func (p *storeProvider) ConfigMap(namespace, name string) *manifest.ConfigMap {
	c, _ := p.s.GetByName[*manifest.ConfigMap](manifest.KindConfigMap, namespace, name)
	return c
}

func (p *storeProvider) Secret(namespace, name string) *manifest.Secret {
	s, _ := p.s.GetByName[*manifest.Secret](manifest.KindSecret, namespace, name)
	return s
}

// DeepMergeInto merges override into dst using Helm's merge semantics.
// Callers MUST own dst and its sub-maps. Non-colliding override sub-maps
// are inserted by reference; override remains read-only.
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

// deepMergeShared merges override's keys into dst IN PLACE, treating
// override (and every map reachable from it) as read-only. dst's
// top-level map is owned by the caller and mutated directly; its
// sub-maps may be BORROWED by reference from a previous override (e.g.
// a shared cache canonical). To preserve that read-only borrow, a
// map/map key collision shallow-clones the existing dst node into a
// fresh owned map BEFORE recursing — copy-on-write — so the recursion
// never writes through a borrowed (shared) sub-tree. Non-colliding
// override sub-maps are inserted by reference.
//
// The accumulator is owned, but its sub-maps may alias immutable cache
// entries. Clone colliding maps before writing through a borrowed node.
func deepMergeShared(dst, override map[string]any) map[string]any {
	for k, v := range override {
		existing, ok := dst[k]
		if !ok {
			dst[k] = v
			continue
		}
		ebm, eok := existing.(map[string]any)
		vbm, vok := v.(map[string]any)
		if eok && vok {
			// existing may be borrowed from a shared canonical: shallow-
			// clone it into an owned map before merging override's sub-map
			// in, so we never mutate the borrowed (read-only) node.
			owned := make(map[string]any, len(ebm)+len(vbm))
			maps.Copy(owned, ebm)
			dst[k] = deepMergeShared(owned, vbm)
			continue
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

// lookup returns the cached parsed-values map for key. Callers MUST
// NOT mutate the returned map; the cache's defensive-clone happens at
// the call site (ExpandValueReferences) which knows whether the value
// will be merged through DeepMergeInto (mutating) or read-only.
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
//
// cache may be nil — tests and embedders without an orchestrator pass
// nil and pay the (small) per-ref yaml.Unmarshal cost. Orchestrators
// supply a Cache constructed at startup so refs shared across HRs
// (a common pattern for platform-wide values CMs) parse exactly once.
func ExpandValueReferences(hr *manifest.HelmRelease, provider Provider, cache *Cache) error {
	if hr == nil || len(hr.ValuesFrom) == 0 {
		return nil
	}
	// A TargetPath ref writes through the accumulator in place (strvals
	// navigates and mutates existing nodes), which is unsafe once the
	// accumulator aliases shared cache sub-trees. Decide the merge
	// strategy once for the whole HR: if NO ref has a TargetPath, share
	// the cached canonicals by reference (cheap, the common platform-CM
	// pattern); if ANY ref has one, keep the eager fully-owned copy so the
	// in-place write can't reach the cache. See updateHelmReleaseValues.
	share := !slices.ContainsFunc(hr.ValuesFrom, func(ref manifest.ValuesReference) bool {
		return ref.TargetPath != ""
	})
	values := map[string]any{}
	for _, ref := range hr.ValuesFrom {
		found, err := lookupValueRef(ref, hr.Namespace, provider)
		if err != nil {
			if _, ok := errors.AsType[*missingValueRefError](err); ok && ref.Optional {
				continue
			}
			return fmt.Errorf("building HelmRelease %s: %w", hr.Named().NamespacedName(), err)
		}
		if values, err = updateHelmReleaseValues(ref, found, values, cache, share); err != nil {
			return fmt.Errorf("building HelmRelease %s: %w", hr.Named().NamespacedName(), err)
		}
	}
	if len(hr.Values) > 0 {
		// hr.Values is the inline-values map decoded from the HR manifest.
		// The Prepare path clones hr before calling here, so its sub-trees
		// are owned by this reconcile. Build the inline layer ON TOP of
		// values (inline wins on collision). In the share path values may
		// alias cache sub-trees, so use deepMergeShared (copy-on-write, so a
		// shared node is cloned before it is written through); in the owned
		// path DeepMergeInto is fine and avoids that clone.
		if share {
			// Owned accumulator, read-only hr.Values: merge in place with
			// copy-on-write so sub-maps still aliasing the cache canonical
			// are cloned before the inline layer writes into them.
			deepMergeShared(values, hr.Values)
		} else {
			DeepMergeInto(values, hr.Values)
		}
	}
	hr.Values = values
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
	value, ok, err := data.Value(key)
	if err != nil {
		return "", err
	}
	if !ok {
		return "", &missingValueRefError{kind: ref.Kind, namespace: namespace, name: ref.Name, key: key}
	}
	return value, nil
}

// Empty bags retain the same missing-resource disposition as absent objects.
func resourceData(kind, namespace, name string, p Provider) (manifest.Data, error) {
	switch kind {
	case manifest.KindSecret:
		if s := p.Secret(namespace, name); s != nil && len(s.Data)+len(s.StringData) > 0 {
			return s, nil
		}
	case manifest.KindConfigMap:
		if c := p.ConfigMap(namespace, name); c != nil && len(c.Data) > 0 {
			return c, nil
		}
	default:
		return nil, fmt.Errorf("%w: unsupported kind %s", manifest.ErrInvalidValuesReference, kind)
	}
	return nil, nil
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

// updateHelmReleaseValues merges one valuesFrom ref into the values
// accumulator and returns the (possibly new) accumulator.
//
// When ref.TargetPath is set, write through Helm's strvals parser
// (path=value form). This matches upstream Flux's
// chartutil.ChartValuesFromReferences (which calls strvals.ParseInto)
// and gives the correct type coercion: "3" → int 3, "true" → bool,
// "null" → nil. Single/double-quoted values force string-coercion
// (strvals.ParseIntoString). A naive `inner[k] = found` left every
// targetPath value as a literal string, which broke chart-schema
// validation against `replicaCount: integer` and similar. The targetPath
// path bypasses the cache — strvals parsing already mutates values in
// place per-call and is comparatively cheap (no map allocation for the
// parsed tree). A ref with `literal: true` skips all of that syntax and
// lands the value verbatim as a string (`helm --set-literal` semantics,
// via replaceLiteralValueAtPath).
//
// Otherwise the found YAML is parsed (memoized in cache when non-nil, keyed
// by content) and deep-merged.
//
// share selects the merge strategy, decided once per HR by the caller:
//   - share=true (the HR has NO TargetPath ref): deepMergeShared, which
//     folds the canonical into the owned accumulator in place and SHARES
//     its non-colliding sub-trees by reference instead of deep-copying
//     them (map collisions copy-on-write so the canonical is never
//     mutated). Safe because nothing in this HR mutates a shared node and
//     the cache canonical is read-only downstream (helm copies the input
//     on entry). M HRs referencing the same ConfigMap then share one
//     parsed tree instead of each deep-copying it.
//   - share=false (the HR has a TargetPath ref somewhere): eager
//     DeepMergeInto over a DeepCopyMap of the canonical, so the
//     accumulator is FULLY OWNED and the in-place strvals write that a
//     TargetPath ref performs can't reach (and corrupt) the shared cache.
func updateHelmReleaseValues(ref manifest.ValuesReference, found string, values map[string]any, cache *Cache, share bool) (map[string]any, error) {
	if ref.TargetPath != "" {
		// Only reached when share==false (the caller sets share false the
		// moment any ref carries a TargetPath), so `values` is owned and
		// the in-place strvals write below is safe.
		if ref.Literal {
			_, err := replaceLiteralValueAtPath(values, ref.TargetPath, found)
			return values, err
		}
		_, err := replaceValueAtPath(values, ref.TargetPath, found)
		return values, err
	}

	parsed, ok := cache.lookup(found)
	if !ok {
		if err := yaml.Unmarshal([]byte(found), &parsed); err != nil {
			// Wiped Secret values surface here as the literal placeholder
			// string. yaml.Unmarshal of a scalar string into a map errors out
			// — treat as empty so a wiped values-file (common pattern: kustomize
			// `secretGenerator` wrapping a SOPS-encrypted values.yaml) doesn't
			// block the whole HR render.
			if manifest.HasValuePlaceholder(found) {
				return values, nil
			}
			return values, fmt.Errorf("expected '%s' values to be valid YAML: %w", ref.Name, err)
		}
		if parsed == nil {
			return values, nil
		}
		// Cache the parsed tree (canonical, never mutated) before merging.
		cache.put(found, parsed)
	}
	if share {
		// Folds parsed into the owned accumulator in place: non-colliding
		// sub-trees are shared by reference; map collisions copy-on-write
		// so the cache canonical (parsed) is never mutated. Avoids the
		// per-ref full clone of the growing top-level map across N valuesFrom refs.
		return deepMergeShared(values, parsed), nil
	}
	// Owned-accumulator path: clone the canonical so the later in-place
	// TargetPath write can't corrupt the shared cache entry.
	DeepMergeInto(values, manifest.DeepCopyMap(parsed))
	return values, nil
}

// replaceValueAtPath writes value into values at path using Helm's
// strvals parser. Matches upstream Flux's chartutil.ReplacePathValue:
// single/double-quoted values use ParseIntoString (forced string);
// bare values use ParseInto (type-coerced: "3" → int, "true" → bool,
// "null" → nil). Strvals also handles list indices (foo.bar[0]) and
// escaped dot keys (foo\\.bar).
func replaceValueAtPath(values map[string]any, path, value string) (map[string]any, error) {
	var err error
	if unquoted, ok := stripMatchingQuotes(value); ok {
		err = strvals.ParseIntoString(path+"="+unquoted, values)
	} else {
		err = strvals.ParseInto(path+"="+value, values)
	}
	if err != nil {
		return nil, fmt.Errorf("targetPath %q: %w", path, err)
	}
	return values, nil
}

// replaceLiteralValueAtPath writes value verbatim at the dot-notation path,
// mirroring upstream Flux's chartutil.ReplacePathLiteralValue. Flux does not
// use helm's strvals.ParseLiteralInto here: that parser has no backslash
// escape support in the path, which would break `\.` targetPath segments
// (e.g. `annotations.prometheus\.io/scrape`). Instead the value has its
// strvals metacharacters pre-escaped and goes through ParseIntoString, which
// keeps escape-aware path parsing while the value round-trips verbatim.
func replaceLiteralValueAtPath(values map[string]any, path, value string) (map[string]any, error) {
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
		return nil, fmt.Errorf("targetPath %q: %w", path, err)
	}
	return values, nil
}

// stripMatchingQuotes reports whether value is wrapped in a matching pair
// of single or double quotes and, if so, returns the inner contents. The
// len >= 2 guard is load-bearing: a single-char "'" or `"` has its first
// byte equal to its last, which would otherwise pass the match and then
// trip a value[1:0] slice — a runtime panic on untrusted Secret data.
func stripMatchingQuotes(value string) (string, bool) {
	if len(value) >= 2 {
		if q := value[0]; (q == '\'' || q == '"') && value[len(value)-1] == q {
			return value[1 : len(value)-1], true
		}
	}
	return value, false
}

// ExpandPostBuildSubstituteReference resolves substituteFrom references
// against the provider and updates ks.PostBuildSubstitute (and its raw
// Contents). Missing references are logged (Secrets silently) and the
// substitution proceeds with what's available.
func ExpandPostBuildSubstituteReference(ks *manifest.Kustomization, p Provider) error {
	return ExpandPostBuildSubstituteReferenceWithSubstitutions(ks, p, nil)
}

// ExpandPostBuildSubstituteReferenceWithSubstitutions applies overlay after
// referenced and inline values. An overlay never enables absent postBuild.
func ExpandPostBuildSubstituteReferenceWithSubstitutions(ks *manifest.Kustomization, p Provider, overlay map[string]string) error {
	if ks == nil || (len(ks.PostBuildSubstitute) == 0 && len(ks.PostBuildSubstituteFrom) == 0 &&
		(ks.PostBuild == nil || len(overlay) == 0)) {
		return nil
	}
	if len(ks.PostBuildSubstituteFrom) > 0 && ks.Namespace == "" {
		return fmt.Errorf("%w: Kustomization with substituteFrom has no namespace", manifest.ErrInvalidSubstituteReference)
	}

	// Flux gives inline substitutions precedence over references.
	values := map[string]any{}
	for _, ref := range ks.PostBuildSubstituteFrom {
		data, err := resourceData(ref.Kind, ks.Namespace, ref.Name, p)
		if err != nil {
			if errors.Is(err, manifest.ErrInvalidValuesReference) {
				// Unsupported kind — log and skip rather than failing the
				// whole KS, matching upstream's lenient substituteFrom handling.
				slog.Debug("values: unsupported SubstituteReference kind",
					"id", ks.Named().String(), "kind", ref.Kind)
				continue
			}
			return err
		}
		if data == nil {
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
	// Layer inline spec.postBuild.substitute on top — inline wins on
	// key collision per upstream LoadVariables order.
	maps.Copy(values, ks.PostBuildSubstitute)
	if ks.PostBuild != nil {
		for name, value := range overlay {
			values[name] = value
		}
	}

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
	ks.UpdatePostBuildSubstitutions(values)
	return nil
}

// UnreadableSubstituteSecrets returns the names of ks.PostBuildSubstituteFrom
// Secret references whose data flate can't read offline — an absent Secret or
// an ExternalSecret-synthesized empty target, both of which yield no data so
// the ${VAR}s they would supply expand to the empty string. SOPS-encrypted
// Secrets are NOT reported: their data is wiped to placeholders, so their vars
// still substitute. Optional refs are skipped (the author marked them
// best-effort). The result is sorted and de-duplicated; nil when none.
//
// This is a pure advisory query — it does not affect substitution. The
// Kustomization controller surfaces the result as a WarnUnresolvedSubstitution
// Warning so an unreadable secret is explained rather than silently leaving
// empty values behind.
func UnreadableSubstituteSecrets(ks *manifest.Kustomization, p Provider) []string {
	if ks == nil || ks.Namespace == "" {
		return nil
	}
	var unreadable []string
	for _, ref := range ks.PostBuildSubstituteFrom {
		if ref.Kind != manifest.KindSecret || ref.Optional {
			continue
		}
		if data, err := resourceData(ref.Kind, ks.Namespace, ref.Name, p); err == nil && data == nil {
			unreadable = append(unreadable, ref.Name)
		}
	}
	if len(unreadable) == 0 {
		return nil
	}
	slices.Sort(unreadable)
	return slices.Compact(unreadable)
}
