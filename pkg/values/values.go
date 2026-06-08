package values

import (
	"encoding/base64"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/fnv"
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

// NewStoreProvider returns a Provider backed by the central Store.
func NewStoreProvider(s *store.Store) Provider { return &storeProvider{s: s} }

type storeProvider struct{ s *store.Store }

func (p *storeProvider) ConfigMap(namespace, name string) *manifest.ConfigMap {
	c, _ := store.GetByName[*manifest.ConfigMap](p.s, manifest.KindConfigMap, namespace, name)
	return c
}

func (p *storeProvider) Secret(namespace, name string) *manifest.Secret {
	s, _ := store.GetByName[*manifest.Secret](p.s, manifest.KindSecret, namespace, name)
	return s
}

// DeepMerge returns a new map with override's keys merged into base.
// Nested maps recurse; lists and scalars from override fully replace
// values from base — matching Helm's merge semantics. Both inputs
// are read-only.
//
// Implemented as deepMergeShared over a fresh shallow copy of base:
// the copy makes the top-level map owned, and deepMergeShared's
// copy-on-collision recursion keeps every borrowed (base) sub-tree
// read-only, so neither input is mutated.
func DeepMerge(base, override map[string]any) map[string]any {
	out := make(map[string]any, len(base)+len(override))
	maps.Copy(out, base)
	return deepMergeShared(out, override)
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

// deepMergeShared merges override's keys into dst IN PLACE, treating
// override (and every map reachable from it) as read-only. dst's
// top-level map is owned by the caller and mutated directly; its
// sub-maps may be BORROWED by reference from a previous override (e.g.
// a shared cache canonical). To preserve that read-only borrow, a
// map/map key collision shallow-clones the existing dst node into a
// fresh owned map BEFORE recursing — copy-on-write — so the recursion
// never writes through a borrowed (shared) sub-tree. Non-colliding
// override sub-maps are inserted by reference, exactly like DeepMerge.
//
// The resulting value graph is identical to a DeepMerge(dst, override)
// chain, but without re-copying the whole top-level accumulator on each
// call: ExpandValueReferences's share path folds N valuesFrom refs into
// one accumulator with N in-place merges instead of N functional
// DeepMerges that each clone the growing top level. Returns dst.
//
// Safety invariant: override is never mutated (only read), and any dst
// sub-map that aliases a shared canonical is cloned before it is
// written to. The cache canonical therefore stays pristine, matching
// the contract DeepMerge upheld in the old share path.
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

// Cache memoizes parsed-YAML output of valuesFrom refs across HRs.
// One HR with N valuesFrom refs hits each entry once; M HRs sharing
// the same ConfigMap/Secret/key tuple (a common pattern when a
// platform values CM is referenced by every app HR) re-yaml.Unmarshal'd
// the same bytes M times. Cache key folds the content hash so a
// mutation to the underlying object (re-AddObject) invalidates
// naturally without an explicit listener.
//
// Stored values are TREATED AS IMMUTABLE — callers receive a deep
// clone before mutation so concurrent ExpandValueReferences calls
// can't observe a partially-modified sub-tree. The cache itself is
// safe for concurrent use.
//
// Zero value is a no-op cache: NewCache or a non-nil *Cache must
// be supplied to opt into memoization. nil is the legacy fast path
// for tests / one-shot embedders.
type Cache struct {
	m sync.Map // map[uint64]map[string]any
}

// NewCache returns an empty *Cache ready for use. Construct one per
// orchestrator run and pass to ExpandValueReferences.
func NewCache() *Cache { return &Cache{} }

// lookup returns the cached parsed-values map for key. Callers MUST
// NOT mutate the returned map; the cache's defensive-clone happens at
// the call site (ExpandValueReferences) which knows whether the value
// will be merged through DeepMergeInto (mutating) or read-only.
func (c *Cache) lookup(key uint64) (map[string]any, bool) {
	if c == nil {
		return nil, false
	}
	v, ok := c.m.Load(key)
	if !ok {
		return nil, false
	}
	return v.(map[string]any), true
}

// store memoizes parsed under key. No-op when c is nil so the
// non-cached path stays branchless on the caller.
func (c *Cache) store(key uint64, parsed map[string]any) {
	if c == nil {
		return
	}
	c.m.Store(key, parsed)
}

// valuesRefCacheKey folds (ref.Kind, namespace, ref.Name, valuesKey,
// content-hash) into a FNV64 key. Including the content hash means
// any mutation to the underlying ConfigMap/Secret data shifts the
// key — re-AddObject changes the bytes, the new bytes hash differently,
// and the next lookup misses naturally. No explicit invalidation needed.
//
// Why FNV64 (stdlib hash/fnv) over sha256: collision probability across
// the small key-space a single flate run produces (≤ thousands of
// entries) is effectively zero, and FNV is ~10× cheaper. The previous-
// cache convention elsewhere in flate (chartValuesCache, store hashing)
// uses sha256 for stable cross-run keys; this cache is single-run only,
// so the faster hash is fine.
func valuesRefCacheKey(kind, namespace, name, valuesKey, content string) uint64 {
	// hash.Hash.Write never returns an error per its contract; the
	// _, _ = drains the (int, error) tuple so gosec G104 doesn't flag
	// every byte fed into the digest.
	h := fnv.New64a()
	// Each field is followed by a NUL so adjacent fields can't run
	// together (e.g. name="ab"+key="c" vs name="a"+key="bc").
	writeField := func(s string) {
		_, _ = h.Write([]byte(s))
		_, _ = h.Write([]byte{0})
	}
	writeField(kind)
	writeField(namespace)
	writeField(name)
	writeField(valuesKey)
	// Mix the content length into the hash explicitly so two refs that
	// happen to FNV-collide on the same prefix don't end up sharing
	// a cache entry; binary.LittleEndian.PutUint64 produces 8 stable
	// bytes regardless of the host architecture.
	var b [8]byte
	binary.LittleEndian.PutUint64(b[:], uint64(len(content)))
	_, _ = h.Write(b[:])
	_, _ = h.Write([]byte(content))
	return h.Sum64()
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
		if values, err = updateHelmReleaseValues(ref, found, values, hr.Namespace, cache, share); err != nil {
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

// lookupValueRef returns the raw string value referenced by ref.
func lookupValueRef(ref manifest.ValuesReference, namespace string, p Provider) (string, error) {
	data, err := lookupResourceData(ref.Kind, namespace, ref.Name, p)
	if err != nil {
		return "", err
	}
	if data == nil {
		return "", &missingValueRefError{kind: ref.Kind, namespace: namespace, name: ref.Name}
	}
	key := ref.GetValuesKey()
	val, ok := data[key]
	if !ok {
		return "", &missingValueRefError{kind: ref.Kind, namespace: namespace, name: ref.Name, key: key}
	}
	return val, nil
}

// lookupResourceData fetches and decodes the data bag for a ConfigMap or
// Secret. Returns nil when the resource is not found (no error). Hard errors
// (unsupported kind, malformed binaryData) always surface regardless of
// Optional.
func lookupResourceData(kind, namespace, name string, p Provider) (map[string]string, error) {
	switch kind {
	case manifest.KindSecret:
		if s := p.Secret(namespace, name); s != nil {
			return decodeBag(s.StringData, s.Data)
		}
		return nil, nil
	case manifest.KindConfigMap:
		if c := p.ConfigMap(namespace, name); c != nil {
			// valuesFrom/substituteFrom read only ConfigMap.data; upstream
			// fluxcd/pkg/chartutil/values.go ChartValuesFromReferences
			// pulls from typedRes.Data exclusively and never touches
			// BinaryData. Pass nil so a ConfigMap carrying binaryData
			// doesn't quietly leak base64-decoded entries — flate would
			// render with keys real Flux never sees.
			return decodeBag(c.Data, nil)
		}
		return nil, nil
	default:
		return nil, fmt.Errorf("%w: unsupported kind %s", manifest.ErrInvalidValuesReference, kind)
	}
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
// Also used by VarsMap for scalar postBuild substitution values.
func bagValueAsString(v any) (string, error) {
	switch t := v.(type) {
	case string:
		return t, nil
	case []byte:
		// Secret.Data is spec-correct as []byte; a future schema fix or a
		// non-standard decoder could land values here.
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
// parsed tree).
//
// Otherwise the found YAML is parsed (memoized in cache when non-nil, keyed
// by kind, namespace, name, valuesKey, content-hash) and deep-merged.
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
func updateHelmReleaseValues(ref manifest.ValuesReference, found string, values map[string]any, namespace string, cache *Cache, share bool) (map[string]any, error) {
	if ref.TargetPath != "" {
		// Only reached when share==false (the caller sets share false the
		// moment any ref carries a TargetPath), so `values` is owned and
		// the in-place strvals write below is safe.
		_, err := replaceValueAtPath(values, ref.TargetPath, found)
		return values, err
	}

	// Wiped Secret values surface here as the literal placeholder
	// string. yaml.Unmarshal of a scalar string into a map errors out
	// — treat as empty so a wiped values-file (common pattern: kustomize
	// `secretGenerator` wrapping a SOPS-encrypted values.yaml) doesn't
	// block the whole HR render.
	if manifest.IsValuePlaceholder(found) {
		return values, nil
	}
	key := valuesRefCacheKey(ref.Kind, namespace, ref.Name, ref.GetValuesKey(), found)
	parsed, ok := cache.lookup(key)
	if !ok {
		if err := yaml.Unmarshal([]byte(found), &parsed); err != nil {
			return values, fmt.Errorf("expected '%s' values to be valid YAML: %w", ref.Name, err)
		}
		if parsed == nil {
			return values, nil
		}
		// Cache the parsed tree (canonical, never mutated) before merging.
		cache.store(key, parsed)
	}
	if share {
		// Folds parsed into the owned accumulator in place: non-colliding
		// sub-trees are shared by reference; map collisions copy-on-write
		// so the cache canonical (parsed) is never mutated. Avoids the
		// per-ref full clone of the growing top-level map that a functional
		// DeepMerge chain pays across N valuesFrom refs.
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
		data, err := lookupResourceData(ref.Kind, ks.Namespace, ref.Name, p)
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
// kustomize.Substitute. Non-string scalar values are stringified;
// nested maps/slices and other unsupported shapes are silently
// dropped with a Debug log rather than rendered as Go's default
// `map[k:v]` / `[1 2 3]` representation, which produced literal
// garbage substitutions diverging from upstream kustomize-controller
// (whose LoadVariables only accepts flat string→string). Newline
// characters are stripped from every value — upstream does the same
// so multi-line entries can't break inline substitution into
// single-line YAML fields.
func VarsMap(in map[string]any) map[string]string {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]string, len(in))
	for k, v := range in {
		s, err := bagValueAsString(v)
		if err != nil {
			slog.Debug("values: dropping non-scalar substitute var", "key", k, "err", err)
			continue
		}
		out[k] = strings.ReplaceAll(s, "\n", "")
	}
	return out
}
