package values

import (
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"testing"

	helmv2 "github.com/fluxcd/helm-controller/api/v2"

	"github.com/home-operations/flate/pkg/manifest"
)

// TestDeepMergeInto_MutatesDstPreservesSemantics pins the in-place
// variant's contract: dst is mutated, override is read-only, and
// the resulting tree matches DeepMerge's output for the same inputs.
// Sub-maps from override are inserted by reference when no key
// collides (same as DeepMerge); on a collision the recursive merge
// produces a single in-place result, not a fresh allocation per
// level. Used in ExpandValueReferences's hot path to avoid O(N²)
// clones across N valuesFrom refs.
func TestDeepMergeInto_MutatesDstPreservesSemantics(t *testing.T) {
	dst := map[string]any{
		"a": 1,
		"b": map[string]any{"x": 1, "y": 2},
	}
	override := map[string]any{
		"a": 2,
		"b": map[string]any{"y": 99, "z": 3},
		"c": []any{1, 2},
	}
	result := DeepMergeInto(dst, override)
	if result["a"] != 2 {
		t.Errorf("scalar override failed: %v", result["a"])
	}
	bb := result["b"].(map[string]any)
	if bb["x"] != 1 || bb["y"] != 99 || bb["z"] != 3 {
		t.Errorf("nested merge wrong: %v", bb)
	}
	// In-place semantics: dst is mutated AND returned. Maps don't
	// have native pointer identity, but the two share state — write
	// through dst, observe in result.
	dst["sentinel"] = "v"
	if result["sentinel"] != "v" {
		t.Error("DeepMergeInto must return dst itself, got distinct map")
	}
	if dst["a"] != 2 || dst["b"].(map[string]any)["y"] != 99 {
		t.Error("dst not mutated as expected")
	}
}

func TestDeepMerge(t *testing.T) {
	base := map[string]any{
		"a": 1,
		"b": map[string]any{"x": 1, "y": 2},
		"l": []any{1, 2, 3},
	}
	over := map[string]any{
		"a": 2,
		"b": map[string]any{"y": 99},
		"l": []any{9},
	}
	out := DeepMerge(base, over)
	if out["a"] != 2 {
		t.Errorf("scalar override failed: %v", out["a"])
	}
	bb := out["b"].(map[string]any)
	if bb["x"] != 1 || bb["y"] != 99 {
		t.Errorf("nested merge wrong: %v", bb)
	}
	ll := out["l"].([]any)
	if len(ll) != 1 || ll[0] != 9 {
		t.Errorf("list should be replaced, got %v", ll)
	}
}

func TestExpandValueReferences_ConfigMap(t *testing.T) {
	cm := &manifest.ConfigMap{
		Name: "extra", Namespace: "default",
		Data: map[string]any{"values.yaml": "replicaCount: 5\nimage:\n  tag: v2\n"},
	}
	provider := &SliceProvider{ConfigMaps: []*manifest.ConfigMap{cm}}
	hr := &manifest.HelmRelease{
		Name: "demo", Namespace: "default",
		HelmReleaseSpec: helmv2.HelmReleaseSpec{
			ValuesFrom: []manifest.ValuesReference{{Kind: "ConfigMap", Name: "extra"}},
		},
		Values: map[string]any{"image": map[string]any{"repository": "x"}},
	}
	if err := ExpandValueReferences(hr, provider, nil); err != nil {
		t.Fatalf("ExpandValueReferences: %v", err)
	}
	if hr.Values["replicaCount"] != float64(5) {
		t.Errorf("replicaCount: %v", hr.Values["replicaCount"])
	}
	img := hr.Values["image"].(map[string]any)
	if img["tag"] != "v2" || img["repository"] != "x" {
		t.Errorf("image merge wrong: %+v", img)
	}
}

// TestExpandValueReferences_IgnoresConfigMapBinaryData locks the
// upstream contract: valuesFrom on a ConfigMap reads only .data,
// never .binaryData. fluxcd/pkg/chartutil/values.go's
// ChartValuesFromReferences pulls from typedRes.Data exclusively.
// A ConfigMap carrying binaryData must not leak those entries into
// hr.Values — that would render keys real Flux never sees.
func TestExpandValueReferences_IgnoresConfigMapBinaryData(t *testing.T) {
	cm := &manifest.ConfigMap{
		Name: "mixed", Namespace: "default",
		Data:       map[string]any{"values.yaml": "fromData: \"yes\"\n"},
		BinaryData: map[string]any{"hidden.yaml": "fromBinary: \"leaked\"\n"},
	}
	provider := &SliceProvider{ConfigMaps: []*manifest.ConfigMap{cm}}
	hr := &manifest.HelmRelease{
		Name: "demo", Namespace: "default",
		HelmReleaseSpec: helmv2.HelmReleaseSpec{
			ValuesFrom: []manifest.ValuesReference{{Kind: "ConfigMap", Name: "mixed"}},
		},
	}
	if err := ExpandValueReferences(hr, provider, nil); err != nil {
		t.Fatalf("ExpandValueReferences: %v", err)
	}
	if hr.Values["fromData"] != "yes" {
		t.Errorf("data key should have merged; got %v", hr.Values["fromData"])
	}
	if _, leaked := hr.Values["fromBinary"]; leaked {
		t.Errorf("binaryData key leaked into hr.Values: %+v", hr.Values)
	}
}

// TestExpandValueReferences_MalformedSiblingTolerated documents the
// deliberate, Flux-aligning narrowing of the valuesFrom error surface in
// lookupValueRef: it decodes only the referenced key, so a malformed SIBLING
// key no longer fails the ref. Upstream Flux (chartutil.ChartValuesFromReferences)
// likewise indexes typedRes.Data[valuesKey] and never validates siblings. The
// old full-bag decode (still used by substituteFrom, which legitimately
// consumes every key) errored the whole ref on the broken sibling here.
func TestExpandValueReferences_MalformedSiblingTolerated(t *testing.T) {
	cm := &manifest.ConfigMap{
		Name: "extra", Namespace: "default",
		Data: map[string]any{
			"values.yaml": "replicaCount: 3\n",
			// A non-scalar value bagValueAsString cannot coerce. decodeBag
			// would error on this sibling regardless of map iteration order;
			// the single-key path never reads it.
			"broken": map[string]any{"nested": true},
		},
	}
	hr := &manifest.HelmRelease{
		Name: "demo", Namespace: "default",
		HelmReleaseSpec: helmv2.HelmReleaseSpec{
			ValuesFrom: []manifest.ValuesReference{{Kind: "ConfigMap", Name: "extra"}},
		},
	}
	if err := ExpandValueReferences(hr, &SliceProvider{ConfigMaps: []*manifest.ConfigMap{cm}}, nil); err != nil {
		t.Fatalf("malformed sibling must not fail the ref (Flux reads only the named key): %v", err)
	}
	if got := hr.Values["replicaCount"]; fmt.Sprint(got) != "3" {
		t.Errorf("replicaCount = %v, want 3 (values.yaml read despite broken sibling)", got)
	}
}

// TestExpandValueReferences_MalformedRequestedKeyStillFails pins that the
// error surface narrows ONLY for siblings: a malformed value at the REQUESTED
// key still fails the ref, with decodeBag's original "stringData[key]" wording.
func TestExpandValueReferences_MalformedRequestedKeyStillFails(t *testing.T) {
	cm := &manifest.ConfigMap{
		Name: "extra", Namespace: "default",
		Data: map[string]any{"values.yaml": []any{"not", "a", "scalar"}},
	}
	hr := &manifest.HelmRelease{
		Name: "demo", Namespace: "default",
		HelmReleaseSpec: helmv2.HelmReleaseSpec{
			ValuesFrom: []manifest.ValuesReference{{Kind: "ConfigMap", Name: "extra"}},
		},
	}
	err := ExpandValueReferences(hr, &SliceProvider{ConfigMaps: []*manifest.ConfigMap{cm}}, nil)
	if err == nil {
		t.Fatal("malformed requested key must still fail the ref")
	}
	if !strings.Contains(err.Error(), "stringData[values.yaml]") {
		t.Errorf("error should name the requested key as decodeBag did: %v", err)
	}
}

func TestExpandValueReferences_TargetPath(t *testing.T) {
	cm := &manifest.ConfigMap{
		Name: "k", Namespace: "default",
		Data: map[string]any{"v": "secret-value"},
	}
	provider := &SliceProvider{ConfigMaps: []*manifest.ConfigMap{cm}}
	hr := &manifest.HelmRelease{
		Name: "demo", Namespace: "default",
		HelmReleaseSpec: helmv2.HelmReleaseSpec{
			ValuesFrom: []manifest.ValuesReference{
				{Kind: "ConfigMap", Name: "k", ValuesKey: "v", TargetPath: "auth.password"},
			},
		},
	}
	if err := ExpandValueReferences(hr, provider, nil); err != nil {
		t.Fatalf("ExpandValueReferences: %v", err)
	}
	auth := hr.Values["auth"].(map[string]any)
	if auth["password"] != "secret-value" {
		t.Errorf("password: %v", auth["password"])
	}
}

func TestExpandValueReferences_MissingOptionalTargetPath(t *testing.T) {
	hr := &manifest.HelmRelease{
		Name: "demo", Namespace: "default",
		Values: map[string]any{"existing": "kept"},
		HelmReleaseSpec: helmv2.HelmReleaseSpec{
			ValuesFrom: []manifest.ValuesReference{
				{Kind: "ConfigMap", Name: "absent", ValuesKey: "v", TargetPath: "k", Optional: true},
			},
		},
	}
	provider := &SliceProvider{}
	if err := ExpandValueReferences(hr, provider, nil); err != nil {
		t.Fatalf("ExpandValueReferences: %v", err)
	}
	if _, ok := hr.Values["k"]; ok {
		t.Errorf("optional missing targetPath ref should be skipped, got %v", hr.Values["k"])
	}
	if hr.Values["existing"] != "kept" {
		t.Errorf("inline values should remain: %+v", hr.Values)
	}
}

func TestExpandValueReferences_MissingRequiredTargetPathFails(t *testing.T) {
	hr := &manifest.HelmRelease{
		Name: "demo", Namespace: "default",
		HelmReleaseSpec: helmv2.HelmReleaseSpec{
			ValuesFrom: []manifest.ValuesReference{
				{Kind: "ConfigMap", Name: "absent", ValuesKey: "v", TargetPath: "k"},
			},
		},
	}

	err := ExpandValueReferences(hr, &SliceProvider{}, nil)
	if !errors.Is(err, manifest.ErrObjectNotFound) {
		t.Fatalf("missing required targetPath ref = %v, want ErrObjectNotFound", err)
	}
}

func TestExpandValueReferences_MissingOptionalKeySkipped(t *testing.T) {
	cm := &manifest.ConfigMap{
		Name: "extra", Namespace: "default",
		Data: map[string]any{"other.yaml": "ignored: true\n"},
	}
	hr := &manifest.HelmRelease{
		Name: "demo", Namespace: "default",
		Values: map[string]any{"existing": "kept"},
		HelmReleaseSpec: helmv2.HelmReleaseSpec{
			ValuesFrom: []manifest.ValuesReference{
				{Kind: "ConfigMap", Name: "extra", ValuesKey: "missing.yaml", Optional: true},
			},
		},
	}

	if err := ExpandValueReferences(hr, &SliceProvider{ConfigMaps: []*manifest.ConfigMap{cm}}, nil); err != nil {
		t.Fatalf("ExpandValueReferences: %v", err)
	}
	if hr.Values["existing"] != "kept" || len(hr.Values) != 1 {
		t.Errorf("optional missing key should leave values unchanged: %+v", hr.Values)
	}
}

// TestExpandValueReferences_CacheHits pins Item 6: two lookups for
// the same (kind, ns, name, key, content) tuple return the same
// parsed value from one yaml.Unmarshal — and a mutation to the
// underlying ConfigMap.Data content causes the cache to miss because
// the content-hash component of the key shifts.
func TestExpandValueReferences_CacheHits(t *testing.T) {
	cm := &manifest.ConfigMap{
		Name: "platform", Namespace: "default",
		Data: map[string]any{"values.yaml": "replicaCount: 5\nimage:\n  tag: v2\n"},
	}
	provider := &SliceProvider{ConfigMaps: []*manifest.ConfigMap{cm}}
	cache := NewCache()
	hr := func() *manifest.HelmRelease {
		return &manifest.HelmRelease{
			Name: "demo", Namespace: "default",
			HelmReleaseSpec: helmv2.HelmReleaseSpec{
				ValuesFrom: []manifest.ValuesReference{{Kind: "ConfigMap", Name: "platform"}},
			},
		}
	}

	a := hr()
	if err := ExpandValueReferences(a, provider, cache); err != nil {
		t.Fatalf("first ExpandValueReferences: %v", err)
	}
	if a.Values["replicaCount"] != float64(5) {
		t.Fatalf("first: replicaCount=%v", a.Values["replicaCount"])
	}

	// Second HR sharing the same ref must hit the cache. Verify by
	// mutating the ConfigMap.Data IN A WAY that would change the
	// PARSE result but keep the SAME RAW BYTES — impossible by
	// construction. So instead we verify behaviorally: the cache
	// must serve identical output regardless of how many parsers
	// raced. The next test (CacheInvalidatesOnContentChange)
	// covers the inverse: mutating the bytes DOES invalidate.
	b := hr()
	if err := ExpandValueReferences(b, provider, cache); err != nil {
		t.Fatalf("second ExpandValueReferences: %v", err)
	}
	if b.Values["replicaCount"] != float64(5) {
		t.Errorf("second: replicaCount=%v", b.Values["replicaCount"])
	}

	// Overwrite a TOP-LEVEL key on a's result. Each HR gets its own
	// top-level values map (DeepMerge allocates a fresh one), so a
	// top-level write never reaches the shared cache canonical — a third
	// HR still sees the parsed value. (Nested sub-trees ARE shared with
	// the canonical and must not be mutated in place — that contract is
	// pinned by the TargetPath and concurrency tests below.)
	a.Values["replicaCount"] = "stomped"
	c := hr()
	if err := ExpandValueReferences(c, provider, cache); err != nil {
		t.Fatalf("third ExpandValueReferences: %v", err)
	}
	if c.Values["replicaCount"] != float64(5) {
		t.Errorf("cache aliased prior call: replicaCount=%v", c.Values["replicaCount"])
	}
}

// TestExpandValueReferences_CacheInvalidatesOnContentChange pins the
// natural-invalidation contract: when the underlying ConfigMap
// content changes (re-AddObject lands new bytes), the FNV key
// shifts and the cache misses — no explicit listener needed.
func TestExpandValueReferences_CacheInvalidatesOnContentChange(t *testing.T) {
	cm := &manifest.ConfigMap{
		Name: "platform", Namespace: "default",
		Data: map[string]any{"values.yaml": "replicaCount: 5\n"},
	}
	provider := &SliceProvider{ConfigMaps: []*manifest.ConfigMap{cm}}
	cache := NewCache()
	mk := func() *manifest.HelmRelease {
		return &manifest.HelmRelease{
			Name: "demo", Namespace: "default",
			HelmReleaseSpec: helmv2.HelmReleaseSpec{
				ValuesFrom: []manifest.ValuesReference{{Kind: "ConfigMap", Name: "platform"}},
			},
		}
	}

	a := mk()
	if err := ExpandValueReferences(a, provider, cache); err != nil {
		t.Fatalf("first: %v", err)
	}
	if a.Values["replicaCount"] != float64(5) {
		t.Fatalf("first replicaCount=%v", a.Values["replicaCount"])
	}

	// Mutate the underlying ConfigMap's content — different bytes
	// → different FNV hash component of the cache key → miss.
	cm.Data["values.yaml"] = "replicaCount: 99\n"

	b := mk()
	if err := ExpandValueReferences(b, provider, cache); err != nil {
		t.Fatalf("second: %v", err)
	}
	if b.Values["replicaCount"] != float64(99) {
		t.Errorf("cache served stale parse; want 99, got %v", b.Values["replicaCount"])
	}
}

// TestExpandValueReferences_NilCache pins the nil-cache contract:
// ExpandValueReferences with a nil *Cache works identically to the
// non-cached path — tests and one-shot embedders pass nil.
func TestExpandValueReferences_NilCache(t *testing.T) {
	cm := &manifest.ConfigMap{
		Name: "extra", Namespace: "default",
		Data: map[string]any{"values.yaml": "replicaCount: 7\n"},
	}
	provider := &SliceProvider{ConfigMaps: []*manifest.ConfigMap{cm}}
	hr := &manifest.HelmRelease{
		Name: "demo", Namespace: "default",
		HelmReleaseSpec: helmv2.HelmReleaseSpec{
			ValuesFrom: []manifest.ValuesReference{{Kind: "ConfigMap", Name: "extra"}},
		},
	}
	if err := ExpandValueReferences(hr, provider, nil); err != nil {
		t.Fatalf("ExpandValueReferences nil cache: %v", err)
	}
	if hr.Values["replicaCount"] != float64(7) {
		t.Errorf("nil-cache path: replicaCount=%v", hr.Values["replicaCount"])
	}
}

func TestExpandPostBuildSubstituteReference(t *testing.T) {
	cm := &manifest.ConfigMap{
		Name: "vars", Namespace: "flux-system",
		Data: map[string]any{"DOMAIN": "example.com"},
	}
	provider := &SliceProvider{ConfigMaps: []*manifest.ConfigMap{cm}}
	ks := &manifest.Kustomization{
		Name: "apps", Namespace: "flux-system",
		Contents: map[string]any{"spec": map[string]any{}},
		PostBuildSubstituteFrom: []manifest.SubstituteReference{
			{Kind: "ConfigMap", Name: "vars"},
		},
	}
	if err := ExpandPostBuildSubstituteReference(ks, provider); err != nil {
		t.Fatalf("ExpandPostBuildSubstituteReference: %v", err)
	}
	if ks.PostBuildSubstitute["DOMAIN"] != "example.com" {
		t.Errorf("substitute: %+v", ks.PostBuildSubstitute)
	}
}

// TestExpandPostBuildSubstituteReference_RejectsInvalidVarName locks
// the upstream contract (fluxcd/pkg/kustomize varSubstitution): any
// var name failing `^[_[:alpha:]][_[:alpha:][:digit:]]*$` fails the
// whole postBuild rather than being silently dropped. A ConfigMap key
// with a dash makes upstream Flux fail the Kustomization; flate must
// surface the same error.
func TestExpandPostBuildSubstituteReference_RejectsInvalidVarName(t *testing.T) {
	ks := &manifest.Kustomization{Name: "k", Namespace: "ns"}
	ks.PostBuildSubstituteFrom = []manifest.SubstituteReference{{Kind: manifest.KindConfigMap, Name: "cm"}}
	provider := &SliceProvider{
		ConfigMaps: []*manifest.ConfigMap{{
			Name: "cm", Namespace: "ns",
			Data: map[string]any{"my-var": "v", "ok_name": "v"},
		}},
	}
	err := ExpandPostBuildSubstituteReference(ks, provider)
	if err == nil {
		t.Fatal("expected error for dashed var name")
	}
	if !strings.Contains(err.Error(), "my-var") {
		t.Errorf("error should name the invalid var; got %v", err)
	}
}

// TestReplaceValueAtPath_SingleCharQuoteNoPanic pins the
// len(value) >= 2 guard: a single `'` or `"` previously slipped past
// the prefix+suffix check (prefix == suffix on a single byte) and
// then tripped a value[1:0] slice — runtime panic. Now treated as a
// plain scalar by ParseInto, which returns either ok or a sensible
// error — never a panic.
func TestReplaceValueAtPath_SingleCharQuoteNoPanic(t *testing.T) {
	for _, val := range []string{"'", `"`, "''"} {
		// Wrap in func() so a panic surfaces as test failure rather
		// than aborting the whole test binary.
		func() {
			defer func() {
				if r := recover(); r != nil {
					t.Errorf("replaceValueAtPath panicked on value %q: %v", val, r)
				}
			}()
			_, _ = replaceValueAtPath(map[string]any{}, "leaf", val)
		}()
	}
}

// TestReplaceValueAtPath_TypeCoercion locks the upstream Flux contract
// (chartutil.ReplacePathValue → strvals.ParseInto): a value flowing
// in through ValuesReference.TargetPath is parsed as a Helm CLI
// `--set foo=value` would be, with full type coercion. Without this,
// `replicaCount` came back as the string "3" and chart schemas with
// `replicaCount: integer` rejected the HR.
func TestReplaceValueAtPath_TypeCoercion(t *testing.T) {
	cases := []struct {
		name string
		path string
		val  string
		want any
	}{
		{"int", "replicaCount", "3", float64(3)},
		{"bool", "enabled", "true", true},
		{"null", "extra", "null", nil},
		{"nested map", "image.repository", "nginx", "nginx"},
		{"quoted string forces string", "tag", `"123"`, "123"},
		{"list index", "ports[0]", "8080", float64(8080)},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := replaceValueAtPath(map[string]any{}, tc.path, tc.val)
			if err != nil {
				t.Fatalf("replaceValueAtPath: %v", err)
			}
			// Walk got along path to extract the leaf.
			leaf := walkPath(t, got, tc.path)
			if !equalish(leaf, tc.want) {
				t.Errorf("path %q: got %v (%T), want %v (%T)", tc.path, leaf, leaf, tc.want, tc.want)
			}
		})
	}
}

func walkPath(t *testing.T, m map[string]any, path string) any {
	t.Helper()
	cur := any(m)
	parts := strings.SplitSeq(path, ".")
	for p := range parts {
		if key, _, ok := strings.Cut(p, "["); ok {
			if cm, ok := cur.(map[string]any); ok {
				cur = cm[key]
			}
			// Strip "[0]" → 0 and index.
			cur = cur.([]any)[0]
			continue
		}
		if cm, ok := cur.(map[string]any); ok {
			cur = cm[p]
		}
	}
	return cur
}

func equalish(a, b any) bool {
	if a == nil && b == nil {
		return true
	}
	if a == nil || b == nil {
		return false
	}
	// strvals.ParseInto returns int64 for integers; allow comparison
	// against float64 literals in test cases.
	switch av := a.(type) {
	case int64:
		if bv, ok := b.(float64); ok {
			return float64(av) == bv
		}
	case int:
		if bv, ok := b.(float64); ok {
			return float64(av) == bv
		}
	}
	return a == b
}

// TestDeepMergeMatchesDeepMergeInto pins the equivalence the copy-on-collision
// optimization relies on: functional DeepMerge(base, override) produces the
// SAME value graph as the eager DeepMergeInto(copy(base), copy(override)) the
// share path replaced. If these ever diverge, render output would change.
func TestDeepMergeMatchesDeepMergeInto(t *testing.T) {
	cases := []struct {
		name           string
		base, override map[string]any
	}{
		{"disjoint", map[string]any{"a": 1.0}, map[string]any{"b": 2.0}},
		{"scalar-over-scalar", map[string]any{"a": 1.0}, map[string]any{"a": 2.0}},
		{"nested-collision", map[string]any{"m": map[string]any{"x": 1.0, "y": 2.0}}, map[string]any{"m": map[string]any{"y": 9.0, "z": 3.0}}},
		{"list-replace", map[string]any{"l": []any{1.0, 2.0}}, map[string]any{"l": []any{3.0}}},
		{"scalar-over-map", map[string]any{"k": map[string]any{"x": 1.0}}, map[string]any{"k": "scalar"}},
		{"map-over-scalar", map[string]any{"k": "scalar"}, map[string]any{"k": map[string]any{"x": 1.0}}},
		{"nil-override", map[string]any{"k": map[string]any{"x": 1.0}}, map[string]any{"k": nil}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			functional := DeepMerge(tc.base, tc.override)
			eager := manifest.DeepCopyMap(tc.base)
			DeepMergeInto(eager, manifest.DeepCopyMap(tc.override))
			if !reflect.DeepEqual(functional, eager) {
				t.Errorf("DeepMerge != DeepMergeInto\n functional=%#v\n eager=%#v", functional, eager)
			}
		})
	}
}

// nestedValuesCM returns a ConfigMap whose values.yaml is a nested map +
// list document — the shape a platform-wide values CM takes.
func nestedValuesCM() *manifest.ConfigMap {
	return &manifest.ConfigMap{
		Name: "platform", Namespace: "default",
		Data: map[string]any{"values.yaml": "image:\n  repository: nginx\n  tag: v1\nenv:\n  - name: A\n    value: \"1\"\n"},
	}
}

func wholeDocHR() *manifest.HelmRelease {
	return &manifest.HelmRelease{
		Name: "demo", Namespace: "default",
		HelmReleaseSpec: helmv2.HelmReleaseSpec{
			ValuesFrom: []manifest.ValuesReference{{Kind: "ConfigMap", Name: "platform"}},
		},
	}
}

// TestExpandValueReferences_ConcurrentSharedCache pins the concurrency
// contract: many HRs referencing the same ConfigMap share ONE cached parsed
// tree by reference (the optimization), and concurrent expansion must be
// race-free and correct. Run under -race, this fails if any goroutine writes
// through a shared cache sub-tree.
func TestExpandValueReferences_ConcurrentSharedCache(t *testing.T) {
	provider := &SliceProvider{ConfigMaps: []*manifest.ConfigMap{nestedValuesCM()}}
	cache := NewCache()
	const n = 64
	var wg sync.WaitGroup
	errs := make([]error, n)
	for i := range n {
		wg.Go(func() {
			hr := wholeDocHR()
			if err := ExpandValueReferences(hr, provider, cache); err != nil {
				errs[i] = err
				return
			}
			img, ok := hr.Values["image"].(map[string]any)
			if !ok || img["tag"] != "v1" || img["repository"] != "nginx" {
				errs[i] = errInvalidResult
				return
			}
			env, ok := hr.Values["env"].([]any)
			if !ok || len(env) != 1 {
				errs[i] = errInvalidResult
			}
		})
	}
	wg.Wait()
	for i, err := range errs {
		if err != nil {
			t.Fatalf("goroutine %d: %v", i, err)
		}
	}
}

var errInvalidResult = errors.New("unexpected merged values")

// TestExpandValueReferences_TargetPathDoesNotCorruptSharedCache pins HOLE 1/2
// of the design: an HR with a TargetPath ref takes the eager fully-owned path,
// so its in-place strvals write into a key that ALSO exists in the cached
// canonical must NOT leak into the cache (which whole-doc-only HRs share).
func TestExpandValueReferences_TargetPathDoesNotCorruptSharedCache(t *testing.T) {
	secret := &manifest.ConfigMap{
		Name: "inject", Namespace: "default",
		Data: map[string]any{"v": "Always"},
	}
	provider := &SliceProvider{ConfigMaps: []*manifest.ConfigMap{nestedValuesCM(), secret}}
	cache := NewCache()

	// Prime the cache with a whole-doc-only HR (shares the canonical).
	a := wholeDocHR()
	if err := ExpandValueReferences(a, provider, cache); err != nil {
		t.Fatalf("prime: %v", err)
	}

	// HR with a TargetPath ref writing image.pullPolicy — image already
	// exists in the cached canonical, so a sharing+in-place write would
	// corrupt it. The TargetPath presence must force the eager path.
	b := &manifest.HelmRelease{
		Name: "demo", Namespace: "default",
		HelmReleaseSpec: helmv2.HelmReleaseSpec{
			ValuesFrom: []manifest.ValuesReference{
				{Kind: "ConfigMap", Name: "platform"},
				{Kind: "ConfigMap", Name: "inject", ValuesKey: "v", TargetPath: "image.pullPolicy"},
			},
		},
	}
	if err := ExpandValueReferences(b, provider, cache); err != nil {
		t.Fatalf("targetpath: %v", err)
	}
	if img := b.Values["image"].(map[string]any); img["pullPolicy"] != "Always" {
		t.Fatalf("targetpath write did not land: image=%#v", img)
	}

	// A third whole-doc-only HR must NOT see the injected pullPolicy — i.e.
	// b's in-place write never reached the shared cache canonical.
	c := wholeDocHR()
	if err := ExpandValueReferences(c, provider, cache); err != nil {
		t.Fatalf("recheck: %v", err)
	}
	img := c.Values["image"].(map[string]any)
	if _, leaked := img["pullPolicy"]; leaked {
		t.Errorf("TargetPath write corrupted the shared cache canonical: image=%#v", img)
	}
	// And the priming HR's view is likewise uncorrupted.
	if _, leaked := a.Values["image"].(map[string]any)["pullPolicy"]; leaked {
		t.Errorf("TargetPath write leaked into a prior HR's shared values: %#v", a.Values["image"])
	}
}
