package values

import (
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"testing"

	"helm.sh/helm/v4/pkg/chart/v2/loader"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/pkg/manifest"
)

// SliceProvider is a test-only Provider backed by in-memory slices.
// Production wires NewStoreProvider against the central Store.
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

// TestDeepMergeInto_MutatesDstPreservesSemantics pins the in-place
// variant's contract: dst is mutated, override is read-only, and
// the resulting tree matches Helm's output for the same inputs.
// Sub-maps from override are inserted by reference when no key
// collides (matching Helm); on a collision the recursive merge
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

func TestExpandValueReferences_ConfigMap(t *testing.T) {
	cm := &manifest.ConfigMap{
		Name: "extra", Namespace: "default",
		Data: map[string]any{"values.yaml": "replicaCount: 5\nimage:\n  tag: v2\n"},
	}
	provider := &SliceProvider{ConfigMaps: []*manifest.ConfigMap{cm}}
	hr := &manifest.HelmRelease{
		Name: "demo", Namespace: "default",
		ValuesFrom: []manifest.ValuesReference{{Kind: "ConfigMap", Name: "extra"}},
		Values:     map[string]any{"image": map[string]any{"repository": "x"}},
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
		ValuesFrom: []manifest.ValuesReference{{Kind: "ConfigMap", Name: "extra"}},
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
		ValuesFrom: []manifest.ValuesReference{{Kind: "ConfigMap", Name: "extra"}},
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
		ValuesFrom: []manifest.ValuesReference{
			{Kind: "ConfigMap", Name: "k", ValuesKey: "v", TargetPath: "auth.password"},
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

func TestExpandValueReferences_LiteralTargetPath(t *testing.T) {
	config := `concurrent = 4
environment = ["FOO=bar", "BAZ=qux"]
`
	cm := &manifest.ConfigMap{
		Name: "runner", Namespace: "default",
		Data: map[string]any{"config.toml": config},
	}
	hr := &manifest.HelmRelease{
		Name: "demo", Namespace: "default",
		ValuesFrom: []manifest.ValuesReference{
			{
				Kind:       "ConfigMap",
				Name:       "runner",
				ValuesKey:  "config.toml",
				TargetPath: "runners.config",
				Literal:    true,
			},
		},
	}
	if err := ExpandValueReferences(hr, &SliceProvider{ConfigMaps: []*manifest.ConfigMap{cm}}, nil); err != nil {
		t.Fatalf("ExpandValueReferences: %v", err)
	}
	runners := hr.Values["runners"].(map[string]any)
	if runners["config"] != config {
		t.Errorf("config: %q, want %q", runners["config"], config)
	}
}

// TestExpandValueReferences_LiteralTargetPathEscapedDot pins the Flux
// chartutil.ReplacePathLiteralValue contract that a literal ref keeps `\.`
// escape support in the targetPath (helm's own --set-literal parser does
// not): the escaped segment stays one key, and the value — commas and all —
// lands verbatim.
func TestExpandValueReferences_LiteralTargetPathEscapedDot(t *testing.T) {
	cm := &manifest.ConfigMap{
		Name: "scrape", Namespace: "default",
		Data: map[string]any{"value": "true,false"},
	}
	hr := &manifest.HelmRelease{
		Name: "demo", Namespace: "default",
		ValuesFrom: []manifest.ValuesReference{
			{
				Kind:       "ConfigMap",
				Name:       "scrape",
				ValuesKey:  "value",
				TargetPath: `annotations.prometheus\.io/scrape`,
				Literal:    true,
			},
		},
	}
	if err := ExpandValueReferences(hr, &SliceProvider{ConfigMaps: []*manifest.ConfigMap{cm}}, nil); err != nil {
		t.Fatalf("ExpandValueReferences: %v", err)
	}
	annotations := hr.Values["annotations"].(map[string]any)
	if got := annotations["prometheus.io/scrape"]; got != "true,false" {
		t.Errorf(`annotations = %#v, want key "prometheus.io/scrape" = "true,false"`, annotations)
	}
}

func TestExpandValueReferences_MissingOptionalTargetPath(t *testing.T) {
	hr := &manifest.HelmRelease{
		Name: "demo", Namespace: "default",
		Values: map[string]any{"existing": "kept"},
		ValuesFrom: []manifest.ValuesReference{
			{Kind: "ConfigMap", Name: "absent", ValuesKey: "v", TargetPath: "k", Optional: true},
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
		ValuesFrom: []manifest.ValuesReference{
			{Kind: "ConfigMap", Name: "absent", ValuesKey: "v", TargetPath: "k"},
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
		ValuesFrom: []manifest.ValuesReference{
			{Kind: "ConfigMap", Name: "extra", ValuesKey: "missing.yaml", Optional: true},
		},
	}

	if err := ExpandValueReferences(hr, &SliceProvider{ConfigMaps: []*manifest.ConfigMap{cm}}, nil); err != nil {
		t.Fatalf("ExpandValueReferences: %v", err)
	}
	if hr.Values["existing"] != "kept" || len(hr.Values) != 1 {
		t.Errorf("optional missing key should leave values unchanged: %+v", hr.Values)
	}
}

// Identical YAML shares one immutable parse across resource identities.
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
			ValuesFrom: []manifest.ValuesReference{{Kind: "ConfigMap", Name: "platform"}},
		}
	}

	a := hr()
	if err := ExpandValueReferences(a, provider, cache); err != nil {
		t.Fatalf("first ExpandValueReferences: %v", err)
	}
	if a.Values["replicaCount"] != float64(5) {
		t.Fatalf("first: replicaCount=%v", a.Values["replicaCount"])
	}

	other := &manifest.ConfigMap{Name: "other", Namespace: "apps",
		Data: map[string]any{"extra.yaml": cm.Data["values.yaml"]}}
	provider.ConfigMaps = append(provider.ConfigMaps, other)
	b := hr()
	b.Namespace = other.Namespace
	b.ValuesFrom = []manifest.ValuesReference{{Kind: "ConfigMap", Name: other.Name, ValuesKey: "extra.yaml"}}
	if err := ExpandValueReferences(b, provider, cache); err != nil {
		t.Fatalf("second ExpandValueReferences: %v", err)
	}
	if b.Values["replicaCount"] != float64(5) {
		t.Errorf("second: replicaCount=%v", b.Values["replicaCount"])
	}

	entries := 0
	cache.m.Range(func(_, _ any) bool { entries++; return true })
	if entries != 1 {
		t.Fatalf("identical content cached %d times, want once", entries)
	}

	// Top-level writes must not reach the shared cache canonical.
	a.Values["replicaCount"] = "stomped"
	c := hr()
	if err := ExpandValueReferences(c, provider, cache); err != nil {
		t.Fatalf("third ExpandValueReferences: %v", err)
	}
	if c.Values["replicaCount"] != float64(5) {
		t.Errorf("cache aliased prior call: replicaCount=%v", c.Values["replicaCount"])
	}
}

// Changed YAML must resolve to a fresh parse without explicit invalidation.
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
			ValuesFrom: []manifest.ValuesReference{{Kind: "ConfigMap", Name: "platform"}},
		}
	}

	a := mk()
	if err := ExpandValueReferences(a, provider, cache); err != nil {
		t.Fatalf("first: %v", err)
	}
	if a.Values["replicaCount"] != float64(5) {
		t.Fatalf("first replicaCount=%v", a.Values["replicaCount"])
	}

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
		ValuesFrom: []manifest.ValuesReference{{Kind: "ConfigMap", Name: "extra"}},
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

func TestExpandPostBuildSubstituteReference_MalformedDataLeavesSubstitutionsUntouched(t *testing.T) {
	for _, kind := range []string{manifest.KindConfigMap, manifest.KindSecret} {
		t.Run(kind, func(t *testing.T) {
			provider := &SliceProvider{
				ConfigMaps: []*manifest.ConfigMap{{Name: "vars", Namespace: "ns", Data: map[string]any{"INVALID": []any{"nested"}}}},
				Secrets:    []*manifest.Secret{{Name: "vars", Namespace: "ns", Data: map[string]any{"INVALID": "invalid-base64"}}},
			}
			ks := &manifest.Kustomization{
				Name: "app", Namespace: "ns",
				PostBuildSubstitute:     map[string]any{"INLINE": "kept"},
				PostBuildSubstituteFrom: []manifest.SubstituteReference{{Kind: kind, Name: "vars"}},
				Contents:                map[string]any{"spec": map[string]any{}},
			}
			if err := ExpandPostBuildSubstituteReference(ks, provider); !errors.Is(err, manifest.ErrInput) {
				t.Fatalf("got %v, want ErrInput", err)
			}
			assert.Diff(t, ks.PostBuildSubstitute, map[string]any{"INLINE": "kept"})
			assert.Diff(t, ks.Contents, map[string]any{"spec": map[string]any{}})
		})
	}
}

func TestExpandPostBuildSubstituteReference_MalformedKeysDeterministic(t *testing.T) {
	sec := &manifest.Secret{Name: "vars", Namespace: "ns", Data: map[string]any{"C": "!", "A": "!", "B": "!"}}
	provider := &SliceProvider{Secrets: []*manifest.Secret{sec}}
	ks := &manifest.Kustomization{Name: "app", Namespace: "ns", PostBuildSubstituteFrom: []manifest.SubstituteReference{{Kind: manifest.KindSecret, Name: "vars"}}}
	_, _, want := sec.Value("A")
	for range 50 {
		err := ExpandPostBuildSubstituteReference(ks, provider)
		if err == nil || err.Error() != want.Error() {
			t.Fatalf("got %v, want %v", err, want)
		}
	}
}

func TestExpandPostBuildSubstituteReference_RejectsInvalidVarName(t *testing.T) {
	for _, source := range []string{"ConfigMap", "inline"} {
		t.Run(source, func(t *testing.T) {
			ks := &manifest.Kustomization{Name: "k", Namespace: "ns"}
			vars := map[string]any{"z-var": "v", "my-var": "v", "ok_name": "v"}
			provider := &SliceProvider{}
			if source == "inline" {
				ks.PostBuildSubstitute = vars
			} else {
				ks.PostBuildSubstituteFrom = []manifest.SubstituteReference{{Kind: manifest.KindConfigMap, Name: "cm"}}
				provider.ConfigMaps = []*manifest.ConfigMap{{Name: "cm", Namespace: "ns", Data: vars}}
			}

			err := ExpandPostBuildSubstituteReference(ks, provider)
			if !errors.Is(err, manifest.ErrInvalidSubstituteReference) {
				t.Fatalf("expected invalid substitute reference, got %v", err)
			}
			if !strings.Contains(err.Error(), "[my-var z-var]") {
				t.Errorf("error should name invalid vars in sorted order; got %v", err)
			}
		})
	}
}

// TestUnreadableSubstituteSecrets reports only substituteFrom Secret refs whose
// data flate can't read offline. A SOPS-wiped Secret (placeholder data) and a
// real Secret are readable; a ConfigMap ref and an optional ref are skipped.
func TestUnreadableSubstituteSecrets(t *testing.T) {
	provider := &SliceProvider{
		ConfigMaps: []*manifest.ConfigMap{{Name: "cm", Namespace: "flux-system", Data: map[string]any{"X": "1"}}},
		Secrets: []*manifest.Secret{
			// SOPS-wiped: data resolves to a placeholder, so its vars still substitute.
			{Name: "sops-vars", Namespace: "flux-system", StringData: map[string]any{"Y": fmt.Sprintf(manifest.ValuePlaceholderTemplate, "Y")}},
			// A real, readable Secret.
			{Name: "real", Namespace: "flux-system", StringData: map[string]any{"Z": "z"}},
		},
	}
	ks := &manifest.Kustomization{
		Name: "apps", Namespace: "flux-system",
		PostBuildSubstituteFrom: []manifest.SubstituteReference{
			{Kind: manifest.KindSecret, Name: "absent-vars"},                // unreadable → reported
			{Kind: manifest.KindSecret, Name: "sops-vars"},                  // placeholder data → readable
			{Kind: manifest.KindSecret, Name: "real"},                       // real data → readable
			{Kind: manifest.KindConfigMap, Name: "cm"},                      // not a Secret → skipped
			{Kind: manifest.KindSecret, Name: "opt-absent", Optional: true}, // optional → skipped
		},
	}
	if got, want := UnreadableSubstituteSecrets(ks, provider), []string{"absent-vars"}; !reflect.DeepEqual(got, want) {
		t.Errorf("UnreadableSubstituteSecrets = %v, want %v", got, want)
	}
	if got := UnreadableSubstituteSecrets(&manifest.Kustomization{Name: "x", Namespace: "ns"}, provider); got != nil {
		t.Errorf("no substituteFrom should yield nil, got %v", got)
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

// TestDeepMergeInto_MatchesHelmMerge pins flate's in-place merge against Helm.
func TestDeepMergeInto_MatchesHelmMerge(t *testing.T) {
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
			functional := loader.MergeMaps(tc.base, tc.override)
			eager := manifest.DeepCopyMap(tc.base)
			DeepMergeInto(eager, manifest.DeepCopyMap(tc.override))
			if !reflect.DeepEqual(functional, eager) {
				t.Errorf("Helm MergeMaps != DeepMergeInto\n functional=%#v\n eager=%#v", functional, eager)
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
		ValuesFrom: []manifest.ValuesReference{{Kind: "ConfigMap", Name: "platform"}},
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
		ValuesFrom: []manifest.ValuesReference{
			{Kind: "ConfigMap", Name: "platform"},
			{Kind: "ConfigMap", Name: "inject", ValuesKey: "v", TargetPath: "image.pullPolicy"},
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

// TestExpandValueReferences_YAMLContainingPlaceholderIsMerged verifies that
// a ConfigMap whose values.yaml contains placeholder tokens inside valid YAML
// values (e.g. from postBuild substitution on a configMapGenerator) is correctly
// parsed and merged into hr.Values instead of being discarded.
func TestExpandValueReferences_YAMLContainingPlaceholderIsMerged(t *testing.T) {
	cm := &manifest.ConfigMap{
		Name:      "generator-cm",
		Namespace: "default",
		Data: map[string]any{
			"values.yaml": "domain: ..PLACEHOLDER_DOMAIN..\nport: 8080\n",
		},
	}
	provider := &SliceProvider{ConfigMaps: []*manifest.ConfigMap{cm}}
	hr := &manifest.HelmRelease{
		Name:      "demo",
		Namespace: "default",
		ValuesFrom: []manifest.ValuesReference{
			{Kind: "ConfigMap", Name: "generator-cm"},
		},
	}
	if err := ExpandValueReferences(hr, provider, nil); err != nil {
		t.Fatalf("ExpandValueReferences failed: %v", err)
	}
	if hr.Values["domain"] != "..PLACEHOLDER_DOMAIN.." {
		t.Errorf("domain = %v, want ..PLACEHOLDER_DOMAIN..", hr.Values["domain"])
	}
	if fmt.Sprint(hr.Values["port"]) != "8080" {
		t.Errorf("port = %v, want 8080", hr.Values["port"])
	}
}

// TestExpandValueReferences_WipedScalarPlaceholderTolerated verifies that
// a Secret whose values.yaml is a wiped scalar placeholder string (e.g. from
// a SOPS-encrypted values file) is safely treated as empty rather than failing
// YAML unmarshaling.
func TestExpandValueReferences_WipedScalarPlaceholderTolerated(t *testing.T) {
	sec := &manifest.Secret{
		Name:      "sops-secret",
		Namespace: "default",
		StringData: map[string]any{
			"values.yaml": "..PLACEHOLDER_values.yaml..",
		},
	}
	provider := &SliceProvider{Secrets: []*manifest.Secret{sec}}
	hr := &manifest.HelmRelease{
		Name:      "demo",
		Namespace: "default",
		ValuesFrom: []manifest.ValuesReference{
			{Kind: "Secret", Name: "sops-secret"},
		},
	}
	if err := ExpandValueReferences(hr, provider, nil); err != nil {
		t.Fatalf("ExpandValueReferences failed for wiped scalar placeholder: %v", err)
	}
	if len(hr.Values) != 0 {
		t.Errorf("expected empty values, got %v", hr.Values)
	}
}
