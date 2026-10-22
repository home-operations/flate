package values

import (
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"strconv"
	"strings"
	"sync"
	"testing"

	"helm.sh/helm/v4/pkg/chart/v2/loader"
	"sigs.k8s.io/yaml"

	kustomizev1 "github.com/fluxcd/kustomize-controller/api/v1"
	"github.com/home-operations/flate/internal/assert"

	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

// SliceProvider is a test-only Provider backed by in-memory slices.
// Production wires NewStoreProvider against the central Store.
type SliceProvider struct {
	ConfigMaps []*manifest.ConfigMap
	Secrets    []*manifest.Secret
}

// Data finds data by its full identity.
func (p *SliceProvider) Data(id manifest.NamedResource) manifest.Data {
	for _, c := range p.ConfigMaps {
		if c.Named() == id {
			return c
		}
	}
	for _, s := range p.Secrets {
		if s.Named() == id {
			return s
		}
	}
	return nil
}

func expandValuesForTest(t *testing.T, hr *manifest.HelmRelease, provider Provider, cache *Cache) {
	t.Helper()
	_, err := ExpandValueReferences(hr, provider, cache)
	assert.NoError(t, err)
}

func TestDeepMergeInto_MutatesDstPreservesSemantics(t *testing.T) {
	t.Parallel()
	dst := map[string]any{
		"a": 1,
		"b": map[string]any{"x": 1, "y": 2},
	}
	override := map[string]any{
		"a": 2,
		"b": map[string]any{"y": 99, "z": 3},
		"c": []any{1, 2},
	}
	borrowed := dst["b"].(map[string]any)
	before := manifest.DeepCopyMap(borrowed)
	overrideBefore := manifest.DeepCopyMap(override)
	result := DeepMergeInto(dst, override)
	if result["a"] != 2 {
		t.Errorf("scalar override failed: %v", result["a"])
	}
	bb := result["b"].(map[string]any)
	if bb["x"] != 1 || bb["y"] != 99 || bb["z"] != 3 {
		t.Errorf("nested merge wrong: %v", bb)
	}
	dst["sentinel"] = "v"
	if result["sentinel"] != "v" {
		t.Error("DeepMergeInto must return dst itself, got distinct map")
	}
	if dst["a"] != 2 || dst["b"].(map[string]any)["y"] != 99 {
		t.Error("dst not mutated as expected")
	}
	assert.Diff(t, borrowed, before)
	assert.Diff(t, override, overrideBefore)
}

func TestExpandValueReferences_ConfigMap(t *testing.T) {
	t.Parallel()
	cm := &manifest.ConfigMap{
		Name: "extra", Namespace: "default",
		Data: map[string]any{"values.yaml": "replicaCount: 5\nimage:\n  tag: v2\n"},
	}
	st := store.New()
	st.AddObject(cm)
	st.AddObject(&manifest.RawObject{APIVersion: "example.test/v1", Kind: manifest.KindConfigMap, Name: cm.Name, Namespace: cm.Namespace})
	provider := NewStoreProvider(st, true, nil)
	hr := &manifest.HelmRelease{
		Name: "demo", Namespace: "default",
		ValuesFrom: []manifest.ValuesReference{{Kind: "ConfigMap", Name: "extra"}},
		Values:     map[string]any{"image": map[string]any{"repository": "x"}},
	}
	if _, err := ExpandValueReferences(hr, provider, nil); err != nil {
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
	t.Parallel()
	for _, obj := range []manifest.BaseManifest{
		&manifest.ConfigMap{Name: "extra", Namespace: "default", Data: map[string]any{"values.yaml": "replicaCount: 3\n", "broken": map[string]any{"nested": true}}},
		&manifest.Secret{Name: "extra", Namespace: "default", Data: map[string]any{"values.yaml": "cmVwbGljYUNvdW50OiAzCg==", "broken": "!"}},
	} {
		t.Run(obj.Named().Kind, func(t *testing.T) {
			t.Parallel()
			st := store.New()
			st.AddObject(obj)
			hr := &manifest.HelmRelease{Name: "demo", Namespace: "default", ValuesFrom: []manifest.ValuesReference{{Kind: obj.Named().Kind, Name: "extra"}}}
			expandValuesForTest(t, hr, NewStoreProvider(st, true, nil), nil)
			if got := hr.Values["replicaCount"]; fmt.Sprint(got) != "3" {
				t.Errorf("replicaCount = %v, want 3", got)
			}
		})
	}
}

func TestExpandValueReferences_MalformedRequestedKeyStillFails(t *testing.T) {
	t.Parallel()
	for _, obj := range []manifest.BaseManifest{
		&manifest.ConfigMap{Name: "extra", Namespace: "default", Data: map[string]any{"values.yaml": []any{"not", "a", "scalar"}}},
		&manifest.Secret{Name: "extra", Namespace: "default", Data: map[string]any{"values.yaml": "!"}},
	} {
		for _, optional := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/optional=%t", obj.Named().Kind, optional), func(t *testing.T) {
				t.Parallel()
				st := store.New()
				st.AddObject(obj)
				hr := &manifest.HelmRelease{Name: "demo", Namespace: "default", ValuesFrom: []manifest.ValuesReference{{Kind: obj.Named().Kind, Name: "extra", Optional: optional}}}
				_, err := ExpandValueReferences(hr, NewStoreProvider(st, true, nil), nil)
				if !errors.Is(err, manifest.ErrInput) || !strings.Contains(err.Error(), "data[values.yaml]") {
					t.Fatalf("malformed requested key must fail with its name: %v", err)
				}
			})
		}
	}
}

func TestExpandValueReferences_TargetPath(t *testing.T) {
	t.Parallel()
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
	if _, err := ExpandValueReferences(hr, provider, nil); err != nil {
		t.Fatalf("ExpandValueReferences: %v", err)
	}
	auth := hr.Values["auth"].(map[string]any)
	if auth["password"] != "secret-value" {
		t.Errorf("password: %v", auth["password"])
	}
}

func TestExpandValueReferences_LiteralTargetPath(t *testing.T) {
	t.Parallel()
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
	if _, err := ExpandValueReferences(hr, &SliceProvider{ConfigMaps: []*manifest.ConfigMap{cm}}, nil); err != nil {
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
	t.Parallel()
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
	if _, err := ExpandValueReferences(hr, &SliceProvider{ConfigMaps: []*manifest.ConfigMap{cm}}, nil); err != nil {
		t.Fatalf("ExpandValueReferences: %v", err)
	}
	annotations := hr.Values["annotations"].(map[string]any)
	if got := annotations["prometheus.io/scrape"]; got != "true,false" {
		t.Errorf(`annotations = %#v, want key "prometheus.io/scrape" = "true,false"`, annotations)
	}
}

func TestExpandValueReferences_MissingOptionalTargetPath(t *testing.T) {
	t.Parallel()
	hr := &manifest.HelmRelease{
		Name: "demo", Namespace: "default",
		Values: map[string]any{"existing": "kept"},
		ValuesFrom: []manifest.ValuesReference{
			{Kind: "ConfigMap", Name: "absent", ValuesKey: "v", TargetPath: "k", Optional: true},
		},
	}
	provider := &SliceProvider{}
	if _, err := ExpandValueReferences(hr, provider, nil); err != nil {
		t.Fatalf("ExpandValueReferences: %v", err)
	}
	if _, ok := hr.Values["k"]; ok {
		t.Errorf("optional missing targetPath ref should be skipped, got %v", hr.Values["k"])
	}
	if hr.Values["existing"] != "kept" {
		t.Errorf("inline values should remain: %+v", hr.Values)
	}
}

func TestExpandValueReferences_MissingReferences(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name, kind string
		object     manifest.BaseManifest
		want       error
	}{
		{"absent configmap", manifest.KindConfigMap, nil, manifest.ErrObjectNotFound},
		{"absent secret", manifest.KindSecret, nil, manifest.ErrObjectNotFound},
		{"empty configmap", manifest.KindConfigMap, &manifest.ConfigMap{Name: "vars", Namespace: "default"}, manifest.ErrObjectNotFound},
		{"empty secret", manifest.KindSecret, &manifest.Secret{Name: "vars", Namespace: "default"}, manifest.ErrObjectNotFound},
		{"empty bags", manifest.KindSecret, &manifest.Secret{Name: "vars", Namespace: "default", Data: map[string]any{}, StringData: map[string]any{}}, manifest.ErrObjectNotFound},
		{"empty configmap value", manifest.KindConfigMap, &manifest.ConfigMap{Name: "vars", Namespace: "default", Data: map[string]any{"v": ""}}, nil},
		{"empty secret data value", manifest.KindSecret, &manifest.Secret{Name: "vars", Namespace: "default", Data: map[string]any{"v": ""}}, nil},
		{"empty secret stringdata value", manifest.KindSecret, &manifest.Secret{Name: "vars", Namespace: "default", StringData: map[string]any{"v": ""}}, nil},
		{"empty secret shadow value", manifest.KindSecret, &manifest.Secret{Name: "vars", Namespace: "default", Data: map[string]any{"v": "!"}, StringData: map[string]any{"v": ""}}, nil},
		{"nonempty configmap", manifest.KindConfigMap, &manifest.ConfigMap{Name: "vars", Namespace: "default", Data: map[string]any{"other": "!"}}, manifest.ErrInvalidValuesReference},
		{"nonempty secret data", manifest.KindSecret, &manifest.Secret{Name: "vars", Namespace: "default", Data: map[string]any{"other": "!"}}, manifest.ErrInvalidValuesReference},
		{"nonempty secret stringdata", manifest.KindSecret, &manifest.Secret{Name: "vars", Namespace: "default", StringData: map[string]any{"other": ""}}, manifest.ErrInvalidValuesReference},
		{"wrong namespace", manifest.KindSecret, &manifest.Secret{Name: "vars", Namespace: "other", StringData: map[string]any{"v": "present"}}, manifest.ErrObjectNotFound},
		{"wrong kind", manifest.KindSecret, &manifest.ConfigMap{Name: "vars", Namespace: "default", Data: map[string]any{"v": "present"}}, manifest.ErrObjectNotFound},
		{"unsupported kind", "Deployment", nil, manifest.ErrInvalidValuesReference},
	} {
		for _, optional := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/optional=%t", tc.name, optional), func(t *testing.T) {
				t.Parallel()
				st := store.New()
				if tc.object != nil {
					st.AddObject(tc.object)
				}
				hr := &manifest.HelmRelease{Name: "demo", Namespace: "default", Values: map[string]any{"existing": "kept"},
					ValuesFrom: []manifest.ValuesReference{{Kind: tc.kind, Name: "vars", ValuesKey: "v", TargetPath: "k", Optional: optional}}}
				_, err := ExpandValueReferences(hr, NewStoreProvider(st, true, nil), nil)
				if optional && tc.kind != "Deployment" && tc.want != nil {
					assert.NoError(t, err)
					assert.Diff(t, hr.Values, map[string]any{"existing": "kept"})
					return
				}
				if !errors.Is(err, tc.want) {
					t.Fatalf("got %v, want %v", err, tc.want)
				}
				if err == nil {
					assert.Diff(t, hr.Values, map[string]any{"existing": "kept", "k": ""})
					return
				}
				if tc.kind != "Deployment" && strings.Contains(err.Error(), `key "v"`) != errors.Is(tc.want, manifest.ErrInvalidValuesReference) {
					t.Fatalf("wrong missing-resource/key disposition: %v", err)
				}
			})
		}
	}
}

func TestExpandValueReferences_MissingOptionalKeySkipped(t *testing.T) {
	t.Parallel()
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

	if _, err := ExpandValueReferences(hr, &SliceProvider{ConfigMaps: []*manifest.ConfigMap{cm}}, nil); err != nil {
		t.Fatalf("ExpandValueReferences: %v", err)
	}
	if hr.Values["existing"] != "kept" || len(hr.Values) != 1 {
		t.Errorf("optional missing key should leave values unchanged: %+v", hr.Values)
	}
}

// Identical YAML shares one immutable parse across resource identities.
func TestExpandValueReferences_CacheHits(t *testing.T) {
	t.Parallel()
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
	if _, err := ExpandValueReferences(a, provider, cache); err != nil {
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
	if _, err := ExpandValueReferences(b, provider, cache); err != nil {
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
	if _, err := ExpandValueReferences(c, provider, cache); err != nil {
		t.Fatalf("third ExpandValueReferences: %v", err)
	}
	if c.Values["replicaCount"] != float64(5) {
		t.Errorf("cache aliased prior call: replicaCount=%v", c.Values["replicaCount"])
	}
}

// Changed YAML must resolve to a fresh parse without explicit invalidation.
func TestExpandValueReferences_CacheInvalidatesOnContentChange(t *testing.T) {
	t.Parallel()
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
	if _, err := ExpandValueReferences(a, provider, cache); err != nil {
		t.Fatalf("first: %v", err)
	}
	if a.Values["replicaCount"] != float64(5) {
		t.Fatalf("first replicaCount=%v", a.Values["replicaCount"])
	}

	cm.Data["values.yaml"] = "replicaCount: 99\n"

	b := mk()
	if _, err := ExpandValueReferences(b, provider, cache); err != nil {
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
	t.Parallel()
	cm := &manifest.ConfigMap{
		Name: "extra", Namespace: "default",
		Data: map[string]any{"values.yaml": "replicaCount: 7\n"},
	}
	provider := &SliceProvider{ConfigMaps: []*manifest.ConfigMap{cm}}
	hr := &manifest.HelmRelease{
		Name: "demo", Namespace: "default",
		ValuesFrom: []manifest.ValuesReference{{Kind: "ConfigMap", Name: "extra"}},
	}
	if _, err := ExpandValueReferences(hr, provider, nil); err != nil {
		t.Fatalf("ExpandValueReferences nil cache: %v", err)
	}
	if hr.Values["replicaCount"] != float64(7) {
		t.Errorf("nil-cache path: replicaCount=%v", hr.Values["replicaCount"])
	}
}

func TestExpandPostBuildSubstituteReference(t *testing.T) {
	t.Parallel()
	cm := &manifest.ConfigMap{
		Name: "vars", Namespace: "flux-system",
		Data: map[string]any{"DOMAIN": "example.com"},
	}
	st := store.New()
	st.AddObject(cm)
	st.AddObject(&manifest.RawObject{APIVersion: "example.test/v1", Kind: manifest.KindConfigMap, Name: cm.Name, Namespace: cm.Namespace})
	st.AddObject(&manifest.Secret{Name: "secret-vars", Namespace: cm.Namespace, StringData: map[string]any{"TOKEN": "core"}})
	st.AddObject(&manifest.RawObject{APIVersion: "example.test/v1", Kind: manifest.KindSecret, Name: "secret-vars", Namespace: cm.Namespace})
	provider := NewStoreProvider(st, true, nil)
	ks := &manifest.Kustomization{
		Name: "apps", Namespace: "flux-system",

		PostBuild: &kustomizev1.PostBuild{SubstituteFrom: []manifest.SubstituteReference{
			{Kind: "Unsupported", Name: "vars"},
			{Kind: "ConfigMap", Name: "vars"},
			{Kind: "Secret", Name: "secret-vars"},
		}},
	}
	postBuild := ks.PostBuild
	if err := ExpandPostBuildSubstituteReference(ks, provider); err != nil {
		t.Fatalf("ExpandPostBuildSubstituteReference: %v", err)
	}
	assert.Equal(t, ks.PostBuild, postBuild)
	if ks.PostBuild.Substitute["DOMAIN"] != "example.com" || ks.PostBuild.Substitute["TOKEN"] != "core" {
		t.Errorf("substitute: %+v", ks.PostBuild.Substitute)
	}
}

func TestExpandPostBuildSubstituteReference_MalformedDataLeavesSubstitutionsUntouched(t *testing.T) {
	t.Parallel()
	for _, kind := range []string{manifest.KindConfigMap, manifest.KindSecret} {
		t.Run(kind, func(t *testing.T) {
			t.Parallel()
			provider := &SliceProvider{
				ConfigMaps: []*manifest.ConfigMap{{Name: "vars", Namespace: "ns", Data: map[string]any{"INVALID": []any{"nested"}}}},
				Secrets:    []*manifest.Secret{{Name: "vars", Namespace: "ns", Data: map[string]any{"INVALID": "invalid-base64"}}},
			}
			ks := &manifest.Kustomization{
				Name: "app", Namespace: "ns",
				PostBuild: &kustomizev1.PostBuild{Substitute: map[string]string{"INLINE": "kept"}, SubstituteFrom: []manifest.SubstituteReference{{Kind: kind, Name: "vars"}}},
			}
			if err := ExpandPostBuildSubstituteReference(ks, provider); !errors.Is(err, manifest.ErrInput) {
				t.Fatalf("got %v, want ErrInput", err)
			}
			assert.Diff(t, ks.PostBuild.Substitute, map[string]string{"INLINE": "kept"})
		})
	}
}

func TestExpandPostBuildSubstituteReference_MalformedKeysDeterministic(t *testing.T) {
	t.Parallel()
	sec := &manifest.Secret{Name: "vars", Namespace: "ns", Data: map[string]any{"C": "!", "A": "!", "B": "!"}}
	provider := &SliceProvider{Secrets: []*manifest.Secret{sec}}
	ks := &manifest.Kustomization{Name: "app", Namespace: "ns", PostBuild: &kustomizev1.PostBuild{SubstituteFrom: []manifest.SubstituteReference{{Kind: manifest.KindSecret, Name: "vars"}}}}
	_, _, want := sec.Value("A")
	for range 50 {
		err := ExpandPostBuildSubstituteReference(ks, provider)
		if err == nil || err.Error() != want.Error() {
			t.Fatalf("got %v, want %v", err, want)
		}
	}
}

func TestExpandPostBuildSubstituteReference_RejectsInvalidVarName(t *testing.T) {
	t.Parallel()
	for _, source := range []string{"ConfigMap", "inline"} {
		t.Run(source, func(t *testing.T) {
			t.Parallel()
			ks := &manifest.Kustomization{Name: "k", Namespace: "ns"}
			vars := map[string]any{"z-var": "v", "my-var": "v", "ok_name": "v"}
			provider := &SliceProvider{}
			if source == "inline" {
				ks.PostBuild = &kustomizev1.PostBuild{Substitute: map[string]string{"z-var": "v", "my-var": "v", "ok_name": "v"}}
			} else {
				ks.PostBuild = &kustomizev1.PostBuild{SubstituteFrom: []manifest.SubstituteReference{{Kind: manifest.KindConfigMap, Name: "cm"}}}
				provider.ConfigMaps = []*manifest.ConfigMap{{Name: "cm", Namespace: "ns", Data: vars}}
			}

			before := ks.Clone()
			err := ExpandPostBuildSubstituteReference(ks, provider)
			assert.Diff(t, ks.PostBuild.Substitute, before.PostBuild.Substitute)
			if !errors.Is(err, manifest.ErrInvalidSubstituteReference) {
				t.Fatalf("expected invalid substitute reference, got %v", err)
			}
			if !strings.Contains(err.Error(), "[my-var z-var]") {
				t.Errorf("error should name invalid vars in sorted order; got %v", err)
			}
		})
	}
}

func TestStoreProvider_LookupReadability(t *testing.T) {
	t.Parallel()
	s := store.New()
	for _, secret := range []*manifest.Secret{
		{Name: "sops-vars", Namespace: "apps", StringData: map[string]any{"Y": manifest.ValuePlaceholder("Y")}},
		{Name: "real", Namespace: "apps", StringData: map[string]any{"Z": "z"}},
		{Name: "empty", Namespace: "apps"},
		{Name: "empty-value", Namespace: "apps", StringData: map[string]any{"Z": ""}},
	} {
		s.AddObject(secret)
	}
	for _, tc := range []struct {
		name       string
		unreadable bool
	}{
		{"absent-vars", true}, {"empty", true}, {"empty-value", false}, {"sops-vars", false}, {"real", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var reads store.ReadSet
			reads.Init(s, manifest.NamedResource{Kind: manifest.KindKustomization, Namespace: "apps", Name: "reader"}, nil)
			provider := NewStoreProvider(&reads, true, nil)
			id := manifest.NamedResource{Kind: manifest.KindSecret, Namespace: "apps", Name: tc.name}
			input := provider.Lookup(id)
			unreadable := input.Data == nil || !input.Data.HasKeys()
			if unreadable != tc.unreadable {
				t.Fatalf("unreadable=%t, want %t", unreadable, tc.unreadable)
			}
			if provider.Data(id) != input.Data {
				t.Fatal("expansion read a different captured value")
			}
		})
	}
}

func TestReplaceValueAtPath_SingleCharQuoteNoPanic(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct{ value, want string }{{"'", "'"}, {`"`, `"`}, {"''", ""}} {
		t.Run(tc.value, func(t *testing.T) {
			t.Parallel()
			values := map[string]any{}
			assert.NoError(t, replaceValueAtPath(values, "leaf", tc.value))
			assert.Equal(t, values["leaf"], any(tc.want))
		})
	}
}

func TestReplaceValueAtPath_TypeCoercion(t *testing.T) {
	t.Parallel()
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
			t.Parallel()
			got := map[string]any{}
			err := replaceValueAtPath(got, tc.path, tc.val)
			if err != nil {
				t.Fatalf("replaceValueAtPath: %v", err)
			}
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

func TestDeepMergeInto_MatchesHelmMerge(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name           string
		base, override map[string]any
	}{
		{"nil-inputs", nil, nil},
		{"nil-base", nil, map[string]any{"m": map[string]any{"x": 1.0}}},
		{"empty-override", map[string]any{"m": map[string]any{"x": 1.0}}, map[string]any{}},
		{"disjoint", map[string]any{"a": 1.0}, map[string]any{"b": 2.0}},
		{"scalar-over-scalar", map[string]any{"a": 1.0}, map[string]any{"a": 2.0}},
		{"nested-collision", map[string]any{"m": map[string]any{"x": 1.0, "y": 2.0}}, map[string]any{"m": map[string]any{"y": 9.0, "z": 3.0}}},
		{"deep-collision", map[string]any{"m": map[string]any{"n": map[string]any{"x": 1.0}}}, map[string]any{"m": map[string]any{"n": map[string]any{"y": 2.0}}}},
		{"list-map-replace", map[string]any{"l": []any{map[string]any{"x": 1.0}}}, map[string]any{"l": []any{map[string]any{"y": 2.0}}}},
		{"list-replace", map[string]any{"l": []any{1.0, 2.0}}, map[string]any{"l": []any{3.0}}},
		{"scalar-over-map", map[string]any{"k": map[string]any{"x": 1.0}}, map[string]any{"k": "scalar"}},
		{"map-over-scalar", map[string]any{"k": "scalar"}, map[string]any{"k": map[string]any{"x": 1.0}}},
		{"nil-override", map[string]any{"k": map[string]any{"x": 1.0}}, map[string]any{"k": nil}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			baseBefore := manifest.DeepCopyMap(tc.base)
			overrideBefore := manifest.DeepCopyMap(tc.override)
			want := loader.MergeMaps(tc.base, tc.override)
			got := DeepMergeInto(maps.Clone(tc.base), tc.override)
			assert.Diff(t, got, want)
			assert.Diff(t, tc.base, baseBefore)
			assert.Diff(t, tc.override, overrideBefore)
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

func TestExpandValueReferences_ConcurrentSharedCache(t *testing.T) {
	t.Parallel()
	cm := nestedValuesCM()
	provider := &SliceProvider{ConfigMaps: []*manifest.ConfigMap{cm, {
		Name: "inject", Namespace: "default", Data: map[string]any{"v": "Always"},
	}}}
	cache := NewCache()
	prior := wholeDocHR()
	expandValuesForTest(t, prior, provider, cache)
	before := manifest.DeepCopyMap(prior.Values)
	canonical, ok := cache.lookup(cm.Data["values.yaml"].(string))
	if !ok {
		t.Fatal("missing cached document")
	}
	cases := []struct {
		name    string
		refs    []manifest.ValuesReference
		inline  map[string]any
		want    map[string]any
		wantErr bool
	}{
		{name: "ordinary", inline: map[string]any{"image": map[string]any{"tag": "inline"}},
			want: map[string]any{"image": map[string]any{"repository": "nginx", "tag": "inline"}, "env": []any{map[string]any{"name": "A", "value": "1"}}}},
		{name: "target map", refs: []manifest.ValuesReference{{Kind: "ConfigMap", Name: "inject", ValuesKey: "v", TargetPath: "image.tag"}},
			want: map[string]any{"image": map[string]any{"repository": "nginx", "tag": "Always"}, "env": []any{map[string]any{"name": "A", "value": "1"}}}},
		{name: "target list map", refs: []manifest.ValuesReference{{Kind: "ConfigMap", Name: "inject", ValuesKey: "v", TargetPath: "env[0].value"}},
			want: map[string]any{"image": map[string]any{"repository": "nginx", "tag": "v1"}, "env": []any{map[string]any{"name": "A", "value": "Always"}}}},
		{name: "target list element", refs: []manifest.ValuesReference{{Kind: "ConfigMap", Name: "inject", ValuesKey: "v", TargetPath: "env[0]"}},
			want: map[string]any{"image": map[string]any{"repository": "nginx", "tag": "v1"}, "env": []any{"Always"}}},
		{name: "yaml between writes", refs: []manifest.ValuesReference{
			{Kind: "ConfigMap", Name: "inject", ValuesKey: "v", TargetPath: "env[0].name", Literal: true},
			{Kind: "ConfigMap", Name: "platform"},
			{Kind: "ConfigMap", Name: "inject", ValuesKey: "v", TargetPath: "env[0].value"},
		}, want: map[string]any{"image": map[string]any{"repository": "nginx", "tag": "v1"}, "env": []any{map[string]any{"name": "A", "value": "Always"}}}},
		{name: "partial ordinary failure", refs: []manifest.ValuesReference{{Kind: "ConfigMap", Name: "inject", ValuesKey: "v", TargetPath: "env[0].value=changed,broken[bad]"}},
			inline: map[string]any{"untouched": true}, want: map[string]any{"untouched": true}, wantErr: true},
		{name: "partial literal failure", refs: []manifest.ValuesReference{{Kind: "ConfigMap", Name: "inject", ValuesKey: "v", TargetPath: "env[0].value=changed,broken[bad]", Literal: true}},
			inline: map[string]any{"untouched": true}, want: map[string]any{"untouched": true}, wantErr: true},
	}
	start := make(chan struct{})
	var ready, done sync.WaitGroup
	ready.Add(len(cases))
	for _, tc := range cases {
		done.Go(func() {
			hr := wholeDocHR()
			hr.ValuesFrom = append(hr.ValuesFrom, tc.refs...)
			hr.Values = tc.inline
			inlineBefore := manifest.DeepCopyMap(tc.inline)
			ready.Done()
			<-start
			_, err := ExpandValueReferences(hr, provider, cache)
			if (err != nil) != tc.wantErr {
				t.Errorf("%s: error = %v, want error %v", tc.name, err, tc.wantErr)
				return
			}
			assert.Diff(t, hr.Values, tc.want)
			assert.Diff(t, tc.inline, inlineBefore)
		})
	}
	ready.Wait()
	close(start)
	done.Wait()
	assert.Diff(t, canonical, before)
	assert.Diff(t, prior.Values, before)
	after := wholeDocHR()
	expandValuesForTest(t, after, provider, cache)
	assert.Diff(t, after.Values, before)
}

func TestExpandValueReferences_TargetPathDoesNotCorruptSharedCache(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		literal, cached bool
	}{{false, false}, {false, true}, {true, false}, {true, true}} {
		t.Run(fmt.Sprintf("literal=%t/cache=%t", tc.literal, tc.cached), func(t *testing.T) {
			t.Parallel()
			var cache *Cache
			if tc.cached {
				cache = NewCache()
			}
			hr := wholeDocHR()
			cm := nestedValuesCM()
			provider := &SliceProvider{ConfigMaps: []*manifest.ConfigMap{cm, {
				Name: "inject", Namespace: "default", Data: map[string]any{"v": "Always"},
			}}}
			expandValuesForTest(t, hr, provider, cache)
			before := manifest.DeepCopyMap(hr.Values)
			hr.ValuesFrom = append(hr.ValuesFrom, manifest.ValuesReference{
				Kind: "ConfigMap", Name: "inject", ValuesKey: "v",
				TargetPath: "env[0].value=changed,broken[bad]", Literal: tc.literal,
			})
			_, err := ExpandValueReferences(hr, provider, cache)
			if err == nil {
				t.Fatal("malformed target path succeeded")
			}
			assert.Diff(t, hr.Values, before)
			after := wholeDocHR()
			expandValuesForTest(t, after, provider, cache)
			assert.Diff(t, after.Values, before)
			if tc.cached {
				canonical, _ := cache.lookup(cm.Data["values.yaml"].(string))
				assert.Diff(t, canonical, before)
			}
		})
	}
}

func TestExpandValueReferences_InterleavedTargetPaths(t *testing.T) {
	t.Parallel()
	for _, literal := range []bool{false, true} {
		t.Run(fmt.Sprintf("literal=%t", literal), func(t *testing.T) {
			t.Parallel()
			first := nestedValuesCM()
			second := &manifest.ConfigMap{Name: "later", Namespace: "default", Data: map[string]any{
				"values.yaml": "later:\n  image:\n    tag: v2\n  env:\n    - name: B\n      value: two\n",
			}}
			provider := &SliceProvider{ConfigMaps: []*manifest.ConfigMap{first, second, {
				Name: "inject", Namespace: "default", Data: map[string]any{"v": "Always"},
			}}}
			cache := NewCache()
			priorFirst := wholeDocHR()
			priorSecond := &manifest.HelmRelease{Name: "prior", Namespace: "default", ValuesFrom: []manifest.ValuesReference{{Kind: "ConfigMap", Name: second.Name}}}
			expandValuesForTest(t, priorFirst, provider, cache)
			expandValuesForTest(t, priorSecond, provider, cache)
			firstBefore := manifest.DeepCopyMap(priorFirst.Values)
			secondBefore := manifest.DeepCopyMap(priorSecond.Values)
			hr := wholeDocHR()
			hr.Values = map[string]any{"image": map[string]any{"tag": "inline"}}
			hr.ValuesFrom = append(hr.ValuesFrom,
				manifest.ValuesReference{Kind: "ConfigMap", Name: "inject", ValuesKey: "v", TargetPath: "image.pullPolicy"},
				manifest.ValuesReference{Kind: "ConfigMap", Name: "inject", ValuesKey: "v", TargetPath: "env[0].value"},
				manifest.ValuesReference{Kind: "ConfigMap", Name: "missing", Optional: true},
				manifest.ValuesReference{Kind: "ConfigMap", Name: second.Name},
				manifest.ValuesReference{Kind: "ConfigMap", Name: "inject", ValuesKey: "v", TargetPath: "later.image.tag", Literal: literal},
				manifest.ValuesReference{Kind: "ConfigMap", Name: "inject", ValuesKey: "v", TargetPath: "later.env[0].value", Literal: literal},
			)
			expandValuesForTest(t, hr, provider, cache)
			assert.Diff(t, hr.Values, map[string]any{
				"image": map[string]any{"repository": "nginx", "tag": "inline", "pullPolicy": "Always"},
				"env":   []any{map[string]any{"name": "A", "value": "Always"}},
				"later": map[string]any{
					"image": map[string]any{"tag": "Always"},
					"env":   []any{map[string]any{"name": "B", "value": "Always"}},
				},
			})
			assert.Diff(t, priorFirst.Values, firstBefore)
			assert.Diff(t, priorSecond.Values, secondBefore)
			for _, document := range []struct {
				cm   *manifest.ConfigMap
				want map[string]any
			}{{first, firstBefore}, {second, secondBefore}} {
				canonical, ok := cache.lookup(document.cm.Data["values.yaml"].(string))
				if !ok {
					t.Fatal("missing cached document")
				}
				assert.Diff(t, canonical, document.want)
			}
		})
	}
}

// TestExpandValueReferences_YAMLContainingPlaceholderIsMerged verifies that
// a ConfigMap whose values.yaml contains placeholder tokens inside valid YAML
// values (e.g. from postBuild substitution on a configMapGenerator) is correctly
// parsed and merged into hr.Values instead of being discarded.
func TestExpandValueReferences_YAMLContainingPlaceholderIsMerged(t *testing.T) {
	t.Parallel()
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
	if _, err := ExpandValueReferences(hr, provider, nil); err != nil {
		t.Fatalf("ExpandValueReferences failed: %v", err)
	}
	if hr.Values["domain"] != "..PLACEHOLDER_DOMAIN.." {
		t.Errorf("domain = %v, want ..PLACEHOLDER_DOMAIN..", hr.Values["domain"])
	}
	if fmt.Sprint(hr.Values["port"]) != "8080" {
		t.Errorf("port = %v, want 8080", hr.Values["port"])
	}
}

func TestExpandValueReferences_WipedScalarPlaceholderTolerated(t *testing.T) {
	t.Parallel()
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
	omitted, err := ExpandValueReferences(hr, provider, nil)
	if err != nil {
		t.Fatalf("ExpandValueReferences failed for wiped scalar placeholder: %v", err)
	}
	assert.Diff(t, omitted, []manifest.NamedResource{sec.Named()})
	if len(hr.Values) != 0 {
		t.Errorf("expected empty values, got %v", hr.Values)
	}
}

func TestExpandValueReferences_MalformedYAMLClassified(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name, document string
		typeError      bool
	}{
		{"syntax", "a: [", false},
		{"document type", "- name: demo", true},
	} {
		for _, optional := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/optional=%t", tc.name, optional), func(t *testing.T) {
				t.Parallel()
				st := store.New()
				st.AddObject(&manifest.ConfigMap{Name: "extra", Namespace: "default", Data: map[string]any{"requested.yaml": tc.document}})
				hr := &manifest.HelmRelease{Name: "demo", Namespace: "default", ValuesFrom: []manifest.ValuesReference{{Kind: manifest.KindConfigMap, Name: "extra", ValuesKey: "requested.yaml", Optional: optional}}}
				_, err := ExpandValueReferences(hr, NewStoreProvider(st, true, nil), NewCache())
				if !errors.Is(err, manifest.ErrInput) || !errors.Is(err, manifest.ErrFlux) {
					t.Fatalf("malformed values must retain input/domain identity: %v", err)
				}
				for _, detail := range []string{"HelmRelease default/demo", "extra", "requested.yaml"} {
					if !strings.Contains(err.Error(), detail) {
						t.Errorf("error lacks %q: %v", detail, err)
					}
				}
				wrapped, ok := errors.AsType[interface {
					error
					Unwrap() []error
				}](err)
				if !ok {
					t.Fatalf("parser cause is not wrapped beside ErrInput: %v", err)
				}
				causes := wrapped.Unwrap()
				var decoded map[string]any
				parseErr := yaml.Unmarshal([]byte(tc.document), &decoded)
				if len(causes) != 2 || parseErr == nil || causes[1].Error() != parseErr.Error() {
					t.Fatalf("parser cause changed: %v", err)
				}
				if tc.typeError {
					if _, ok := errors.AsType[*json.UnmarshalTypeError](err); !ok {
						t.Fatalf("typed parser cause lost: %v", err)
					}
				}
			})
		}
	}
}

func TestExpandValueReferences_MalformedTargetPathClassified(t *testing.T) {
	t.Parallel()
	for _, literal := range []bool{false, true} {
		t.Run(fmt.Sprintf("literal=%t", literal), func(t *testing.T) {
			t.Parallel()
			const path = "env[bad].value"
			cm := &manifest.ConfigMap{Name: "extra", Namespace: "default", Data: map[string]any{"value": "replacement"}}
			hr := &manifest.HelmRelease{Name: "demo", Namespace: "default", Values: map[string]any{"retained": "yes"}, ValuesFrom: []manifest.ValuesReference{{Kind: manifest.KindConfigMap, Name: cm.Name, ValuesKey: "value", TargetPath: path, Literal: literal, Optional: true}}}
			_, err := ExpandValueReferences(hr, &SliceProvider{ConfigMaps: []*manifest.ConfigMap{cm}}, nil)
			if !errors.Is(err, manifest.ErrInput) || !errors.Is(err, manifest.ErrFlux) || !errors.Is(err, strconv.ErrSyntax) {
				t.Fatalf("target parser identities lost: %v", err)
			}
			cause, ok := errors.AsType[*strconv.NumError](err)
			if !ok || cause.Num != "bad" {
				t.Fatalf("original index parser cause lost: %v", err)
			}
			for _, detail := range []string{"HelmRelease default/demo", `targetPath "` + path + `"`} {
				if !strings.Contains(err.Error(), detail) {
					t.Errorf("error lacks %q: %v", detail, err)
				}
			}
			assert.Diff(t, hr.Values, map[string]any{"retained": "yes"})
		})
	}
}

type countedDataReader struct {
	*store.ReadSet
	objects, productions int
}

func (r *countedDataReader) ReferenceFacts(id manifest.NamedResource, index *manifest.DiscoveryIndex) store.ReferenceFacts {
	r.objects++
	r.productions++
	return r.ReadSet.ReferenceFacts(id, index)
}

func TestStoreProvider_LookupRetainsOneSource(t *testing.T) {
	t.Parallel()
	s := store.New()
	first := &manifest.ConfigMap{Name: "settings", Namespace: "apps", Data: map[string]any{"VALUE": "first"}}
	s.AddObject(first)
	owner := manifest.NamedResource{Group: manifest.FluxKustomizeDomain, Kind: manifest.KindKustomization, Namespace: "apps", Name: "producer"}
	s.SetDiscoveryIndex(&manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{first.Named(): {Productions: []manifest.Production{{Owner: owner, Path: "settings.yaml", Authored: first.Named()}}}}})
	s.AddObject(&manifest.Kustomization{Name: owner.Name, Namespace: owner.Namespace})
	s.RecordEmitted(owner, &store.Emission{Children: []store.Child{{Obj: first}}, Complete: true})
	var reads store.ReadSet
	reader := manifest.NamedResource{Group: manifest.FluxKustomizeDomain, Kind: manifest.KindKustomization, Namespace: "apps", Name: "reader"}
	reads.Init(s, reader, nil)
	counted := &countedDataReader{ReadSet: &reads}
	provider := NewStoreProvider(counted, true, nil)
	input := provider.Lookup(first.Named())
	if input.Data != first || !input.Offline || len(input.Materializers) != 0 {
		t.Fatalf("captured input=%+v", input)
	}
	next := *first
	next.Data = map[string]any{"VALUE": "next"}
	s.RecordEmitted(owner, &store.Emission{Children: []store.Child{{Obj: &next}}, Complete: true})
	if provider.Data(first.Named()) != first || provider.Lookup(first.Named()).Data != first || counted.objects != 1 || counted.productions != 1 {
		t.Fatalf("expansion rescanned captured input: object=%d productions=%d", counted.objects, counted.productions)
	}
	if reads.Valid() {
		t.Fatal("changed consumed data did not invalidate execution")
	}
	var fresh store.ReadSet
	fresh.Init(s, reader, nil)
	if got := NewStoreProvider(&fresh, true, nil).Data(first.Named()); got != &next {
		t.Fatalf("next execution retained preceding value: %v", got)
	}
}

func TestReferenceFacts_DataOnlyProviderPolicy(t *testing.T) {
	t.Parallel()
	st := store.New()
	id := manifest.NamedResource{Kind: manifest.KindSecret, Name: "input", Namespace: "apps"}
	raw := &manifest.Secret{Name: id.Name, Namespace: id.Namespace, StringData: map[string]any{"VALUE": "visible"}}
	owner := manifest.NamedResource{Group: manifest.FluxKustomizeDomain, Kind: manifest.KindKustomization, Name: "owner", Namespace: "apps"}
	st.SetDiscoveryIndex(&manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{id: {Productions: []manifest.Production{{Owner: owner}}}}})
	st.AddObject(raw)
	facts := st.ReferenceFacts(id, nil)
	if facts.Data != raw || facts.Accepted {
		t.Fatalf("covered facts=%+v", facts)
	}
	if NewStoreProvider(st, true, nil).Data(id) != raw {
		t.Fatal("data-only provider rejected visible prediction")
	}
	first, second := facts.Productions, facts.Productions
	assert.Diff(t, second, first)
	assert.Diff(t, st.Productions(id, nil), first)
	visits := 0
	for range facts.Productions {
		visits++
		break
	}
	assert.Equal(t, visits, 1)
}
