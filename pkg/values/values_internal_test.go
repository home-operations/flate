package values

import (
	"errors"
	"fmt"
	"strings"
	"testing"

	kustomizev1 "github.com/fluxcd/kustomize-controller/api/v1"

	"github.com/home-operations/flate/internal/assert"

	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

type sliceProvider struct {
	ConfigMaps []*manifest.ConfigMap
	Secrets    []*manifest.Secret
}

// Data finds data by its full identity.
//
//nolint:ireturn // The provider contract returns both ConfigMap and Secret data.
func (p *sliceProvider) Data(id manifest.NamedResource) manifest.Data {
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
	{
		assert.ErrorIf(t, bb["x"] != 1 || bb["y"] != 99 || bb["z"] != 3, "nested merge wrong: %v", bb)
	}
	dst["sentinel"] = "v"
	{
		assert.ErrorIf(t, result["sentinel"] != "v", "%s", "DeepMergeInto must return dst itself, got distinct map")
	}
	{
		assert.ErrorIf(t, dst["a"] != 2 || dst["b"].(map[string]any)["y"] != 99, "%s", "dst not mutated as expected")
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
	st.AddObject(
		&manifest.RawObject{
			APIVersion: "example.test/v1",
			Kind:       manifest.KindConfigMap,
			Name:       cm.Name,
			Namespace:  cm.Namespace,
		},
	)
	provider := NewStoreProvider(st, true)
	hr := &manifest.HelmRelease{
		Name: "demo", Namespace: "default",
		ValuesFrom: []manifest.ValuesReference{{Kind: "ConfigMap", Name: "extra"}},
		Values:     map[string]any{"image": map[string]any{"repository": "x"}},
	}
	{
		_, err := ExpandValueReferences(hr, provider, nil)
		assert.FatalIf(t, err != nil, "ExpandValueReferences: %v", err)
	}
	if hr.Values["replicaCount"] != float64(5) {
		t.Errorf("replicaCount: %v", hr.Values["replicaCount"])
	}
	img := hr.Values["image"].(map[string]any)
	{
		assert.ErrorIf(t, img["tag"] != "v2" || img["repository"] != "x", "image merge wrong: %+v", img)
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
			hr := &manifest.HelmRelease{
				Name:       "demo",
				Namespace:  "default",
				ValuesFrom: []manifest.ValuesReference{{Kind: obj.Named().Kind, Name: "extra"}},
			}
			expandValuesForTest(t, hr, NewStoreProvider(st, true), nil)
			{
				got := hr.Values["replicaCount"]
				assert.ErrorIf(t, fmt.Sprint(got) != "3", "replicaCount = %v, want 3", got)
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
				hr := &manifest.HelmRelease{
					Name:       "demo",
					Namespace:  "default",
					ValuesFrom: []manifest.ValuesReference{{Kind: obj.Named().Kind, Name: "extra", Optional: optional}},
				}
				_, err := ExpandValueReferences(hr, NewStoreProvider(st, true), nil)
				{
					assert.FatalIf(
						t,
						!errors.Is(err, manifest.ErrInput) || !strings.Contains(err.Error(), "data[values.yaml]"),
						"malformed requested key must fail with its name: %v",
						err,
					)
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
	provider := &sliceProvider{ConfigMaps: []*manifest.ConfigMap{cm}}
	hr := &manifest.HelmRelease{
		Name: "demo", Namespace: "default",
		ValuesFrom: []manifest.ValuesReference{
			{Kind: "ConfigMap", Name: "k", ValuesKey: "v", TargetPath: "auth.password"},
		},
	}
	{
		_, err := ExpandValueReferences(hr, provider, nil)
		assert.FatalIf(t, err != nil, "ExpandValueReferences: %v", err)
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
	{
		_, err := ExpandValueReferences(hr, &sliceProvider{ConfigMaps: []*manifest.ConfigMap{cm}}, nil)
		assert.FatalIf(t, err != nil, "ExpandValueReferences: %v", err)
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
	{
		_, err := ExpandValueReferences(hr, &sliceProvider{ConfigMaps: []*manifest.ConfigMap{cm}}, nil)
		assert.FatalIf(t, err != nil, "ExpandValueReferences: %v", err)
	}
	annotations := hr.Values["annotations"].(map[string]any)
	{
		got := annotations["prometheus.io/scrape"]
		assert.ErrorIf(
			t,
			got != "true,false",
			`annotations = %#v, want key "prometheus.io/scrape" = "true,false"`,
			annotations,
		)
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
	provider := &sliceProvider{}
	{
		_, err := ExpandValueReferences(hr, provider, nil)
		assert.FatalIf(t, err != nil, "ExpandValueReferences: %v", err)
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
				testExpandValueReferencesMissingReferencesCase(
					t,
					&testExpandValueReferencesMissingReferencesCaseInputs{tc: &tc, optional: &optional},
				)
			},
			)
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

	{
		_, err := ExpandValueReferences(hr, &sliceProvider{ConfigMaps: []*manifest.ConfigMap{cm}}, nil)
		assert.FatalIf(t, err != nil, "ExpandValueReferences: %v", err)
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
	provider := &sliceProvider{ConfigMaps: []*manifest.ConfigMap{cm}}
	cache := &Cache{}
	hr := func() *manifest.HelmRelease {
		return &manifest.HelmRelease{
			Name: "demo", Namespace: "default",
			ValuesFrom: []manifest.ValuesReference{{Kind: "ConfigMap", Name: "platform"}},
		}
	}

	a := hr()
	{
		_, err := ExpandValueReferences(a, provider, cache)
		assert.FatalIf(t, err != nil, "first ExpandValueReferences: %v", err)
	}
	if a.Values["replicaCount"] != float64(5) {
		t.Fatalf("first: replicaCount=%v", a.Values["replicaCount"])
	}

	other := &manifest.ConfigMap{
		Name: "other", Namespace: "apps",
		Data: map[string]any{"extra.yaml": cm.Data["values.yaml"]},
	}
	provider.ConfigMaps = append(provider.ConfigMaps, other)
	b := hr()
	b.Namespace = other.Namespace
	b.ValuesFrom = []manifest.ValuesReference{{Kind: "ConfigMap", Name: other.Name, ValuesKey: "extra.yaml"}}
	{
		_, err := ExpandValueReferences(b, provider, cache)
		assert.FatalIf(t, err != nil, "second ExpandValueReferences: %v", err)
	}
	if b.Values["replicaCount"] != float64(5) {
		t.Errorf("second: replicaCount=%v", b.Values["replicaCount"])
	}

	entries := 0
	cache.m.Range(func(_, _ any) bool { entries++; return true })
	{
		assert.FatalIf(t, entries != 1, "identical content cached %d times, want once", entries)
	}

	// Top-level writes must not reach the shared cache canonical.
	a.Values["replicaCount"] = "stomped"
	c := hr()
	{
		_, err := ExpandValueReferences(c, provider, cache)
		assert.FatalIf(t, err != nil, "third ExpandValueReferences: %v", err)
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
	provider := &sliceProvider{ConfigMaps: []*manifest.ConfigMap{cm}}
	cache := &Cache{}
	mk := func() *manifest.HelmRelease {
		return &manifest.HelmRelease{
			Name: "demo", Namespace: "default",
			ValuesFrom: []manifest.ValuesReference{{Kind: "ConfigMap", Name: "platform"}},
		}
	}

	a := mk()
	{
		_, err := ExpandValueReferences(a, provider, cache)
		assert.FatalIf(t, err != nil, "first: %v", err)
	}
	if a.Values["replicaCount"] != float64(5) {
		t.Fatalf("first replicaCount=%v", a.Values["replicaCount"])
	}

	cm.Data["values.yaml"] = "replicaCount: 99\n"

	b := mk()
	{
		_, err := ExpandValueReferences(b, provider, cache)
		assert.FatalIf(t, err != nil, "second: %v", err)
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
	provider := &sliceProvider{ConfigMaps: []*manifest.ConfigMap{cm}}
	hr := &manifest.HelmRelease{
		Name: "demo", Namespace: "default",
		ValuesFrom: []manifest.ValuesReference{{Kind: "ConfigMap", Name: "extra"}},
	}
	{
		_, err := ExpandValueReferences(hr, provider, nil)
		assert.FatalIf(t, err != nil, "ExpandValueReferences nil cache: %v", err)
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
	st.AddObject(
		&manifest.RawObject{
			APIVersion: "example.test/v1",
			Kind:       manifest.KindConfigMap,
			Name:       cm.Name,
			Namespace:  cm.Namespace,
		},
	)
	st.AddObject(
		&manifest.Secret{Name: "secret-vars", Namespace: cm.Namespace, StringData: map[string]any{"TOKEN": "core"}},
	)
	st.AddObject(
		&manifest.RawObject{
			APIVersion: "example.test/v1",
			Kind:       manifest.KindSecret,
			Name:       "secret-vars",
			Namespace:  cm.Namespace,
		},
	)
	provider := NewStoreProvider(st, true)
	ks := &manifest.Kustomization{
		Name: "apps", Namespace: "flux-system",

		PostBuild: &kustomizev1.PostBuild{SubstituteFrom: []manifest.SubstituteReference{
			{Kind: "Unsupported", Name: "vars"},
			{Kind: "ConfigMap", Name: "vars"},
			{Kind: "Secret", Name: "secret-vars"},
		}},
	}
	postBuild := ks.PostBuild
	{
		err := ExpandPostBuildSubstituteReference(ks, provider)
		assert.FatalIf(t, err != nil, "ExpandPostBuildSubstituteReference: %v", err)
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
			provider := &sliceProvider{
				ConfigMaps: []*manifest.ConfigMap{
					{Name: "vars", Namespace: "ns", Data: map[string]any{"INVALID": []any{"nested"}}},
				},
				Secrets: []*manifest.Secret{
					{Name: "vars", Namespace: "ns", Data: map[string]any{"INVALID": "invalid-base64"}},
				},
			}
			ks := &manifest.Kustomization{
				Name:      "app",
				Namespace: "ns",
				PostBuild: &kustomizev1.PostBuild{
					Substitute:     map[string]string{"INLINE": "kept"},
					SubstituteFrom: []manifest.SubstituteReference{{Kind: kind, Name: "vars"}},
				},
			}
			{
				err := ExpandPostBuildSubstituteReference(ks, provider)
				assert.FatalIf(t, !errors.Is(err, manifest.ErrInput), "got %v, want ErrInput", err)
			}
			assert.Diff(t, ks.PostBuild.Substitute, map[string]string{"INLINE": "kept"})
		})
	}
}

func TestExpandPostBuildSubstituteReference_MalformedKeysDeterministic(t *testing.T) {
	t.Parallel()
	sec := &manifest.Secret{Name: "vars", Namespace: "ns", Data: map[string]any{"C": "!", "A": "!", "B": "!"}}
	provider := &sliceProvider{Secrets: []*manifest.Secret{sec}}
	ks := &manifest.Kustomization{
		Name:      "app",
		Namespace: "ns",
		PostBuild: &kustomizev1.PostBuild{
			SubstituteFrom: []manifest.SubstituteReference{{Kind: manifest.KindSecret, Name: "vars"}},
		},
	}
	_, _, want := sec.Value("A")
	for range 50 {
		err := ExpandPostBuildSubstituteReference(ks, provider)
		{
			assert.FatalIf(t, err == nil || err.Error() != want.Error(), "got %v, want %v", err, want)
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
			provider := &sliceProvider{}
			if source == "inline" {
				ks.PostBuild = &kustomizev1.PostBuild{
					Substitute: map[string]string{"z-var": "v", "my-var": "v", "ok_name": "v"},
				}
			} else {
				ks.PostBuild = &kustomizev1.PostBuild{
					SubstituteFrom: []manifest.SubstituteReference{{Kind: manifest.KindConfigMap, Name: "cm"}},
				}
				provider.ConfigMaps = []*manifest.ConfigMap{{Name: "cm", Namespace: "ns", Data: vars}}
			}

			before := ks.Clone()
			err := ExpandPostBuildSubstituteReference(ks, provider)
			assert.Diff(t, ks.PostBuild.Substitute, before.PostBuild.Substitute)
			{
				assert.FatalIf(
					t,
					!errors.Is(err, manifest.ErrInvalidSubstituteReference),
					"expected invalid substitute reference, got %v",
					err,
				)
			}
			{
				assert.ErrorIf(
					t,
					err.Error() != "invalid substitute variables my-var, z-var",
					"error should name invalid vars in sorted order; got %v",
					err,
				)
			}
		})
	}
}

func TestExpandPostBuildSubstituteReference_InvalidSingleName(t *testing.T) {
	t.Parallel()
	ks := &manifest.Kustomization{
		Name:      "k",
		Namespace: "ns",
		PostBuild: &kustomizev1.PostBuild{Substitute: map[string]string{"flate-campaign": "v"}},
	}
	before := ks.Clone()
	err := ExpandPostBuildSubstituteReference(ks, &sliceProvider{})
	assert.Diff(t, ks.PostBuild.Substitute, before.PostBuild.Substitute)
	{
		assert.FatalIf(t, err == nil || err.Error() != "invalid substitute variable flate-campaign" ||
			!errors.Is(err, manifest.ErrInvalidSubstituteReference) ||
			!errors.Is(err, manifest.ErrFlux), "invalid variable: %v", err)
	}
	{
		assert.FatalIf(
			t,
			!strings.Contains(manifest.ErrorDetails(err), varsubRegex.String()),
			"%s",
			"lost validator detail",
		)
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
			reads.Init(
				s,
				manifest.NamedResource{Kind: manifest.KindKustomization, Namespace: "apps", Name: "reader"},
				nil,
			)
			provider := NewStoreProvider(&reads, true)
			id := manifest.NamedResource{Kind: manifest.KindSecret, Namespace: "apps", Name: tc.name}
			input := provider.Lookup(id)
			unreadable := input.Data == nil || !input.Data.HasKeys()
			{
				assert.FatalIf(t, unreadable != tc.unreadable, "unreadable=%t, want %t", unreadable, tc.unreadable)
			}
			{
				assert.FatalIf(t, provider.Data(id) != input.Data, "%s", "expansion read a different captured value")
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

type testExpandValueReferencesMissingReferencesCaseInputs struct {
	tc *struct {
		name   string
		kind   string
		object manifest.BaseManifest
		want   error
	}
	optional *bool
}

func testExpandValueReferencesMissingReferencesCase(
	t *testing.T,
	testCaseInput *testExpandValueReferencesMissingReferencesCaseInputs,
) {
	t.Helper()

	st := store.New()
	if testCaseInput.tc.object != nil {
		st.AddObject(testCaseInput.tc.object)
	}
	hr := &manifest.HelmRelease{
		Name:      "demo",
		Namespace: "default",
		Values:    map[string]any{"existing": "kept"},
		ValuesFrom: []manifest.ValuesReference{
			{
				Kind:       testCaseInput.tc.kind,
				Name:       "vars",
				ValuesKey:  "v",
				TargetPath: "k",
				Optional:   (*testCaseInput.optional),
			},
		},
	}
	_, err := ExpandValueReferences(hr, NewStoreProvider(st, true), nil)
	if (*testCaseInput.optional) && testCaseInput.tc.kind != "Deployment" && testCaseInput.tc.want != nil {
		assert.NoError(t, err)
		assert.Diff(t, hr.Values, map[string]any{"existing": "kept"})
		return
	}
	if !errors.Is(err, testCaseInput.tc.want) {
		t.Fatalf("got %v, want %v", err, testCaseInput.tc.want)
	}
	if err == nil {
		assert.Diff(t, hr.Values, map[string]any{"existing": "kept", "k": ""})
		return
	}
	{
		assert.FatalIf(t, testCaseInput.tc.kind != "Deployment" &&
			strings.Contains(
				err.Error(),
				`key "v"`,
			) != errors.Is(testCaseInput.tc.want, manifest.ErrInvalidValuesReference), "wrong missing-resource/key disposition: %v", err)
	}
}
