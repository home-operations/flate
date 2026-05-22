package values

import (
	"slices"
	"testing"

	helmv2 "github.com/fluxcd/helm-controller/api/v2"

	"github.com/home-operations/flate/pkg/manifest"
)

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
	if err := ExpandValueReferences(hr, provider); err != nil {
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
	if err := ExpandValueReferences(hr, provider); err != nil {
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
		HelmReleaseSpec: helmv2.HelmReleaseSpec{
			ValuesFrom: []manifest.ValuesReference{
				{Kind: "ConfigMap", Name: "absent", ValuesKey: "v", TargetPath: "k", Optional: true},
			},
		},
	}
	provider := &SliceProvider{}
	if err := ExpandValueReferences(hr, provider); err != nil {
		t.Fatalf("ExpandValueReferences: %v", err)
	}
	if got, _ := hr.Values["k"].(string); got == "" {
		t.Errorf("expected placeholder, got %v", hr.Values["k"])
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

func TestSplitDottedPath(t *testing.T) {
	cases := []struct {
		in   string
		want []string
	}{
		{"a.b.c", []string{"a", "b", "c"}},
		{`a\.b.c`, []string{"a.b", "c"}},
		{"single", []string{"single"}},
	}
	for _, tc := range cases {
		t.Run(tc.in, func(t *testing.T) {
			if got := splitDottedPath(tc.in); !slices.Equal(got, tc.want) {
				t.Errorf("got %v, want %v", got, tc.want)
			}
		})
	}
}
