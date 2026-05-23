package values

import (
	"strings"
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
	parts := strings.Split(path, ".")
	for _, p := range parts {
		if idx := strings.IndexByte(p, '['); idx >= 0 {
			key := p[:idx]
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
