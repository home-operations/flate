package manifest

import "testing"

// DeepCopyMap must produce an independent tree — mutating the copy
// can't bleed into the original or vice versa. Used by Kustomization.
// Clone and HelmRelease.Clone to isolate reconcile-time mutations.
func TestDeepCopyMap_Isolation(t *testing.T) {
	src := map[string]any{
		"top": "scalar",
		"nested": map[string]any{
			"k": "v",
			"list": []any{
				map[string]any{"x": "y"},
				"z",
			},
		},
	}
	dst := DeepCopyMap(src)

	// Mutate the copy at every nesting level. Source should be intact.
	dst["top"] = "MUTATED"
	dst["nested"].(map[string]any)["k"] = "MUTATED"
	dst["nested"].(map[string]any)["list"].([]any)[0].(map[string]any)["x"] = "MUTATED"
	dst["nested"].(map[string]any)["list"].([]any)[1] = "MUTATED"

	if src["top"] != "scalar" {
		t.Errorf("source top leaked: %v", src["top"])
	}
	if src["nested"].(map[string]any)["k"] != "v" {
		t.Errorf("source nested leaked: %v", src["nested"])
	}
	if src["nested"].(map[string]any)["list"].([]any)[0].(map[string]any)["x"] != "y" {
		t.Errorf("source deep nested leaked")
	}
	if src["nested"].(map[string]any)["list"].([]any)[1] != "z" {
		t.Errorf("source list leaked")
	}
}

func TestDeepCopyMap_Nil(t *testing.T) {
	if DeepCopyMap(nil) != nil {
		t.Errorf("nil source should produce nil")
	}
}

// HelmRelease.Clone must produce a HR whose mutable fields don't alias
// the source — critical for the store-immutability contract.
func TestHelmRelease_Clone_Isolation(t *testing.T) {
	src := &HelmRelease{
		Name: "plex", Namespace: "media",
		Values:           map[string]any{"replicas": 1, "nested": map[string]any{"k": "v"}},
		ChartValuesFiles: []string{"values.yaml"},
	}
	dst := src.Clone()
	dst.Values["replicas"] = 99
	dst.Values["nested"].(map[string]any)["k"] = "MUTATED"
	dst.ChartValuesFiles[0] = "MUTATED"

	if src.Values["replicas"] != 1 {
		t.Errorf("source Values aliased: %v", src.Values["replicas"])
	}
	if src.Values["nested"].(map[string]any)["k"] != "v" {
		t.Errorf("source nested Values aliased: %v", src.Values["nested"])
	}
	if src.ChartValuesFiles[0] != "values.yaml" {
		t.Errorf("source ChartValuesFiles aliased: %v", src.ChartValuesFiles)
	}
}

// Kustomization.Clone must deep-copy Contents — the nested map that
// UpdatePostBuildSubstitutions walks and writes into.
func TestKustomization_Clone_Isolation(t *testing.T) {
	src := &Kustomization{
		Name: "apps", Namespace: "flux-system",
		PostBuildSubstitute: map[string]any{"K": "v"},
		Contents: map[string]any{
			"spec": map[string]any{
				"postBuild": map[string]any{
					"substitute": map[string]any{"X": "y"},
				},
			},
		},
	}
	dst := src.Clone()
	dst.PostBuildSubstitute["K"] = "MUTATED"
	dst.Contents["spec"].(map[string]any)["postBuild"].(map[string]any)["substitute"].(map[string]any)["X"] = "MUTATED"

	if src.PostBuildSubstitute["K"] != "v" {
		t.Errorf("source PostBuildSubstitute aliased")
	}
	srcSub := src.Contents["spec"].(map[string]any)["postBuild"].(map[string]any)["substitute"].(map[string]any)
	if srcSub["X"] != "y" {
		t.Errorf("source Contents aliased: %v", srcSub)
	}
}
