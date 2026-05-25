package manifest

import (
	"testing"

	helmv2 "github.com/fluxcd/helm-controller/api/v2"
	kustomizev1 "github.com/fluxcd/kustomize-controller/api/v1"
	"github.com/fluxcd/pkg/apis/kustomize"
)

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

// TestHelmRelease_Clone_DeepCopiesEmbeddedSpec pins the deep-copy of
// the embedded helmv2.HelmReleaseSpec pointer/slice fields. The pre-fix
// shallow `out := *h` aliased these to the canonical store-owned HR,
// so a future code path mutating, e.g., Install.DisableHooks on a Clone
// would corrupt the store. Iterate every pointer-shaped field the
// spec exposes — any one regressing breaks the immutability contract.
func TestHelmRelease_Clone_DeepCopiesEmbeddedSpec(t *testing.T) {
	src := &HelmRelease{
		Name: "plex", Namespace: "media",
		HelmReleaseSpec: helmv2.HelmReleaseSpec{
			Install: &helmv2.Install{DisableHooks: false},
			Upgrade: &helmv2.Upgrade{DisableHooks: false},
			Rollback: &helmv2.Rollback{DisableHooks: false},
			Uninstall: &helmv2.Uninstall{DisableHooks: false},
			Test: &helmv2.Test{Enable: false},
			DriftDetection: &helmv2.DriftDetection{
				Mode: helmv2.DriftDetectionWarn,
			},
			ValuesFrom: []helmv2.ValuesReference{
				{Kind: "ConfigMap", Name: "src-cm"},
			},
			DependsOn: []helmv2.DependencyReference{
				{Name: "src-dep"},
			},
		},
	}
	dst := src.Clone()
	dst.Install.DisableHooks = true
	dst.Upgrade.DisableHooks = true
	dst.Rollback.DisableHooks = true
	dst.Uninstall.DisableHooks = true
	dst.Test.Enable = true
	dst.DriftDetection.Mode = helmv2.DriftDetectionEnabled
	dst.ValuesFrom[0].Name = "MUTATED"
	dst.HelmReleaseSpec.DependsOn[0].Name = "MUTATED"

	if src.Install.DisableHooks {
		t.Errorf("source Install aliased after Clone mutation")
	}
	if src.Upgrade.DisableHooks {
		t.Errorf("source Upgrade aliased after Clone mutation")
	}
	if src.Rollback.DisableHooks {
		t.Errorf("source Rollback aliased after Clone mutation")
	}
	if src.Uninstall.DisableHooks {
		t.Errorf("source Uninstall aliased after Clone mutation")
	}
	if src.Test.Enable {
		t.Errorf("source Test aliased after Clone mutation")
	}
	if src.DriftDetection.Mode != helmv2.DriftDetectionWarn {
		t.Errorf("source DriftDetection aliased after Clone mutation")
	}
	if src.ValuesFrom[0].Name != "src-cm" {
		t.Errorf("source ValuesFrom aliased: %v", src.ValuesFrom[0].Name)
	}
	if src.HelmReleaseSpec.DependsOn[0].Name != "src-dep" {
		t.Errorf("source spec DependsOn aliased: %v", src.HelmReleaseSpec.DependsOn[0].Name)
	}
}

// TestKustomization_Clone_DeepCopiesEmbeddedSpec pins the deep-copy of
// the embedded kustomizev1.KustomizationSpec pointer/slice fields.
// Pre-fix the spec's PostBuild.Substitute map, Patches slice, Images
// slice etc. aliased the canonical store-owned Kustomization.
func TestKustomization_Clone_DeepCopiesEmbeddedSpec(t *testing.T) {
	src := &Kustomization{
		Name: "apps", Namespace: "flux-system",
		KustomizationSpec: kustomizev1.KustomizationSpec{
			PostBuild: &kustomizev1.PostBuild{
				Substitute: map[string]string{"K": "v"},
			},
			Patches: []kustomize.Patch{
				{Patch: "src-patch"},
			},
			Components: []string{"src-comp"},
			DependsOn: []kustomizev1.DependencyReference{
				{Name: "src-dep"},
			},
		},
	}
	dst := src.Clone()
	dst.PostBuild.Substitute["K"] = "MUTATED"
	dst.Patches[0].Patch = "MUTATED"
	dst.Components[0] = "MUTATED"
	// KustomizationSpec.DependsOn is shadowed by the flate-side
	// Kustomization.DependsOn ([]DependencyRef); access through the
	// embedded field name to disambiguate.
	dst.KustomizationSpec.DependsOn[0].Name = "MUTATED"

	if src.PostBuild.Substitute["K"] != "v" {
		t.Errorf("source PostBuild.Substitute aliased: %v", src.PostBuild.Substitute["K"])
	}
	if src.Patches[0].Patch != "src-patch" {
		t.Errorf("source Patches aliased: %v", src.Patches[0].Patch)
	}
	if src.Components[0] != "src-comp" {
		t.Errorf("source Components aliased: %v", src.Components[0])
	}
	if src.KustomizationSpec.DependsOn[0].Name != "src-dep" {
		t.Errorf("source spec DependsOn aliased: %v", src.KustomizationSpec.DependsOn[0].Name)
	}
}
