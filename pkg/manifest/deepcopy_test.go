package manifest

import (
	"slices"
	"testing"

	helmv2 "github.com/fluxcd/helm-controller/api/v2"
	kustomizev1 "github.com/fluxcd/kustomize-controller/api/v1"
	"github.com/fluxcd/pkg/apis/kustomize"

	"github.com/home-operations/flate/internal/assert"
)

// TestFillEmptyValueLeaves: empty-string and null leaves (nested, in slices)
// become ..PLACEHOLDER_<key>.. and are reported as sorted dotted paths;
// non-empty leaves (strings, numbers, bools, partial concatenations) are
// untouched; the source map is not mutated.
func TestFillEmptyValueLeaves(t *testing.T) {
	t.Parallel()
	in := map[string]any{
		"timeZone": "",
		"server":   nil,
		"replicas": 3,
		"enabled":  false,
		"url":      "https://x",         // non-empty
		"suffix":   ".cfargotunnel.com", // partial concatenation — non-empty
		"nested": map[string]any{
			"cronjob": map[string]any{"timeZone": ""},
			"list":    []any{"", "keep"},
		},
	}
	out, paths := FillEmptyValueLeaves(in)

	if in["timeZone"] != "" || in["server"] != nil {
		t.Fatalf("source mutated: %#v", in)
	}
	if out["timeZone"] != "..PLACEHOLDER_timeZone.." {
		t.Errorf("timeZone = %v, want placeholder", out["timeZone"])
	}
	if out["server"] != "..PLACEHOLDER_server.." {
		t.Errorf("server = %v, want placeholder", out["server"])
	}
	if out["replicas"] != 3 || out["enabled"] != false || out["url"] != "https://x" || out["suffix"] != ".cfargotunnel.com" {
		t.Errorf("non-empty leaf mutated: %#v", out)
	}
	if got := out["nested"].(map[string]any)["cronjob"].(map[string]any)["timeZone"]; got != "..PLACEHOLDER_timeZone.." {
		t.Errorf("nested timeZone = %v, want placeholder", got)
	}
	if list := out["nested"].(map[string]any)["list"].([]any); list[0] != "..PLACEHOLDER_list.." || list[1] != "keep" {
		t.Errorf("list = %v, want [placeholder keep]", list)
	}
	want := []string{"nested.cronjob.timeZone", "nested.list[0]", "server", "timeZone"}
	if !slices.Equal(paths, want) {
		t.Errorf("paths = %v, want %v", paths, want)
	}

	// Nil input yields no copy and no paths.
	if cp, p := FillEmptyValueLeaves(nil); cp != nil || p != nil {
		t.Errorf("nil input: got (%v, %v), want (nil, nil)", cp, p)
	}
}

// DeepCopyMap must produce an independent tree — mutating the copy
// can't bleed into the original or vice versa. Used by Kustomization.
// Clone and HelmRelease.Clone to isolate reconcile-time mutations.
func TestDeepCopyMap_Isolation(t *testing.T) {
	t.Parallel()
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
	t.Parallel()
	if DeepCopyMap(nil) != nil {
		t.Errorf("nil source should produce nil")
	}
}

func TestContainsValuePlaceholder_ValuesOnly(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name string
		in   any
		want bool
	}{
		{"nil", nil, false},
		{"literal", "value", false},
		{"placeholder", "..PLACEHOLDER_key..", true},
		{"concatenation", "registry...PLACEHOLDER_DOMAIN..", true},
		{"key only", map[string]any{"..PLACEHOLDER_key..": 1}, false},
		{"nested key", []any{map[string]any{"..PLACEHOLDER_key..": "value"}}, false},
		{"nested value", map[string]any{"data": []any{3, map[string]any{"value": "..PLACEHOLDER_key.."}}}, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := ContainsValuePlaceholder(tc.in); got != tc.want {
				t.Errorf("ContainsValuePlaceholder = %v, want %v", got, tc.want)
			}
		})
	}
}

// HelmRelease.Clone must produce a HR whose mutable fields don't alias
// the source — critical for the store-immutability contract.
func TestHelmRelease_Clone_Isolation(t *testing.T) {
	t.Parallel()
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

func TestKustomization_Clone_Isolation(t *testing.T) {
	t.Parallel()
	doc := mustYAML(t, `apiVersion: kustomize.toolkit.fluxcd.io/v1
kind: Kustomization
metadata: {name: apps, namespace: flux-system}
spec:
  patches: [null]
  components: null
  commonMetadata:
    labels: {hotel: null, delta: null, alpha: null, golf: null, bravo: null, foxtrot: null, charlie: null, echo: null}
    annotations: {zulu: null, tango: null, quebec: null, whiskey: null, romeo: null, xray: null, sierra: null, uniform: null}
  patchesStrategicMerge: [{metadata: {name: original}}]
  patchesJson6902: [{target: {kind: ConfigMap}, patch: [{op: remove, path: /data/old}]}]
`)
	src, err := parseKustomization(doc)
	assert.NoError(t, err)
	assert.Diff(t, src.shape.NullCommonLabels, []string{"alpha", "bravo", "charlie", "delta", "echo", "foxtrot", "golf", "hotel"})
	assert.Diff(t, src.shape.NullCommonAnnotations, []string{"quebec", "romeo", "sierra", "tango", "uniform", "whiskey", "xray", "zulu"})
	snapshot := Fingerprint(src.Content(nil))
	doc["spec"].(map[string]any)["patchesStrategicMerge"].([]any)[0].(map[string]any)["metadata"].(map[string]any)["name"] = "raw-edit"
	assert.Equal(t, Fingerprint(src.Content(nil)), snapshot)
	dst := src.Clone()
	dst.shape.Fields["components"] = "changed"
	dst.shape.NullElements["patches"][0] = 5
	dst.shape.NullCommonLabels[0] = "changed"
	dst.shape.NullCommonAnnotations[0] = "changed"
	dst.shape.Fields["patchesStrategicMerge"].([]any)[0].(map[string]any)["metadata"].(map[string]any)["name"] = "changed"
	dst.shape.Fields["patchesJson6902"].([]any)[0].(map[string]any)["patch"].([]any)[0].(map[string]any)["path"] = "/changed"
	assert.Equal(t, Fingerprint(src.Content(nil)), snapshot)
	if Fingerprint(dst.Content(nil)) == snapshot {
		t.Fatal("auxiliary input edits did not invalidate content")
	}
}

// TestHelmRelease_Clone_DeepCopiesEmbeddedSpec pins the deep-copy of
// the embedded helmv2.HelmReleaseSpec pointer/slice fields. The pre-fix
// shallow `out := *h` aliased these to the canonical store-owned HR,
// so a future code path mutating, e.g., Install.DisableHooks on a Clone
// would corrupt the store. Iterate every pointer-shaped field the
// spec exposes — any one regressing breaks the immutability contract.
func TestHelmRelease_Clone_DeepCopiesEmbeddedSpec(t *testing.T) {
	t.Parallel()
	src := &HelmRelease{
		Name: "plex", Namespace: "media",
		Install:   &helmv2.Install{DisableHooks: false},
		Upgrade:   &helmv2.Upgrade{DisableHooks: false},
		Rollback:  &helmv2.Rollback{DisableHooks: false},
		Uninstall: &helmv2.Uninstall{DisableHooks: false},
		Test:      &helmv2.Test{Enable: false},
		DriftDetection: &helmv2.DriftDetection{
			Mode: helmv2.DriftDetectionWarn,
		},
		ValuesFrom: []helmv2.ValuesReference{
			{Kind: "ConfigMap", Name: "src-cm"},
		},
		DependsOn: []helmv2.DependencyReference{
			{Name: "src-dep"},
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
	t.Parallel()
	src := &Kustomization{
		Name: "apps", Namespace: "flux-system",
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
	}
	dst := src.Clone()
	dst.PostBuild.Substitute["K"] = "MUTATED"
	dst.Patches[0].Patch = "MUTATED"
	dst.Components[0] = "MUTATED"
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

func TestWithNamespace_Copy(t *testing.T) {
	t.Parallel()
	ks := &Kustomization{Name: "app", Namespace: "authored"}
	ks.PostBuild = &kustomizev1.PostBuild{SubstituteFrom: []SubstituteReference{{Kind: KindConfigMap, Name: "values"}}}
	for _, tc := range []struct{ name, namespace string }{
		{"projected", "effective"}, {"cleared", ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			projected := ks.WithNamespace(tc.namespace).(*Kustomization)
			assert.Equal(t, projected.Named(), NamedResource{Group: FluxKustomizeDomain, Kind: KindKustomization, Namespace: tc.namespace, Name: "app"})
			assert.Equal(t, ks.Named(), NamedResource{Group: FluxKustomizeDomain, Kind: KindKustomization, Namespace: "authored", Name: "app"})
			assert.Diff(t, ks.PostBuild, &kustomizev1.PostBuild{SubstituteFrom: []SubstituteReference{{Kind: KindConfigMap, Name: "values"}}})
			assert.Diff(t, projected.PostBuild, ks.PostBuild)
		})
	}
}
