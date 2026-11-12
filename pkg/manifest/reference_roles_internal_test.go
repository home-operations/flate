package manifest

import (
	"slices"
	"testing"

	helmv2 "github.com/fluxcd/helm-controller/api/v2"
	kustomizev1 "github.com/fluxcd/kustomize-controller/api/v1"
	sourcev1 "github.com/fluxcd/source-controller/api/v1"

	"github.com/home-operations/flate/internal/assert"
)

func TestReferences_ReadsSourceTree(t *testing.T) {
	t.Parallel()
	for _, kind := range []string{KindGitRepository, KindOCIRepository, KindBucket, KindExternalArtifact} {
		t.Run(kind, func(t *testing.T) {
			t.Parallel()
			obj := &Kustomization{
				Namespace: "apps", Path: "./apps",
				SourceRef: kustomizev1.CrossNamespaceSourceReference{Kind: kind, Name: "input", Namespace: "sources"},
			}
			refs := slices.Collect(References(obj, ReferenceAll))
			assert.Equal(t, len(refs), 1)
			for _, ref := range refs {
				assert.Equal(t, ref.ReadsSourceTree(), true)
				assert.Equal(t, ref.Clone().ReadsSourceTree(), true)
				assert.Equal(t, ref.BindsSource(), true)
				assert.Equal(t, ref.IsSource(), true)
				assert.Equal(t, ref.Content, true)
				assert.Equal(t, ref.Gate, GateInputs|GateArtifacts)
				assert.Equal(t, ref.ArtifactStages, GateInputs|GateArtifacts)
			}
		})
	}
	for _, tc := range []struct {
		name string
		obj  BaseManifest
		tree bool
	}{
		{"chart", &HelmRelease{Namespace: "apps", ChartRef: &helmv2.CrossNamespaceSourceReference{Kind: KindOCIRepository, Name: "input"}}, false},
		{"chart backing source", &HelmChartSource{Namespace: "apps", SourceRef: sourcev1.LocalHelmChartSourceReference{Kind: KindGitRepository, Name: "input"}}, false},
		{"generator input", &RawObject{APIVersion: "source.extensions.fluxcd.io/v1beta1", Kind: "ArtifactGenerator", Namespace: "apps", Spec: map[string]any{"sources": []any{map[string]any{"kind": KindOCIRepository, "name": "input"}}}}, true},
		{"operator input", &RawObject{APIVersion: FluxOperatorDomain + "/v1", Kind: "FluxInstance", Namespace: "apps", Spec: map[string]any{"sync": map[string]any{"kind": KindOCIRepository, "url": "oci://fixture.invalid/cluster", "ref": "latest", "path": "./apps"}}}, false},
		{"ordering", &Kustomization{Namespace: "apps", DependsOn: []kustomizev1.DependencyReference{{Name: "input"}}}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			refs := slices.Collect(References(tc.obj, ReferenceRelations))
			assert.Equal(t, len(refs), 1)
			assert.Equal(t, refs[0].ReadsSourceTree(), tc.tree)
			assert.Equal(t, refs[0].Clone().ReadsSourceTree(), tc.tree)
		})
	}
	t.Run("foreign source group", func(t *testing.T) {
		t.Parallel()
		obj := &Kustomization{Namespace: "apps", SourceRef: kustomizev1.CrossNamespaceSourceReference{
			APIVersion: "foreign.example/v1", Kind: KindOCIRepository, Name: "input",
		}}
		refs := slices.Collect(References(obj, ReferenceRelations))
		assert.Equal(t, len(refs), 1)
		assert.Equal(t, refs[0].ReadsSourceTree(), true)
		assert.Equal(t, refs[0].CanBindLocal(), false)
	})
}

func TestReferences_GeneratedChartDoesNotBind(t *testing.T) {
	t.Parallel()
	obj := &HelmRelease{Name: "app", Namespace: "apps"}
	obj.HelmReleaseSpec.Chart = &helmv2.HelmChartTemplate{
		Kind: KindOCIRepository,
		Spec: helmv2.HelmChartTemplateSpec{
			OCIRepositorySpec: sourcev1.OCIRepositorySpec{URL: "oci://registry.test/app"},
		},
	}
	assert.NoError(t, obj.normalizeChartSource())
	refs := slices.Collect(References(obj, ReferenceRelations))
	assert.Equal(t, len(refs), 1)
	assert.Equal(t, refs[0].Use, "chart")
	assert.Equal(t, refs[0].IsSource(), true)
	assert.Equal(t, refs[0].BindsSource(), false)
	assert.Equal(t, refs[0].Clone().BindsSource(), false)
	assert.Equal(t, refs[0].ReadsSourceTree(), false)
	obj.ChartRef = &helmv2.CrossNamespaceSourceReference{Kind: KindOCIRepository, Name: "input"}
	obj.HelmReleaseSpec.Chart = nil
	assert.NoError(t, obj.normalizeChartSource())
	refs = slices.Collect(References(obj, ReferenceRelations))
	assert.Equal(t, len(refs), 1)
	assert.Equal(t, refs[0].Use, "chart")
	assert.Equal(t, refs[0].BindsSource(), true)
	assert.Equal(t, refs[0].Clone().BindsSource(), true)
	assert.Equal(t, refs[0].ReadsSourceTree(), false)
}
