package discovery

import (
	"testing"
	"testing/fstest"

	kustomizev1 "github.com/fluxcd/kustomize-controller/api/v1"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
	"github.com/home-operations/flate/pkg/values"
)

func TestBindEntry_AuthoredOwnerKeepsGates(t *testing.T) {
	t.Parallel()
	const path = "/virtual/effective-reference-context"
	input := &store.SourceArtifact{LocalPath: path, FS: fstest.MapFS{}}
	st := store.New()
	st.SetArtifact(manifest.BootstrapSourceID, input)
	applier := &manifest.Kustomization{
		Name:      "owner",
		Namespace: "apps",
		Path:      ".",
		SourceRef: kustomizev1.CrossNamespaceSourceReference{
			Kind: manifest.BootstrapSourceID.Kind, Name: manifest.BootstrapSourceID.Name,
			Namespace: manifest.BootstrapSourceID.Namespace,
		},
		DependsOn: []kustomizev1.DependencyReference{{Name: "sibling"}},
		PostBuild: &kustomizev1.PostBuild{
			SubstituteFrom: []kustomizev1.SubstituteReference{{Kind: manifest.KindConfigMap, Name: "settings"}},
		},
	}
	st.AddObject(applier)
	admission := &Admission{store: st, input: input, path: path}
	root, owner, err := admission.entry(t.Context(), st, []manifest.BaseManifest{applier})
	assert.NoError(t, err)
	assert.Equal(t, owner, manifest.BaseManifest(applier))
	assert.Equal(t, root.Named().ReportHidden(), true)
	assert.Equal(t, len(root.DependsOn), 0)
	assert.Equal(t, root.PostBuild == nil, true)
	assert.Equal(t, root.SourceID(), manifest.BootstrapSourceID)
	assert.Equal(t, root.Namespace == applier.Namespace, false)
	for ref := range manifest.References(applier, manifest.ReferenceAll) {
		if ref.Use == "dependsOn" || ref.Use == "substituteFrom" {
			assert.Equal(t, ref.Namespace, "apps")
		}
	}
	st.AddObject(&manifest.ConfigMap{Name: "settings", Namespace: "apps", Data: map[string]any{"VALUE": "application"}})
	provider := values.NewStoreProvider(st)
	resolved := applier.Clone()
	assert.NoError(t, values.ExpandPostBuildSubstituteReference(resolved, provider))
	assert.Equal(t, resolved.PostBuild.Substitute["VALUE"], "application")
	assert.Equal(t, len(applier.DependsOn), 1)
	assert.Equal(t, len(applier.PostBuild.SubstituteFrom), 1)
}
