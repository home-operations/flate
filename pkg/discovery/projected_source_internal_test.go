package discovery

import (
	"testing"
	"testing/fstest"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

func TestRun_ProjectedSourceAuthority(t *testing.T) {
	t.Parallel()
	cfg := authoredSourceConfig(false)
	cfg.Trees = testTrees(t)
	cfg.Input.FS = fstest.MapFS{
		"entry/kustomization.yaml": {Data: []byte("resources: [instance.yaml]\n")},
		"entry/instance.yaml": {Data: []byte(`apiVersion: fluxcd.controlplane.io/v1
kind: FluxInstance
metadata: {name: flux, namespace: apps}
spec:
  sync: {kind: GitRepository, name: self, url: https://fixture.invalid/self, ref: refs/heads/unavailable, path: ./entry}
  kustomize:
    patches:
    - target: {kind: GitRepository}
      patch: '[{"op":"add","path":"/spec/ignore","value":"excluded/**"}]'
`)},
	}
	result, err := Run(t.Context(), cfg)
	assert.NoError(t, err)
	id := manifest.NamedResource{
		Group:     manifest.SourceDomain,
		Kind:      manifest.KindGitRepository,
		Namespace: "apps",
		Name:      "self",
	}
	assert.Equal(t, result.Binding.Source, id)
	source, ok := cfg.Store.GetDeclaration(id).(*manifest.GitRepository)
	assert.FatalIf(t, !ok, "projected source lacks declaration authority")
	assert.Equal(t, *source.Ignore, "excluded/**")
	assert.Equal(t, cfg.Store.GetArtifact(id, store.ArtifactContent), nil)
	assert.Diff(t, cfg.Store.Productions(id, nil), []manifest.Production{{Source: manifest.BootstrapSourceID}})
	var captured store.ReadSet
	captured.Init(cfg.Store, id, nil)
	defer captured.Retire()
	assert.Equal(t, captured.GetDeclaration(id), manifest.BaseManifest(source))
	resolved, err := result.Admission.Resolve(t.Context(), &captured, ResolveRequest{ID: id, Object: source})
	assert.NoError(t, err)
	assert.Equal(t, resolved.Local, true)
	for gate := range captured.Gates() {
		t.Fatalf("projected bootstrap source formed an owner edge: %+v", gate)
	}
	next := *cfg.Input
	next.Content = "replacement-input"
	cfg.Store.SetArtifact(manifest.BootstrapSourceID, &next)
	assert.Equal(t, captured.Valid(), false)
	assert.Equal(t, cfg.Store.GetDeclaration(id), nil)
	assert.Equal(t, len(cfg.Store.Productions(id, nil)), 0)
}
