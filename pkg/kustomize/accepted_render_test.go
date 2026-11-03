package kustomize

import (
	kustomizev1 "github.com/fluxcd/kustomize-controller/api/v1"
	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/values"
	"testing"
)

func TestRenderKey_DependsOnProjection(t *testing.T) {
	t.Parallel()
	root := testutil.WriteTree(t, map[string]string{"kustomization.yaml": existingKustomization("cm.yaml"), "cm.yaml": cmYAML("original")})
	spec := &manifest.Kustomization{Name: "emitter", Namespace: "apps", Path: "."}
	spec.DependsOn = []kustomizev1.DependencyReference{{Name: "${PREFIX:-dev-}ready", Namespace: "${TEAM:-apps}"}}
	req := RenderRequest{Source: localSource(t, root), Spec: spec, SubPath: "."}
	snapshot := values.NewDependencySubstitutions([]*manifest.Kustomization{{PostBuild: &kustomizev1.PostBuild{Substitute: map[string]string{"PREFIX": "prod-", "TEAM": "control"}}}})
	projected := req
	projected.Spec = manifest.MapDependsOn(spec, snapshot.Resolve, snapshot.Resolve).(*manifest.Kustomization)
	cache := newTestTreeCache(t, t.TempDir())
	before, err := cache.prepareRender(t.Context(), req)
	assert.NoError(t, err)
	after, err := cache.prepareRender(t.Context(), projected)
	assert.NoError(t, err)
	assert.Equal(t, before.key, after.key)
	assert.Equal(t, spec.DependsOn[0].Name, "${PREFIX:-dev-}ready")
	for _, change := range []struct {
		name   string
		mutate func(*manifest.Kustomization)
	}{
		{"path", func(k *manifest.Kustomization) { k.Path = "other" }},
		{"name", func(k *manifest.Kustomization) { k.Name = "other" }},
		{"namespace", func(k *manifest.Kustomization) { k.Namespace = "other" }},
		{"target namespace", func(k *manifest.Kustomization) { k.TargetNamespace = "other" }},
		{"inline", func(k *manifest.Kustomization) {
			k.PostBuild = &kustomizev1.PostBuild{Substitute: map[string]string{"VALUE": "other"}}
		}},
	} {
		t.Run(change.name, func(t *testing.T) {
			t.Parallel()
			changed := req
			changed.Spec = spec.Clone()
			change.mutate(changed.Spec)
			next, err := cache.prepareRender(t.Context(), changed)
			assert.NoError(t, err)
			if next.binding.equal(before.binding) {
				t.Fatal("accepted render change retained identity")
			}
			assert.Equal(t, next.key == before.key, change.name == "inline")
		})
	}
}
