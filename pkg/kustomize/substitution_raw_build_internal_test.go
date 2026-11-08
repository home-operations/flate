package kustomize

import (
	"slices"
	"sync/atomic"
	"testing"

	kustomizev1 "github.com/fluxcd/kustomize-controller/api/v1"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
	sourcecache "github.com/home-operations/flate/pkg/source/cache"
)

func TestRender_SubstitutionRetainsRawBuild(t *testing.T) {
	t.Parallel()
	for _, postBuild := range []struct {
		name  string
		value *kustomizev1.PostBuild
	}{
		{"inline", &kustomizev1.PostBuild{Substitute: map[string]string{"CLUSTER_NAME": "home"}}},
		{
			"reference",
			&kustomizev1.PostBuild{
				SubstituteFrom: []kustomizev1.SubstituteReference{{Kind: "ConfigMap", Name: "settings"}},
				Substitute:     map[string]string{"CLUSTER_NAME": "home"},
			},
		},
		{"strategy", &kustomizev1.PostBuild{SubstituteStrategy: "Always"}},
	} {
		t.Run(postBuild.name, func(t *testing.T) {
			t.Parallel()
			testRenderSubstitutionRetainsRawBuildCase(
				t,
				&postBuild,
			)
		},
		)
	}
}

func testRenderSubstitutionRetainsRawBuildCase(
	t *testing.T,
	testCaseInput *struct {
		name  string
		value *kustomizev1.PostBuild
	},
) {
	t.Helper()

	root := testutil.WriteTree(
		t,
		map[string]string{
			"kustomization.yaml": existingKustomization("cm.yaml"),
			"cm.yaml":            cmYAML("original"),
		},
	)
	cache := newTestTreeCache(t, t.TempDir())
	var builds atomic.Int64
	source := localSource(t, root)
	source.FS = &pausedRenderFS{FS: source.FS, builds: &builds}
	spec := demoSpec.Clone()
	req := RenderRequest{Source: source, Spec: spec, SubPath: "."}
	content := func(request RenderRequest, docs []manifest.DocOccurrence) any {
		private := slices.Clone(docs)
		for i := range private {
			private[i].Document = manifest.DeepCopyMap(private[i].Document)
		}
		return PrepareShape(&request, private).Content()
	}
	docs, receipt, err := Render(t.Context(), cache, req, sourcecache.RetryConfig{})
	assert.NoError(t, err)
	assert.Equal(t, builds.Load(), int64(1))
	assert.NoError(
		t,
		cache.RecordRender(
			t.Context(),
			RenderAcceptance{
				Owner:   spec.Named(),
				Receipt: receipt,
				Records: emptyRenderInventory,
				Content: content(req, docs),
			},
		),
	)
	history := func(request RenderRequest) bool {
		raw, pending, found := cache.PreviousRender(t.Context(), request)
		if !found {
			return false
		}
		_, known := cache.RenderHistory(spec.Named(), pending, content(request, raw))
		return known
	}
	if cache.owner.CanPersistRenders() {
		known := history(req)
		assert.Equal(t, known, true)
	}
	changed := req
	changed.Spec = spec.Clone()
	changed.Spec.PostBuild = testCaseInput.value
	if cache.owner.CanPersistRenders() {
		known := history(changed)
		assert.Equal(t, known, false)
	}
	again, next, err := Render(t.Context(), cache, changed, sourcecache.RetryConfig{})
	assert.NoError(t, err)
	assert.Diff(t, again, docs)
	assert.Equal(t, builds.Load(), int64(1))
	beforeFrame := receipt.Value().(renderFrame)
	afterFrame := next.Value().(renderFrame)
	assert.Equal(t, beforeFrame.header.Key, afterFrame.header.Key)
	assert.Equal(t, manifest.ValueEqual(content(req, docs), content(changed, again)), false)
	_, accepted := cache.RenderHistory(spec.Named(), next, content(changed, again))
	assert.Equal(t, accepted, false)
	assert.Equal(t, spec.PostBuild == nil, true)
	assert.NoError(
		t,
		cache.RecordRender(
			t.Context(),
			RenderAcceptance{
				Owner:   spec.Named(),
				Receipt: next,
				Records: emptyRenderInventory,
				Content: content(changed, again),
			},
		),
	)
	if cache.owner.CanPersistRenders() {
		known := history(changed)
		assert.Equal(t, known, true)
		known = history(req)
		assert.Equal(t, known, false)
	}
}
