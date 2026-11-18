package kustomize

import (
	"testing"

	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/internal/testutil/assert"
	"github.com/home-operations/flate/pkg/manifest"
)

func TestPreparation_Matches(t *testing.T) {
	t.Parallel()
	root := testutil.WriteTree(t, map[string]string{"kustomization.yaml": "resources: []\n"})
	cache := newTestTreeCache(t, "")
	var preparation Preparation
	request := RenderRequest{
		Preparation: &preparation, Source: localSource(t, root), Label: root, SubPath: ".",
		Spec: &manifest.Kustomization{Name: "owner", Namespace: "apps", Path: "."},
	}
	_, err := cache.prepareRender(t.Context(), &request)
	assert.NoError(t, err)
	assert.Equal(t, (*Preparation)(nil).Matches(cache, &request), false)
	assert.Equal(t, (&Preparation{cache: cache, spec: request.Spec}).Matches(cache, &request), false)
	assert.Equal(t, preparation.Matches(nil, &request), false)
	for _, tc := range []struct {
		name   string
		change func(*RenderRequest)
		want   bool
	}{
		{"same owner", func(*RenderRequest) {}, true},
		{"equal declaration", func(r *RenderRequest) { r.Spec = r.Spec.Clone() }, true},
		{"missing declaration", func(r *RenderRequest) { r.Spec = nil }, false},
		{"different declaration", func(r *RenderRequest) { r.Spec = r.Spec.Clone(); r.Spec.Path = "other" }, false},
		{"different label", func(r *RenderRequest) { r.Label = "other" }, false},
		{"different path", func(r *RenderRequest) { r.SubPath = "other" }, false},
		{"different ignore policy", func(r *RenderRequest) { r.ApplyIgnore = true }, false},
		{"different strict policy", func(r *RenderRequest) { r.StrictSubstitutions = true }, false},
		{"different skip policy", func(r *RenderRequest) { r.SkipKinds = []string{manifest.KindSecret} }, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			current := request
			tc.change(&current)
			assert.Equal(t, preparation.Matches(cache, &current), tc.want)
		})
	}
}
