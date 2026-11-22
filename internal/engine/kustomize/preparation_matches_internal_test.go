package kustomize

import (
	"io/fs"
	"path/filepath"
	"testing"
	"testing/fstest"

	"sigs.k8s.io/kustomize/kyaml/filesys"

	"github.com/home-operations/flate/internal/engine/store"
	"github.com/home-operations/flate/internal/engine/tree"

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
	assert.Equal(t, (&Preparation{cache: cache.owner, spec: request.Spec}).Matches(cache, &request), false)
	assert.Equal(t, preparation.Matches(nil, &request), false)
	assert.Equal(t, preparation.Matches(newTestTreeCache(t, ""), &request), false)
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

		{"different strict policy", func(r *RenderRequest) { r.StrictSubstitutions = true }, false},
		{"added aggregate resource", func(r *RenderRequest) {
			source := *r.Source
			source.FS = fstest.MapFS{"kustomization.yaml": {Data: []byte("resources: [one, two]\n")}}
			r.Source = &source
		}, true},
		{"removed aggregate resource", func(r *RenderRequest) {
			r.Source = &store.SourceArtifact{LocalPath: r.Source.LocalPath, FS: fstest.MapFS{
				"kustomization.yaml": {Data: []byte("resources: [one]\n")},
			}}
		}, true},
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

type aggregateNativeFS struct{ filesys.FileSystem }

func (aggregateNativeFS) Walk(string, filepath.WalkFunc) error {
	return fs.ErrInvalid
}

func TestPreparation_ExplicitAggregateDoesNotScan(t *testing.T) {
	t.Parallel()
	const root = "/virtual/explicit-aggregate"
	lower := fstest.MapFS{"plain.yaml": {Data: []byte(cmYAML("plain"))}}
	overlay := tree.NewBuilder(lower)
	const resources = "apiVersion: kustomize.config.k8s.io/v1beta1\nkind: Kustomization\nresources: [plain.yaml]\n"
	assert.NoError(t, overlay.AddFile("kustomization.yaml", []byte(resources), 0o644))
	input, err := overlay.Build()
	assert.NoError(t, err)
	file, err := findOrGenerateKustomization(aggregateNativeFS{newTreeFS(input, root, nil)}, root)
	assert.NoError(t, err)
	assert.Equal(t, file.existing, true)
	assert.Equal(t, string(file.data), resources)
	_, err = fs.Stat(lower, "kustomization.yaml")
	assert.Equal(t, err != nil, true)
}
