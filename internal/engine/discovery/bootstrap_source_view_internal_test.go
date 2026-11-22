package discovery

import (
	"errors"
	"io/fs"
	"testing"
	"testing/fstest"

	sourcev1 "github.com/fluxcd/source-controller/api/v1"

	"github.com/home-operations/flate/internal/engine/store"
	"github.com/home-operations/flate/internal/testutil/assert"
	"github.com/home-operations/flate/pkg/manifest"
)

func TestEntrySourceView_BucketIgnore(t *testing.T) {
	t.Parallel()
	input := &store.SourceArtifact{
		LocalPath: t.TempDir(),
		FS: fstest.MapFS{
			"apps/first.yaml":  {},
			"apps/second.yaml": {},
		},
	}
	bucket := &manifest.Bucket{Name: "bucket", Namespace: "sources"}
	bucket.Ignore = new("/apps/first.yaml")
	a := Admission{aliasViews: sourceViews{}}
	candidate := entryResolution{object: bucket, input: input, provider: store.ProviderAlias}
	first, firstKey, err := a.entrySourceView(t.Context(), &candidate)
	assert.NoError(t, err)
	assert.Equal(t, first.Extent, store.ExtentFiltered)
	_, err = fs.Stat(first.FS, "apps/first.yaml")
	assert.Equal(t, errors.Is(err, fs.ErrNotExist), true)
	_, err = fs.Stat(first.FS, "apps/second.yaml")
	assert.NoError(t, err)

	a.aliasViews.remember(firstKey, first)
	changed := *bucket
	changed.Ignore = new("/apps/second.yaml")
	candidate.object = &changed
	second, secondKey, err := a.entrySourceView(t.Context(), &candidate)
	assert.NoError(t, err)
	assert.Equal(t, secondKey.mount == firstKey.mount, false)
	assert.Equal(t, second.FS == first.FS, false)
	_, err = fs.Stat(second.FS, "apps/first.yaml")
	assert.NoError(t, err)
	_, err = fs.Stat(second.FS, "apps/second.yaml")
	assert.Equal(t, errors.Is(err, fs.ErrNotExist), true)
}

func TestEntrySourceView_CacheCompatibility(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name          string
		provider      store.SourceProvider
		differentMode bool
	}{
		{"same provider with include", store.ProviderAlias, true},
		{"other provider with include", store.ProviderInput, true},
		{"other provider with new input", store.ProviderInput, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			input := &store.SourceArtifact{
				LocalPath: t.TempDir(),
				FS: fstest.MapFS{
					".sourceignore":      {Data: []byte("/apps/excluded.yaml\n")},
					"apps/excluded.yaml": {},
					"apps/control.yaml":  {},
				},
			}
			repo := &manifest.GitRepository{Name: "git", Namespace: "sources"}
			a := Admission{aliasViews: sourceViews{}}
			candidate := entryResolution{object: repo, git: repo, input: input, provider: store.ProviderAlias}
			first, firstKey, err := a.entrySourceView(t.Context(), &candidate)
			assert.NoError(t, err)
			_, err = fs.Stat(first.FS, "apps/excluded.yaml")
			assert.Equal(t, errors.Is(err, fs.ErrNotExist), true)
			a.aliasViews.remember(firstKey, first)

			candidate.provider = tc.provider
			if tc.differentMode {
				including := *repo
				including.Include = []sourcev1.GitRepositoryInclude{
					{GitRepositoryRef: manifest.LocalObjectReference{Name: "included"}},
				}
				candidate.object, candidate.git = &including, &including
			} else {
				candidate.input = &store.SourceArtifact{
					LocalPath: input.LocalPath,
					FS:        fstest.MapFS{"apps/excluded.yaml": {}, "apps/control.yaml": {}},
				}
			}
			second, secondKey, err := a.entrySourceView(t.Context(), &candidate)
			assert.NoError(t, err)
			assert.Equal(t, second.FS == first.FS, false)
			assert.Equal(t, secondKey.mount == firstKey.mount, tc.provider == store.ProviderAlias)
			assert.Equal(t, secondKey.input, candidate.input)
			assert.Equal(t, secondKey.mode == firstKey.mode, !tc.differentMode)
			_, err = fs.Stat(second.FS, "apps/excluded.yaml")
			assert.NoError(t, err)
			_, err = fs.Stat(second.FS, "apps/control.yaml")
			assert.NoError(t, err)
		})
	}
}
