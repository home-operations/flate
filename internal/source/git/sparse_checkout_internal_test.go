package git

import (
	"io/fs"
	"slices"
	"testing"
	"time"

	"github.com/home-operations/flate/internal/testutil/fetchtest"

	"github.com/home-operations/flate/internal/assert"
	sourcecache "github.com/home-operations/flate/internal/source/cache"
	"github.com/home-operations/flate/pkg/manifest"
)

type testFetcherSparseCheckoutCaseInputs struct {
	f         **Fetcher
	files     *map[string]string
	recursive *bool
	src       *string
	tc        *struct {
		prefix string
		want   []string
	}
}

func testFetcherSparseCheckoutCase(t *testing.T, captures *testFetcherSparseCheckoutCaseInputs) {
	t.Helper()
	spec := &manifest.GitRepository{
		Name: "test", Namespace: "flux-system", URL: "file://" + (*captures.src),
		SparseCheckout: []string{captures.tc.prefix}, RecurseSubmodules: (*captures.recursive),
	}
	before := *spec
	before.SparseCheckout = slices.Clone(spec.SparseCheckout)
	want := make(map[string]string, len(captures.tc.want))
	for _, file := range captures.tc.want {
		want[file] = (*captures.files)[file]
	}
	var previous map[string]string
	for run := range 2 {
		art, err := fetchtest.Fetch(
			t.Context(),
			(*captures.f).Fetch,
			spec,
			(testInputs{}).readSet(spec),
			&sourcecache.Policy{MaxAge: time.Hour},
			nil,
		)
		assert.NoError(t, err)
		got := map[string]string{}
		err = fs.WalkDir(art.FS, ".", func(file string, entry fs.DirEntry, err error) error {
			if err != nil || entry.IsDir() {
				return err
			}
			data, err := fs.ReadFile(art.FS, file)
			got[file] = string(data)
			return err
		})
		assert.NoError(t, err)
		assert.Diff(t, got, want)
		assert.Equal(t, manifest.Equal(spec, &before), true)
		if run > 0 {
			assert.Diff(t, got, previous)
		}
		previous = got
	}
}
