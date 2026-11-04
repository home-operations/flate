package git

import (
	"io/fs"
	"slices"
	"testing"
	"time"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/pkg/manifest"
	sourcecache "github.com/home-operations/flate/pkg/source/cache"
)

type testFetcherSparseCheckoutCaseInputs struct {
	src   *string
	files *map[string]string
	f     **Fetcher
	tc    *struct {
		prefix string
		want   []string
	}
	recursive *bool
}

func testFetcherSparseCheckoutCase(t *testing.T, testCaseInput testFetcherSparseCheckoutCaseInputs) {
	t.Helper()

	spec := &manifest.GitRepository{
		Name: "test", Namespace: "flux-system", URL: "file://" + (*testCaseInput.src),
		SparseCheckout: []string{testCaseInput.tc.prefix}, RecurseSubmodules: (*testCaseInput.recursive),
	}
	before := *spec
	before.SparseCheckout = slices.Clone(spec.SparseCheckout)
	want := make(map[string]string, len(testCaseInput.tc.want))
	for _, file := range testCaseInput.tc.want {
		want[file] = (*testCaseInput.files)[file]
	}
	var previous map[string]string
	for run := range 2 {
		art, err := (*testCaseInput.f).Fetch(
			t.Context(),
			spec,
			(testInputs{}).readSet(spec),
			sourcecache.Policy{MaxAge: time.Hour},
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
		assert.Diff(t, *spec, before)
		if run > 0 {
			assert.Diff(t, got, previous)
		}
		previous = got
	}
}
