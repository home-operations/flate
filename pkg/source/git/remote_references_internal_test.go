package git

import (
	"io/fs"
	"os"
	"strings"
	"testing"

	sourcev1 "github.com/fluxcd/source-controller/api/v1"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil/cachetest"
	"github.com/home-operations/flate/pkg/manifest"
	sourcecache "github.com/home-operations/flate/pkg/source/cache"
)

func testFetcherReferencesCase(t *testing.T, src *string,
	tc *struct {
		name       string
		ref        *sourcev1.GitRepositoryRef
		depth      int
		sparse     []string
		want       string
		files      []string
		absent     []string
		warmCommit string
		recurse    bool
		wantErr    bool
		errText    string
	},
) {
	t.Helper()

	f := &Fetcher{Cache: cachetest.New(t, t.TempDir(), sourcecache.New), Depth: tc.depth}
	repo := &manifest.GitRepository{
		Name:              "test",
		Namespace:         "flux-system",
		URL:               "file://" + (*src),
		Reference:         tc.ref,
		SparseCheckout:    tc.sparse,
		RecurseSubmodules: tc.recurse,
	}
	if tc.warmCommit != "" {
		unconstrained := *repo
		unconstrained.Reference = &sourcev1.GitRepositoryRef{Commit: tc.warmCommit}
		_, err := f.Fetch(
			t.Context(),
			&unconstrained,
			(testInputs{}).readSet(&unconstrained),
			sourcecache.Policy{MaxAge: unconstrained.SourceInterval()},
		)
		assert.NoError(t, err)
	}
	art, err := f.Fetch(
		t.Context(),
		repo,
		(testInputs{}).readSet(repo),
		sourcecache.Policy{MaxAge: repo.SourceInterval()},
	)
	if tc.wantErr {
		if err == nil || tc.
			errText != "" && !strings.Contains(err.Error(), tc.errText) {
			t.Fatalf("Fetch error = %v, want failure containing %q", err, tc.errText)
		}
		return
	}
	assert.NoError(t, err)
	assert.Equal(t, art.Revision, tc.want)
	for _, file := range tc.files {
		{
			_, err := fs.Stat(art.FS, file)
			assert.ErrorIf(t, err != nil, "included file %s missing: %v", file, err)
		}
	}
	for _, file := range tc.absent {
		{
			_, err := fs.Stat(art.FS, file)
			assert.ErrorIf(t, !os.IsNotExist(err), "excluded file %s present: %v", file, err)
		}
	}
}
