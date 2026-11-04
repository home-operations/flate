package git

import (
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	sourcev1 "github.com/fluxcd/source-controller/api/v1"
	"github.com/go-git/go-git/v5/plumbing"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil/cachetest"
	"github.com/home-operations/flate/pkg/manifest"
	sourcecache "github.com/home-operations/flate/pkg/source/cache"
)

func testCheckoutRefPreservesOriginFirstBranchLookupCase(
	t *testing.T, kind *string,
) {
	t.Helper()

	src, remote, first, second := localFixture(t)
	branch := plumbing.NewBranchReferenceName("pinned")
	assert.NoError(t, remote.Storer.SetReference(plumbing.NewHashReference(branch, first)))
	assert.NoError(t, remote.Storer.SetReference(plumbing.NewSymbolicReference(plumbing.HEAD, branch)))
	var advertisements atomic.Int32
	handler := gitHTTPHandler(t, src)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet && advertisements.Add(1) == 2 {
			if err := remote.Storer.SetReference(plumbing.NewHashReference(branch, second)); err != nil {
				t.Error(err)
				http.Error(w, "move branch", http.StatusInternalServerError)
				return
			}
		}
		handler.ServeHTTP(w, r)
	}))
	defer srv.Close()
	f := &Fetcher{Cache: cachetest.New(t, "", sourcecache.New)}
	ref := &sourcev1.GitRepositoryRef{Name: branch.String()}
	if (*kind) == "commit-branch" {
		ref = &sourcev1.GitRepositoryRef{Commit: second.String(), Branch: "pinned"}
	}
	spec := &manifest.GitRepository{
		URL: srv.URL, Reference: ref, RecurseSubmodules: true,
	}
	artifact, err := f.Fetch(
		t.Context(),
		spec,
		(testInputs{}).readSet(spec),
		sourcecache.Policy{MaxAge: time.Hour},
	)
	assert.NoError(t, err)
	acquired := openGeneration(t, f, spec, artifact)
	local, err := acquired.Reference(branch, true)
	assert.NoError(t, err)
	origin, err := acquired.Reference(plumbing.NewRemoteReferenceName("origin", "pinned"), true)
	assert.NoError(t, err)
	assert.Equal(t, local.Hash(), second)
	assert.Equal(t, origin.Hash(), first)
	resolved, err := resolveRefHash(acquired, spec.Reference, true)
	assert.NoError(t, err)
	assert.Equal(t, resolved, second)
	assert.Equal(t, artifact.Revision, resolved.String())
	assertWorktree(t, artifact.FS, "value.txt", "B")
}
