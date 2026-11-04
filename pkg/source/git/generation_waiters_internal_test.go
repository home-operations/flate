package git

import (
	"context"
	"io/fs"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/plumbing"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil/cachetest"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source"
	sourcecache "github.com/home-operations/flate/pkg/source/cache"
	"github.com/home-operations/flate/pkg/store"
)

func testGenerationWaiter(
	t *testing.T,
	disk bool,
	fetch func(context.Context, *Fetcher, string) (*store.SourceArtifact, error),
	want []string,
) {
	t.Helper()
	src := t.TempDir()
	mustInitRepoWithFiles(t, src, map[string]string{
		"apps/keep.yaml": "keep", "apps/drop.tmp": "drop", "other/keep.yaml": "other",
		".hidden/secret.yaml": "hidden", ".sourceignore": "other/\n",
	})
	repository, err := git.PlainOpen(src)
	assert.NoError(t, err)
	head, err := repository.Head()
	assert.NoError(t, err)
	assert.NoError(
		t,
		repository.Storer.SetReference(
			plumbing.NewHashReference(head.Name(), submoduleCommit(t, repository, head.Hash())),
		),
	)
	revision := mustHead(t, src)
	started, release := make(chan struct{}), make(chan struct{})
	unblock := sync.OnceFunc(func() { close(release) })
	var advertisements atomic.Int32
	handler := gitHTTPHandler(t, src)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet && advertisements.Add(1) == 1 {
			close(started)
			select {
			case <-release:
			case <-r.Context().Done():
				return
			}
		}
		handler.ServeHTTP(w, r)
	}))
	t.Cleanup(srv.Close)
	t.Cleanup(unblock)
	root := ""
	if disk {
		root = t.TempDir()
	}
	f := &Fetcher{Cache: cachetest.New(t, root, sourcecache.New)}

	results := make(chan testtestGenerationWaiterResult, 2)
	go func() {
		artifact, fetchErr3 := fetch(t.Context(), f, srv.URL)
		results <- testtestGenerationWaiterResult{artifact, fetchErr3}
	}()
	<-started
	waiter := &acquisitionWaitContext{Context: t.Context(), waiting: make(chan struct{})}
	go func() {
		artifact, fetchErr4 := fetch(waiter, f, srv.URL)
		results <- testtestGenerationWaiterResult{artifact, fetchErr4}
	}()
	<-waiter.waiting
	assert.Equal(t, advertisements.Load(), 1)
	unblock()
	first, second := <-results, <-results
	assert.NoError(t, first.err)
	assert.NoError(t, second.err)
	assert.FatalIf(t, first.artifact.FS == second.artifact.FS, "%s", "waiter reused the producer's view")

	check := func(artifact *store.SourceArtifact) {
		assert.Equal(t, artifact.Revision, revision)
		assert.Equal(t, artifact.Key, first.artifact.Key)
		_, identity, viewErr := source.OperationView(t.Context(), artifact.FS)
		assert.NoError(t, viewErr)
		assert.Equal(t, artifact.Content, identity)
		assert.Equal(t, artifact.Content, first.artifact.Content)
		assert.FatalIf(t, artifact.Content == "", "%s", "missing acquired filesystem identity")
		assert.FatalIf(t, artifact.Key == "", "%s", "missing cache identity")

		if len(artifact.Warnings) != 1 || artifact.Warnings[0].Category != manifest.WarnSkippedSubmodule ||
			artifact.Warnings[0].Detail[0] != "module" {
			t.Fatalf("generation warning lost: %+v", artifact.Warnings)
		}
		var files []string
		assert.NoError(t, fs.WalkDir(artifact.FS, ".", func(name string, entry fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if !entry.IsDir() {
				files = append(files, name)
			}
			return nil
		}))
		assert.Diff(t, files, want)
		assertWorktree(t, artifact.FS, "apps/keep.yaml", "keep")
	}
	check(first.artifact)
	check(second.artifact)
	before := advertisements.Load()
	srv.Close()
	if disk {
		assert.NoError(t, f.Cache.Close())
		f = &Fetcher{Cache: cachetest.New(t, root, sourcecache.New)}
	}
	hit, err := fetch(t.Context(), f, srv.URL)
	assert.NoError(t, err)
	assert.Equal(t, advertisements.Load(), before)
	check(hit)
}

type testtestGenerationWaiterResult struct {
	artifact *store.SourceArtifact
	err      error
}
