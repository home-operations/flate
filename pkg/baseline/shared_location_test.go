package baseline

import (
	"sync"
	"testing"

	sourcev1 "github.com/fluxcd/source-controller/api/v1"
	"github.com/go-git/go-git/v5"
	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil/cachetest"
	"github.com/home-operations/flate/pkg/manifest"
	sourcecache "github.com/home-operations/flate/pkg/source/cache"
	sourcegit "github.com/home-operations/flate/pkg/source/git"
	"github.com/home-operations/flate/pkg/source/gittree"
)

func TestReview_SharedLocationAfterResolve(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	base := initRepoWithFile(t, root, "value.txt", "before")
	writeAndCommit(t, root, "value.txt", "after")
	repo := openHelper(t, root)
	assert.NoError(t, repo.RepackObjects(&git.RepackConfig{}))
	result, err := AutoResolve(t.Context(), root, base.String(), cachetest.New(t, "", sourcecache.New))
	assert.NoError(t, err)
	location := result.Source.FS.(*gittree.FS).Location()
	repository := &manifest.GitRepository{Name: "pinned", Namespace: "flux-system"}
	repository.URL = "git://fixture.invalid/cluster"
	repository.Reference = &sourcev1.GitRepositoryRef{Commit: base.String()}
	start := make(chan struct{})
	var ready, readers sync.WaitGroup
	ready.Add(2)
	for range 2 {
		readers.Go(func() {
			ready.Done()
			<-start
			_, err := sourcegit.ResolveLocal(t.Context(), location, []*manifest.GitRepository{repository}, "")
			if err != nil {
				t.Error(err)
			}
		})
	}
	ready.Wait()
	close(start)
	readers.Wait()
}
