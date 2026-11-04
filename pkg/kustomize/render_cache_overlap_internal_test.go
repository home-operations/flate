package kustomize

import (
	"context"
	"fmt"
	"io"
	"io/fs"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	sourcecache "github.com/home-operations/flate/pkg/source/cache"
	"github.com/home-operations/flate/pkg/store"
	"github.com/home-operations/flate/pkg/task"
)

func TestRender_CacheAcquisitionOverlap(t *testing.T) {
	t.Parallel()
	for _, disk := range []bool{false, true} {
		t.Run(fmt.Sprintf("disk_%t", disk), func(t *testing.T) {
			t.Parallel()
			files := map[string]string{"kustomization.yaml": existingKustomization("cm.yaml"), "cm.yaml": cmYAML("old")}
			root, changed := testutil.WriteTree(t, files), testutil.WriteTree(t, files)
			storage := ""
			if disk {
				storage = t.TempDir()
			}
			owner := newTestTreeCache(t, storage).owner
			firstCache, secondCache := NewTreeCache(
				owner,
				nil,
				task.NewBounded(2),
			), NewTreeCache(
				owner,
				nil,
				task.NewBounded(2),
			)
			defer firstCache.Close()
			defer secondCache.Close()
			var builds atomic.Int64
			synctest.Test(t, func(t *testing.T) {
				first := &pausedRenderFS{
					FS:      os.DirFS(root),
					entered: make(chan struct{}),
					release: make(chan struct{}),
					builds:  &builds,
				}
				second := &pausedRenderFS{FS: os.DirFS(changed), builds: &builds}
				run := func(c *TreeCache, input fs.FS, want string) {
					out, _, err := testutil.RenderMaps(
						Render(
							t.Context(),
							c,
							RenderRequest{
								Source:  &store.SourceArtifact{FS: input},
								Label:   root,
								SubPath: ".",
								Spec:    demoSpec,
							},
							sourcecache.RetryConfig{},
						),
					)
					assert.NoError(t, err)
					assert.Equal(t, hasRenderField(out, "name", want), true)
				}
				done := make(chan struct{})
				go func() { defer close(done); run(firstCache, first, "old") }()
				<-first.entered
				var queued sync.WaitGroup
				for range 2 {
					queued.Go(func() { run(secondCache, second, "new") })
				}
				synctest.Wait()
				// A different acquisition key MUST progress while this render owns its key.
				_, _, _, err := owner.Blob(
					t.Context(),
					sourcecache.BlobKey{Content: "unrelated-source"},
					sourcecache.Policy{MaxAge: sourcecache.Forever},
					func(_ context.Context, w sourcecache.ProducerInput) (sourcecache.ProducerResult, error) {
						_, err := io.WriteString(w, "source")
						return sourcecache.ProducerResult{}, err
					},
				)
				assert.NoError(t, err)

				testutil.WriteFile(t, changed, "cm.yaml", cmYAML("new"))
				close(first.release)
				<-done
				queued.Wait()
				assert.Equal(t, builds.Load(), int64(2))
				run(secondCache, second, "new")
				assert.Equal(t, builds.Load(), int64(2))
			})
		})
	}
}
