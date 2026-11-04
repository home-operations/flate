package orchestrator

import (
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"testing/synctest"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/change"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

type pairedBarrierReader struct {
	fs.FS

	entered    chan struct{}
	release    chan struct{}
	blocked    atomic.Bool
	inputOpens atomic.Int64
}

func (r *pairedBarrierReader) Open(name string) (fs.File, error) {
	if name == "apps/app-a/render-input.env" {
		r.inputOpens.Add(1)
	}
	if name == "block.env" && r.entered != nil && r.blocked.CompareAndSwap(false, true) {
		close(r.entered)
		<-r.release
	}
	return r.FS.Open(name)
}

func TestRenderTrees_JoinsPreparationBeforeDispatch(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		roots := [2]string{t.TempDir(), t.TempDir()}
		var readers [2]*pairedBarrierReader
		var inputs [2]*store.SourceArtifact
		entered, release := make(chan struct{}), make(chan struct{})
		for side, root := range roots {
			writeCluster(t, root, []string{"before", "after"}[side], "same")
			for _, name := range []string{"ks-a.yaml", "ks-b.yaml"} {
				body, err := os.ReadFile(filepath.Join(root, name))
				assert.NoError(t, err)
				testutil.WriteFile(
					t,
					root,
					name,
					strings.ReplaceAll(
						string(body),
						"name: flux-system, namespace: flux-system",
						"name: selected, namespace: flux-system",
					),
				)
			}
			testutil.WriteFile(t, root, "block.env", "BLOCK=once\n")
			testutil.WriteFile(t, root, "apps/app-a/render-input.env", "VALUE=selected\n")
			testutil.WriteFile(
				t,
				root,
				"apps/app-a/kustomization.yaml",
				"resources: [cm.yaml]\nconfigMapGenerator: [{name: generated, envs: [render-input.env]}]\n",
			)
			readers[side] = &pairedBarrierReader{FS: os.DirFS(root)}
			if side == 1 {
				readers[side].entered, readers[side].release = entered, release
			}
			inputs[side] = &store.SourceArtifact{LocalPath: root, LocalRoot: root, FS: readers[side]}
		}
		done := make(chan error, 1)
		go func() {
			_, err := RenderTrees(
				t.Context(),
				[]Tree{
					{
						RepoRoot:        roots[0],
						ID:              manifest.TreeID{Cluster: ".", Side: manifest.TreeBase},
						Input:           inputs[0],
						BaseInput:       inputs[1],
						ExternalChanges: change.NewSet([]string{"apps/app-a/cm.yaml"}),
					},
					{
						RepoRoot:        roots[1],
						ID:              manifest.TreeID{Cluster: ".", Side: manifest.TreeHead},
						Input:           inputs[1],
						BaseInput:       inputs[0],
						ExternalChanges: change.NewSet([]string{"apps/app-a/cm.yaml"}),
					},
				},
				Config{Concurrency: 2},
			)
			done <- err
		}()
		<-entered
		synctest.Wait()
		assert.Equal(t, readers[0].inputOpens.Load(), int64(2))
		close(release)
		assert.NoError(t, <-done)
		for side, reader := range readers {
			assert.Equal(t, reader.inputOpens.Load(), int64(7))
			t.Logf("side=%d render input opens=%d", side, reader.inputOpens.Load())
		}
	})
}
