package source

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"testing/fstest"

	"github.com/home-operations/flate/pkg/discovery"
	"github.com/home-operations/flate/pkg/source/cache"
	"github.com/home-operations/flate/pkg/task"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

//nolint:gocognit // Both kinds and both event boundaries share the same synchronized witness.
func TestAdmission_PublicationPreservesLateAuthority(t *testing.T) {
	t.Parallel()
	for _, kind := range []string{manifest.KindOCIRepository, manifest.KindGitRepository} {
		t.Run(kind, func(t *testing.T) {
			t.Parallel()
			for _, boundary := range []struct {
				name  string
				event store.EventKind
			}{
				{"declaration", store.EventObjectAdded}, {"artifact", store.EventArtifactUpdated},
			} {
				t.Run(boundary.name, func(t *testing.T) {
					t.Parallel()
					st := store.New()
					result, err := runDiscovery(t.Context(), discovery.Config{Path: t.TempDir(), Store: st})
					assert.NoError(t, err)
					t.Cleanup(result.Close)
					id := manifest.NamedResource{
						Group:     manifest.SourceDomain,
						Kind:      kind,
						Namespace: "apps",
						Name:      "shared",
					}
					reads := aliasRequestReads(t, st, id)
					tasks := task.NewBounded(2)
					controller := New(st, tasks, nil)
					controller.Configure(FetchOptions{Admission: result.Admission}, cache.RetryConfig{})
					entered, release := make(chan struct{}), make(chan struct{})
					var blocked atomic.Bool
					unsubscribe := st.AddListener(boundary.event, func(added manifest.NamedResource, _ any) {
						if added == id && blocked.CompareAndSwap(false, true) {
							close(entered)
							<-release
						}
					}, false)
					t.Cleanup(unsubscribe)
					var workers sync.WaitGroup
					var failure error
					done := make(chan struct{})
					workers.Go(func() {
						defer close(done)
						_, failure = result.Admission.Publish(
							t.Context(),
							id,
							reads,
							func(store.Child) (bool, error) { return true, nil },
						)
						if failure == nil {
							tasks.Go(
								t.Context(),
								"construct source",
								func(ctx context.Context) { failure = controller.Construct(ctx, id) },
							)
							tasks.BlockTillDone()
						}
					})
					select {
					case <-entered:
					case <-done:
						t.Fatal("publisher did not reach event boundary")
					}
					actual, bindErr := id.BindLocal("/authoritative")
					if bindErr != nil {
						close(release)
						workers.Wait()
						t.Fatal(bindErr)
					}
					st.AddObject(actual)
					material := &store.SourceArtifact{FS: fstest.MapFS{}, LocalPath: t.TempDir()}
					st.SetArtifact(id, material)
					st.UpdateStatus(
						id,
						store.StatusInfo{Status: store.StatusPending, Message: "supplied source awaits acquisition"},
					)
					close(release)
					workers.Wait()
					assert.NoError(t, failure)
					assert.Equal(t, st.GetDeclaration(id) == actual, true)
					assert.Equal(t, st.GetArtifact(id, store.ArtifactContent) == material, true)
					info, exists := st.GetStatus(id)
					assert.Equal(t, exists, true)
					assert.Equal(t, info.Status, store.StatusPending)
					assert.Equal(t, info.Message, "supplied source awaits acquisition")
				})
			}
		})
	}
}
