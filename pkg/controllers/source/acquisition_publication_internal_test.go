package source

import (
	"context"
	"testing"
	"testing/fstest"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/schedule"
	src "github.com/home-operations/flate/pkg/source"
	"github.com/home-operations/flate/pkg/store"
)

func TestController_PublicationPreservesAcquisitionAuthority(t *testing.T) {
	t.Parallel()
	for _, change := range []string{"declaration"} {
		t.Run(change, func(t *testing.T) {
			t.Parallel()
			entered, release := make(chan struct{}), make(chan struct{})
			fetcher := &fakeFetcher{
				artifact: &store.SourceArtifact{FS: fstest.MapFS{}, Content: "fetched"},
				during: func() {
					close(entered)
					<-release
				},
			}
			controller, st := newController(t, map[string]src.Fetcher{manifest.KindGitRepository: fetcher})
			repository := &manifest.GitRepository{
				Name:      "source",
				Namespace: "apps",
				URL:       "https://fixture.invalid/first",
			}
			st.AddObject(repository)
			controller.Tasks.Go(t.Context(), "source", func(ctx context.Context) {
				execution := controller.ReconcileNode(ctx, repository.Named(), manifest.DrainNone, schedule.Result{})
				execution.Close()
			})
			<-entered
			actual := repository
			if change == "declaration" {
				replacement := *repository
				replacement.URL = "https://fixture.invalid/second"
				actual = &replacement
				st.AddObject(actual)
			}
			supplied := &store.SourceArtifact{FS: fstest.MapFS{}, Content: "supplied"}
			st.SetArtifact(repository.Named(), supplied)
			st.UpdateStatus(
				repository.Named(),
				store.StatusInfo{Status: store.StatusPending, Message: "supplied source awaits acquisition"},
			)
			close(release)
			controller.Tasks.BlockTillDone()
			assert.Equal(t, st.GetDeclaration(repository.Named()) == actual, true)
			assert.Equal(t, st.GetArtifact(repository.Named(), store.ArtifactContent) == supplied, true)
			info, exists := st.GetStatus(repository.Named())
			assert.Equal(t, exists, true)
			if change == "declaration" {
				assert.Equal(t, info.Status, store.StatusPending)
				assert.Equal(t, info.Message, "supplied source awaits acquisition")
			} else {
				assert.Equal(t, info.Status, store.StatusReady)
				assert.Equal(t, fetcher.calls, 1)
			}
		})
	}
}
