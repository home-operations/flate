package base

import (
	"context"
	"testing"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/pkg/depwait"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/schedule"
	"github.com/home-operations/flate/pkg/source"
	"github.com/home-operations/flate/pkg/store"
	"github.com/home-operations/flate/pkg/task"
)

func TestAcquire_PreparedCleanup(t *testing.T) {
	t.Parallel()
	for _, phase := range []string{"completed", "superseded", "settled", "retired", "closed", "invalidated"} {
		t.Run(phase, func(t *testing.T) {
			t.Parallel()
			st := store.New()
			owner := &manifest.GitRepository{Name: "source", Namespace: "apps", URL: "https://example.test/repo"}
			st.AddObject(owner)
			tasks := task.NewBounded(2)
			run := gateExecution(New(st, tasks), owner.Named())
			defer run.Close()
			closed, fetched := 0, 0
			blocked := phase != "completed"
			artifact := &store.SourceArtifact{}
			acquisition := Acquisition{
				Fetch: func(context.Context) (source.PreparedFetch, error) {
					if phase == "superseded" {
						next := *owner
						next.URL += "/changed"
						st.AddObject(&next)
					}
					return source.PreparedFetch{
						Close: func() { closed++ },
						Fetch: func(context.Context, *store.SourceArtifact) (*store.SourceArtifact, error) {
							fetched++
							return artifact, nil
						},
					}, nil
				},
				Resolve: func(context.Context, manifest.DrainLevel) (*store.SourceArtifact, error) {
					if blocked {
						return nil, &depwait.ErrBlocked{Deps: []manifest.NamedResource{owner.Named()}}
					}
					return artifact, nil
				},
				Complete: func(context.Context, *store.SourceArtifact, error, manifest.DrainLevel) error { return nil },
			}
			tasks.Go(t.Context(), "acquire", func(ctx context.Context) {
				run.Dispatch(
					ctx,
					manifest.DrainNone,
					owner,
					func(owner *manifest.GitRepository, ctx context.Context, _ manifest.BaseManifest, level manifest.DrainLevel) error {
						return run.Acquire(ctx, owner, acquisition, level)
					},
				)
			})
			tasks.BlockTillDone()
			if phase == "completed" || phase == "superseded" {
				assert.Equal(t, closed, 1)
				wantFetched := 0
				if phase == "completed" {
					wantFetched = 1
				}
				assert.Equal(t, fetched, wantFetched)
				return
			}
			assert.Equal(t, closed, 0)
			assert.Equal(t, run.Outcome(), schedule.Blocked)
			switch phase {
			case "settled":
				blocked = false
				tasks.Go(t.Context(), "settle", func(ctx context.Context) { run.Settle(ctx, manifest.DrainCascade) })
				tasks.BlockTillDone()
				assert.Equal(t, fetched, 1)
			case "retired":
				run.Retire()
			case "closed":
				run.Close()
			case "invalidated":
				next := *owner
				next.URL += "/changed"
				st.AddObject(&next)
				run.Settle(t.Context(), manifest.DrainCascade)
			}
			assert.Equal(t, closed, 1)
			run.Close()
			assert.Equal(t, closed, 1)
		})
	}
}
