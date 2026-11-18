package base

import (
	"context"
	"testing"

	"github.com/home-operations/flate/internal/engine/depwait"
	"github.com/home-operations/flate/internal/engine/schedule"
	"github.com/home-operations/flate/internal/engine/source"
	"github.com/home-operations/flate/internal/engine/store"
	"github.com/home-operations/flate/internal/engine/task"
	"github.com/home-operations/flate/internal/testutil/assert"
	"github.com/home-operations/flate/pkg/manifest"
)

func TestAcquire_PreparedCleanup(t *testing.T) {
	t.Parallel()
	for _, phase := range []string{"completed", "sequential", "failed", "superseded", "settled", "retired", "closed", "invalidated"} {
		t.Run(phase, func(t *testing.T) {
			t.Parallel()
			testAcquirePreparedCleanup(t, phase)
		})
	}
}

func testAcquirePreparedCleanup(t *testing.T, phase string) {
	t.Helper()
	st := store.New()
	owner := &manifest.GitRepository{Name: "source", Namespace: "apps", URL: "https://example.test/repo"}
	st.AddObject(owner)
	tasks := task.NewBounded(2)
	run := gateExecution(New(st, tasks), owner.Named())
	defer run.Close()
	closed, fetched := 0, 0
	blocked := phase != "completed" && phase != "sequential" && phase != "failed"
	artifact := &store.SourceArtifact{}
	var acquisition Acquisition
	acquisition = Acquisition{
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
				return nil, &depwait.BlockedError{Deps: []manifest.NamedResource{owner.Named()}}
			}
			return artifact, nil
		},
		Complete: func(ctx context.Context, acquired *store.SourceArtifact, err error, level manifest.DrainLevel) error {
			assert.NoError(t, err)
			assert.Equal(t, acquired == artifact, true)
			assert.Equal(t, closed, 0)
			if phase == "sequential" && fetched == 1 {
				return run.Acquire(ctx, owner, acquisition, level)
			}
			if phase == "failed" {
				return manifest.NewError(manifest.ErrInput, nil, "dependency completion failed")
			}
			return nil
		},
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
	if phase == "completed" || phase == "superseded" || phase == "sequential" || phase == "failed" {
		wantClosed := 1
		wantFetched := 0
		switch phase {
		case "completed", "failed":
			wantFetched = 1
		case "sequential":
			wantClosed, wantFetched = 2, 2
		}
		assert.Equal(t, closed, wantClosed)
		assert.Equal(t, fetched, wantFetched)
		run.Close()
		assert.Equal(t, closed, wantClosed)
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
}
