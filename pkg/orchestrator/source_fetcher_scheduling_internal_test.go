package orchestrator

import (
	"context"
	"slices"
	"strconv"
	"sync/atomic"
	"testing"

	src "github.com/home-operations/flate/internal/source"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/controllers/base"
	"github.com/home-operations/flate/internal/depwait"
	"github.com/home-operations/flate/internal/schedule"
	sourcecache "github.com/home-operations/flate/internal/source/cache"
	"github.com/home-operations/flate/internal/store"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/internal/testutil/controllertest"
	"github.com/home-operations/flate/pkg/manifest"
)

type emptySourceFetcher struct{}

func (emptySourceFetcher) Fetch(
	context.Context,
	manifest.BaseManifest,
	*store.ReadSet,
	*sourcecache.Policy,
) (src.PreparedFetch, error) {
	return src.PreparedFetch{
		Fetch: func(ctx context.Context, local *store.SourceArtifact) (*store.SourceArtifact, error) {
			if local != nil {
				return local, ctx.Err()
			}

			return nil, nil //nolint:nilnil // A nil result denotes successful absence.
		},
	}, nil
}

//nolint:paralleltest // The test calls a helper that changes process-global state.
func TestDAG_SourceWithoutFetcherIsNotSeeded(t *testing.T) {
	for _, tc := range []struct {
		fetcher    bool
		dispatched int64
	}{{false, 0}, {true, 1}} {
		t.Run(strconv.FormatBool(tc.fetcher), func(t *testing.T) {
			testDAGSourceWithoutFetcherIsNotSeededCase(t, &tc)
		},
		)
	}
}

//nolint:paralleltest // The test calls a helper that changes process-global state.
func TestSourceFetchers_ExistenceOnlyDisabled(t *testing.T) {
	dir := t.TempDir()
	o, err := newTestOrchestrator(t, Tree{Path: dir, RepoRoot: dir}, Config{Concurrency: 2})
	assert.NoError(t, err)
	t.Cleanup(func() { o.stop() })
	repo := &manifest.HelmRepository{Name: "repo", Namespace: "ns", URL: "https://charts.example"}
	o.store.AddObject(repo)
	seeds, err := o.seedNodes()
	assert.NoError(t, err)

	{
		f, registered := o.src.Fetchers[manifest.KindHelmRepository]
		assert.FatalIf(t, !registered || f != nil || !o.schedulable(repo) ||
			!slices.Contains(
				seeds,
				repo.Named(),
			), "%v", "default nil reservation did not enable scheduling")
	}

	delete(o.src.Fetchers, manifest.KindHelmRepository)
	seeds, err = o.seedNodes()
	assert.NoError(t, err)

	assert.FatalIf(
		t,
		o.schedulable(repo) || slices.Contains(seeds, repo.Named()),
		"%v",
		"unregistered existence-only source still scheduled",
	)

	{
		_, registered := o.src.Fetchers[manifest.KindHelmRepository]
		assert.FatalIf(t, registered, "%v", "nil override retained map entry")
	}
}

func testDAGSourceWithoutFetcherIsNotSeededCase(
	t *testing.T,
	testCaseInput *struct {
		fetcher bool

		dispatched int64
	},
) {
	t.Helper()

	dir := t.TempDir()
	o, err := newTestOrchestrator(
		t,
		Tree{Path: dir, RepoRoot: dir},
		Config{CacheDir: t.TempDir(), Concurrency: 2},
	)
	assert.FatalIf(t, err != nil, "%v", err)
	defer func() {
		o.stop()
	}()
	delete(o.src.Fetchers, manifest.KindExternalArtifact)
	if testCaseInput.fetcher {
		o.src.Fetchers[manifest.KindExternalArtifact] = emptySourceFetcher{}
	}
	obj := &manifest.ExternalArtifact{Name: "external", Namespace: "apps"}
	o.store.AddObject(obj)
	seeds, err := o.seedNodes()
	assert.NoError(t, err)
	assert.Equal(t, slices.Contains(seeds, obj.Named()), testCaseInput.fetcher)
	obj = &manifest.ExternalArtifact{Name: "arriving", Namespace: "apps"}
	producer, consumer := testutil.KustomizationID(
		"flux-system",
		"producer",
	), testutil.KustomizationID(
		"flux-system",
		"consumer",
	)
	o.store.AddObject(&manifest.Kustomization{Name: consumer.Name, Namespace: consumer.Namespace})
	attempted := make(chan struct{})
	var dispatched atomic.Int64
	var completed atomic.Bool
	d := fixtureDispatcher{
		store: o.store,
		dispatch: func(ctx context.Context, id schedule.NodeID, level manifest.DrainLevel, _ schedule.Result) schedule.Result {
			switch id {
			case producer:
				o.tasks.YieldSlot(func() { <-attempted })
				o.store.AddObject(obj)
			case consumer:
				execution := controllertest.NewExecution(base.New(o.store, o.tasks), id)
				execution.Dispatch(
					ctx,
					level,
					execution,
					func(execution *base.Execution, _ context.Context, _ *manifest.Kustomization, _ manifest.DrainLevel) error {
						if execution.Reads.GetObject(obj.Named()) == nil {
							close(attempted)
							return &depwait.ErrBlocked{Deps: []manifest.NamedResource{obj.Named()}}
						}
						completed.Store(true)
						return nil
					},
				)
				return schedule.Result{Assessment: execution}
			// Sources must be scheduled under the identity they reconcile.
			case obj.Named():
				dispatched.Add(1)
				return dagDispatcher{o}.Dispatch(ctx, id, level, schedule.Result{})
			}
			return schedule.Result{}
		},
	}
	sched := schedule.New(o.tasks, d)
	unsubscribe := o.watchDAG(sched)
	defer unsubscribe()
	sched.Seed([]schedule.NodeID{producer, consumer})
	{
		err := sched.Run(t.Context())
		assert.FatalIf(t, err != nil, "%v", err)
	}
	assert.Equal(t, completed.Load(), true)
	assert.Equal(t, dispatched.Load(), testCaseInput.dispatched)
}
