package orchestrator

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/controllers/base"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/schedule"
	"github.com/home-operations/flate/pkg/store"
)

type fixtureDispatcher struct {
	store    *store.Store
	dispatch func(context.Context, schedule.NodeID, manifest.DrainLevel) schedule.Result
}

func (d fixtureDispatcher) Dispatch(
	ctx context.Context,
	id schedule.NodeID,
	level manifest.DrainLevel,
) schedule.Result {
	return d.dispatch(ctx, id, level)
}

func (d fixtureDispatcher) Sweep(context.Context, bool) []schedule.NodeID {
	return slices.Collect(d.store.Sweep())
}

//nolint:paralleltest // The test calls a helper that changes process-global state.
func TestDAGDispatcher_RecoveredAndPrimaryFailures(t *testing.T) {
	for _, workers := range []int{2, 16} {
		t.Run(fmt.Sprintf("workers=%d", workers), func(t *testing.T) {
			testDAGDispatcherRecoveredAndPrimaryFailuresCase(
				t,
				&workers,
			)
		},
		)
	}
}

//nolint:paralleltest // The test calls a helper that changes process-global state.
func TestDAG_RedispatchErrorPropagates(t *testing.T) {
	dir := t.TempDir()
	testutil.WriteFile(t, dir, "flux/app.yaml", ksYAML("app", "app", ""))
	testutil.WriteFile(t, dir, "app/kustomization.yaml", "resources:\n- cm.yaml\n")
	testutil.WriteFile(t, dir, "app/cm.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: app}\ndata: {k: v}\n")
	o, err := newTestOrchestrator(
		t,
		Tree{Path: dir, RepoRoot: dir},
		Config{CacheDir: t.TempDir(), Concurrency: 2},
	)
	assert.FatalIf(t, err != nil, "%v", err)
	victim := testutil.KustomizationID("flux-system", "app")
	unsub := o.store.AddListener(store.EventStatusUpdated, func(id manifest.NamedResource, payload any) {
		if info, ok := payload.(store.StatusInfo); ok && id == victim && info.Status == store.StatusReady {
			current, _ := o.store.Get[*manifest.Kustomization](id)
			fresh := current.Clone()
			if fresh.Path == "./app" {
				fresh.Path = "app"
			} else {
				fresh.Path = "./app"
			}
			o.store.AddObject(fresh)
		}
	}, false)
	defer unsub()
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	_, err = o.render(ctx)
	assert.FatalIf(t, err == nil || !strings.Contains(err.Error(), "exceeded 32 redispatches") || ctx.Err() != nil,
		"Render error=%v context=%v, want scheduler redispatch error", err, ctx.Err())
}

func testDAGDispatcherRecoveredAndPrimaryFailuresCase(
	t *testing.T,
	testCaseInput *int,
) {
	t.Helper()

	dir := t.TempDir()
	testutil.WriteFile(t, dir, "flux/consumer.yaml", ksYAML("consumer", "consumer", "ghost"))
	testutil.WriteFile(t, dir, "consumer/kustomization.yaml", "resources:\n- cm.yaml\n")
	testutil.WriteFile(
		t,
		dir,
		"consumer/cm.yaml",
		"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: consumer}\ndata: {k: v}\n",
	)
	o, err := newTestOrchestrator(
		t,
		Tree{Path: dir, RepoRoot: dir},
		Config{CacheDir: t.TempDir(), Concurrency: (*testCaseInput)},
	)
	assert.FatalIf(t, err != nil, "%v", err)
	{
		err := o.bootstrap(t.Context())
		assert.FatalIf(t, err != nil, "%v", err)
	}
	reconcileEntryForTest(t, o)
	o.configureControllers()
	d := dagDispatcher{o}
	victim, ghost := testutil.KustomizationID(
		"flux-system",
		"consumer",
	), testutil.KustomizationID(
		"flux-system",
		"ghost",
	)
	var result schedule.Result
	o.tasks.Go(
		t.Context(),
		"dispatch fixture",
		func(ctx context.Context) { result = d.Dispatch(ctx, victim, manifest.DrainCascade) },
	)
	o.tasks.BlockTillDone()
	info, _ := o.store.GetStatus(victim)
	assert.FatalIf(t, result.Assessment.Outcome() == schedule.Blocked || !info.IsDependencyFailure() ||
		!slices.Equal(
			info.BlockedBy(),
			[]manifest.NamedResource{ghost},
		), "derived failure = %+v, want ghost blocker", info)
	o.store.AddObject(&manifest.Kustomization{Name: ghost.Name, Namespace: ghost.Namespace})
	o.store.UpdateStatus(ghost, store.StatusInfo{Status: store.StatusReady, Reason: "", Message: ""})
	o.tasks.Go(
		t.Context(),
		"dispatch fixture",
		func(ctx context.Context) { result = d.Dispatch(ctx, victim, manifest.DrainNone) },
	)
	o.tasks.BlockTillDone()
	info, _ = o.store.GetStatus(victim)
	assert.FatalIf(t, result.Assessment.Outcome() == schedule.Blocked || info.Status != store.StatusReady ||
		len(info.BlockedBy()) != 0 ||
		len(o.store.FailedResources()) != 0,

		"recovery status=%+v blockers=%v failures=%v",
		info,
		info.BlockedBy(),
		o.store.FailedResources())

	ks, _ := o.store.Get[*manifest.Kustomization](victim)
	ks = ks.Clone()
	ks.Path = "./missing-directory"
	o.store.AddObject(ks)
	o.store.UpdateFailure(
		victim,
		&manifest.DependencyFailedError{Parent: victim, Failed: []manifest.NamedResource{ghost}},
	)
	o.tasks.Go(
		t.Context(),
		"dispatch fixture",
		func(ctx context.Context) { result = d.Dispatch(ctx, victim, manifest.DrainNone) },
	)
	o.tasks.BlockTillDone()
	info, _ = o.store.GetStatus(victim)
	assert.FatalIf(t, result.Assessment.Outcome() == schedule.Blocked || info.Status != store.StatusFailed ||
		len(info.BlockedBy()) != 0,
		"primary failure misclassified: status=%+v blockers=%v", info, info.BlockedBy())

	t.Run("released pin recovery", func(t *testing.T) {
		t.Parallel()
		testDAGReleasedPinRecovery(t, dir, testCaseInput, ks)
	})
	o.stop()
}

func testDAGReleasedPinRecovery(t *testing.T, dir string, workers *int, ks *manifest.Kustomization) {
	t.Helper()
	localO, err := newTestOrchestrator(
		t,
		Tree{Path: dir, RepoRoot: dir},
		Config{CacheDir: t.TempDir(), Concurrency: (*workers)},
	)
	assert.FatalIf(t, err != nil, "%v", err)
	defer localO.stop()
	localO.configureControllers()
	d := dagDispatcher{localO}
	consumer := ks.Clone()
	consumer.Name, consumer.Path, consumer.DependsOn = "released-consumer", "./consumer", nil
	consumer.SourceRef.Kind, consumer.SourceRef.Name = manifest.KindOCIRepository, "app"
	repo := &manifest.OCIRepository{
		Name:      "app",
		Namespace: consumer.Namespace,
		URL:       "oci://fixture.invalid/app",
	}
	fetcher := &generatedOCIFetcher{root: dir}
	localO.src.Fetchers[manifest.KindOCIRepository] = fetcher
	localO.src.StartLifecycle()
	localO.ksc.StartLifecycle()
	localO.store.AddObject(repo)
	localO.store.AddObject(consumer)
	localO.store.SetArtifact(
		repo.Named(),
		&store.SourceArtifact{Kind: manifest.KindOCIRepository, LocalPath: dir, Key: "fixture"},
	)
	// Ready must precede the consumer's first blocked-edge registration.
	localO.store.UpdateStatus(repo.Named(), store.StatusInfo{Status: store.StatusReady, Reason: "", Message: ""})
	attempted, acquired := make(chan struct{}), make(chan struct{})
	dispatch := fixtureDispatcher{
		store: localO.store,
		dispatch: func(ctx context.Context, id schedule.NodeID, level manifest.DrainLevel) schedule.Result {
			assert.Equal(t, level, manifest.DrainNone)
			if id == repo.Named() {
				localO.tasks.YieldSlot(func() {
					select {
					case <-attempted:
					case <-ctx.Done():
					}
				})
				result := d.Dispatch(ctx, id, level)
				info, _ := localO.store.GetStatus(id)
				assert.Equal(t, info.Status, store.StatusReady)
				assert.Equal(t, fetcher.fetched.Load(), true)
				close(acquired)
				return result
			}
			result := d.Dispatch(ctx, id, level)
			if result.Assessment.Outcome() == schedule.Blocked {
				assert.Diff(t, result.Assessment.(*base.Execution).Blocked, []schedule.NodeID{repo.Named()})
				close(attempted)

				localO.tasks.YieldSlot(func() {
					select {
					case <-acquired:
					case <-ctx.Done():
					}
				})
			}
			return result
		},
	}
	sched := schedule.New(localO.tasks, dispatch)
	unsubscribe := localO.watchDAG(sched)
	defer unsubscribe()

	sched.Seed([]schedule.NodeID{repo.Named(), consumer.Named()})
	{
		err := sched.Run(t.Context())
		assert.FatalIf(t, err != nil, "%v", err)
	}
	localO.tasks.BlockTillDone()
	assert.Equal(t, localO.tasks.Failures(), int64(0))
	assert.Equal(t, t.Context().Err(), nil)
	assert.Equal(t, len(sched.Results()), 2)
	assert.Equal(t, len(localO.store.FailedResources()), 0)
	info, _ := localO.store.GetStatus(consumer.Named())
	assert.Equal(t, info.Status, store.StatusReady)
	art, ok := localO.store.GetArtifact(consumer.Named(), store.ArtifactContent).(*store.Emission)
	assert.FatalIf(t, !ok || len(art.Manifests) != 1, "recovered output = %+v", art)
	assert.Diff(t, art.Manifests[0].Document["data"], map[string]any{"k": "v"})
}
