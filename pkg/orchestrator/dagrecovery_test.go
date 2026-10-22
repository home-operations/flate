package orchestrator

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/internal/testutil/controllertest"
	"github.com/home-operations/flate/pkg/controllers/base"
	"github.com/home-operations/flate/pkg/depwait"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/schedule"
	sourcecache "github.com/home-operations/flate/pkg/source/cache"
	"github.com/home-operations/flate/pkg/store"
)

type emptySourceFetcher struct{}

func (emptySourceFetcher) Fetch(context.Context, manifest.BaseManifest, *store.ReadSet, sourcecache.Policy) (*store.SourceArtifact, error) {
	return nil, nil
}

type fixtureDispatcher struct {
	store    *store.Store
	dispatch func(context.Context, schedule.NodeID, manifest.DrainLevel) schedule.Result
}

func (d fixtureDispatcher) Dispatch(ctx context.Context, id schedule.NodeID, level manifest.DrainLevel) schedule.Result {
	return d.dispatch(ctx, id, level)
}

func (d fixtureDispatcher) Sweep(context.Context, bool) []schedule.NodeID {
	return slices.Collect(d.store.Sweep())
}

func TestDAG_SourceWithoutFetcherIsNotSeeded(t *testing.T) {
	for _, tc := range []struct {
		fetcher    bool
		dispatched int64
	}{{false, 0}, {true, 1}} {
		t.Run(fmt.Sprint(tc.fetcher), func(t *testing.T) {
			dir := t.TempDir()
			o, err := New(t.Context(), Config{Path: dir, RepoRoot: dir, CacheDir: t.TempDir(), Concurrency: 2})
			if err != nil {
				t.Fatal(err)
			}
			defer func() {
				if err := o.Stop(); err != nil {
					t.Errorf("Stop: %v", err)
				}
			}()
			delete(o.src.Fetchers, manifest.KindExternalArtifact)
			if tc.fetcher {
				o.src.Fetchers[manifest.KindExternalArtifact] = emptySourceFetcher{}
			}
			obj := &manifest.ExternalArtifact{Name: "external", Namespace: "apps"}
			o.store.AddObject(obj)
			assert.Equal(t, slices.Contains(o.seedNodes(), obj.Named()), tc.fetcher)
			obj = &manifest.ExternalArtifact{Name: "arriving", Namespace: "apps"}
			producer, consumer := testutil.KustomizationID("flux-system", "producer"), testutil.KustomizationID("flux-system", "consumer")
			o.store.AddObject(&manifest.Kustomization{Name: consumer.Name, Namespace: consumer.Namespace})
			attempted := make(chan struct{})
			var dispatched atomic.Int64
			var completed atomic.Bool
			d := fixtureDispatcher{store: o.store, dispatch: func(ctx context.Context, id schedule.NodeID, level manifest.DrainLevel) schedule.Result {
				switch id {
				case producer:
					o.tasks.YieldSlot(func() { <-attempted })
					o.store.AddObject(obj)
				case consumer:
					execution := controllertest.NewExecution(base.New(o.store, o.tasks, "fixture"), id)
					execution.Dispatch(ctx, level, func(context.Context, *manifest.Kustomization, manifest.DrainLevel) error {
						if execution.Reads.GetObject(obj.Named()) == nil {
							close(attempted)
							return &depwait.ErrBlocked{Deps: []manifest.NamedResource{obj.Named()}}
						}
						completed.Store(true)
						return nil
					})
					return schedule.Result{Assessment: execution}
				case obj.Named():
					dispatched.Add(1)
					return dagDispatcher{o}.Dispatch(ctx, id, level)
				}
				return schedule.Result{}
			}}
			sched := schedule.New(o.tasks, d)
			unsubscribe := o.watchDAG(sched)
			defer unsubscribe()
			sched.Seed([]schedule.NodeID{producer, consumer})
			if err := sched.Run(t.Context()); err != nil {
				t.Fatal(err)
			}
			assert.Equal(t, completed.Load(), true)
			assert.Equal(t, dispatched.Load(), tc.dispatched)
		})
	}
}

func TestDAGDispatcher_RecoveredAndPrimaryFailures(t *testing.T) {
	dir := t.TempDir()
	testutil.WriteFile(t, dir, "flux/consumer.yaml", ksYAML("consumer", "consumer", "ghost"))
	testutil.WriteFile(t, dir, "consumer/kustomization.yaml", "resources:\n- cm.yaml\n")
	testutil.WriteFile(t, dir, "consumer/cm.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: consumer}\ndata: {k: v}\n")
	o, err := New(t.Context(), Config{Path: dir, RepoRoot: dir, WipeSecrets: true, CacheDir: t.TempDir(), Concurrency: 2})
	if err != nil {
		t.Fatal(err)
	}
	if err := o.Bootstrap(t.Context()); err != nil {
		t.Fatal(err)
	}
	o.configureControllers()
	d := dagDispatcher{o}
	victim, ghost := testutil.KustomizationID("flux-system", "consumer"), testutil.KustomizationID("flux-system", "ghost")
	result := d.Dispatch(t.Context(), victim, manifest.DrainCascade)
	info, _ := o.store.GetStatus(victim)
	if result.Assessment.Outcome() == schedule.Blocked || !info.IsDependencyFailure() || !slices.Equal(info.BlockedBy(), []manifest.NamedResource{ghost}) {
		t.Fatalf("derived failure = %+v, want ghost blocker", info)
	}
	o.store.AddObject(&manifest.Kustomization{Name: ghost.Name, Namespace: ghost.Namespace})
	o.store.UpdateStatus(ghost, store.StatusReady, "", "")
	result = d.Dispatch(t.Context(), victim, manifest.DrainNone)
	info, _ = o.store.GetStatus(victim)
	if result.Assessment.Outcome() == schedule.Blocked || info.Status != store.StatusReady || len(info.BlockedBy()) != 0 || len(o.store.FailedResources()) != 0 {
		t.Fatalf("recovery status=%+v blockers=%v failures=%v", info, info.BlockedBy(), o.store.FailedResources())
	}
	ks, _ := o.store.Get[*manifest.Kustomization](victim)
	ks = ks.Clone()
	ks.Path = "./missing-directory"
	o.store.AddObject(ks)
	o.store.UpdateFailure(victim, &manifest.DependencyFailedError{Parent: victim, Failed: []manifest.NamedResource{ghost}})
	result = d.Dispatch(t.Context(), victim, manifest.DrainNone)
	info, _ = o.store.GetStatus(victim)
	if result.Assessment.Outcome() == schedule.Blocked || info.Status != store.StatusFailed || len(info.BlockedBy()) != 0 {
		t.Fatalf("primary failure misclassified: status=%+v blockers=%v", info, info.BlockedBy())
	}
	t.Run("released pin recovery", func(t *testing.T) {
		o, err := New(t.Context(), Config{Path: dir, RepoRoot: dir, WipeSecrets: true, CacheDir: t.TempDir(), Concurrency: 2})
		if err != nil {
			t.Fatal(err)
		}
		defer o.Stop()
		o.configureControllers()
		d := dagDispatcher{o}
		consumer := ks.Clone()
		consumer.Name, consumer.Path, consumer.DependsOn = "released-consumer", "./consumer", nil
		consumer.SourceRef.Kind, consumer.SourceRef.Name = manifest.KindOCIRepository, "app"
		repo := &manifest.OCIRepository{Name: "app", Namespace: consumer.Namespace, URL: "oci://fixture.invalid/app"}
		fetcher := &generatedOCIFetcher{root: dir}
		o.src.Fetchers[manifest.KindOCIRepository] = fetcher
		o.src.StartLifecycle()
		o.ksc.StartLifecycle()
		o.store.AddObject(repo)
		o.store.AddObject(consumer)
		o.store.SetArtifact(repo.Named(), &store.SourceArtifact{Kind: manifest.KindOCIRepository, LocalPath: dir, Key: "fixture"})
		o.store.UpdateStatus(repo.Named(), store.StatusReady, "", "")
		attempted, acquired := make(chan struct{}), make(chan struct{})
		dispatch := fixtureDispatcher{store: o.store, dispatch: func(ctx context.Context, id schedule.NodeID, level manifest.DrainLevel) schedule.Result {
			assert.Equal(t, level, manifest.DrainNone)
			if id == repo.Named() {
				o.tasks.YieldSlot(func() {
					select {
					case <-attempted:
					case <-ctx.Done():
					}
				})
				result := d.Dispatch(ctx, id, level)
				info, _ := o.store.GetStatus(id)
				assert.Equal(t, info.Status, store.StatusReady)
				assert.Equal(t, fetcher.fetched.Load(), true)
				close(acquired)
				return result
			}
			result := d.Dispatch(ctx, id, level)
			if result.Assessment.Outcome() == schedule.Blocked {
				assert.Diff(t, result.Assessment.(*base.Execution).Blocked, []schedule.NodeID{repo.Named()})
				close(attempted)
				// Ready must precede the consumer's first blocked-edge registration.
				o.tasks.YieldSlot(func() {
					select {
					case <-acquired:
					case <-ctx.Done():
					}
				})
			}
			return result
		}}
		sched := schedule.New(o.tasks, dispatch)
		unsubscribe := o.watchDAG(sched)
		defer unsubscribe()
		// Sources must be scheduled under the identity they reconcile.
		sched.Seed([]schedule.NodeID{repo.Named(), consumer.Named()})
		if err := sched.Run(t.Context()); err != nil {
			t.Fatal(err)
		}
		info, _ := o.store.GetStatus(consumer.Named())
		assert.Equal(t, info.Status, store.StatusReady)
		art, ok := o.store.GetArtifact(consumer.Named()).(*store.Emission)
		if !ok || len(art.Manifests) != 1 {
			t.Fatalf("recovered output = %+v", art)
		}
		assert.Diff(t, art.Manifests[0]["data"], map[string]any{"k": "v"})
	})
	if err := o.Stop(); err != nil {
		t.Fatalf("Stop: %v", err)
	}
}

func TestDAG_RedispatchErrorPropagates(t *testing.T) {
	dir := t.TempDir()
	testutil.WriteFile(t, dir, "flux/app.yaml", ksYAML("app", "app", ""))
	testutil.WriteFile(t, dir, "app/kustomization.yaml", "resources:\n- cm.yaml\n")
	testutil.WriteFile(t, dir, "app/cm.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: app}\ndata: {k: v}\n")
	o, err := New(t.Context(), Config{Path: dir, RepoRoot: dir, WipeSecrets: true, CacheDir: t.TempDir(), Concurrency: 2})
	if err != nil {
		t.Fatal(err)
	}
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
	_, err = o.Render(ctx)
	if err == nil || !strings.Contains(err.Error(), "exceeded 32 redispatches") || ctx.Err() != nil {
		t.Fatalf("Render error=%v context=%v, want scheduler redispatch error", err, ctx.Err())
	}
}

func TestWithFetcher_ExistenceOnlyDisabled(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	o, err := New(t.Context(), Config{Path: dir, RepoRoot: dir, Concurrency: 2})
	assert.NoError(t, err)
	t.Cleanup(func() { assert.NoError(t, o.Stop()) })
	repo := &manifest.HelmRepository{Name: "repo", Namespace: "ns", URL: "https://charts.example"}
	o.store.AddObject(repo)
	if f, registered := o.src.Fetchers[manifest.KindHelmRepository]; !registered || f != nil || !o.schedulable(repo) || !slices.Contains(o.seedNodes(), repo.Named()) {
		t.Fatal("default nil reservation did not enable scheduling")
	}
	o.WithFetcher(manifest.KindHelmRepository, nil)
	if o.schedulable(repo) || slices.Contains(o.seedNodes(), repo.Named()) {
		t.Fatal("unregistered existence-only source still scheduled")
	}
	if _, registered := o.src.Fetchers[manifest.KindHelmRepository]; registered {
		t.Fatal("nil override retained map entry")
	}
}
