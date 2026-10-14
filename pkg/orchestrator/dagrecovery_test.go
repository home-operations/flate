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
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/schedule"
	"github.com/home-operations/flate/pkg/source"
	"github.com/home-operations/flate/pkg/store"
)

type arrivalDispatcher func(context.Context, schedule.NodeID, manifest.DrainLevel) (schedule.Outcome, []schedule.NodeID, bool)

func (d arrivalDispatcher) Dispatch(ctx context.Context, id schedule.NodeID, level manifest.DrainLevel) (schedule.Outcome, []schedule.NodeID, bool) {
	return d(ctx, id, level)
}

func TestDAG_SourceWithoutFetcherIsNotSeeded(t *testing.T) {
	for _, tc := range []struct {
		fetcher    bool
		dispatched int64
	}{{false, 0}, {true, 1}} {
		t.Run(fmt.Sprint(tc.fetcher), func(t *testing.T) {
			dir := t.TempDir()
			o, err := New(Config{Path: dir, RepoRoot: dir, CacheDir: t.TempDir(), Concurrency: 2})
			if err != nil {
				t.Fatal(err)
			}
			defer o.Stop()
			delete(o.src.Fetchers, manifest.KindExternalArtifact)
			if tc.fetcher {
				o.src.Fetchers[manifest.KindExternalArtifact] = source.ExistenceFetcher{}
			}
			obj := &manifest.ExternalArtifact{Name: "external", Namespace: "apps"}
			o.store.AddObject(obj)
			assert.Equal(t, slices.Contains(o.seedNodes(), obj.Named()), tc.fetcher)
			obj = &manifest.ExternalArtifact{Name: "arriving", Namespace: "apps"}
			producer, consumer := ksID("producer"), ksID("consumer")
			attempted := make(chan struct{})
			var dispatched atomic.Int64
			var completed atomic.Bool
			d := arrivalDispatcher(func(ctx context.Context, id schedule.NodeID, level manifest.DrainLevel) (schedule.Outcome, []schedule.NodeID, bool) {
				switch id {
				case producer:
					o.tasks.YieldSlot(func() { <-attempted })
					o.store.AddObject(obj)
				case consumer:
					if o.store.GetObject(obj.Named()) == nil {
						close(attempted)
						return schedule.OutcomeBlocked, []schedule.NodeID{obj.Named()}, false
					}
					completed.Store(true)
				case obj.Named():
					dispatched.Add(1)
					return dagDispatcher{o}.Dispatch(ctx, id, level)
				}
				return schedule.OutcomeTerminal, nil, false
			})
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
	o, err := New(Config{Path: dir, RepoRoot: dir, WipeSecrets: true, CacheDir: t.TempDir(), Concurrency: 2})
	if err != nil {
		t.Fatal(err)
	}
	if err := o.Bootstrap(t.Context()); err != nil {
		t.Fatal(err)
	}
	o.configureControllers()
	d := dagDispatcher{o}
	victim, ghost := ksID("consumer"), ksID("ghost")
	out, deps, rerun := d.Dispatch(t.Context(), victim, manifest.DrainCascade)
	if rerun || out != schedule.OutcomeDependencyFailed || !slices.Equal(deps, []manifest.NamedResource{ghost}) {
		t.Fatalf("derived failure = %v/%v, want ghost blocker", out, deps)
	}
	o.store.AddObject(&manifest.Kustomization{Name: ghost.Name, Namespace: ghost.Namespace})
	o.store.UpdateStatus(ghost, store.StatusReady, "")
	out, deps, rerun = d.Dispatch(t.Context(), victim, manifest.DrainNone)
	info, _ := o.store.GetStatus(victim)
	if rerun || out != schedule.OutcomeTerminal || len(deps) != 0 || info.Status != store.StatusReady || len(info.BlockedBy()) != 0 || len(o.store.FailedResources()) != 0 {
		t.Fatalf("recovery = %v/%v status=%+v blockers=%v failures=%v", out, deps, info, info.BlockedBy(), o.store.FailedResources())
	}
	ks, _ := o.store.Get[*manifest.Kustomization](victim)
	ks = ks.Clone()
	ks.Path = "./missing-directory"
	o.store.AddObject(ks)
	o.store.UpdateFailure(victim, &manifest.DependencyFailedError{Parent: victim, Failed: []manifest.NamedResource{ghost}})
	out, deps, rerun = d.Dispatch(t.Context(), victim, manifest.DrainNone)
	info, _ = o.store.GetStatus(victim)
	if rerun || out != schedule.OutcomeTerminal || len(deps) != 0 || info.Status != store.StatusFailed || len(info.BlockedBy()) != 0 {
		t.Fatalf("primary failure misclassified = %v/%v status=%+v blockers=%v", out, deps, info, info.BlockedBy())
	}
	o.Stop()
}

func TestDAG_RedispatchErrorPropagates(t *testing.T) {
	dir := t.TempDir()
	testutil.WriteFile(t, dir, "flux/app.yaml", ksYAML("app", "app", ""))
	testutil.WriteFile(t, dir, "app/kustomization.yaml", "resources:\n- cm.yaml\n")
	testutil.WriteFile(t, dir, "app/cm.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: app}\ndata: {k: v}\n")
	o, err := New(Config{Path: dir, RepoRoot: dir, WipeSecrets: true, CacheDir: t.TempDir(), Concurrency: 2})
	if err != nil {
		t.Fatal(err)
	}
	victim := ksID("app")
	unsub := o.store.AddListener(store.EventStatusUpdated, func(id manifest.NamedResource, payload any) {
		if info, ok := payload.(store.StatusInfo); ok && id == victim && info.Status == store.StatusReady {
			o.store.Refire(id)
		}
	}, false)
	defer unsub()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	_, err = o.Render(ctx)
	if err == nil || !strings.Contains(err.Error(), "exceeded 32 redispatches") || ctx.Err() != nil {
		t.Fatalf("Render error=%v context=%v, want scheduler redispatch error", err, ctx.Err())
	}
}
