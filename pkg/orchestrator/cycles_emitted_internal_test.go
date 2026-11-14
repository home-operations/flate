package orchestrator

import (
	"context"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/home-operations/flate/internal/admissiontest"
	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/internal/testutil/controllertest"
	"github.com/home-operations/flate/pkg/baseline"
	"github.com/home-operations/flate/pkg/change"
	"github.com/home-operations/flate/pkg/controllers/base"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/schedule"
	"github.com/home-operations/flate/pkg/store"
	"github.com/home-operations/flate/pkg/task"
)

func TestFailDependsOnCycles_EmittedSelection(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name               string
		changedOnly, cycle bool
	}{
		{name: "full/acyclic"},
		{name: "full/cycle", cycle: true},
		{name: "changed/removal", changedOnly: true},
		{name: "changed/introduction", changedOnly: true, cycle: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			testEmittedCycleSelection(t, tc.changedOnly, tc.cycle)
		})
	}
}

func testEmittedCycleSelection(t *testing.T, changedOnly, cycle bool) {
	t.Helper()
	st, tasks := store.New(), task.NewBounded(2)
	a := makeKS("a", "flux-system")
	b := makeKS("b", "flux-system", a.Named())
	parent := makeKS("parent", "flux-system")
	if cycle {
		a = makeKS("a", "flux-system", b.Named())
	}
	st.AddObjects([]manifest.BaseManifest{a, parent})
	root := t.TempDir()
	index := &manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{
		a.Named():      {File: "a.yaml"},
		parent.Named(): {File: "parent.yaml"},
		b.Named(): {
			File:   "parent/b.yaml",
			Path:   filepath.Join(root, "parent", "b.yaml"),
			Parent: parent.Named(),
			Productions: []manifest.Production{
				{Owner: parent.Named(), Authored: b.Named(), File: "parent/b.yaml", Source: manifest.BootstrapSourceID},
			},
		},
	}}
	st.SetDiscoveryIndex(index)
	var changes *change.Set
	if changedOnly {
		changes = emittedCycleChanges(t, root, cycle)
	}
	filterLoader, session := admissiontest.New(t, admissiontest.Config{Changes: changes, Index: index, Inventory: st})
	admissiontest.Commit(t, session, nil)
	c := base.New(st, tasks)
	c.Configure(base.Options{Session: session, Loader: filterLoader, Index: index})
	c.StartLifecycle()
	o := &Orchestrator{store: st, depGraph: newDependencyGraph(), index: index, selection: session}
	assert.NoError(t, o.failDependsOnCycles(t.Context()))
	assert.Equal(t, index.IsFileIndexed(b.Named()), true)
	assert.Equal(t, st.GetObject(b.Named()), nil)
	assert.Equal(t, len(o.depGraph.Failures()), 0)
	captured, peerSettled := make(chan struct{}), make(chan struct{})
	var early atomic.Bool
	var settledOnce sync.Once
	dispatcher := fixtureDispatcher{
		store: st,
		dispatch: func(ctx context.Context, id schedule.NodeID, level manifest.DrainLevel, previous schedule.Result) schedule.Result {
			e := controllertest.NewExecution(c, id)
			e.Take(previous)
			e.Dispatch(
				ctx,
				level,
				e,
				func(e *base.Execution, ctx context.Context, obj *manifest.Kustomization, level manifest.DrainLevel) error {
					if id == a.Named() && early.CompareAndSwap(false, true) {
						assert.Equal(t, e.Reads.Preflight(), "")
						close(captured)
						tasks.YieldSlot(func() { <-peerSettled })
						assert.Equal(t, cycle && e.Valid(), false)
					}
					if id == parent.Named() {
						tasks.YieldSlot(func() { <-captured })
						_, _, next := e.Reads.GetEmission(id)
						defer next.Finish()
						next.Children, next.Complete = []store.Child{{Obj: b}}, true
						err := e.Emit(ctx, id, &next, nil)
						if !cycle {
							settledOnce.Do(func() { close(peerSettled) })
						}
						return err
					}
					var deps []manifest.DependencyRef
					for _, dep := range sameKindDepTargets(obj, nil) {
						deps = append(deps, manifest.DependencyRef{NamedResource: dep})
					}
					return e.Require(ctx, deps, level)
				},
			)
			if id == b.Named() && cycle {
				info, _ := st.GetStatus(id)
				assert.Equal(t, info.Outcome(), store.OutcomeFailed)
				settledOnce.Do(func() { close(peerSettled) })
			}
			return schedule.Result{Assessment: e}
		},
	}
	unsubCycles := st.AddListener(store.EventObjectAdded, func(id manifest.NamedResource, _ any) {
		assert.NoError(t, o.updateDependencyGraphFor(t.Context(), id))
	}, false)
	defer unsubCycles()
	scheduler := schedule.New(tasks, dispatcher)
	unsubArrival := o.watchDAG(scheduler)
	defer unsubArrival()
	scheduler.Seed([]schedule.NodeID{a.Named(), parent.Named()})
	assert.NoError(t, scheduler.Run(t.Context()))
	assert.Equal(t, early.Load(), true)
	assert.Equal(t, tasks.Failures(), int64(0))
	assert.Equal(t, t.Context().Err(), nil)
	assert.Equal(t, len(scheduler.Results()), 3)
	assertEmittedCycleSelection(t, o, scheduler, cycle)
}

func emittedCycleChanges(t *testing.T, root string, cycle bool) *change.Set {
	t.Helper()
	before, after := t.TempDir(), root
	for side, dir := range []string{before, after} {
		dep := ""
		if cycle == (side == 1) {
			dep = "b"
		}
		testutil.WriteFile(t, dir, "a.yaml", ksYAML("a", "a", dep))
		testutil.WriteFile(t, dir, "parent.yaml", ksYAML("parent", "parent-"+dep, ""))
		testutil.WriteFile(t, dir, "parent/b.yaml", ksYAML("b", "b", "a"))
	}
	oldInput, _, err := baseline.AdmitDirectory(t.Context(), before, before, manifest.WorkScope{})
	assert.NoError(t, err)
	newInput, _, err := baseline.AdmitDirectory(t.Context(), after, after, manifest.WorkScope{})
	assert.NoError(t, err)
	changes, err := change.Detect(t.Context(), oldInput, newInput, change.DetectConfig{})
	assert.NoError(t, err)
	assert.Equal(t, changes.Contains("parent/b.yaml"), false)
	assert.Equal(t, changes.Contains("a.yaml"), true)
	return changes
}

func assertEmittedCycleSelection(t *testing.T, o *Orchestrator, scheduler *schedule.Scheduler, cycle bool) {
	t.Helper()
	st, session := o.store, o.selection
	a, b, parent := testutil.KustomizationID("flux-system", "a"), testutil.KustomizationID("flux-system", "b"),
		testutil.KustomizationID("flux-system", "parent")
	aName, bName := o.diagnostic(a).String(), o.diagnostic(b).String()
	wantCycle := "dependency cycle detected: " + aName + " → " + bName + " → " + aName
	for _, id := range []manifest.NamedResource{a, b, parent} {
		assert.Equal(t, session.Snapshot().Reconcile(id), true)
		result, present := scheduler.Results()[id]
		assert.Equal(t, present, true)
		if present {
			assert.Equal(t, result.Assessment.Outcome(), schedule.Terminal)
		}
		info, present := st.GetStatus(id)
		assert.Equal(t, present, true)
		assert.Equal(t, len(info.BlockedBy()), 0)
		assert.Equal(t, info.IsDependencyFailure(), false)
		if cycle && id != parent {
			assert.Equal(t, info.Outcome(), store.OutcomeFailed)
			assert.Equal(t, info.String(), wantCycle)
		} else {
			assert.Equal(t, info.Status, store.StatusReady)
		}
	}
	failures := o.depGraph.Failures()
	if cycle {
		assert.Equal(t, len(failures), 2)
		assert.Equal(t, len(st.FailedResources()), 2)
		for _, id := range []manifest.NamedResource{a, b} {
			assert.Equal(t, failures[id] != nil, true)
		}
		warnings := st.Warnings()
		assert.Equal(t, len(warnings), 1)
		if len(warnings) == 1 {
			assert.Equal(t, warnings[0].Category, manifest.WarnDependencyCycle)
			assert.Equal(t, warnings[0].Count, 1)
		}
	} else {
		assert.Equal(t, len(failures), 0)
		assert.Equal(t, len(st.FailedResources()), 0)
		assert.Equal(t, len(st.Warnings()), 0)
	}
	assert.Equal(t, st.EmissionOwner(b), parent)
}
