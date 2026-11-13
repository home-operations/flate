package orchestrator

import (
	"context"
	"fmt"
	"log/slog"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	fluxopv1 "github.com/controlplaneio-fluxcd/flux-operator/api/v1"
	helmv2 "github.com/fluxcd/helm-controller/api/v2"
	kustomizev1 "github.com/fluxcd/kustomize-controller/api/v1"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/internal/testutil/controllertest"
	"github.com/home-operations/flate/pkg/controllers/base"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/schedule"
	"github.com/home-operations/flate/pkg/store"
	"github.com/home-operations/flate/pkg/task"
)

func capturedPreflight(s *store.Store, id manifest.NamedResource) (string, bool) {
	var reads store.ReadSet
	reads.Init(s, id, nil)
	defer reads.Retire()
	message := reads.Preflight()
	return message, message != ""
}

func makeKS(name, ns string, deps ...manifest.NamedResource) *manifest.Kustomization {
	refs := make([]kustomizev1.DependencyReference, len(deps))
	for i, dep := range deps {
		refs[i] = kustomizev1.DependencyReference{Name: dep.Name, Namespace: dep.Namespace}
	}
	return &manifest.Kustomization{
		Name: name, Namespace: ns,
		Path:      "./" + name,
		DependsOn: refs,
	}
}

// TestFailDependsOnCycles_RecordsPreflightFailures verifies the runtime
// behavior the user cares about: cycle members fail before render, while
// their Flux dependsOn graph remains intact.
func TestFailDependsOnCycles_RecordsPreflightFailures(t *testing.T) {
	t.Parallel()
	idA := testutil.KustomizationID("ns", "a")
	idB := testutil.KustomizationID("ns", "b")
	// `extra` is an acyclic dep — it must not be marked failed.
	idExtra := testutil.KustomizationID("ns", "extra")

	o := &Orchestrator{store: store.New(), depGraph: newDependencyGraph()}
	o.store.AddObject(makeKS("a", "ns", idB, idExtra))
	o.store.AddObject(makeKS("b", "ns", idA))
	o.store.AddObject(makeKS("extra", "ns"))

	o.failDependsOnCycles()

	a, _ := o.store.GetObject(idA).(*manifest.Kustomization)
	b, _ := o.store.GetObject(idB).(*manifest.Kustomization)
	assert.FatalIf(t, a == nil || b == nil, "%v", "post-Bootstrap objects went missing")
	if len(a.DependsOn) != 2 || len(b.DependsOn) != 1 {
		t.Fatalf("cycle detection must not rewrite dependsOn: a=%+v b=%+v", a.DependsOn, b.DependsOn)
	}
	for _, id := range []manifest.NamedResource{idA, idB} {
		msg, ok := capturedPreflight(o.store, id)
		assert.FatalIf(
			t,
			!ok || !strings.Contains(msg, "dependency cycle detected"),
			"%s preflight failure = %q, %v",
			id,
			msg,
			ok,
		)
	}
	{
		msg, ok := capturedPreflight(o.store, idExtra)
		assert.FatalIf(t, ok, "acyclic dependency was marked failed: %q", msg)
	}
}

func TestFailDependsOnCycles_ClearsResolvedCycle(t *testing.T) {
	t.Parallel()
	idA := testutil.KustomizationID("ns", "a")
	idB := testutil.KustomizationID("ns", "b")

	o := &Orchestrator{store: store.New(), depGraph: newDependencyGraph()}
	o.store.AddObject(makeKS("a", "ns", idB))
	o.store.AddObject(makeKS("b", "ns", idA))
	o.failDependsOnCycles()
	{
		msg, ok := capturedPreflight(o.store, idA)
		assert.FatalIf(
			t,
			!ok || !strings.Contains(msg, "dependency cycle detected"),
			"initial cycle was not recorded: %q, %v",
			msg,
			ok,
		)
	}

	o.store.AddObject(makeKS("a", "ns"))
	o.failDependsOnCycles()
	for _, id := range []manifest.NamedResource{idA, idB} {
		{
			msg, ok := capturedPreflight(o.store, id)
			assert.FatalIf(t, ok, "resolved cycle member still has preflight failure %s: %q", id, msg)
		}
	}
}

func TestFailDependsOnCycles_InvalidatesResolvedMembers(t *testing.T) {
	t.Parallel()
	idA := testutil.KustomizationID("ns", "a")
	idB := testutil.KustomizationID("ns", "b")

	o := &Orchestrator{store: store.New(), depGraph: newDependencyGraph()}
	o.store.AddObject(makeKS("a", "ns", idB))
	o.store.AddObject(makeKS("b", "ns", idA))
	o.failDependsOnCycles()
	var reads store.ReadSet
	reads.Init(o.store, idB, nil)
	reads.GetObject(idB)
	defer reads.Retire()

	seen := map[manifest.NamedResource]int{}
	o.store.AddListener(store.EventObjectAdded, func(id manifest.NamedResource, _ any) {
		seen[id]++
	}, false)

	o.store.AddObject(makeKS("a", "ns"))
	o.failDependsOnCycles()

	changed := slices.Collect(o.store.Sweep())
	assert.FatalIf(
		t,
		reads.Valid() || !slices.Contains(changed, idB) || seen[idB] != 0,
		"%v",
		"cleared preflight did not invalidate its retained assessment without an arrival",
	)
}

func TestFailDependsOnCycles_EmittedAfterCapture(t *testing.T) {
	t.Parallel()
	st, tasks := store.New(), task.NewBounded(2)
	a := makeKS("a", "ns", testutil.KustomizationID("ns", "b"))
	b := makeKS("b", "ns", a.Named())
	parent := makeKS("parent", "ns")
	st.AddObjects([]manifest.BaseManifest{a, parent})
	o := &Orchestrator{store: st, depGraph: newDependencyGraph()}
	o.failDependsOnCycles()
	unsubCycle := st.AddListener(store.EventObjectAdded, func(id manifest.NamedResource, _ any) {
		o.updateDependencyGraphFor(id)
	}, false)
	defer unsubCycle()
	c := base.New(st, tasks)
	captured, peerSettled := make(chan struct{}), make(chan struct{})
	var early atomic.Bool
	var peerOnce sync.Once
	dispatcher := fixtureDispatcher{
		store: st,
		dispatch: func(ctx context.Context, id schedule.NodeID, level manifest.DrainLevel, _ schedule.Result) schedule.Result {
			e := controllertest.NewExecution(c, id)
			e.Dispatch(
				ctx,
				level,
				e,
				func(e *base.Execution, ctx context.Context, obj *manifest.Kustomization, level manifest.DrainLevel) error {
					if id == a.Named() && early.CompareAndSwap(false, true) {
						assert.Equal(t, e.Reads.Preflight(), "")
						close(captured)
						tasks.YieldSlot(func() { <-peerSettled })
						assert.Equal(t, e.Valid(), false)
					}
					if id == parent.Named() {
						tasks.YieldSlot(func() { <-captured })
						_, _, next := e.Reads.GetEmission(id)
						defer next.Finish()
						next.Children, next.Complete = []store.Child{{Obj: b}}, true
						return e.Emit(ctx, id, &next, nil)
					}
					var deps []manifest.DependencyRef
					for _, dep := range sameKindDepTargets(obj, nil) {
						deps = append(deps, manifest.DependencyRef{NamedResource: dep})
					}
					return e.Require(ctx, deps, level)
				},
			)
			if id == b.Named() {
				info, _ := st.GetStatus(id)
				assert.Equal(t, info.Outcome(), store.OutcomeFailed)
				peerOnce.Do(func() { close(peerSettled) })
			}
			return schedule.Result{Assessment: e}
		},
	}
	scheduler := schedule.New(tasks, dispatcher)
	unsubArrival := st.AddListener(store.EventObjectAdded, func(id manifest.NamedResource, payload any) {
		scheduler.OnArrival(id, payload != nil)
	}, false)
	defer unsubArrival()
	scheduler.Seed([]schedule.NodeID{a.Named(), parent.Named()})
	assert.NoError(t, scheduler.Run(t.Context()))
	assert.Equal(t, early.Load(), true)
	assert.Equal(t, tasks.Failures(), int64(0))
	assert.Equal(t, len(scheduler.Results()), 3)
	assert.Equal(t, len(o.depGraph.Failures()), 2)
	want := "dependency cycle detected: " + o.diagnostic(a.Named()).String() + " → " +
		o.diagnostic(b.Named()).String() + " → " + o.diagnostic(a.Named()).String()
	for _, id := range []manifest.NamedResource{a.Named(), b.Named()} {
		info, _ := st.GetStatus(id)
		assert.Equal(t, info.Outcome(), store.OutcomeFailed)
		assert.Equal(t, info.String(), want)
		t.Logf("settled %s: %s", id, info.String())
	}
}

// TestFailDependsOnCycles_RenderEmittedCycle verifies the runtime
// cycle-detection listener: when a parent KS's render emits two
// children whose dependsOn closes a loop, the orchestrator's
// post-Bootstrap listener re-runs failDependsOnCycles and records
// preflight failures. Bootstrap's one-shot pass never saw the emitted
// nodes.
//
// The test invokes failDependsOnCycles directly (mirrors what the
// Run-time listener does) AFTER adding the emit'd children to the
// store. End-to-end coverage of the listener wiring sits in the
// e2e suite — but pinning the load-bearing behavior here keeps the
// listener honest if it ever gets refactored.
//
//nolint:paralleltest // The test calls a helper that changes process-global state.
func TestFailDependsOnCycles_RenderEmittedCycle(t *testing.T) {
	root := t.TempDir()
	testutil.WriteFile(t, root, "flux/parent.yaml", ksYAML("parent", "parent", ""))
	testutil.WriteFile(t, root, "parent/kustomization.yaml", "resources: [trigger.yaml]\n")
	testutil.WriteFile(
		t,
		root,
		"parent/trigger.yaml",
		"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: trigger, namespace: ns}\n",
	)
	o, err := newTestOrchestrator(t, Tree{Path: root, RepoRoot: root}, Config{CacheDir: t.TempDir(), Concurrency: 2})
	assert.NoError(t, err)
	defer o.stop()
	assert.NoError(t, o.bootstrap(t.Context()))
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	manifest.KindOf(&manifest.RawObject{APIVersion: manifest.FluxOperatorDomain + "/v1", Kind: manifest.KindResourceSet}).
		Namespaced()
	var injected atomic.Bool
	o.store.AddListener(store.EventObjectAdded, func(id manifest.NamedResource, payload any) {
		if id.Kind != manifest.KindConfigMap || id.Name != "trigger" || payload == nil ||
			!injected.CompareAndSwap(false, true) {
			return
		}
		defer cancel()
		idA := testutil.KustomizationID("ns", "a")
		idB := idA
		idB.Name = "b"
		o.store.AddObject(makeKS("a", "ns", idB))
		o.store.AddObject(makeKS("b", "ns", idA))
		for _, id := range []manifest.NamedResource{idA, idB} {
			{
				msg, ok := capturedPreflight(o.store, id)
				assert.ErrorIf(
					t,
					!ok || !strings.Contains(msg, "dependency cycle detected"),
					"%s preflight failure = %q, %v",
					id,
					msg,
					ok,
				)
			}
		}
		for _, pair := range [][2]string{{"rs-a", "rs-b"}, {"rs-b", "rs-a"}} {
			o.store.AddObject(
				&manifest.ResourceSet{
					Name:      pair[0],
					Namespace: "ns",
					DependsOn: []fluxopv1.Dependency{
						{
							APIVersion: manifest.FluxOperatorDomain + "/v1",
							Kind:       manifest.KindResourceSet,
							Name:       pair[1],
							Namespace:  "ns",
						},
					},
				},
			)
		}
		for _, name := range []string{"rs-a", "rs-b"} {
			id := manifest.NamedResource{
				Group:     manifest.FluxOperatorDomain,
				Kind:      manifest.KindResourceSet,
				Namespace: "ns",
				Name:      name,
			}
			{
				msg, ok := capturedPreflight(o.store, id)
				assert.ErrorIf(t, ok, "non-render %s admitted to cycle graph: %s", id, msg)
			}
		}
	}, false)
	if err := o.run(ctx); err == nil || !injected.Load() {
		t.Fatalf("render-emitted fixture: injected=%t error=%v", injected.Load(), err)
	}
}

func TestUpdateDependencyGraphFor_LargeAcyclicAddThenCycle(t *testing.T) {
	t.Parallel()
	const nodeCount = 10000
	o := &Orchestrator{store: store.New(), depGraph: newDependencyGraph()}

	ids := make([]manifest.NamedResource, nodeCount)
	for i := range nodeCount {
		ids[i] = testutil.KustomizationID("ns", fmt.Sprintf("ks-%05d", i))
	}
	for i := range nodeCount {
		var deps []manifest.NamedResource
		if i+1 < nodeCount {
			deps = []manifest.NamedResource{ids[i+1]}
		}
		o.store.AddObject(makeKS(ids[i].Name, ids[i].Namespace, deps...))
		o.updateDependencyGraphFor(ids[i])
	}
	{
		got := o.depGraph.Failures()
		assert.FatalIf(t, len(got) != 0, "acyclic chain reported cycles: %v", got)
	}
	for _, id := range ids {
		{
			msg, ok := capturedPreflight(o.store, id)
			assert.FatalIf(t, ok, "acyclic %s flagged: %q", id, msg)
		}
	}

	o.store.AddObject(makeKS(ids[nodeCount-1].Name, ids[nodeCount-1].Namespace, ids[0]))
	o.updateDependencyGraphFor(ids[nodeCount-1])

	failures := o.depGraph.Failures()
	assert.FatalIf(
		t,
		len(failures) != nodeCount,
		"cycle-closing edge should flag every chain member; got %d / %d",
		len(failures),
		nodeCount,
	)
	for _, id := range ids {
		{
			msg, ok := capturedPreflight(o.store, id)
			assert.FatalIf(
				t,
				!ok || !strings.Contains(msg, "dependency cycle detected"),
				"%s preflight after cycle close = %q, %v",
				id,
				msg,
				ok,
			)
		}
	}
}

// TestUpdateDependencyGraphFor_BreaksCycle exercises the remove path:
// stand up a cycle via the incremental API, then rewrite one id's
// dependsOn to break it; every previously-failed member must clear.
func TestUpdateDependencyGraphFor_BreaksCycle(t *testing.T) {
	t.Parallel()
	idA := testutil.KustomizationID("ns", "a")
	idB := testutil.KustomizationID("ns", "b")
	idC := testutil.KustomizationID("ns", "c")

	o := &Orchestrator{store: store.New(), depGraph: newDependencyGraph()}
	o.store.AddObject(makeKS("a", "ns", idB))
	o.updateDependencyGraphFor(idA)
	o.store.AddObject(makeKS("b", "ns", idC))
	o.updateDependencyGraphFor(idB)
	o.store.AddObject(makeKS("c", "ns", idA))
	o.updateDependencyGraphFor(idC)

	for _, id := range []manifest.NamedResource{idA, idB, idC} {
		{
			msg, ok := capturedPreflight(o.store, id)
			assert.FatalIf(
				t,
				!ok || !strings.Contains(msg, "dependency cycle detected"),
				"%s pre-break = %q, %v",
				id,
				msg,
				ok,
			)
		}
	}

	// Rewrite c to drop its dep on a; cycle dissolves.
	o.store.AddObject(makeKS("c", "ns"))
	o.updateDependencyGraphFor(idC)

	for _, id := range []manifest.NamedResource{idA, idB, idC} {
		{
			msg, ok := capturedPreflight(o.store, id)
			assert.FatalIf(t, ok, "%s post-break still failed: %q", id, msg)
		}
	}
}

// TestUpdateDependencyGraphFor_HelmRelease verifies the same
// incremental update works for HelmRelease objects — the listener
// dispatches both kinds through one path.
func TestUpdateDependencyGraphFor_HelmRelease(t *testing.T) {
	t.Parallel()
	idX := testutil.HelmReleaseID("ns", "x")
	idY := testutil.HelmReleaseID("ns", "y")

	o := &Orchestrator{store: store.New(), depGraph: newDependencyGraph()}
	o.store.AddObject(&manifest.HelmRelease{
		Name: "x", Namespace: "ns",
		DependsOn: []helmv2.DependencyReference{{Name: idY.Name, Namespace: idY.Namespace}},
	})
	o.updateDependencyGraphFor(idX)
	o.store.AddObject(&manifest.HelmRelease{
		Name: "y", Namespace: "ns",
		DependsOn: []helmv2.DependencyReference{{Name: idX.Name, Namespace: idX.Namespace}},
	})
	o.updateDependencyGraphFor(idY)

	for _, id := range []manifest.NamedResource{idX, idY} {
		{
			msg, ok := capturedPreflight(o.store, id)
			assert.FatalIf(
				t,
				!ok || !strings.Contains(msg, "dependency cycle detected"),
				"HR %s preflight = %q, %v",
				id,
				msg,
				ok,
			)
		}
	}
}

//nolint:paralleltest // This test changes process-global state.
//nolint:paralleltest // The test calls a helper that changes process-global state.
func TestOrchestrator_Allocations(t *testing.T) {
	t.Run("SameKindDepTargets_CallerBuffer", func(t *testing.T) {
		for _, count := range []int{1, 8, 12} {
			t.Run(strconv.Itoa(count), func(t *testing.T) {
				want := make([]manifest.NamedResource, count)
				for i := range want {
					want[i] = testutil.KustomizationID("ns", strconv.Itoa(i))
				}
				ks := makeKS("app", "ns", want...)
				var buf [8]manifest.NamedResource
				assert.Diff(t, sameKindDepTargets(ks, buf[:0]), want)
				if count <= len(buf) {
					allocs := testing.AllocsPerRun(100, func() {
						_ = sameKindDepTargets(ks, buf[:0])
					})
					assert.Equal(t, allocs, float64(0))
				}
			})
		}
	})
	t.Run("Work_DisabledDebugAllocations", func(t *testing.T) {
		previous := slog.Default()
		slog.SetDefault(slog.New(slog.DiscardHandler))
		defer slog.SetDefault(previous)
		o := &Orchestrator{work: manifest.WorkSummary{{Counts: manifest.WorkCounts{Attempts: 1}}}}
		allocations := testing.AllocsPerRun(10, func() {
			o.logSummary(nil, 1, 0)
		})
		assert.Equal(t, allocations, float64(0))
	})
}

//nolint:paralleltest // This test changes process-global state.
//nolint:paralleltest // The test calls a helper that changes process-global state.
func TestDependencyCycles_WarningsDeduplicate(t *testing.T) {
	o := &Orchestrator{store: store.New(), depGraph: newDependencyGraph()}
	const members = 32
	var wg sync.WaitGroup
	for i := range members {
		from := testutil.KustomizationID("ns", fmt.Sprintf("ks-%02d", i))
		to := testutil.KustomizationID("ns", fmt.Sprintf("ks-%02d", (i+1)%members))
		wg.Go(func() {
			o.depGraph.ReplaceEdges(from, []manifest.NamedResource{to})
			o.syncPreflightFailures()
		})
	}
	wg.Wait()
	for range 2 {
		o.syncPreflightFailures()
		warnings := o.store.Warnings()
		if len(warnings) != 1 || warnings[0].Category != manifest.WarnDependencyCycle || warnings[0].Count != 1 ||
			len(o.preflightFailures) != members {
			t.Fatalf("cycle warning/failure state = %+v / %+v", warnings, o.preflightFailures)
		}
	}
	{
		got := testing.AllocsPerRun(10, o.syncPreflightFailures)
		assert.FatalIf(
			t,
			got > members,
			"cycle warning publication allocates %g times for %d members",
			got,
			members,
		)
	}
}
