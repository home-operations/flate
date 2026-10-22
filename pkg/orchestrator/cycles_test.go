package orchestrator

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	fluxopv1 "github.com/controlplaneio-fluxcd/flux-operator/api/v1"
	helmv2 "github.com/fluxcd/helm-controller/api/v2"
	kustomizev1 "github.com/fluxcd/kustomize-controller/api/v1"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

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
	if a == nil || b == nil {
		t.Fatal("post-Bootstrap objects went missing")
	}
	if len(a.DependsOn) != 2 || len(b.DependsOn) != 1 {
		t.Fatalf("cycle detection must not rewrite dependsOn: a=%+v b=%+v", a.DependsOn, b.DependsOn)
	}
	for _, id := range []manifest.NamedResource{idA, idB} {
		msg, ok := o.preflightFailure(id)
		if !ok || !strings.Contains(msg, "dependency cycle detected") {
			t.Fatalf("%s preflight failure = %q, %v", id, msg, ok)
		}
	}
	if msg, ok := o.preflightFailure(idExtra); ok {
		t.Fatalf("acyclic dependency was marked failed: %q", msg)
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
	if msg, ok := o.preflightFailure(idA); !ok || !strings.Contains(msg, "dependency cycle detected") {
		t.Fatalf("initial cycle was not recorded: %q, %v", msg, ok)
	}

	o.store.AddObject(makeKS("a", "ns"))
	o.failDependsOnCycles()
	for _, id := range []manifest.NamedResource{idA, idB} {
		if msg, ok := o.preflightFailure(id); ok {
			t.Fatalf("resolved cycle member still has preflight failure %s: %q", id, msg)
		}
	}
}

func TestFailDependsOnCycles_RefiresResolvedMembers(t *testing.T) {
	t.Parallel()
	idA := testutil.KustomizationID("ns", "a")
	idB := testutil.KustomizationID("ns", "b")

	o := &Orchestrator{store: store.New(), depGraph: newDependencyGraph()}
	o.store.AddObject(makeKS("a", "ns", idB))
	o.store.AddObject(makeKS("b", "ns", idA))
	o.failDependsOnCycles()

	seen := map[manifest.NamedResource]int{}
	o.store.AddListener(store.EventObjectAdded, func(id manifest.NamedResource, _ any) {
		seen[id]++
	}, false)

	o.store.AddObject(makeKS("a", "ns"))
	o.failDependsOnCycles()

	if seen[idB] == 0 {
		t.Fatal("resolved cycle member b was not refired after preflight failure cleared")
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
func TestFailDependsOnCycles_RenderEmittedCycle(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	testutil.WriteFile(t, root, "flux/parent.yaml", ksYAML("parent", "parent", ""))
	testutil.WriteFile(t, root, "parent/kustomization.yaml", "resources: [trigger.yaml]\n")
	testutil.WriteFile(t, root, "parent/trigger.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: trigger, namespace: ns}\n")
	o, err := New(t.Context(), Config{Path: root, RepoRoot: root, CacheDir: t.TempDir(), Concurrency: 2})
	assert.NoError(t, err)
	defer o.Stop()
	assert.NoError(t, o.Bootstrap(t.Context()))
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	manifest.KindOf(&manifest.RawObject{APIVersion: manifest.FluxOperatorDomain + "/v1", Kind: manifest.KindResourceSet}).Namespaced()
	var injected atomic.Bool
	o.store.AddListener(store.EventObjectAdded, func(id manifest.NamedResource, payload any) {
		if id.Kind != manifest.KindConfigMap || id.Name != "trigger" || payload == nil || !injected.CompareAndSwap(false, true) {
			return
		}
		defer cancel()
		idA := testutil.KustomizationID("ns", "a")
		idB := idA
		idB.Name = "b"
		o.store.AddObject(makeKS("a", "ns", idB))
		o.store.AddObject(makeKS("b", "ns", idA))
		for _, id := range []manifest.NamedResource{idA, idB} {
			if msg, ok := o.preflightFailure(id); !ok || !strings.Contains(msg, "dependency cycle detected") {
				t.Errorf("%s preflight failure = %q, %v", id, msg, ok)
			}
		}
		for _, pair := range [][2]string{{"rs-a", "rs-b"}, {"rs-b", "rs-a"}} {
			o.store.AddObject(&manifest.ResourceSet{Name: pair[0], Namespace: "ns", DependsOn: []fluxopv1.Dependency{{APIVersion: manifest.FluxOperatorDomain + "/v1", Kind: manifest.KindResourceSet, Name: pair[1], Namespace: "ns"}}})
		}
		for _, name := range []string{"rs-a", "rs-b"} {
			id := manifest.NamedResource{Group: manifest.FluxOperatorDomain, Kind: manifest.KindResourceSet, Namespace: "ns", Name: name}
			if msg, ok := o.preflightFailure(id); ok {
				t.Errorf("non-render %s admitted to cycle graph: %s", id, msg)
			}
		}
	}, false)
	if err := o.Run(ctx); err == nil || !injected.Load() {
		t.Fatalf("render-emitted fixture: injected=%t error=%v", injected.Load(), err)
	}
}

func TestUpdateDependencyGraphFor_LargeAcyclicAddThenCycle(t *testing.T) {
	const N = 10000
	o := &Orchestrator{store: store.New(), depGraph: newDependencyGraph()}

	ids := make([]manifest.NamedResource, N)
	for i := range N {
		ids[i] = testutil.KustomizationID("ns", fmt.Sprintf("ks-%05d", i))
	}
	for i := range N {
		var deps []manifest.NamedResource
		if i+1 < N {
			deps = []manifest.NamedResource{ids[i+1]}
		}
		o.store.AddObject(makeKS(ids[i].Name, ids[i].Namespace, deps...))
		o.updateDependencyGraphFor(ids[i])
	}
	if got := o.depGraph.Failures(); len(got) != 0 {
		t.Fatalf("acyclic chain reported cycles: %v", got)
	}
	for _, id := range ids {
		if msg, ok := o.preflightFailure(id); ok {
			t.Fatalf("acyclic %s flagged: %q", id, msg)
		}
	}

	if got := testing.AllocsPerRun(10, func() { o.syncPreflightFailures() }); got != 0 {
		t.Fatalf("acyclic warning collection allocates %g times", got)
	}

	o.store.AddObject(makeKS(ids[N-1].Name, ids[N-1].Namespace, ids[0]))
	o.updateDependencyGraphFor(ids[N-1])

	warnings := o.store.Warnings()
	if len(warnings) != 1 || warnings[0].Category != manifest.WarnDependencyCycle || warnings[0].Count != 1 || len(warnings[0].Detail) != 1 {
		t.Fatalf("large cycle warning = %+v", warnings)
	}
	failures := o.depGraph.Failures()
	if len(failures) != N {
		t.Fatalf("cycle-closing edge should flag every chain member; got %d / %d", len(failures), N)
	}
	for _, id := range ids {
		if msg, ok := o.preflightFailure(id); !ok || !strings.Contains(msg, "dependency cycle detected") {
			t.Fatalf("%s preflight after cycle close = %q, %v", id, msg, ok)
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
		if msg, ok := o.preflightFailure(id); !ok || !strings.Contains(msg, "dependency cycle detected") {
			t.Fatalf("%s pre-break = %q, %v", id, msg, ok)
		}
	}

	// Rewrite c to drop its dep on a; cycle dissolves.
	o.store.AddObject(makeKS("c", "ns"))
	o.updateDependencyGraphFor(idC)

	for _, id := range []manifest.NamedResource{idA, idB, idC} {
		if msg, ok := o.preflightFailure(id); ok {
			t.Fatalf("%s post-break still failed: %q", id, msg)
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
		if msg, ok := o.preflightFailure(id); !ok || !strings.Contains(msg, "dependency cycle detected") {
			t.Fatalf("HR %s preflight = %q, %v", id, msg, ok)
		}
	}
}

func TestSameKindDepTargets_CallerBuffer(t *testing.T) {
	for _, count := range []int{1, 8, 12} {
		t.Run(fmt.Sprint(count), func(t *testing.T) {
			want := make([]manifest.NamedResource, count)
			for i := range want {
				want[i] = testutil.KustomizationID("ns", fmt.Sprint(i))
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
}

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
		if len(warnings) != 1 || warnings[0].Category != manifest.WarnDependencyCycle || warnings[0].Count != 1 || len(o.preflightFailures) != members {
			t.Fatalf("cycle warning/failure state = %+v / %+v", warnings, o.preflightFailures)
		}
	}
	if got := testing.AllocsPerRun(10, o.syncPreflightFailures); got > members {
		t.Fatalf("cycle warning publication allocates %g times for %d members", got, members)
	}
}
