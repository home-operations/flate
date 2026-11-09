package orchestrator

import (
	"sync/atomic"
	"testing"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/discovery"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

type preflightSnapshotError struct {
	calls   atomic.Int64
	pause   [2]int64
	reached [2]chan struct{}
	release [2]chan struct{}
}

func (b *preflightSnapshotError) Error() string {
	// Publications already captured graph membership before these calls.
	call := b.calls.Add(1)
	for i, pause := range b.pause {
		if call == pause {
			close(b.reached[i])
			<-b.release[i]
		}
	}
	return "entry binding failed"
}

func TestSyncPreflightFailures_OverlappingPublishers(t *testing.T) {
	t.Parallel()
	for _, introduction := range []bool{true, false} {
		name := "cycle removal"
		if introduction {
			name = "cycle introduction"
		}
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			for _, olderFirst := range []bool{true, false} {
				name := "newer completes first"
				if olderFirst {
					name = "older completes first"
				}
				t.Run(name, func(t *testing.T) {
					t.Parallel()
					testOverlappingPreflightPublishers(t, introduction, olderFirst, false)
				})
			}
		})
	}
}

func TestUpdateDependencyGraphFor_ReplacedDeclaration(t *testing.T) {
	t.Parallel()
	for _, introduction := range []bool{true, false} {
		name := "cycle removal"
		if introduction {
			name = "cycle introduction"
		}
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			testOverlappingPreflightPublishers(t, introduction, false, true)
		})
	}
}

func TestSyncPreflightFailures_StaleWritesAfterClear(t *testing.T) {
	t.Parallel()
	a, b := makeKS(
		"a",
		"ns",
		testutil.KustomizationID("ns", "b"),
	), makeKS(
		"b",
		"ns",
		testutil.KustomizationID("ns", "a"),
	)
	entry := makeKS("entry", "ns")
	barrier := &preflightSnapshotError{
		pause:   [2]int64{2, 4},
		reached: [2]chan struct{}{make(chan struct{}), make(chan struct{})},
		release: [2]chan struct{}{make(chan struct{}), make(chan struct{})},
	}
	o := &Orchestrator{
		store:        store.New(),
		depGraph:     newDependencyGraph(),
		entryBinding: discovery.Binding{Producer: entry.Named(), Failure: barrier},
	}
	o.store.AddObjects([]manifest.BaseManifest{a, b, entry})
	o.failDependsOnCycles()
	message, _ := capturedPreflight(o.store, a.Named())
	olderDone := make(chan struct{})
	go func() {
		o.updateDependencyGraphFor(a.Named())
		close(olderDone)
	}()
	<-barrier.reached[0]
	o.store.AddObject(makeKS("b", "ns"))
	o.updateDependencyGraphFor(b.Named())
	close(barrier.release[0])
	<-barrier.reached[1]
	o.syncPreflightFailures()
	assert.Diff(t, o.preflightFailures, map[manifest.NamedResource]string{entry.Named(): "entry binding failed"})
	for _, id := range []manifest.NamedResource{a.Named(), b.Named()} {
		cleared, _ := capturedPreflight(o.store, id)
		assert.Equal(t, cleared, "")
		// This store-phase injection represents old writes after the newer clear.
		o.store.SetPreflight(id, message)
	}
	close(barrier.release[1])
	<-olderDone
	assert.Equal(t, len(o.depGraph.Failures()), 0)
	assert.Diff(t, o.preflightFailures, map[manifest.NamedResource]string{entry.Named(): "entry binding failed"})
	for _, id := range []manifest.NamedResource{a.Named(), b.Named()} {
		cleared, _ := capturedPreflight(o.store, id)
		assert.Equal(t, cleared, "")
	}
	assert.Diff(t, o.store.Warnings(), []manifest.Warning{{
		Related:  []manifest.NamedResource{a.Named(), b.Named(), a.Named()},
		Category: manifest.WarnDependencyCycle,
		Message:  "dependency cycle detected",
		Detail:   []string{message},
		Count:    1,
	}})
}

func testOverlappingPreflightPublishers(t *testing.T, introduction, olderFirst, replaceID bool) {
	t.Helper()
	a, b := makeKS("a", "ns", testutil.KustomizationID("ns", "b")), makeKS("b", "ns")
	if !introduction {
		b = makeKS("b", "ns", a.Named())
	}
	if replaceID {
		b = makeKS("b", "ns", a.Named())
		a = makeKS("a", "ns")
		if !introduction {
			a = makeKS("a", "ns", b.Named())
		}
	}
	entry, extra := makeKS("entry", "ns"), makeKS("extra", "ns")
	barrier := &preflightSnapshotError{
		pause:   [2]int64{2, 3},
		reached: [2]chan struct{}{make(chan struct{}), make(chan struct{})},
		release: [2]chan struct{}{make(chan struct{}), make(chan struct{})},
	}
	o := &Orchestrator{
		store:        store.New(),
		depGraph:     newDependencyGraph(),
		entryBinding: discovery.Binding{Producer: entry.Named(), Failure: barrier},
	}
	o.store.AddObjects([]manifest.BaseManifest{a, b, entry, extra})
	o.store.SetPreflight(extra.Named(), "unrelated acyclic failure")
	o.failDependsOnCycles()
	unsub := o.store.AddListener(store.EventObjectAdded, func(id manifest.NamedResource, _ any) {
		o.updateDependencyGraphFor(id)
	}, false)
	defer unsub()

	olderDone, newerDone := make(chan struct{}), make(chan struct{})
	go func() {
		older := *a
		older.Path = "./changed-a"
		o.store.AddObject(&older)
		close(olderDone)
	}()
	<-barrier.reached[0]
	retained, _ := o.store.Lookup(a.Named())
	go func() {
		name, target := "b", a.Named()
		if replaceID {
			name, target = "a", b.Named()
		}
		newer := makeKS(name, "ns")
		if introduction {
			newer = makeKS(name, "ns", target)
		}
		o.store.AddObject(newer)
		close(newerDone)
	}()
	<-barrier.reached[1]
	first, second := 1, 0
	done := [2]chan struct{}{olderDone, newerDone}
	if olderFirst {
		first, second = 0, 1
	}
	close(barrier.release[first])
	<-done[first]
	if replaceID {
		// This graph-phase injection represents the older callback's retained Lookup.
		o.depGraph.ReplaceEdges(a.Named(), sameKindDepTargets(retained, nil))
	}
	close(barrier.release[second])
	<-done[second]

	message := "dependency cycle detected: " + o.diagnostic(a.Named()).String() + " → " +
		o.diagnostic(b.Named()).String() + " → " + o.diagnostic(a.Named()).String()
	wantFailures := map[manifest.NamedResource]string{entry.Named(): "entry binding failed"}
	var wantPaths map[manifest.NamedResource][]manifest.NamedResource
	if introduction {
		path := []manifest.NamedResource{a.Named(), b.Named(), a.Named()}
		wantPaths = map[manifest.NamedResource][]manifest.NamedResource{a.Named(): path, b.Named(): path}
		wantFailures[a.Named()], wantFailures[b.Named()] = message, message
	}
	assert.Diff(t, cyclePaths(o.depGraph.Failures()), wantPaths)
	assert.Diff(t, o.preflightFailures, wantFailures)
	for _, id := range []manifest.NamedResource{a.Named(), b.Named(), entry.Named()} {
		got, _ := capturedPreflight(o.store, id)
		assert.Equal(t, got, wantFailures[id])
	}
	unrelated, _ := capturedPreflight(o.store, extra.Named())
	assert.Equal(t, unrelated, "unrelated acyclic failure")
	assert.Diff(t, o.store.Warnings(), []manifest.Warning{{
		Related:  []manifest.NamedResource{a.Named(), b.Named(), a.Named()},
		Category: manifest.WarnDependencyCycle,
		Message:  "dependency cycle detected",
		Detail:   []string{message},
		Count:    1,
	}})
}
