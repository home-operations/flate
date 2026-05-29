package orchestrator

import (
	"log/slog"
	"maps"
	"slices"
	"strings"

	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

// detectDependsOnCycles runs a DFS over the dependsOn graph within
// each reconcilable kind (Kustomization, HelmRelease) and returns
// the cycle paths it finds. A cycle path is the closed loop, e.g.
// [A, B, C, A]. Without this pre-check, depwait would burn the full
// 30s per-dep timeout on every cycle member before failing.
//
// We don't reach across kinds — dependsOn on a Kustomization refers
// to Kustomizations only, dependsOn on a HelmRelease refers to
// HelmReleases only (Flux spec).
//
// This path remains in place for explicit cycle-list consumers (tests,
// the bootstrap full-rebuild). The hot listener path uses the
// incremental dependencyGraph instead — see updateDependencyGraphFor.
func (o *Orchestrator) detectDependsOnCycles() [][]manifest.NamedResource {
	var all [][]manifest.NamedResource
	for _, kind := range []string{manifest.KindKustomization, manifest.KindHelmRelease} {
		all = append(all, findDependencyCycles(o.store, kind)...)
	}
	return all
}

// failDependsOnCycles refreshes the dependency graph from the store
// and installs the resulting cycle membership as preflight failures.
// Called once at Bootstrap (covers the file-loaded objects) and again
// by tests that mutate the store directly — see
// updateDependencyGraphFor for the per-event incremental path the
// post-Bootstrap listener takes.
//
// Flux blocks cyclic dependency graphs; flate must fail those
// resources before render instead of stripping edges and rendering
// manifests that would not reconcile in-cluster.
//
// preflightMu serializes the write to preflightFailures so concurrent
// listeners do not overwrite each other's deltas. The graph itself
// owns finer-grained synchronization for its in-memory state.
func (o *Orchestrator) failDependsOnCycles() {
	// Lazy-init for test harnesses that construct an Orchestrator
	// literal (bypassing New). Production code goes through New
	// which seeds depGraph.
	if o.depGraph == nil {
		o.depGraph = newDependencyGraph()
	}
	// Refresh the graph from the canonical store state. This walks
	// every KS + HR and pushes their current dependsOn list through
	// ReplaceEdges. After Bootstrap this is the only path that runs
	// (the per-event listener uses updateDependencyGraphFor); on
	// re-Bootstrap (test harnesses) the graph picks up the new state.
	o.rebuildDependencyGraphFromStore()
	o.syncPreflightFailures()
}

// updateDependencyGraphFor is the per-event incremental path the
// post-Bootstrap listener calls when a single Kustomization /
// HelmRelease lands in the store. It pushes the changed id's edges
// through the graph (O(reachable from new dst) per added edge in a
// healthy graph) and reconciles preflightFailures with whatever the
// graph reports.
//
// Returns nothing — failures land in preflightFailures and the
// controllers read them via PreflightFailure on their next status
// check.
func (o *Orchestrator) updateDependencyGraphFor(id manifest.NamedResource) {
	if o.depGraph == nil {
		o.depGraph = newDependencyGraph()
	}
	obj := o.store.GetObject(id)
	if obj == nil {
		// Object went away between the event fire and our read.
		// Drop edges; revalidate any failed nodes that depended on
		// it.
		o.depGraph.ReplaceEdges(id, nil)
		o.syncPreflightFailures()
		return
	}
	deps := sameKindDepTargets(obj, id.Kind)
	o.depGraph.ReplaceEdges(id, deps)
	o.syncPreflightFailures()
}

// syncPreflightFailures snapshots the dependency-graph's current
// failure set and installs it under preflightMu, refiring any ids
// whose status was cleared. Shared by the bootstrap and incremental
// paths so the lock-and-refire dance lives in one place.
//
// Logs each newly-introduced cycle once (deduped by message) so
// render-emitted children that close a loop still surface in the
// log — the listener that calls this path would otherwise be
// silent.
func (o *Orchestrator) syncPreflightFailures() {
	failures := o.depGraph.Failures()
	o.preflightMu.Lock()
	prev := o.preflightFailures
	cleared := o.replacePreflightFailures(failures)
	o.preflightMu.Unlock()
	logNewCycleMessages(prev, failures)
	o.refireClearedPreflightFailures(cleared)
}

// logNewCycleMessages logs cycle messages that appear in next but
// were absent from prev. Dedupes within next so a multi-member
// cycle still produces one log line.
func logNewCycleMessages(prev, next map[manifest.NamedResource]string) {
	if len(next) == 0 {
		return
	}
	prevMsgs := make(map[string]struct{}, len(prev))
	for _, msg := range prev {
		prevMsgs[msg] = struct{}{}
	}
	seen := make(map[string]struct{}, len(next))
	for _, msg := range next {
		if _, old := prevMsgs[msg]; old {
			continue
		}
		if _, dup := seen[msg]; dup {
			continue
		}
		seen[msg] = struct{}{}
		slog.Warn("dependency cycle detected", "message", msg)
	}
}

// rebuildDependencyGraphFromStore pushes every KS + HR's current
// dependsOn list through ReplaceEdges. Idempotent: ReplaceEdges
// fast-paths the no-change case (equal old vs new edge sets), so
// re-running this on a stable graph is cheap. Used by the bootstrap
// full sweep and the legacy failDependsOnCycles entry-point so test
// code that calls failDependsOnCycles after a direct store mutation
// still observes the latest cycle state.
func (o *Orchestrator) rebuildDependencyGraphFromStore() {
	for _, kind := range []string{manifest.KindKustomization, manifest.KindHelmRelease} {
		for _, obj := range o.store.ListObjects(kind) {
			id := obj.Named()
			deps := sameKindDepTargets(obj, kind)
			o.depGraph.ReplaceEdges(id, deps)
		}
	}
}

// sameKindDepTargets pulls the dependsOn list from a Kustomization /
// HelmRelease and filters it down to entries of the same kind. Flux's
// spec.dependsOn is kind-homogeneous (KS deps on KS, HR deps on HR);
// stripping cross-kind entries here matches the legacy buildDepGraph
// behavior and keeps the graph's invariant intact.
func sameKindDepTargets(obj manifest.BaseManifest, kind string) []manifest.NamedResource {
	var deps []manifest.DependencyRef
	switch v := obj.(type) {
	case *manifest.Kustomization:
		deps = v.DependsOn
	case *manifest.HelmRelease:
		deps = v.DependsOn
	default:
		return nil
	}
	if len(deps) == 0 {
		return nil
	}
	out := make([]manifest.NamedResource, 0, len(deps))
	for _, d := range deps {
		if d.Kind != kind {
			continue
		}
		out = append(out, d.NamedResource)
	}
	return out
}

func (o *Orchestrator) refireClearedPreflightFailures(ids []manifest.NamedResource) {
	slices.SortFunc(ids, manifest.NamedResource.Compare)
	for _, id := range ids {
		if id.Kind == manifest.KindKustomization || id.Kind == manifest.KindHelmRelease {
			o.store.Refire(id)
		}
	}
}

// findDependencyCycles walks the dependsOn graph of one kind using
// the standard tri-color DFS (WHITE/GRAY/BLACK) and returns every
// cycle as the closed loop. Visit order is sorted so cycle output is
// deterministic across runs — CI/log diffs depend on it.
//
// Retained for detectDependsOnCycles consumers (tests, future tooling
// that wants the cycle paths rather than just the membership). The
// incremental hot path lives in dependencyGraph.
func findDependencyCycles(s *store.Store, kind string) [][]manifest.NamedResource {
	graph := buildDepGraph(s, kind)
	if len(graph) == 0 {
		return nil
	}
	const (
		white = iota
		gray
		black
	)
	color := map[manifest.NamedResource]int{}
	var stack []manifest.NamedResource
	var cycles [][]manifest.NamedResource

	// Sort the visit order so cycle output is deterministic across
	// runs. NamedResource.Compare orders by (kind, namespace, name);
	// every node here shares the same kind, so the comparison
	// reduces to (namespace, name) — the historical inline ordering.
	nodes := slices.SortedFunc(maps.Keys(graph), manifest.NamedResource.Compare)

	var visit func(n manifest.NamedResource)
	visit = func(n manifest.NamedResource) {
		color[n] = gray
		stack = append(stack, n)
		// Sort outgoing edges for determinism. Clone the slice first
		// so the SortFunc doesn't mutate the underlying graph entry.
		out := append([]manifest.NamedResource(nil), graph[n]...)
		slices.SortFunc(out, manifest.NamedResource.Compare)
		for _, m := range out {
			switch color[m] {
			case white:
				visit(m)
			case gray:
				// Back-edge to a node currently on the stack → cycle.
				start := max(slices.Index(stack, m), 0)
				cycle := append([]manifest.NamedResource(nil), stack[start:]...)
				cycle = append(cycle, m) // close the loop visually
				cycles = append(cycles, cycle)
			}
		}
		color[n] = black
		stack = stack[:len(stack)-1]
	}
	for _, n := range nodes {
		if color[n] == white {
			visit(n)
		}
	}
	return cycles
}

// buildDepGraph extracts the (id → []deps) adjacency map for one
// kind. Cross-kind deps (a Kustomization depending on a HelmRelease,
// for instance — not legal under Flux spec) are filtered out so the
// graph stays kind-homogeneous and DFS stops at kind boundaries.
func buildDepGraph(s *store.Store, kind string) map[manifest.NamedResource][]manifest.NamedResource {
	graph := map[manifest.NamedResource][]manifest.NamedResource{}
	for _, obj := range s.ListObjects(kind) {
		var deps []manifest.DependencyRef
		switch v := obj.(type) {
		case *manifest.Kustomization:
			deps = v.DependsOn
		case *manifest.HelmRelease:
			deps = v.DependsOn
		default:
			continue
		}
		id := obj.Named()
		targets := make([]manifest.NamedResource, 0, len(deps))
		for _, d := range deps {
			if d.Kind != kind {
				continue
			}
			targets = append(targets, d.NamedResource)
		}
		graph[id] = targets
	}
	return graph
}

// formatCyclePath returns the cycle as "A → B → C → A" for log output.
func formatCyclePath(path []manifest.NamedResource) string {
	parts := make([]string, len(path))
	for i, id := range path {
		parts[i] = id.String()
	}
	return strings.Join(parts, " → ")
}
