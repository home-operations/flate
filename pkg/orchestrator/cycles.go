package orchestrator

import (
	"log/slog"
	"slices"
	"strings"

	"github.com/home-operations/flate/pkg/manifest"
)

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
	o.ensureDepGraph()
	// Refresh the graph from the canonical store state. This walks
	// every KS + HR and pushes their current dependsOn list through
	// ReplaceEdges. After Bootstrap this is the only path that runs
	// (the per-event listener uses updateDependencyGraphFor); on
	// re-Bootstrap (test harnesses) the graph picks up the new state.
	o.rebuildDependencyGraphFromStore()
	o.syncPreflightFailures()
}

// ensureDepGraph lazy-inits depGraph for test harnesses that construct
// an Orchestrator literal (bypassing New). Production code goes through
// New, which seeds depGraph, so this is a no-op there.
func (o *Orchestrator) ensureDepGraph() {
	if o.depGraph == nil {
		o.depGraph = newDependencyGraph()
	}
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
	o.ensureDepGraph()
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
	// Seed the seen-set with prev's messages so a cycle carried over
	// from the previous snapshot is suppressed, then let it double as
	// the within-next dedup so a multi-member cycle logs once.
	seen := make(map[string]struct{}, len(prev)+len(next))
	for _, msg := range prev {
		seen[msg] = struct{}{}
	}
	for _, msg := range next {
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
	for _, kind := range reconcilableKinds {
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
		if isReconcilableKind(id.Kind) {
			o.store.Refire(id)
		}
	}
}

// formatCyclePath returns the cycle as "A → B → C → A" for log output.
func formatCyclePath(path []manifest.NamedResource) string {
	parts := make([]string, len(path))
	for i, id := range path {
		parts[i] = id.String()
	}
	return strings.Join(parts, " → ")
}
