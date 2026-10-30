package orchestrator

import (
	"strings"

	"github.com/home-operations/flate/pkg/manifest"
)

func (o *Orchestrator) failDependsOnCycles() {
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
// controllers read them through the base preflight gate on their next status
// check.
func (o *Orchestrator) updateDependencyGraphFor(id manifest.NamedResource) {
	obj := o.store.GetObject(id)
	if obj == nil {
		// Object went away between the event fire and our read.
		// Drop edges; revalidate any failed nodes that depended on
		// it.
		o.depGraph.ReplaceEdges(id, nil)
		o.syncPreflightFailures()
		return
	}
	var buf [8]manifest.NamedResource
	deps := sameKindDepTargets(obj, buf[:0])
	o.depGraph.ReplaceEdges(id, deps)
	o.syncPreflightFailures()
}

func (o *Orchestrator) syncPreflightFailures() {
	paths := o.depGraph.Failures()
	var failures map[manifest.NamedResource]string
	if len(paths) != 0 || o.entryBinding.Failure != nil {
		failures = make(map[manifest.NamedResource]string, len(paths))
	}
	if o.entryBinding.Failure != nil {
		failures[o.entryBinding.Producer] = o.entryBinding.Failure.Error()
	}
	for id, cycle := range paths {
		failures[id] = cycle.diagnostic(o)
	}
	o.preflightMu.Lock()
	cleared := o.replacePreflightFailures(failures)
	o.preflightMu.Unlock()
	unique := make(map[*dependencyCycle]struct{})
	for _, cycle := range paths {
		unique[cycle] = struct{}{}
	}
	for cycle := range unique {
		o.store.AddWarning(manifest.Warning{Related: cycle.path, Category: manifest.WarnDependencyCycle, Message: "dependency cycle detected", Detail: []string{cycle.diagnostic(o)}})
	}

	for id, message := range failures {
		o.store.SetPreflight(id, message)
	}
	for _, id := range cleared {
		o.store.SetPreflight(id, "")
	}
}

// rebuildDependencyGraphFromStore pushes every KS + HR's current
// dependsOn list through ReplaceEdges. Idempotent: ReplaceEdges
// fast-paths the no-change case (equal old vs new edge sets), so
// re-running this on a stable graph is cheap. Used by the bootstrap
// full sweep and failDependsOnCycles so test
// code that calls failDependsOnCycles after a direct store mutation
// still observes the latest cycle state.
func (o *Orchestrator) rebuildDependencyGraphFromStore() {
	for _, info := range reconcilableKinds {
		for _, obj := range o.store.ListObjects(info.Group, info.Kind) {
			id := obj.Named()
			var buf [8]manifest.NamedResource
			deps := sameKindDepTargets(obj, buf[:0])
			o.depGraph.ReplaceEdges(id, deps)
		}
	}
}

// Flux Kustomization and HelmRelease dependsOn references are kind-homogeneous.
func sameKindDepTargets(obj manifest.BaseManifest, out []manifest.NamedResource) []manifest.NamedResource {
	for ref := range manifest.References(obj, manifest.ReferenceRelations) {
		if ref.Use == "dependsOn" {
			out = append(out, ref.NamedResource)
		}
	}
	return out
}

// formatCyclePath returns the cycle as "A → B → C → A" for log output.
func (o *Orchestrator) formatCyclePath(path []manifest.NamedResource) string {
	parts := make([]string, len(path))
	for i, id := range path {
		parts[i] = o.diagnostic(id).String()
	}
	return strings.Join(parts, " → ")
}
