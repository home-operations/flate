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
func (o *Orchestrator) detectDependsOnCycles() [][]manifest.NamedResource {
	var all [][]manifest.NamedResource
	for _, kind := range []string{manifest.KindKustomization, manifest.KindHelmRelease} {
		all = append(all, findDependencyCycles(o.store, kind)...)
	}
	return all
}

// failDependsOnCycles detects cycles and records preflight failures for
// every cycle member. Flux blocks cyclic dependency graphs; flate must
// fail those resources before render instead of stripping edges and
// rendering manifests that would not reconcile in-cluster.
//
// preflightMu serializes the full detect-replace unit: the write lock
// is held for cycle detection + map replacement, then released before
// the refire calls so store listeners do not re-enter this path under
// the lock.
func (o *Orchestrator) failDependsOnCycles() {
	cycles := o.detectDependsOnCycles()
	failures := map[manifest.NamedResource]string{}
	for _, path := range cycles {
		msg := "dependency cycle detected: " + formatCyclePath(path)
		slog.Warn("dependency cycle detected", "cycle", formatCyclePath(path))
		for _, id := range path {
			if id.Kind == manifest.KindKustomization || id.Kind == manifest.KindHelmRelease {
				failures[id] = msg
			}
		}
	}
	o.preflightMu.Lock()
	cleared := o.replacePreflightFailures(failures)
	o.preflightMu.Unlock()
	o.refireClearedPreflightFailures(cleared)
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
