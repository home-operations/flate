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

// breakDependsOnCycles detects cycles and strips the dependsOn edges
// between cycle members so depwait completes immediately. The cycle
// path is logged for the user to see — flate's reconcile output may
// then show whatever real failure (or success) the cycle members
// have once they're no longer waiting on each other. Without this,
// every cycle member burned its full per-dep budget waiting for a
// peer that would never become Ready.
//
// Stripping is the pragmatic choice: the alternative (pre-marking
// every member Failed with the cycle message) loses the message when
// the controller's reconcile overwrites status. The warn log keeps
// the cycle information visible regardless.
func (o *Orchestrator) breakDependsOnCycles() {
	cycles := o.detectDependsOnCycles()
	if len(cycles) == 0 {
		return
	}
	// Collect every id involved in any cycle, keyed by kind, so we can
	// strip its dependsOn edges only against cycle peers of the same
	// kind. A KS in a cycle with another KS still keeps any legitimate
	// dependsOn entry on a non-cycle KS — only the cycle edge goes.
	inCycle := map[string]map[manifest.NamedResource]struct{}{
		manifest.KindKustomization: {},
		manifest.KindHelmRelease:   {},
	}
	for _, path := range cycles {
		slog.Warn("dependency cycle detected; stripping cycle edges to fail fast",
			"cycle", formatCyclePath(path))
		for _, id := range path {
			if set, ok := inCycle[id.Kind]; ok {
				set[id] = struct{}{}
			}
		}
	}
	for _, k := range store.ListAs[*manifest.Kustomization](o.store, manifest.KindKustomization) {
		if _, member := inCycle[manifest.KindKustomization][k.Named()]; !member {
			continue
		}
		stripped := stripCycleDeps(k.DependsOn, inCycle[manifest.KindKustomization])
		store.Mutate(o.store, k.Named(), func(x *manifest.Kustomization) { x.DependsOn = stripped })
	}
	for _, h := range store.ListAs[*manifest.HelmRelease](o.store, manifest.KindHelmRelease) {
		if _, member := inCycle[manifest.KindHelmRelease][h.Named()]; !member {
			continue
		}
		stripped := stripCycleDeps(h.DependsOn, inCycle[manifest.KindHelmRelease])
		store.Mutate(o.store, h.Named(), func(x *manifest.HelmRelease) { x.DependsOn = stripped })
	}
}

// stripCycleDeps returns deps minus any entry whose target is in the
// cycle set. The original slice is left untouched (the caller swaps
// the returned value into a fresh Clone via store.Mutate).
func stripCycleDeps(deps []manifest.DependencyRef, cycleMembers map[manifest.NamedResource]struct{}) []manifest.DependencyRef {
	if len(deps) == 0 {
		return deps
	}
	out := make([]manifest.DependencyRef, 0, len(deps))
	for _, d := range deps {
		if _, ok := cycleMembers[d.NamedResource]; ok {
			continue
		}
		out = append(out, d)
	}
	return out
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
				start := 0
				for i, sNode := range stack {
					if sNode == m {
						start = i
						break
					}
				}
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
