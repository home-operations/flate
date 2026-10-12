package orchestrator

import (
	"maps"
	"slices"
	"sync"

	"github.com/home-operations/flate/pkg/manifest"
)

// dependencyGraph tracks the same-kind dependsOn graph across every
// Kustomization and HelmRelease. It exists so cycle detection runs
// incrementally on edge updates instead of rebuilding the full graph
// and re-running tri-color DFS on every EventObjectAdded.
//
// Edges are partitioned by kind: a Kustomization's dependsOn refers
// only to other Kustomizations (Flux spec), and the same for
// HelmReleases. Cross-kind edges are dropped by callers before they
// reach ReplaceEdges so cycle detection stays kind-homogeneous.
//
// All operations are safe for concurrent use; the mutex guards the
// in/out edge maps and the failure-membership cache. ReplaceEdges
// holds the lock for its entire body so two concurrent
// EventObjectAdded fires for two distinct ids cannot observe a
// partially-updated graph.
type dependencyGraph struct {
	mu sync.Mutex
	// outEdges[src] contains only nodes with outgoing dependencies.
	outEdges map[manifest.NamedResource]map[manifest.NamedResource]struct{}
	// failed maps each cycle member to the human-readable message
	// recorded in the orchestrator's preflightFailures map. Acts as
	// the source-of-truth snapshot the orchestrator reads after each
	// ReplaceEdges call.
	failed map[manifest.NamedResource]string
}

func newDependencyGraph() *dependencyGraph {
	return &dependencyGraph{
		outEdges: map[manifest.NamedResource]map[manifest.NamedResource]struct{}{},
		failed:   map[manifest.NamedResource]string{},
	}
}

// ReplaceEdges installs deps, which MUST share id.Kind, and refreshes changed cycles.
func (g *dependencyGraph) ReplaceEdges(id manifest.NamedResource, deps []manifest.NamedResource) {
	g.mu.Lock()
	defer g.mu.Unlock()

	oldSet := g.outEdges[id]

	if edgeSetEqual(oldSet, deps) {
		return
	}

	newSet := make(map[manifest.NamedResource]struct{}, len(deps))
	for _, d := range deps {
		newSet[d] = struct{}{}
	}
	if len(newSet) == 0 {
		delete(g.outEdges, id)
	} else {
		g.outEdges[id] = newSet
	}

	removed := false
	for dst := range oldSet {
		if _, kept := newSet[dst]; !kept {
			removed = true
			break
		}
	}
	found := g.closesCycleLocked(id, oldSet)
	if found {
		g.markCycleMembersLocked(id)
	}
	if len(g.failed) > 0 && (removed || found) {
		g.revalidateFailedLocked()
	}
}

// Terminal destinations cannot close a cycle and must not allocate traversal state.
func (g *dependencyGraph) closesCycleLocked(id manifest.NamedResource, oldSet map[manifest.NamedResource]struct{}) bool {
	seen := map[manifest.NamedResource]bool{}
	var queue []manifest.NamedResource
	for dst := range g.outEdges[id] {
		if _, had := oldSet[dst]; had || len(g.outEdges[dst]) == 0 {
			continue
		}
		if dst == id {
			return true
		}
		seen[dst] = true
		queue = append(queue, dst)
	}
	for i := 0; i < len(queue); i++ {
		for dst := range g.outEdges[queue[i]] {
			if dst == id {
				return true
			}
			if !seen[dst] && len(g.outEdges[dst]) > 0 {
				seen[dst] = true
				queue = append(queue, dst)
			}
		}
	}
	return false
}

// Cycle members must be reachable from id and able to reach id.
func (g *dependencyGraph) markCycleMembersLocked(id manifest.NamedResource) {
	incoming := map[manifest.NamedResource][]manifest.NamedResource{}
	reachable := map[manifest.NamedResource]bool{id: true}
	queue := []manifest.NamedResource{id}
	for i := 0; i < len(queue); i++ {
		src := queue[i]
		for dst := range g.outEdges[src] {
			if len(g.outEdges[dst]) == 0 {
				continue
			}
			incoming[dst] = append(incoming[dst], src)
			if !reachable[dst] {
				reachable[dst] = true
				queue = append(queue, dst)
			}
		}
	}
	// Consumed reverse edges bound revisits without another visited set.
	queue = []manifest.NamedResource{id}
	for i := 0; i < len(queue); i++ {
		member := queue[i]
		g.failed[member] = ""
		queue = append(queue, incoming[member]...)
		delete(incoming, member)
	}
}

// findPathLocked uses sorted iterative DFS to return a path containing at least one edge.
func (g *dependencyGraph) findPathLocked(
	start, target manifest.NamedResource,
) ([]manifest.NamedResource, bool) {
	type frame struct {
		node     manifest.NamedResource
		children []manifest.NamedResource
		idx      int
	}
	visited := map[manifest.NamedResource]bool{}
	stack := []frame{{node: start, children: sortedNeighbors(g.outEdges[start])}}
	for len(stack) > 0 {
		top := &stack[len(stack)-1]
		if top.idx >= len(top.children) {
			stack = stack[:len(stack)-1]
			continue
		}
		next := top.children[top.idx]
		top.idx++
		if next == target {
			// Reconstruct the path: stack node values + target.
			path := make([]manifest.NamedResource, 0, len(stack)+1)
			for _, f := range stack {
				path = append(path, f.node)
			}
			path = append(path, target)
			return path, true
		}
		if visited[next] || next == start {
			continue
		}
		visited[next] = true
		stack = append(stack, frame{node: next, children: sortedNeighbors(g.outEdges[next])})
	}
	return nil, false
}

// edgeSetEqual reports whether the set of distinct entries in deps
// equals the key set of oldSet, without allocating. Used by ReplaceEdges
// to skip the newSet map build on the no-change fast path (the common
// case on every post-Bootstrap reconcile replay).
//
// Correct even when deps contains duplicates: the first loop rejects any
// dep missing from oldSet, and the second loop rejects any oldSet member
// missing from deps. Both passing implies dedup(deps) == keys(oldSet)
// regardless of repeats. deps lists are tiny (Flux dependsOn is a
// handful of entries), so the inner linear scan over deps is cheap.
func edgeSetEqual(oldSet map[manifest.NamedResource]struct{}, deps []manifest.NamedResource) bool {
	for _, d := range deps {
		if _, ok := oldSet[d]; !ok {
			return false
		}
	}
	for k := range oldSet {
		if !slices.Contains(deps, k) {
			return false
		}
	}
	return true
}

// sortedNeighbors returns the keys of edges in deterministic order
// (NamedResource.Compare). Callers range over the result, so an empty
// edge set yielding an empty slice is safe.
func sortedNeighbors(edges map[manifest.NamedResource]struct{}) []manifest.NamedResource {
	neighbors := make([]manifest.NamedResource, 0, len(edges))
	for dst := range edges {
		neighbors = append(neighbors, dst)
	}
	slices.SortFunc(neighbors, manifest.NamedResource.Compare)
	return neighbors
}

// Sorted roots and minimal rotations stabilize messages for overlapping cycles.
func (g *dependencyGraph) revalidateFailedLocked() {
	checked := make(map[manifest.NamedResource]struct{}, len(g.failed))
	for _, id := range slices.SortedFunc(maps.Keys(g.failed), manifest.NamedResource.Compare) {
		if _, seen := checked[id]; seen {
			continue
		}
		path, ok := g.findPathLocked(id, id)
		if !ok {
			delete(g.failed, id)
			continue
		}
		first := slices.Index(path, slices.MinFunc(path, manifest.NamedResource.Compare))
		cycle := slices.Concat(path[first:len(path)-1], path[:first+1])
		msg := "dependency cycle detected: " + formatCyclePath(cycle)
		for _, member := range path {
			if _, seen := checked[member]; seen {
				continue
			}
			g.failed[member] = msg
			checked[member] = struct{}{}
		}
	}
}

// snapshotFailedLocked returns a defensive copy of the failure map so
// the orchestrator can install it under preflightMu without aliasing
// the graph's internal state.
func (g *dependencyGraph) snapshotFailedLocked() map[manifest.NamedResource]string {
	if len(g.failed) == 0 {
		return nil
	}
	return maps.Clone(g.failed)
}

// Failures returns a snapshot of the current cycle-member set. Used by
// the orchestrator's bootstrap-time sweep to read the post-rebuild
// failure map without going through ReplaceEdges (the graph has
// already been populated by per-id ReplaceEdges calls).
func (g *dependencyGraph) Failures() map[manifest.NamedResource]string {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.snapshotFailedLocked()
}

// snapshotEdgesLocked returns the dependsOn graph as src → sorted dependency
// list, a defensive copy that doesn't alias the graph's internal sets. Nodes
// registered with no out-edges are omitted (only nodes that declare a
// dependency appear); dependencies are sorted for deterministic output. Returns
// nil when nothing depends on anything. Caller holds mu.
func (g *dependencyGraph) snapshotEdgesLocked() map[manifest.NamedResource][]manifest.NamedResource {
	out := make(map[manifest.NamedResource][]manifest.NamedResource, len(g.outEdges))
	for src, dsts := range g.outEdges {
		if len(dsts) == 0 {
			continue
		}
		out[src] = sortedNeighbors(dsts)
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

// Edges returns a snapshot of the declared dependsOn graph: each Kustomization
// or HelmRelease that names dependencies → the sorted ids it depends on
// (same-kind only, per the Flux spec). The orchestrator installs it into
// Result.DependsOn for impact / blast-radius analysis. Safe for concurrent use.
//
// The declared graph is returned verbatim: cycle members and self-edges (A → A)
// are NOT pruned — cycle reporting is the separate concern of Failures(), and a
// consumer walking these edges must tolerate them (e.g. a visited set). Returns
// nil (not an empty map) when nothing declares a dependsOn, mirroring Failures().
func (g *dependencyGraph) Edges() map[manifest.NamedResource][]manifest.NamedResource {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.snapshotEdgesLocked()
}
