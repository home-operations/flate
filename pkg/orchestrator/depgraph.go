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
	// outEdges[src] is the set of dsts src depends on. Empty entries
	// (id present, value empty) are kept so ReplaceEdges sees the
	// prior-empty state and can diff against it (distinguishes
	// "never registered" from "registered with no deps").
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

// ReplaceEdges replaces id's out-edge set with deps. deps must already
// be kind-filtered (every entry shares id.Kind); cross-kind entries
// would create heterogeneous SCCs that the DFS walks below assume
// cannot occur.
//
// Callers read the post-update failure set via Failures().
// replacePreflightFailures already diffs the new failures map against
// the prior preflightFailures map to compute the "cleared" set for
// refire, so ReplaceEdges does not duplicate that delta computation.
//
// Algorithm:
//
//   - Diff old vs new out-edges for id. If unchanged, no-op.
//   - Apply the diff to outEdges.
//   - For each ADDED edge (id → dst), forward-DFS from dst checking
//     whether dst can reach id. Each such path is a new cycle; flag
//     every node on the path as failed.
//   - If any edges were REMOVED, an existing cycle involving id may
//     have broken. Re-validate every currently-failed node by checking
//     it's still in some cycle (reachable from itself via outEdges).
//     Re-validation is bounded by the number of currently-failed
//     nodes — in healthy graphs that's zero.
//
// Failure messages use the same "dependency cycle detected: A → B → A"
// format as the legacy formatCyclePath so downstream consumers
// (controllers' PreflightFailure lookups, log lines) see identical
// strings.
func (g *dependencyGraph) ReplaceEdges(id manifest.NamedResource, deps []manifest.NamedResource) {
	g.mu.Lock()
	defer g.mu.Unlock()

	oldSet := g.outEdges[id]

	// No-change fast path. The post-Bootstrap listener replays a
	// ReplaceEdges per id on every reconcile, and in a stable graph
	// almost every call passes deps identical to what's already
	// installed. Detect that against the existing oldSet WITHOUT
	// allocating the newSet map (the dominant per-reconcile allocation
	// on the warm path). The set built from deps equals oldSet iff every
	// dep is already in oldSet and every oldSet member appears in deps
	// — the second scan over the (tiny) deps slice makes this correct
	// even when deps carries duplicates, matching the map-dedup the
	// slow path below performs.
	if edgeSetEqual(oldSet, deps) {
		return
	}

	newSet := make(map[manifest.NamedResource]struct{}, len(deps))
	for _, d := range deps {
		// Self-edges produce a trivial 1-node cycle. Keep them so
		// the cycle-detection step records the failure; callers
		// don't filter them.
		newSet[d] = struct{}{}
	}

	// Diff so we can take the fast paths below.
	var addedDsts []manifest.NamedResource
	for d := range newSet {
		if _, had := oldSet[d]; !had {
			addedDsts = append(addedDsts, d)
		}
	}
	hasRemoved := false
	for d := range oldSet {
		if _, keep := newSet[d]; !keep {
			hasRemoved = true
			break
		}
	}

	// Install the new out-edge set.
	if len(newSet) == 0 {
		delete(g.outEdges, id)
	} else {
		g.outEdges[id] = newSet
	}

	// New cycles can only appear through ADDED edges. For each one,
	// forward-DFS from dst looking for a path back to id; every
	// node along such a path forms a new cycle.
	for _, dst := range addedDsts {
		path, ok := g.findPathLocked(dst, id)
		if !ok {
			continue
		}
		// path = [dst, ..., id]. The closed cycle is
		// [id, dst, ..., id]; prepending id yields the same
		// "close the loop visually" shape the legacy tri-color
		// DFS emitted, so formatCyclePath renders identical
		// messages downstream.
		cycle := make([]manifest.NamedResource, 0, len(path)+1)
		cycle = append(cycle, id)
		cycle = append(cycle, path...)
		msg := "dependency cycle detected: " + formatCyclePath(cycle)
		for _, m := range cycle {
			g.failed[m] = msg
		}
	}

	// If edges were removed, any previously-failed node may have
	// dropped out of every cycle. Revalidate the failed set.
	// Pure-add updates cannot break an existing cycle, so the
	// revalidation pass is skipped on the hot path (Bootstrap and
	// render-emit-of-new-objects).
	if hasRemoved {
		g.revalidateFailedLocked()
	}
}

// findPathLocked runs an iterative forward DFS from start looking for
// target. Returns the node-path start → ... → target (inclusive of
// both endpoints) when found. Iterative (stack-based) to avoid Go's
// goroutine-stack growth on adversarial inputs — a 5k-node chain
// would blow recursion in tests with -gcflags=-l.
//
// Visit ordering inside the DFS is sorted so multi-cycle outputs are
// deterministic across runs — log diffs and golden-file tests in
// downstream consumers rely on this. The legacy DFS sorted outgoing
// edges; this preserves the same behavior.
func (g *dependencyGraph) findPathLocked(
	start, target manifest.NamedResource,
) ([]manifest.NamedResource, bool) {
	// NOTE: start == target is NOT a short-circuit hit — the caller
	// is asking "does start reach target via at least one edge". The
	// reachability test in revalidateFailedLocked depends on this:
	// a failed node that's lost all outgoing edges must report
	// false (not "trivially yes I am myself") so its stale failure
	// entry gets cleared. Self-loops still work because the DFS
	// below visits the self-edge and reports [start, start].
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
			// Skip nodes we've already explored from. The next ==
			// start guard prevents re-pushing the root frame when
			// a non-self-loop back-edge points at start; the
			// outer-loop visited map intentionally omits start so
			// the self-loop case (start ∈ out[start]) still reports
			// the cycle on the first iteration.
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
	return slices.SortedFunc(maps.Keys(edges), manifest.NamedResource.Compare)
}

// revalidateFailedLocked walks every currently-failed node and checks
// whether it's still in a cycle. Nodes that are no longer reachable
// from themselves are removed from the failed map. Bounded by the
// size of the failed set — typically zero, never more than a handful
// of cycle members in production graphs.
//
// The cycle-membership check is "can node reach itself via outEdges":
// equivalent to "node is in a non-trivial SCC OR has a self-loop".
func (g *dependencyGraph) revalidateFailedLocked() {
	if len(g.failed) == 0 {
		return
	}
	// Snapshot keys so we can delete while iterating.
	for _, id := range slices.Collect(maps.Keys(g.failed)) {
		if _, ok := g.findPathLocked(id, id); !ok {
			delete(g.failed, id)
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
		// Collect-and-sort by hand (pre-sized) rather than the sortedNeighbors
		// helper: sortedNeighbors goes through slices.SortedFunc(maps.Keys(...)),
		// which starts from a nil slice with no capacity hint and allocates an
		// iterator closure per call. This snapshot walks the whole graph, so the
		// pre-sized make() is the leaner form — don't "DRY" it into sortedNeighbors.
		deps := make([]manifest.NamedResource, 0, len(dsts))
		for dst := range dsts {
			deps = append(deps, dst)
		}
		slices.SortFunc(deps, manifest.NamedResource.Compare)
		out[src] = deps
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
