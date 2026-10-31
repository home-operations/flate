package orchestrator

import (
	"fmt"
	"slices"
	"testing"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
)

func TestDependencyGraph_CycleMessages(t *testing.T) {
	t.Parallel()
	a, b, c := testutil.KustomizationID("ns", "a"), testutil.KustomizationID("ns", "b"), testutil.KustomizationID("ns", "c")
	ab := []manifest.NamedResource{a, b, a}
	ac := []manifest.NamedResource{a, c, a}
	t.Run("intersecting cycles", func(t *testing.T) {
		t.Parallel()
		for _, order := range [][]manifest.NamedResource{{a, b, c}, {c, b, a}, {b, a, c}} {
			g := newDependencyGraph()
			for _, id := range order {
				deps := []manifest.NamedResource{a}
				if id == a {
					deps = []manifest.NamedResource{c, b, c}
				}
				g.ReplaceEdges(id, deps)
			}
			assert.Diff(t, cyclePaths(g.Failures()), map[manifest.NamedResource][]manifest.NamedResource{a: ab, b: ab, c: ac})
		}
	})
	t.Run("partial break", func(t *testing.T) {
		t.Parallel()
		g := newDependencyGraph()
		g.ReplaceEdges(a, []manifest.NamedResource{b, c})
		g.ReplaceEdges(c, []manifest.NamedResource{a})
		g.ReplaceEdges(b, []manifest.NamedResource{a})
		g.ReplaceEdges(a, []manifest.NamedResource{c})
		assert.Diff(t, cyclePaths(g.Failures()), map[manifest.NamedResource][]manifest.NamedResource{a: ac, c: ac})
	})
	t.Run("self loop", func(t *testing.T) {
		t.Parallel()
		g := newDependencyGraph()
		g.ReplaceEdges(a, []manifest.NamedResource{a})
		assert.Diff(t, cyclePaths(g.Failures()), map[manifest.NamedResource][]manifest.NamedResource{
			a: {a, a},
		})
		g.ReplaceEdges(a, nil)
		assert.Equal(t, len(g.Failures()), 0)
	})
}

func TestDependencyGraph_NonMembersStayHealthy(t *testing.T) {
	t.Parallel()
	a, b, c, d, e := testutil.KustomizationID("ns", "a"), testutil.KustomizationID("ns", "b"), testutil.KustomizationID("ns", "c"), testutil.KustomizationID("ns", "d"), testutil.KustomizationID("ns", "e")
	g := newDependencyGraph()
	g.ReplaceEdges(c, []manifest.NamedResource{d})
	g.ReplaceEdges(d, []manifest.NamedResource{testutil.KustomizationID("ns", "leaf")})
	g.ReplaceEdges(a, []manifest.NamedResource{b})
	g.ReplaceEdges(b, []manifest.NamedResource{a, c}) // tail c -> d hangs off the cycle
	g.ReplaceEdges(e, []manifest.NamedResource{a})    // upstream of the cycle, not on it
	msg := []manifest.NamedResource{a, b, a}
	assert.Diff(t, cyclePaths(g.Failures()), map[manifest.NamedResource][]manifest.NamedResource{a: msg, b: msg})
}

func TestDependencyGraph_CycleMembershipRegistrationOrder(t *testing.T) {
	t.Parallel()
	x, y, w, z := testutil.KustomizationID("ns", "x"), testutil.KustomizationID("ns", "y"), testutil.KustomizationID("ns", "w"), testutil.KustomizationID("ns", "z")
	xw := []manifest.NamedResource{w, z, x, w}
	xy := []manifest.NamedResource{x, y, z, x}
	want := map[manifest.NamedResource][]manifest.NamedResource{w: xw, x: xw, y: xy, z: xw}
	deps := map[manifest.NamedResource][]manifest.NamedResource{x: {y, w}, y: {z}, w: {z}, z: {x}}
	nodes := []manifest.NamedResource{x, y, w, z}
	var checkOrder func([]manifest.NamedResource)
	checkOrder = func(order []manifest.NamedResource) {
		if len(order) == len(nodes) {
			t.Run(fmt.Sprint(order), func(t *testing.T) {
				g := newDependencyGraph()
				for _, id := range order {
					g.ReplaceEdges(id, deps[id])
				}
				assert.Diff(t, cyclePaths(g.Failures()), want)
			})
			return
		}
		for _, node := range nodes {
			if !slices.Contains(order, node) {
				checkOrder(append(order, node))
			}
		}
	}
	checkOrder(nil)
}

func cyclePaths(failures map[manifest.NamedResource]*dependencyCycle) map[manifest.NamedResource][]manifest.NamedResource {
	if failures == nil {
		return nil
	}
	paths := make(map[manifest.NamedResource][]manifest.NamedResource, len(failures))
	for id, cycle := range failures {
		paths[id] = cycle.path
	}
	return paths
}
