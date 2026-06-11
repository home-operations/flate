package orchestrator

import (
	"context"
	"testing"

	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
)

// ksNode builds a Kustomization NamedResource in the "ns" namespace for the
// dependency-graph snapshot tests.
func ksNode(name string) manifest.NamedResource {
	return manifest.NamedResource{Kind: manifest.KindKustomization, Namespace: "ns", Name: name}
}

// TestDependencyGraph_Edges pins the snapshot the orchestrator installs into
// Result.DependsOn: dependencies are sorted, nodes with no declared deps are
// omitted, an empty graph yields nil, and the result is an independent copy.
func TestDependencyGraph_Edges(t *testing.T) {
	a, b, c, lonely := ksNode("a"), ksNode("b"), ksNode("c"), ksNode("lonely")

	g := newDependencyGraph()
	g.ReplaceEdges(a, []manifest.NamedResource{c, b}) // deliberately out of order
	g.ReplaceEdges(lonely, nil)                       // registered, but declares no deps

	edges := g.Edges()
	if len(edges) != 1 {
		t.Fatalf("Edges = %+v, want only 'a' (lonely declares no deps)", edges)
	}
	if got := edges[a]; len(got) != 2 || got[0] != b || got[1] != c {
		t.Fatalf("Edges[a] = %+v, want sorted [b c]", got)
	}
	if _, ok := edges[lonely]; ok {
		t.Error("a node with no dependencies must be omitted from Edges")
	}

	// Mutating a returned slice must not corrupt a later snapshot.
	edges[a][0] = manifest.NamedResource{}
	if again := g.Edges()[a]; again[0] != b {
		t.Errorf("Edges must return an independent snapshot; second call got %+v", again)
	}

	if got := newDependencyGraph().Edges(); got != nil {
		t.Errorf("empty graph Edges = %+v, want nil", got)
	}
}

// TestDependencyGraph_Edges_CyclesAndSelfEdges pins the load-bearing contract
// that Edges() returns cycle members and self-edges VERBATIM — the declared
// graph, unpruned. konflate's blast-radius walk relies on this: its visited-set
// cycle tolerance exists precisely because the producer emits cycle edges, so a
// refactor that silently filtered them would pass every other test while
// corrupting that analysis. (Cycles are still flagged via Failures(); that is a
// separate concern from the declared edges this snapshot exposes.)
func TestDependencyGraph_Edges_CyclesAndSelfEdges(t *testing.T) {
	a, b, self := ksNode("a"), ksNode("b"), ksNode("self")

	g := newDependencyGraph()
	g.ReplaceEdges(a, []manifest.NamedResource{b})       // a → b
	g.ReplaceEdges(b, []manifest.NamedResource{a})       // b → a : a 2-cycle
	g.ReplaceEdges(self, []manifest.NamedResource{self}) // self → self : a trivial 1-node cycle

	edges := g.Edges()
	if got := edges[a]; len(got) != 1 || got[0] != b {
		t.Errorf("cycle member a: Edges[a] = %+v, want [b] verbatim", got)
	}
	if got := edges[b]; len(got) != 1 || got[0] != a {
		t.Errorf("cycle member b: Edges[b] = %+v, want [a] verbatim", got)
	}
	if got := edges[self]; len(got) != 1 || got[0] != self {
		t.Errorf("self-edge: Edges[self] = %+v, want [self] verbatim", got)
	}
}

// TestDependencyGraph_Edges_TransitiveChain covers the canonical multi-level
// shape a → b → c, where b is simultaneously a dependency (of a) and a dependent
// (of c) — the structure konflate inverts and walks for transitive blast radius.
// Edges() exposes only DIRECTLY declared edges (no transitive closure), and the
// leaf c is omitted as a key while still appearing as b's dependency.
func TestDependencyGraph_Edges_TransitiveChain(t *testing.T) {
	a, b, c := ksNode("a"), ksNode("b"), ksNode("c")

	g := newDependencyGraph()
	g.ReplaceEdges(a, []manifest.NamedResource{b})
	g.ReplaceEdges(b, []manifest.NamedResource{c})

	edges := g.Edges()
	if len(edges) != 2 {
		t.Fatalf("Edges = %+v, want keys {a, b} (c is a leaf)", edges)
	}
	if got := edges[a]; len(got) != 1 || got[0] != b {
		t.Errorf("Edges[a] = %+v, want [b] — direct only, NOT transitive [b c]", got)
	}
	if got := edges[b]; len(got) != 1 || got[0] != c {
		t.Errorf("Edges[b] = %+v, want [c]", got)
	}
	if _, ok := edges[c]; ok {
		t.Error("leaf c declares no deps; it must be omitted as a key")
	}
}

// TestOrchestrator_Render_ExposesDependsOn confirms a full render surfaces the
// declared spec.dependsOn graph on Result.DependsOn — the blast-radius input
// konflate consumes — keyed by the dependent, with declarationless nodes omitted.
func TestOrchestrator_Render_ExposesDependsOn(t *testing.T) {
	dir := t.TempDir()
	testutil.WriteFile(t, dir, "flux/base.yaml", `apiVersion: kustomize.toolkit.fluxcd.io/v1
kind: Kustomization
metadata: {name: base, namespace: flux-system}
spec:
  path: ./base
  sourceRef: {kind: GitRepository, name: flux-system, namespace: flux-system}
`)
	testutil.WriteFile(t, dir, "flux/app.yaml", `apiVersion: kustomize.toolkit.fluxcd.io/v1
kind: Kustomization
metadata: {name: app, namespace: flux-system}
spec:
  path: ./app
  dependsOn:
    - name: base
  sourceRef: {kind: GitRepository, name: flux-system, namespace: flux-system}
`)
	testutil.WriteFile(t, dir, "base/kustomization.yaml", "resources:\n- cm.yaml\n")
	testutil.WriteFile(t, dir, "base/cm.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: base}\ndata: {k: v}\n")
	testutil.WriteFile(t, dir, "app/kustomization.yaml", "resources:\n- cm.yaml\n")
	testutil.WriteFile(t, dir, "app/cm.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: app}\ndata: {k: v}\n")

	o, err := New(Config{Path: dir, WipeSecrets: true, Concurrency: 4})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	res, err := o.Render(context.Background())
	if err != nil {
		t.Fatalf("Render: %v", err)
	}

	appID := manifest.NamedResource{Kind: manifest.KindKustomization, Namespace: "flux-system", Name: "app"}
	baseID := manifest.NamedResource{Kind: manifest.KindKustomization, Namespace: "flux-system", Name: "base"}
	if got := res.DependsOn[appID]; len(got) != 1 || got[0] != baseID {
		t.Fatalf("DependsOn[app] = %+v, want [%v]", got, baseID)
	}
	if got, ok := res.DependsOn[baseID]; ok {
		t.Errorf("base declares no dependsOn; it must be omitted, got %+v", got)
	}
}
