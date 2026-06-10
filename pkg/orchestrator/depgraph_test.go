package orchestrator

import (
	"context"
	"testing"

	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
)

// TestDependencyGraph_Edges pins the snapshot the orchestrator installs into
// Result.DependsOn: dependencies are sorted, nodes with no declared deps are
// omitted, an empty graph yields nil, and the result is an independent copy.
func TestDependencyGraph_Edges(t *testing.T) {
	mk := func(name string) manifest.NamedResource {
		return manifest.NamedResource{Kind: manifest.KindKustomization, Namespace: "ns", Name: name}
	}
	a, b, c, lonely := mk("a"), mk("b"), mk("c"), mk("lonely")

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
