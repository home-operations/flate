package orchestrator

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

// ksYAML is a minimal Kustomization manifest for the dag tests.
func ksYAML(name, path, dependsOn string) string {
	dep := ""
	if dependsOn != "" {
		dep = "  dependsOn:\n    - name: " + dependsOn + "\n"
	}
	return "apiVersion: kustomize.toolkit.fluxcd.io/v1\n" +
		"kind: Kustomization\n" +
		"metadata:\n  name: " + name + "\n  namespace: flux-system\n" +
		"spec:\n  path: ./" + path + "\n" + dep +
		"  sourceRef: {kind: GitRepository, name: flux-system, namespace: flux-system}\n"
}

func ksID(name string) manifest.NamedResource {
	return manifest.NamedResource{Kind: manifest.KindKustomization, Namespace: "flux-system", Name: name}
}

// TestDAG_RenderEmittedDependencyResolves is the KS-A2 scenario: a consumer
// dependsOn a Kustomization that does not exist on disk but is emitted by
// another KS's render of a REMOTE resources: URL. The dag scheduler must
// discover the render-emitted dependency and resolve the consumer Ready —
// exactly the case a static dangling-dep oracle could not see.
func TestDAG_RenderEmittedDependencyResolves(t *testing.T) {
	dir := t.TempDir()
	produced := ksYAML("produced", "produced", "")
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(produced))
	}))
	t.Cleanup(srv.Close)
	testutil.WriteFile(t, dir, "flux/producer.yaml", ksYAML("producer", "producer", ""))
	testutil.WriteFile(t, dir, "flux/consumer.yaml", ksYAML("consumer", "consumer", "produced"))
	testutil.WriteFile(t, dir, "producer/kustomization.yaml", "resources:\n- "+srv.URL+"/produced.yaml\n")
	testutil.WriteFile(t, dir, "consumer/kustomization.yaml", "resources:\n- cm.yaml\n")
	testutil.WriteFile(t, dir, "consumer/cm.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: consumer}\ndata: {k: v}\n")
	testutil.WriteFile(t, dir, "produced/kustomization.yaml", "resources:\n- cm.yaml\n")
	testutil.WriteFile(t, dir, "produced/cm.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: produced}\ndata: {k: v}\n")

	o, err := New(Config{Path: dir, WipeSecrets: true, Concurrency: 4})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := o.Bootstrap(context.Background()); err != nil {
		t.Fatalf("Bootstrap: %v", err)
	}
	if err := o.Run(context.Background()); err != nil {
		t.Fatalf("Run: %v", err)
	}
	for _, name := range []string{"produced", "consumer", "producer"} {
		info, ok := o.Store().GetStatus(ksID(name))
		if !ok || info.Status != store.StatusReady {
			t.Fatalf("%s status = (%+v, %v), want Ready under dag", name, info, ok)
		}
	}
}

// TestDAG_DanglingDependencyCascadesAndTerminates verifies the structural
// fixpoint terminator: a chain leaf→(absent) fails "dependency not found" and
// its consumer cascades with the leaf's terminal message — without riding any
// timeout. The bounded context proves termination is structural, not timed.
func TestDAG_DanglingDependencyCascadesAndTerminates(t *testing.T) {
	dir := t.TempDir()
	// leaf dependsOn a Kustomization that is never defined or emitted; mid
	// dependsOn leaf.
	testutil.WriteFile(t, dir, "flux/leaf.yaml", ksYAML("leaf", "leaf", "ghost"))
	testutil.WriteFile(t, dir, "flux/mid.yaml", ksYAML("mid", "mid", "leaf"))
	testutil.WriteFile(t, dir, "leaf/kustomization.yaml", "resources:\n- cm.yaml\n")
	testutil.WriteFile(t, dir, "leaf/cm.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: leaf}\ndata: {k: v}\n")
	testutil.WriteFile(t, dir, "mid/kustomization.yaml", "resources:\n- cm.yaml\n")
	testutil.WriteFile(t, dir, "mid/cm.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: mid}\ndata: {k: v}\n")

	o, err := New(Config{Path: dir, WipeSecrets: true, Concurrency: 4})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	// A short bounded context: the structural fixpoint must terminate well
	// within it (no per-dep timeout cap). A regression that reintroduces a
	// blocking wait would hit this deadline.
	ctx, cancel := context.WithTimeout(context.Background(), 20_000_000_000) // 20s ceiling
	defer cancel()
	res, _ := o.Render(ctx)
	if res == nil {
		t.Fatal("Render returned nil result")
	}
	leafInfo, ok := res.Failed[ksID("leaf")]
	if !ok || !strings.Contains(leafInfo.Message, "dependency not found") {
		t.Fatalf("leaf: want FAILED 'dependency not found', got %+v (ok=%v)", leafInfo, ok)
	}
	midInfo, ok := res.Failed[ksID("mid")]
	if !ok || !strings.Contains(midInfo.Message, "leaf") {
		t.Fatalf("mid: want FAILED cascading leaf's failure, got %+v (ok=%v)", midInfo, ok)
	}
}
