package orchestrator

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

func TestDAG_ParallelRenderedReplayConverges(t *testing.T) {
	for _, workers := range []int{2, 4} {
		t.Run(fmt.Sprintf("workers_%d", workers), func(t *testing.T) {
			dir := t.TempDir()
			hr := `apiVersion: helm.toolkit.fluxcd.io/v2
kind: HelmRelease
metadata: {name: app, namespace: flux-system}
spec:
  interval: 10m
  chart:
    spec:
      chart: charts/app
      sourceRef: {kind: GitRepository, name: flux-system, namespace: flux-system}
  valuesFrom:
    - kind: ConfigMap
      name: shared-values
`
			testutil.WriteFile(t, dir, "flux/source.yaml", `apiVersion: source.toolkit.fluxcd.io/v1
kind: GitRepository
metadata: {name: flux-system, namespace: flux-system}
spec:
  url: https://example.test/cluster.git
`)
			testutil.WriteFile(t, dir, "flux/ks.yaml", ksYAML("apps", "apps", ""))
			testutil.WriteFile(t, dir, "apps/kustomization.yaml", "resources:\n- hr.yaml\n- values.yaml\n")
			testutil.WriteFile(t, dir, "apps/hr.yaml", hr)
			testutil.WriteFile(t, dir, "apps/values.yaml", `apiVersion: v1
kind: ConfigMap
metadata: {name: shared-values, namespace: flux-system}
data:
  values.yaml: |
    greeting: hello
`)
			testutil.WriteFile(t, dir, "charts/app/Chart.yaml", "apiVersion: v2\nname: app\nversion: 0.1.0\n")
			// A chart may emit Flux resources; replaying its own unchanged HR
			// must not request another reconcile while that HR is running.
			testutil.WriteFile(t, dir, "charts/app/templates/hr.yaml", hr)
			testutil.WriteFile(t, dir, "charts/app/templates/cm.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: greeting, namespace: flux-system}\ndata:\n  greeting: {{ .Values.greeting | quote }}\n")
			o, err := New(Config{Path: dir, RepoRoot: dir, WipeSecrets: true, CacheDir: t.TempDir(), Concurrency: workers})
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			res, err := o.Render(ctx)
			if err != nil {
				t.Fatalf("Render must converge: %v", err)
			}
			if ctx.Err() != nil || len(res.Failed) != 0 {
				t.Fatalf("Render did not converge successfully: context=%v, failures=%v", ctx.Err(), res.Failed)
			}
			for _, id := range []manifest.NamedResource{ksID("apps"), {Kind: manifest.KindHelmRelease, Namespace: "flux-system", Name: "app"}} {
				info, ok := o.Store().GetStatus(id)
				if !ok || info.Status != store.StatusReady {
					t.Fatalf("%s status = (%+v, %v), want Ready", id, info, ok)
				}
			}
			cm, ok := o.Store().Get[*manifest.ConfigMap](manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: "flux-system", Name: "greeting"})
			if !ok || cm.Data["greeting"] != "hello" {
				t.Fatalf("shared values were not rendered: %+v", cm)
			}
		})
	}
}

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
