package orchestrator

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/change"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/schedule"
	"github.com/home-operations/flate/pkg/store"
)

func TestDAG_SuspensionAfterRefresh(t *testing.T) {
	for _, changedOnly := range []bool{false, true} {
		for _, retained := range []bool{false, true} {
			var previous []byte
			for _, workers := range []int{2, 8} {
				t.Run(fmt.Sprintf("changed_%t/retained_%t/workers_%d", changedOnly, retained, workers), func(t *testing.T) {
					root := t.TempDir()
					gate, child := ksID("gate"), ksID("child")
					testutil.WriteFile(t, root, "flux/gate.yaml", strings.Replace(ksYAML("gate", "unused", ""), "spec:\n", "spec:\n  suspend: true\n", 1))
					for _, name := range []string{"child", "dependent", "independent"} {
						dep := ""
						switch name {
						case "child":
							dep = "gate"
						case "dependent":
							dep = "child"
						}
						testutil.WriteFile(t, root, "flux/"+name+".yaml", ksYAML(name, name, dep))
						testutil.WriteFile(t, root, name+"/kustomization.yaml", "resources: [cm.yaml]\n")
						testutil.WriteFile(t, root, name+"/cm.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: "+name+"}\ndata: {value: active}\n")
					}
					cfg := Config{Path: root, RepoRoot: root, CacheDir: t.TempDir(), Concurrency: workers}
					if changedOnly {
						cfg.ExternalChanges = change.NewSet([]string{"flux/gate.yaml", "flux/child.yaml", "dependent/cm.yaml", "independent/cm.yaml"})
					}
					o, err := New(cfg)
					if err != nil {
						t.Fatal(err)
					}
					t.Cleanup(o.Stop)
					if err := o.Bootstrap(t.Context()); err != nil {
						t.Fatal(err)
					}
					original, ok := o.store.Get[*manifest.Kustomization](child)
					if !ok || original.Suspend {
						t.Fatal("active child missing")
					}
					o.store.UpdateStatus(gate, store.StatusReady, store.MsgSuspended)
					o.store.UpdateStatus(child, store.StatusPending, "queued")
					o.configureControllers()
					var outcome schedule.Outcome
					var blocked []manifest.NamedResource
					o.tasks.Go(t.Context(), "dependent", func(ctx context.Context) {
						outcome, blocked, _ = (dagDispatcher{o}).Dispatch(ctx, ksID("dependent"), manifest.DrainNone)
					})
					o.tasks.BlockTillDone()
					if outcome != schedule.OutcomeBlocked || !slices.Contains(blocked, child) {
						t.Fatalf("dependent must first wait on child: outcome=%v blocked=%v", outcome, blocked)
					}
					retainedID := manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: child.Namespace, Name: "retained"}
					var artifact *store.KustomizationArtifact
					if retained {
						doc := map[string]any{"apiVersion": "v1", "kind": manifest.KindConfigMap, "metadata": map[string]any{"name": retainedID.Name, "namespace": retainedID.Namespace}, "data": map[string]any{"value": "prior"}}
						o.ksc.Emit(child, []map[string]any{doc}, false, true)
						artifact = &store.KustomizationArtifact{Manifests: []map[string]any{doc}, Fingerprint: "prior"}
						o.store.SetArtifact(child, artifact)
					}
					var publication, arrivals atomic.Int64
					unsub := o.store.AddListener(store.EventStatusUpdated, func(id manifest.NamedResource, payload any) {
						info, ok := payload.(store.StatusInfo)
						if id != child || !ok || info.Message != "resolving dependencies" || !publication.CompareAndSwap(0, 1) {
							return
						}
						docs, err := manifest.DecodeDocs(strings.NewReader(strings.Replace(ksYAML("child", "child", "gate"), "spec:\n", "spec:\n  suspend: true\n", 1)))
						if err != nil {
							t.Error(err)
							return
						}
						o.ksc.Emit(gate, docs, false, true)
					}, false)
					t.Cleanup(unsub)
					unsubArrivals := o.store.AddListener(store.EventObjectAdded, func(id manifest.NamedResource, _ any) {
						if id == child {
							owner, owned := o.store.EmittedParent(child)
							if !owned || owner != gate || changedOnly && !o.filter.ShouldReconcile(child) {
								t.Errorf("child arrived before ownership and selection: owner=%s owned=%t", owner, owned)
							}
						}
						if id == retainedID || id == (manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: child.Namespace, Name: child.Name}) {
							arrivals.Add(1)
						}
					}, false)
					t.Cleanup(unsubArrivals)
					res, err := o.Render(t.Context())
					if err != nil || len(res.Failed) != 0 || len(res.Blocked) != 0 || o.tasks.Failures() != 0 {
						t.Fatalf("scheduler failed to drain: error=%v failures=%v blocked=%v", err, res.Failed, res.Blocked)
					}
					info, _ := o.store.GetStatus(child)
					fresh, _ := o.store.Get[*manifest.Kustomization](child)
					if publication.Load() != 1 || arrivals.Load() != 0 || !store.IsSuspended(info) || !fresh.Suspend || original.Suspend {
						t.Fatalf("late suspension: publications=%d arrivals=%d status=%+v original=%+v fresh=%+v", publication.Load(), arrivals.Load(), info, original, fresh)
					}
					if owner, ok := o.store.EmittedParent(child); !ok || owner != gate {
						t.Fatalf("child owner=%s, found=%t", owner, ok)
					}
					if !slices.Contains(slices.Collect(o.index.ReadersOfFile("child/cm.yaml")), child) {
						t.Fatal("suspension lost the child's file read tracking")
					}
					if retained {
						if o.store.GetArtifact(child) != artifact || o.store.GetObject(retainedID) == nil || len(o.store.EmittedChildrenByParent()[child]) != 1 {
							t.Fatal("suspension pruned retained artifact or emission")
						}
					} else if o.store.GetArtifact(child) != nil || len(o.store.EmittedChildrenByParent()[child]) != 0 {
						t.Fatal("suspended child produced output")
					}
					for _, name := range []string{"dependent", "independent"} {
						id := manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: child.Namespace, Name: name}
						if o.store.GetObject(id) == nil || len(res.Manifests[ksID(name)]) != 1 {
							t.Fatalf("active output missing: %s", id)
						}
					}
					output, err := json.Marshal(canonicalBatches(res))
					if err != nil {
						t.Fatal(err)
					}
					if previous != nil && !bytes.Equal(previous, output) {
						t.Fatal("output bytes differ across worker counts")
					}
					previous = output
				})
			}
		}
	}
}

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
	// within it. A regression that reintroduces a
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

func TestDAG_SelectorResourceSetChain(t *testing.T) {
	for _, workers := range []int{2, 4} {
		t.Run(fmt.Sprintf("workers_%d", workers), func(t *testing.T) {
			const depth = 40
			dir := t.TempDir()
			testutil.WriteFile(t, dir, "ks.yaml", ksYAML("apps", "apps", ""))
			testutil.WriteFile(t, dir, "apps/kustomization.yaml", "resources:\n- sets.yaml\n- seed.yaml\n")
			testutil.WriteFile(t, dir, "apps/seed.yaml", `apiVersion: fluxcd.controlplane.io/v1
kind: ResourceSetInputProvider
metadata: {name: provider-00, namespace: flux-system, labels: {stage: "0"}}
spec: {type: Static, defaultValues: {value: ready}}
`)
			var sets strings.Builder
			for i := range depth {
				fmt.Fprintf(&sets, `---
apiVersion: fluxcd.controlplane.io/v1
kind: ResourceSet
metadata: {name: stage-%02d, namespace: flux-system}
spec:
  inputsFrom:
    - apiVersion: fluxcd.controlplane.io/v1
      kind: ResourceSetInputProvider
      selector:
        matchLabels: {stage: "%d"}
  resourcesTemplate: |
    << if inputs >>
    apiVersion: fluxcd.controlplane.io/v1
    kind: ResourceSetInputProvider
    metadata: {name: provider-%02d, namespace: flux-system, labels: {stage: "%d"}}
    spec: {type: Static, defaultValues: {value: ready}}
    ---
    apiVersion: example.com/v1
    kind: Widget
    metadata: {name: proof-%02d, namespace: flux-system}
    << end >>
`, i, i, i+1, i+1, i+1)
			}
			testutil.WriteFile(t, dir, "apps/sets.yaml", sets.String())
			ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
			defer cancel()
			var previous []byte
			for range 3 {
				o, err := New(Config{Path: dir, RepoRoot: dir, WipeSecrets: true, CacheDir: t.TempDir(), Concurrency: workers})
				if err != nil {
					t.Fatal(err)
				}
				res, err := o.Render(ctx)
				if err != nil || len(res.Failed) != 0 {
					t.Fatalf("selector chain failed: error=%v failures=%v", err, res.Failed)
				}
				for i := range depth + 1 {
					nid := manifest.NamedResource{Kind: manifest.KindResourceSetInputProvider, Namespace: "flux-system", Name: fmt.Sprintf("provider-%02d", i)}
					if _, ok := o.Store().Get[*manifest.ResourceSetInputProvider](nid); !ok {
						t.Fatalf("missing chain output %s", nid)
					}
				}
				if count := len(o.Store().ListObjects(manifest.KindResourceSetInputProvider)); count != depth+1 {
					t.Fatalf("provider count=%d, want %d", count, depth+1)
				}
				proofs := 0
				for _, doc := range res.Manifests[ksID("apps")] {
					if doc["kind"] == "Widget" {
						proofs++
					}
				}
				if proofs != depth {
					t.Fatalf("rendered provider proofs=%d, want %d", proofs, depth)
				}
				output, err := json.Marshal(res.Manifests[ksID("apps")])
				if err != nil {
					t.Fatal(err)
				}
				if previous != nil && !bytes.Equal(previous, output) {
					t.Fatal("repeated render bytes differ")
				}
				previous = output
			}
		})
	}
}
