package orchestrator

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"

	"github.com/home-operations/flate/internal/admissiontest"
	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/change"
	"github.com/home-operations/flate/pkg/controllers/base"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

//nolint:paralleltest // The test calls a helper that changes process-global state.
func TestDAG_ParallelRenderedReplayConverges(t *testing.T) {
	dir := t.TempDir()
	testutil.WriteFile(t, dir, "kustomization.yaml", "resources: [flux/source.yaml, flux/ks.yaml]\n")
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
	testutil.WriteFile(
		t,
		dir,
		"charts/app/templates/cm.yaml",
		"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: greeting, namespace: flux-system}\ndata:\n  greeting: {{ .Values.greeting | quote }}\n",
	)
	testutil.WriteFile(
		t,
		dir,
		"charts/app/templates/bare.yaml",
		"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: bare}\ndata: {value: retained}\n",
	)
	o, err := newTestOrchestrator(
		t,
		Tree{Path: dir, RepoRoot: dir},
		Config{CacheDir: t.TempDir(), Concurrency: 2},
	)
	assert.FatalIf(t, err != nil, "%v", err)
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	res, err := o.render(ctx)
	assert.FatalIf(t, err != nil, "Render must converge: %v", err)
	assert.FatalIf(t, ctx.Err() != nil || len(res.Failed) != 0,
		"Render did not converge successfully: context=%v, failures=%v", ctx.Err(), res.Failed)

	for _, id := range []manifest.NamedResource{testutil.KustomizationID("flux-system", "apps"), testutil.HelmReleaseID("flux-system", "app")} {
		info, ok := o.Store().GetStatus(id)
		assert.FatalIf(
			t,
			!ok || info.Status != store.StatusReady,
			"%s status = (%+v, %v), want Ready",
			id,
			info,
			ok,
		)
	}
	cm, ok := o.Store().Get[*manifest.ConfigMap](
		manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: "flux-system", Name: "greeting"},
	)
	assert.FatalIf(t, !ok || cm.Data["greeting"] != "hello", "shared values were not rendered: %+v", cm)
	ksArt := o.store.GetArtifact(testutil.KustomizationID("flux-system", "apps"), store.ArtifactContent).(*store.Emission)
	var childDoc map[string]any
	for _, doc := range ksArt.Manifests {
		if manifest.DocKind(doc.Document) == manifest.KindHelmRelease {
			childDoc = doc.Document
		}
	}
	assert.FatalIf(t, childDoc == nil, "%v", "bare release document missing")
	metadata := manifest.DocMetadata(childDoc)
	ns := metadata.Namespace
	assert.FatalIf(t, ns != "flux-system", "release document namespace = %q", ns)
	hrID := testutil.HelmReleaseID("flux-system", "app")
	childBefore := manifest.DeepCopyMap(childDoc)
	hrArt := o.store.GetArtifact(hrID, store.ArtifactContent).(*store.Emission)
	retained := make([]map[string]any, len(hrArt.Manifests))
	for i, doc := range hrArt.Manifests {
		retained[i] = manifest.DeepCopyMap(doc.Document)
		{
			metadata2 := manifest.DocMetadata(doc.Document)
			name, localNs := metadata2.Name, metadata2.Namespace
			assert.FatalIf(
				t,
				name == "bare" && localNs != hrID.Namespace,
				"bare chart namespace = %q, want %q",
				localNs,
				hrID.Namespace,
			)
		}
	}
	bare := manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: hrID.Namespace, Name: "bare"}
	assert.FatalIf(t, o.store.GetObject(bare) == nil, "%v", "bare chart child lost release namespace identity")
	o.store.DeleteObject(bare)
	publishDocuments(
		o.ksc.Controller,
		testutil.KustomizationID("flux-system", "apps"),
		testutil.DocumentMaps(ksArt.Manifests),
		manifest.ParseDocOptions{DefaultNamespace: "flux-system"},
	)
	publishDocumentsResult1 := publishDocuments(
		o.hrc.Controller,
		hrID,
		testutil.DocumentMaps(hrArt.Manifests),
		manifest.ParseDocOptions{DefaultNamespace: hrID.Namespace},
	)
	_, replay := publishDocumentsResult1.Children, publishDocumentsResult1.Documents
	assert.FatalIf(t, o.store.GetObject(bare) == nil, "%v", "replay lost release namespace identity")
	next, err := testutil.DecodeDocs(
		strings.NewReader("apiVersion: v1\nkind: ConfigMap\nmetadata: {name: next, namespace: other}\n"),
	)
	assert.FatalIf(t, err != nil, "%v", err)
	_, err = manifest.ParseDoc(next[0], manifest.ParseDocOptions{DefaultNamespace: ""})
	assert.FatalIf(t, err != nil, "%v", err)
	manifest.ReleaseDoc(next[0])
	{
		delta := cmp.Diff(childBefore, childDoc)
		assert.FatalIf(t, delta != "", "replay changed bare release (-want +got):\n%s", delta)
	}
	{
		delta := cmp.Diff(retained, replay)
		assert.FatalIf(t, delta != "", "replay changed retained documents (-want +got):\n%s", delta)
	}
	metadata3 := manifest.DocMetadata(childDoc)
	ns = metadata3.Namespace
	assert.FatalIf(t, ns != "flux-system", "replay release namespace %q", ns)
}

func ksYAML(name, path, dependsOn string) string {
	dep := ""
	if dependsOn != "" {
		dep = "  dependsOn:\n    - name: " + dependsOn + "\n"
	}
	return testutil.KustomizationYAML(name, "flux-system", "  path: ./"+path+"\n"+dep+
		"  sourceRef: {kind: GitRepository, name: flux-system, namespace: flux-system}\n")
}

// TestDAG_RenderEmittedDependencyResolves is the KS-A2 scenario: a consumer
// dependsOn a Kustomization that does not exist on disk but is emitted by
// another KS's render of a REMOTE resources: URL. The dag scheduler must
// discover the render-emitted dependency and resolve the consumer Ready —
// exactly the case a static dangling-dep oracle could not see.
//
//nolint:paralleltest // The test calls a helper that changes process-global state.
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
	testutil.WriteFile(
		t,
		dir,
		"consumer/cm.yaml",
		"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: consumer}\ndata: {k: v}\n",
	)
	testutil.WriteFile(t, dir, "produced/kustomization.yaml", "resources:\n- cm.yaml\n")
	testutil.WriteFile(
		t,
		dir,
		"produced/cm.yaml",
		"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: produced}\ndata: {k: v}\n",
	)

	o, err := newTestOrchestrator(t, Tree{Path: dir}, Config{Concurrency: 4})
	assert.FatalIf(t, err != nil, "New: %v", err)
	{
		err := o.bootstrap(context.Background())
		assert.FatalIf(t, err != nil, "Bootstrap: %v", err)
	}
	{
		msg, ok := o.preflightFailure(testutil.KustomizationID("flux-system", "consumer"))
		assert.FatalIf(t, ok, "consumer should wait for rendered dependency, not fail preflight: %s", msg)
	}
	{
		err := o.run(context.Background())
		assert.FatalIf(t, err != nil, "Run: %v", err)
	}
	for _, name := range []string{"produced", "consumer", "producer"} {
		info, ok := o.Store().GetStatus(testutil.KustomizationID("flux-system", name))
		assert.FatalIf(
			t,
			!ok || info.Status != store.StatusReady,
			"%s status = (%+v, %v), want Ready under dag",
			name,
			info,
			ok,
		)
	}
}

// TestDAG_DanglingDependencyCascadesAndTerminates verifies the structural
// fixpoint terminator: a chain leaf→(absent) fails "dependency not found" and
// its consumer cascades with the leaf's terminal message — without riding any
// timeout. The bounded context proves termination is structural, not timed.
//
//nolint:paralleltest // The test calls a helper that changes process-global state.
func TestDAG_DanglingDependencyCascadesAndTerminates(t *testing.T) {
	dir := t.TempDir()
	// leaf dependsOn a Kustomization that is never defined or emitted; mid
	// dependsOn leaf.
	testutil.WriteFile(t, dir, "flux/leaf.yaml", ksYAML("leaf", "leaf", "ghost"))
	testutil.WriteFile(t, dir, "flux/mid.yaml", ksYAML("mid", "mid", "leaf"))
	testutil.WriteFile(t, dir, "leaf/kustomization.yaml", "resources:\n- cm.yaml\n")
	testutil.WriteFile(
		t,
		dir,
		"leaf/cm.yaml",
		"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: leaf}\ndata: {k: v}\n",
	)
	testutil.WriteFile(t, dir, "mid/kustomization.yaml", "resources:\n- cm.yaml\n")
	testutil.WriteFile(t, dir, "mid/cm.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: mid}\ndata: {k: v}\n")

	o, err := newTestOrchestrator(t, Tree{Path: dir}, Config{Concurrency: 4})
	assert.FatalIf(t, err != nil, "New: %v", err)
	// A short bounded context: the structural fixpoint must terminate well
	// within it. A regression that reintroduces a
	// blocking wait would hit this deadline.
	ctx, cancel := context.WithTimeout(context.Background(), 20_000_000_000) // 20s ceiling
	defer cancel()
	res, _ := o.render(ctx)
	assert.FatalIf(t, res == nil, "%v", "Render returned nil result")
	leafInfo, ok := res.Failed[testutil.KustomizationID("flux-system", "leaf")]
	assert.FatalIf(
		t,
		!ok || !strings.Contains(leafInfo.String(), "dependency not found"),
		"leaf: want FAILED 'dependency not found', got %+v (ok=%v)",
		leafInfo,
		ok,
	)
	midInfo, ok := res.Failed[testutil.KustomizationID("flux-system", "mid")]
	assert.FatalIf(
		t,
		!ok || !strings.Contains(midInfo.String(), "leaf"),
		"mid: want FAILED cascading leaf's failure, got %+v (ok=%v)",
		midInfo,
		ok,
	)
}

//nolint:paralleltest // Parallel renders contend for the build mutex within this chain's deadline.
func TestDAG_SelectorResourceSetChain(t *testing.T) {
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
	for i, ref := range []string{"apiVersion: example.test/v1", "apiVersion: v1", "kind: ResourceSet"} {
		fmt.Fprintf(&sets, `---
apiVersion: fluxcd.controlplane.io/v1
kind: ResourceSet
metadata: {name: foreign-%d, namespace: flux-system}
spec:
  inputsFrom:
    - %s
      selector: {matchLabels: {stage: "0"}}
  resourcesTemplate: |
    << if inputs >>
    apiVersion: example.com/v1
    kind: Widget
    metadata: {name: forbidden-%d}
    << end >>
`, i, ref, i)
	}
	manifests := strings.Replace(
		sets.String(),
		"    - apiVersion: fluxcd.controlplane.io/v1\n      kind: ResourceSetInputProvider\n      selector:",
		"    - selector:",
		1,
	)
	testutil.WriteFile(t, dir, "apps/sets.yaml", manifests)
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	var previous []byte
	for range 2 {
		renderedSingleton1, err := RenderTrees(
			ctx,
			[]Tree{{Path: dir, RepoRoot: dir, ID: manifest.TreeID{Cluster: ".", Side: manifest.TreeHead}}},
			Config{CacheDir: t.TempDir(), Concurrency: 2},
		)
		assert.FatalIf(t, len(renderedSingleton1) != 1, "missing render: %v", err)
		o := renderedSingleton1[0].Orchestrator
		res := renderedSingleton1[0].Result
		assert.FatalIf(t, err != nil || len(res.Failed) != 0,
			"selector chain failed: error=%v failures=%v", err, res.Failed)

		for i := range depth + 1 {
			nid := manifest.NamedResource{
				Group:     "fluxcd.controlplane.io",
				Kind:      manifest.KindResourceSetInputProvider,
				Namespace: "flux-system",
				Name:      fmt.Sprintf("provider-%02d", i),
			}
			{
				_, ok := o.Store().Get[*manifest.ResourceSetInputProvider](nid)
				assert.FatalIf(t, !ok, "missing chain output %s", nid)
			}
		}
		{
			count := len(
				o.Store().ListObjects(manifest.FluxOperatorDomain, manifest.KindResourceSetInputProvider),
			)
			assert.FatalIf(t, count != depth+1, "provider count=%d, want %d", count, depth+1)
		}
		proofs := 0
		for _, doc := range outputByParent(res.Manifests)[testutil.KustomizationID("flux-system", "apps")] {
			if doc["kind"] == "Widget" {
				proofs++
			}
		}
		assert.FatalIf(t, proofs != depth, "rendered provider proofs=%d, want %d", proofs, depth)
		output, err := json.Marshal(outputByParent(res.Manifests)[testutil.KustomizationID("flux-system", "apps")])
		assert.FatalIf(t, err != nil, "%v", err)
		assert.FatalIf(t, previous != nil && !bytes.Equal(previous, output), "%v", "repeated render bytes differ")
		previous = output
	}
}

//nolint:paralleltest // The test calls a helper that changes process-global state.
func TestDAG_SeedAdmissionPreservesSourceReaders(t *testing.T) {
	for _, supplied := range []bool{false, true} {
		t.Run(fmt.Sprintf("supplied_%t", supplied), func(t *testing.T) {
			root := t.TempDir()
			o, err := newTestOrchestrator(
				t,
				Tree{Path: root, RepoRoot: root},
				Config{CacheDir: t.TempDir(), Concurrency: 2},
			)
			assert.FatalIf(t, err != nil, "%s", err)

			t.Cleanup(func() {
				o.stop()
			})
			source := &manifest.GitRepository{Name: "ignored", Namespace: "flux-system"}
			consumer := &manifest.Kustomization{Name: "consumer", Namespace: "flux-system"}
			o.store.AddObject(consumer)
			if supplied {
				o.store.AddDeclaration(store.Declaration{Obj: source, Origin: manifest.SuppliedDeclaration})
			} else {
				o.store.AddObject(source)
			}
			index := &manifest.DiscoveryIndex{
				Records: map[manifest.NamedResource]manifest.DiscoveryRecord{consumer.Named(): {File: "consumer.yaml"}},
			}
			if supplied {
				index.Records[source.Named()] = manifest.DiscoveryRecord{
					Declaration: source,
					Origin:      manifest.SuppliedDeclaration,
				}
			}
			o.loader = admissiontest.New(t,
				admissiontest.Config{
					Changes:   change.NewSet([]string{"consumer.yaml"}),
					Index:     index,
					Inventory: o.store,
					Root:      root,
				})
			admissiontest.Commit(t, o.loader, nil)
			o.filter = o.loader.Options.Filter
			o.configureControllers()
			_, err = o.seedNodes()
			assert.NoError(t, err)
			var reads store.ReadSet
			reads.Init(o.store, consumer.Named(), nil)
			defer reads.Close()
			assert.FatalIf(t, reads.GetObject(source.Named()) != source, "%s", "source metadata is unavailable")

			var execution base.Execution
			execution.Init(o.src.Controller, source.Named())
			defer execution.Close()
			assert.FatalIf(t, execution.Reads.Admitted(), "%s", "unchanged source was admitted")
			assert.FatalIf(
				t,
				!reads.Valid(),
				"%s",
				"initial source admission invalidated a consumer that already captured its metadata",
			)
		})
	}
}
