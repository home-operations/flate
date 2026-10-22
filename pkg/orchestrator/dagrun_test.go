package orchestrator

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/internal/testutil/controllertest"
	"github.com/home-operations/flate/pkg/change"
	"github.com/home-operations/flate/pkg/controllers/base"
	"github.com/home-operations/flate/pkg/depwait"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/schedule"
	"github.com/home-operations/flate/pkg/store"
	"github.com/home-operations/flate/pkg/task"
)

func TestDAG_KeepBeforeLateDataPublication(t *testing.T) {
	t.Parallel()
	for _, workers := range []int{2, 16} {
		t.Run(fmt.Sprintf("workers=%d", workers), func(t *testing.T) {
			t.Parallel()
			st, tasks := store.New(), task.NewBounded(workers)
			parent := &manifest.Kustomization{Name: "parent", Namespace: "flux-system", TargetNamespace: "tenant"}
			provider := &manifest.Kustomization{Name: "provider", Namespace: "tenant"}
			sibling := &manifest.Kustomization{Name: "sibling", Namespace: "tenant"}
			child := testutil.KustomizationID("tenant", "child")
			data := manifest.NamedResource{Kind: manifest.KindConfigMap, Name: "settings", Namespace: "tenant"}
			st.AddObjects([]manifest.BaseManifest{parent, provider, sibling})
			index := &manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{
				parent.Named(): {File: "parent.yaml"}, provider.Named(): {File: "provider.yaml"}, sibling.Named(): {File: "sibling.yaml"},
				data: {Productions: []manifest.Production{{Owner: provider.Named(), Authored: data, File: "settings.yaml", Source: manifest.BootstrapSourceID}}},
			}}
			st.SetDiscoveryIndex(index)
			filter := change.NewFilter(change.NewSet([]string{"parent.yaml"}), index, st, "")
			c := base.New(st, tasks)
			c.Configure(base.Options{Filter: filter, Index: index})
			c.StartLifecycle()
			childDocs := []map[string]any{{"apiVersion": "kustomize.toolkit.fluxcd.io/v1", "kind": "Kustomization", "metadata": map[string]any{"name": child.Name}, "spec": map[string]any{"path": "./child", "postBuild": map[string]any{"substituteFrom": []any{map[string]any{"kind": manifest.KindConfigMap, "name": data.Name}}}}}}
			children, retained := c.ParseChildren(parent, childDocs, manifest.ParseDocOptions{})
			assert.Equal(t, len(children), 1)
			assert.Equal(t, children[0].Obj.Named(), child)
			blocked, release := make(chan []manifest.NamedResource, 1), make(chan struct{})
			var childRuns, siblingRuns atomic.Int64
			arrivals := make(chan bool, 2)
			unsubArrival := st.AddListener(store.EventObjectAdded, func(id manifest.NamedResource, payload any) {
				if payload != nil && (id == child || id == data) {
					arrivals <- filter.ShouldReconcile(id)
				}
			}, false)
			defer unsubArrival()
			d := fixtureDispatcher{store: st, dispatch: func(ctx context.Context, id schedule.NodeID, level manifest.DrainLevel) schedule.Result {
				if id == provider.Named() {
					tasks.YieldSlot(func() { <-release })
				}
				e := controllertest.NewExecution(c, id)
				e.Dispatch(ctx, level, e, func(e *base.Execution, _ context.Context, _ *manifest.Kustomization, _ manifest.DrainLevel) error {
					_, _, next := e.Reads.GetEmission(id)
					defer next.Finish()
					switch id {
					case parent.Named():
						next.Children, next.Manifests = children, retained
					case provider.Named():
						next.Children = []store.Child{{Obj: &manifest.ConfigMap{Name: data.Name, Namespace: data.Namespace, Data: map[string]any{"VALUE": "ready"}}}}
					case child:
						childRuns.Add(1)
						obj := e.Reads.GetObject(data)
						if obj == nil {
							return &depwait.ErrBlocked{Deps: []manifest.NamedResource{data}}
						}
						next.Children = []store.Child{{Obj: &manifest.ConfigMap{Name: "output", Namespace: data.Namespace, Data: map[string]any{"VALUE": obj.(*manifest.ConfigMap).Data["VALUE"]}}}}
					case sibling.Named():
						siblingRuns.Add(1)
					}
					next.Complete = true
					return e.Emit(id, &next, nil)
				})
				if id == child && e.Outcome() == schedule.Blocked {
					blocked <- slices.Clone(e.Blocked)
				}
				return schedule.Result{Assessment: e}
			}}
			o := &Orchestrator{store: st}
			s := schedule.New(tasks, d)
			unsubscribe := o.watchDAG(s)
			defer unsubscribe()
			s.Seed([]schedule.NodeID{parent.Named(), provider.Named(), sibling.Named()})
			done := make(chan error, 1)
			go func() { done <- s.Run(t.Context()) }()
			assert.Diff(t, <-blocked, []manifest.NamedResource{data})
			assert.Equal(t, st.GetObject(data), nil)
			assert.Equal(t, filter.ShouldReconcile(child), true)
			assert.Equal(t, filter.ShouldReconcile(provider.Named()), true)
			assert.Equal(t, filter.ShouldReconcile(sibling.Named()), false)
			close(release)
			assert.NoError(t, <-done)
			tasks.BlockTillDone()
			assert.Equal(t, tasks.Failures(), int64(0))
			assert.Equal(t, t.Context().Err(), nil)
			assert.Equal(t, childRuns.Load(), int64(2))
			assert.Equal(t, siblingRuns.Load(), int64(0))
			assert.Equal(t, len(arrivals), 2)
			for range len(arrivals) {
				assert.Equal(t, <-arrivals, true)
			}
			assert.Equal(t, len(s.Results()), 4)
			for _, id := range []manifest.NamedResource{parent.Named(), provider.Named(), child, sibling.Named()} {
				status, ok := st.GetStatus(id)
				assert.Equal(t, ok, true)
				assert.Equal(t, status.Status, store.StatusReady)
				assert.Equal(t, len(status.BlockedBy()), 0)
			}
			assert.Equal(t, len(st.FailedResources()), 0)
			assert.Equal(t, st.GetObject(testutil.KustomizationID("", "child")), nil)
			assert.Equal(t, st.GetObject(testutil.KustomizationID("flux-system", "child")), nil)
			output := st.GetObject(manifest.NamedResource{Kind: manifest.KindConfigMap, Name: "output", Namespace: "tenant"}).(*manifest.ConfigMap)
			assert.Diff(t, output.Data, map[string]any{"VALUE": "ready"})
		})
	}
}

func TestDAG_BoundedPublications(t *testing.T) {
	t.Parallel()
	for _, workers := range []int{2, 16} {
		t.Run(fmt.Sprintf("workers=%d", workers), func(t *testing.T) {
			t.Parallel()
			first := renderScalePublications(t, workers, 160)
			second := renderScalePublications(t, workers, 160)
			assert.Equal(t, bytes.Equal(first, second), true)
		})
	}
}

func TestDAG_ScalePublications(t *testing.T) {
	t.Parallel()
	if os.Getenv("FLATE_E8_SLOW") != "1" {
		t.Skip("set FLATE_E8_SLOW=1 for 1000 producers and 10000 publications")
	}
	for _, workers := range []int{2, 16} {
		t.Run(fmt.Sprintf("workers=%d", workers), func(t *testing.T) {
			t.Parallel()
			first := renderScalePublications(t, workers, 1000)
			second := renderScalePublications(t, workers, 1000)
			assert.Equal(t, bytes.Equal(first, second), true)
		})
	}
}

func renderScalePublications(t *testing.T, workers, producers int) []byte {
	t.Helper()
	root := t.TempDir()
	var resources strings.Builder
	for n := range 10 {
		name := fmt.Sprintf("object-%02d", n)
		fmt.Fprintf(&resources, "- %s.yaml\n", name)
		testutil.WriteFile(t, root, "shared/"+name+".yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: "+name+", namespace: apps}\ndata: {value: shared}\n")
	}
	testutil.WriteFile(t, root, "shared/kustomization.yaml", "resources:\n"+resources.String())
	ids := make([]manifest.NamedResource, producers)
	roots := make(map[manifest.NamedResource]bool, producers-128)
	for n := range producers {
		name := fmt.Sprintf("producer-%04d", n)
		ids[n] = testutil.KustomizationID("flux-system", name)
		var deps strings.Builder
		switch {
		case n > 0 && n < 64:
			fmt.Fprintf(&deps, "  dependsOn: [{name: producer-%04d}]\n", n-1)
		case n >= 64 && n < 128:
			deps.WriteString("  dependsOn: [{name: producer-0000}]\n")
		case n == 128:
			deps.WriteString("  dependsOn:\n")
			for dep := 64; dep < 128; dep++ {
				fmt.Fprintf(&deps, "    - name: producer-%04d\n", dep)
			}
		default:
			roots[ids[n]] = true
		}
		testutil.WriteFile(t, root, "apps/"+name+"/kustomization.yaml", "resources: [../../shared]\nnamePrefix: "+name+"-\n")
		testutil.WriteFile(t, root, "flux/"+name+".yaml", testutil.KustomizationYAML(name, "flux-system", "  path: ./apps/"+name+"\n"+deps.String()+"  sourceRef: {kind: GitRepository, name: flux-system}\n"))
	}
	o, err := newTestOrchestrator(t, Tree{Path: filepath.Join(root, "flux"), RepoRoot: root}, Config{Concurrency: workers})
	assert.NoError(t, err)
	defer o.stop()
	assert.NoError(t, o.bootstrap(t.Context()))
	o.configureControllers()
	o.src.StartLifecycle()
	o.ksc.StartLifecycle()
	firstWave := min(workers, len(roots))
	started, release := make(chan struct{}, firstWave), make(chan struct{})
	var active, overBound, rootStarts, publications atomic.Int64
	d := fixtureDispatcher{store: o.store, dispatch: func(ctx context.Context, id schedule.NodeID, level manifest.DrainLevel) schedule.Result {
		// Input acquisition yields its slot; only admission holds it continuously.
		func() {
			current := active.Add(1)
			defer active.Add(-1)
			if current > int64(workers) {
				overBound.Add(1)
			}
			if roots[id] && rootStarts.Add(1) <= int64(firstWave) {
				started <- struct{}{}
				<-release
			}
		}()
		return (dagDispatcher{o}).Dispatch(ctx, id, level)
	}}
	s := schedule.New(o.tasks, d)
	unsubscribe := o.watchDAG(s)
	defer unsubscribe()
	unsubPublications := o.store.AddListener(store.EventObjectAdded, func(id manifest.NamedResource, payload any) {
		if id.Kind == manifest.KindConfigMap && payload != nil {
			publications.Add(1)
		}
	}, false)
	defer unsubPublications()
	seeds := o.seedNodes()
	s.Seed(seeds)
	done := make(chan error, 1)
	go func() { done <- s.Run(t.Context()) }()
	for range firstWave {
		<-started
	}
	assert.Equal(t, active.Load(), int64(firstWave))
	close(release)
	assert.NoError(t, <-done)
	o.tasks.BlockTillDone()
	assert.NoError(t, o.finalize())
	assert.Equal(t, t.Context().Err(), nil)
	assert.Equal(t, active.Load(), int64(0))
	assert.Equal(t, overBound.Load(), int64(0))
	assert.Equal(t, o.tasks.Failures(), int64(0))
	results := s.Results()
	assert.Equal(t, len(results), len(seeds))
	for _, id := range seeds {
		result, ok := results[id]
		assert.Equal(t, ok, true)
		if ok {
			assert.Equal(t, result.Assessment.Outcome(), schedule.Terminal)
		}
	}
	for _, id := range ids {
		status, ok := o.store.GetStatus(id)
		assert.Equal(t, ok, true)
		assert.Equal(t, status.Status, store.StatusReady)
		for n := range 10 {
			child := manifest.NamedResource{Kind: manifest.KindConfigMap, Name: fmt.Sprintf("%s-object-%02d", id.Name, n), Namespace: "apps"}
			assert.Equal(t, o.store.GetObject(child) != nil, true)
		}
	}
	assert.Equal(t, publications.Load(), int64(producers*10))
	assert.Equal(t, len(o.store.ListObjects("", manifest.KindConfigMap)), producers*10)
	assert.Equal(t, len(o.store.FailedResources()), 0)
	output, err := json.Marshal(o.collectManifests())
	assert.NoError(t, err)
	return output
}

func TestDAG_SuspensionAfterRefresh(t *testing.T) {
	for _, changedOnly := range []bool{false, true} {
		for _, retained := range []bool{false, true} {
			var previous []byte
			for _, workers := range []int{2, 8} {
				if workers == 8 && (!changedOnly || !retained) {
					continue
				}
				t.Run(fmt.Sprintf("changed_%t/retained_%t/workers_%d", changedOnly, retained, workers), func(t *testing.T) {
					root := t.TempDir()
					gate, child := testutil.KustomizationID("flux-system", "gate"), testutil.KustomizationID("flux-system", "child")
					testutil.WriteFile(t, root, "unused/kustomization.yaml", "resources: [child.yaml]\n")
					testutil.WriteFile(t, root, "flux/gate.yaml", strings.Replace(ksYAML("gate", "unused", ""), "spec:\n", "spec:\n  suspend: true\n", 1))
					for _, name := range []string{"child", "dependent", "independent"} {
						dep := ""
						switch name {
						case "child":
							dep = "gate"
						case "dependent":
							dep = "child"
						}
						file := "flux/" + name + ".yaml"
						if name == "child" {
							file = "unused/child.yaml"
						}
						testutil.WriteFile(t, root, file, ksYAML(name, name, dep))
						testutil.WriteFile(t, root, name+"/kustomization.yaml", "resources: [cm.yaml]\n")
						testutil.WriteFile(t, root, name+"/cm.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: "+name+"}\ndata: {value: active}\n")
					}
					cfg := treeConfig{CacheDir: t.TempDir(), Concurrency: workers, Path: root, RepoRoot: root}
					if changedOnly {
						cfg.ExternalChanges = change.NewSet([]string{"flux/gate.yaml", "unused/child.yaml", "dependent/cm.yaml", "independent/cm.yaml"})
					}
					o, err := newTestOrchestrator(t, cfg.Tree, cfg.Config)
					if err != nil {
						t.Fatal(err)
					}
					t.Cleanup(func() {
						o.stop()
					})
					if err := o.bootstrap(t.Context()); err != nil {
						t.Fatal(err)
					}
					original, ok := o.store.Get[*manifest.Kustomization](child)
					if !ok || original.Suspend {
						t.Fatal("active child missing")
					}
					o.store.UpdateStatus(gate, store.StatusInfo{Status: store.StatusReady, Reason: store.ReasonSuspended, Message: "suspended"})
					o.store.UpdateStatus(child, store.StatusInfo{Status: store.StatusPending, Reason: "", Message: "queued"})
					o.configureControllers()
					o.ksc.Emit(gate, &store.Emission{Children: []store.Child{{Obj: original}}, Complete: true}, nil)
					var result schedule.Result
					o.tasks.Go(t.Context(), "dependent", func(ctx context.Context) {
						result = (dagDispatcher{o}).Dispatch(ctx, testutil.KustomizationID("flux-system", "dependent"), manifest.DrainNone)
					})
					o.tasks.BlockTillDone()
					if result.Assessment.Outcome() != schedule.Blocked || !slices.Contains(result.Assessment.(*base.Execution).Blocked, child) {
						t.Fatalf("dependent must first wait on child: %+v", result.Assessment)
					}
					retainedID := manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: child.Namespace, Name: "retained"}
					var artifact *store.Emission
					if retained {
						doc := map[string]any{"apiVersion": "v1", "kind": manifest.KindConfigMap, "metadata": map[string]any{"name": retainedID.Name, "namespace": retainedID.Namespace}, "data": map[string]any{"value": "prior"}}
						publishDocuments(o.ksc.Controller, child, []map[string]any{doc}, manifest.ParseDocOptions{WipeSecrets: false, DefaultNamespace: child.Namespace})
						artifact = o.store.GetArtifact(child, store.ArtifactContent).(*store.Emission)
					}
					var publication, arrivals atomic.Int64
					unsub := o.store.AddListener(store.EventStatusUpdated, func(id manifest.NamedResource, payload any) {
						info, ok := payload.(store.StatusInfo)
						if id != child || !ok || info.String() != "resolving dependencies" || !publication.CompareAndSwap(0, 1) {
							return
						}
						docs, err := manifest.DecodeDocs(strings.NewReader(strings.Replace(ksYAML("child", "child", "gate"), "spec:\n", "spec:\n  suspend: true\n", 1)))
						if err != nil {
							t.Error(err)
							return
						}
						children, retained := parseChildrenForTest(gate, docs, manifest.ParseDocOptions{DefaultNamespace: gate.Namespace})
						for _, emitted := range children {
							if o.filter != nil {
								o.filter.AddEmitted(gate, emitted, o.store)
							}
						}
						_, _, next := o.store.GetEmission(gate)
						defer next.Finish()
						next.Children, next.Manifests, next.Complete = children, retained, true
						o.store.RecordEmitted(gate, &next, nil)
					}, false)
					t.Cleanup(unsub)
					unsubArrivals := o.store.AddListener(store.EventObjectAdded, func(id manifest.NamedResource, _ any) {
						if id == child {
							owners := testutil.Owners(o.store.Productions(child, nil))
							if !slices.Contains(owners, gate) || changedOnly && !o.filter.ShouldReconcile(child) {
								t.Errorf("child arrived before evidence/selection: %v", owners)
							}
						}
						if id == retainedID || id == (manifest.NamedResource{Kind: manifest.KindConfigMap, Name: child.Name}) {
							arrivals.Add(1)
						}
					}, false)
					t.Cleanup(unsubArrivals)
					res, err := o.collectResult(o.run(t.Context()))
					if err != nil || len(res.Failed) != 0 || len(res.Blocked) != 0 || o.tasks.Failures() != 0 {
						t.Fatalf("scheduler failed to drain: error=%v failures=%v blocked=%v", err, res.Failed, res.Blocked)
					}
					info, _ := o.store.GetStatus(child)
					fresh, _ := o.store.Get[*manifest.Kustomization](child)
					if publication.Load() != 1 || arrivals.Load() != 0 || info.Outcome() != store.OutcomeSuspended || !fresh.Suspend || original.Suspend {
						t.Fatalf("late suspension: publications=%d arrivals=%d status=%+v original=%+v fresh=%+v", publication.Load(), arrivals.Load(), info, original, fresh)
					}
					if owners := testutil.Owners(o.store.Productions(child, nil)); !slices.Contains(owners, gate) {
						t.Fatalf("missing independent contributor: %v", owners)
					}
					if !slices.ContainsFunc(slices.Collect(o.index.ReadersOfFile("child/cm.yaml")), func(r manifest.FileReader) bool { return r.Reader == child }) {
						t.Fatal("suspension lost the child's file read tracking")
					}
					_, _, inventory := o.store.GetEmission(child)
					if retained {
						if len(artifact.Manifests) != 1 || o.store.GetArtifact(child, store.ArtifactContent) != nil || o.store.GetObject(retainedID) == nil || len(inventory.Children) != 1 {
							t.Fatal("suspension pruned retained artifact or emission")
						}
					} else if o.store.GetArtifact(child, store.ArtifactContent) != nil || len(inventory.Children) != 0 {
						t.Fatal("suspended child produced output")
					}
					for _, name := range []string{"dependent", "independent"} {
						id := manifest.NamedResource{Kind: manifest.KindConfigMap, Name: name}
						if o.store.GetObject(id) == nil || len(outputByParent(res.Manifests)[testutil.KustomizationID("flux-system", name)]) != 1 {
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
	testutil.WriteFile(t, dir, "charts/app/templates/bare.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: bare}\ndata: {value: retained}\n")
	o, err := newTestOrchestrator(t, Tree{Path: dir, RepoRoot: dir}, Config{WipeSecrets: true, CacheDir: t.TempDir(), Concurrency: 2})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	res, err := o.render(ctx)
	if err != nil {
		t.Fatalf("Render must converge: %v", err)
	}
	if ctx.Err() != nil || len(res.Failed) != 0 {
		t.Fatalf("Render did not converge successfully: context=%v, failures=%v", ctx.Err(), res.Failed)
	}
	for _, id := range []manifest.NamedResource{testutil.KustomizationID("flux-system", "apps"), testutil.HelmReleaseID("flux-system", "app")} {
		info, ok := o.Store().GetStatus(id)
		if !ok || info.Status != store.StatusReady {
			t.Fatalf("%s status = (%+v, %v), want Ready", id, info, ok)
		}
	}
	cm, ok := o.Store().Get[*manifest.ConfigMap](manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: "flux-system", Name: "greeting"})
	if !ok || cm.Data["greeting"] != "hello" {
		t.Fatalf("shared values were not rendered: %+v", cm)
	}
	ksArt := o.store.GetArtifact(testutil.KustomizationID("flux-system", "apps"), store.ArtifactContent).(*store.Emission)
	var childDoc map[string]any
	for _, doc := range ksArt.Manifests {
		if manifest.DocKind(doc) == manifest.KindHelmRelease {
			childDoc = doc
		}
	}
	if childDoc == nil {
		t.Fatal("bare release document missing")
	}
	_, ns := manifest.DocMetadata(childDoc)
	if ns != "flux-system" {
		t.Fatalf("release document namespace = %q", ns)
	}
	hrID := testutil.HelmReleaseID("flux-system", "app")
	childBefore := manifest.DeepCopyMap(childDoc)
	hrArt := o.store.GetArtifact(hrID, store.ArtifactContent).(*store.Emission)
	retained := make([]map[string]any, len(hrArt.Manifests))
	for i, doc := range hrArt.Manifests {
		retained[i] = manifest.DeepCopyMap(doc)
		if name, ns := manifest.DocMetadata(doc); name == "bare" && ns != hrID.Namespace {
			t.Fatalf("bare chart namespace = %q, want %q", ns, hrID.Namespace)
		}
	}
	bare := manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: hrID.Namespace, Name: "bare"}
	if o.store.GetObject(bare) == nil {
		t.Fatal("bare chart child lost release namespace identity")
	}
	o.store.DeleteObject(bare)
	publishDocuments(o.ksc.Controller, testutil.KustomizationID("flux-system", "apps"), ksArt.Manifests, manifest.ParseDocOptions{WipeSecrets: true, DefaultNamespace: "flux-system"})
	_, replay := publishDocuments(o.hrc.Controller, hrID, hrArt.Manifests, manifest.ParseDocOptions{WipeSecrets: true, DefaultNamespace: hrID.Namespace})
	if o.store.GetObject(bare) == nil {
		t.Fatal("replay lost release namespace identity")
	}
	next, err := manifest.DecodeDocs(strings.NewReader("apiVersion: v1\nkind: ConfigMap\nmetadata: {name: next, namespace: other}\n"))
	if err != nil {
		t.Fatal(err)
	}
	_, _, _, err = manifest.ParseDoc(next[0], manifest.ParseDocOptions{DefaultNamespace: ""})
	if err != nil {
		t.Fatal(err)
	}
	manifest.ReleaseDoc(next[0])
	if delta := cmp.Diff(childBefore, childDoc); delta != "" {
		t.Fatalf("replay changed bare release (-want +got):\n%s", delta)
	}
	if delta := cmp.Diff(retained, replay); delta != "" {
		t.Fatalf("replay changed retained documents (-want +got):\n%s", delta)
	}
	_, ns = manifest.DocMetadata(childDoc)
	if ns != "flux-system" {
		t.Fatalf("replay release namespace %q", ns)
	}

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

	o, err := newTestOrchestrator(t, Tree{Path: dir}, Config{WipeSecrets: true, Concurrency: 4})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := o.bootstrap(context.Background()); err != nil {
		t.Fatalf("Bootstrap: %v", err)
	}
	if msg, ok := o.preflightFailure(testutil.KustomizationID("flux-system", "consumer")); ok {
		t.Fatalf("consumer should wait for rendered dependency, not fail preflight: %s", msg)
	}
	if err := o.run(context.Background()); err != nil {
		t.Fatalf("Run: %v", err)
	}
	for _, name := range []string{"produced", "consumer", "producer"} {
		info, ok := o.Store().GetStatus(testutil.KustomizationID("flux-system", name))
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

	o, err := newTestOrchestrator(t, Tree{Path: dir}, Config{WipeSecrets: true, Concurrency: 4})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	// A short bounded context: the structural fixpoint must terminate well
	// within it. A regression that reintroduces a
	// blocking wait would hit this deadline.
	ctx, cancel := context.WithTimeout(context.Background(), 20_000_000_000) // 20s ceiling
	defer cancel()
	res, _ := o.render(ctx)
	if res == nil {
		t.Fatal("Render returned nil result")
	}
	leafInfo, ok := res.Failed[testutil.KustomizationID("flux-system", "leaf")]
	if !ok || !strings.Contains(leafInfo.String(), "dependency not found") {
		t.Fatalf("leaf: want FAILED 'dependency not found', got %+v (ok=%v)", leafInfo, ok)
	}
	midInfo, ok := res.Failed[testutil.KustomizationID("flux-system", "mid")]
	if !ok || !strings.Contains(midInfo.String(), "leaf") {
		t.Fatalf("mid: want FAILED cascading leaf's failure, got %+v (ok=%v)", midInfo, ok)
	}
}

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
	manifests := strings.Replace(sets.String(), "    - apiVersion: fluxcd.controlplane.io/v1\n      kind: ResourceSetInputProvider\n      selector:", "    - selector:", 1)
	testutil.WriteFile(t, dir, "apps/sets.yaml", manifests)
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	var previous []byte
	for range 2 {
		renderedSingleton1, err := RenderTrees(ctx, []Tree{{Path: dir, RepoRoot: dir, ID: manifest.TreeID{Cluster: ".", Side: manifest.TreeHead}}}, Config{WipeSecrets: true, CacheDir: t.TempDir(), Concurrency: 2})
		if len(renderedSingleton1) != 1 {
			t.Fatalf("missing render: %v", err)
		}
		o := renderedSingleton1[0].Orchestrator
		res := renderedSingleton1[0].Result
		if err != nil || len(res.Failed) != 0 {
			t.Fatalf("selector chain failed: error=%v failures=%v", err, res.Failed)
		}
		for i := range depth + 1 {
			nid := manifest.NamedResource{Group: "fluxcd.controlplane.io", Kind: manifest.KindResourceSetInputProvider, Namespace: "flux-system", Name: fmt.Sprintf("provider-%02d", i)}
			if _, ok := o.Store().Get[*manifest.ResourceSetInputProvider](nid); !ok {
				t.Fatalf("missing chain output %s", nid)
			}
		}
		if count := len(o.Store().ListObjects(manifest.FluxOperatorDomain, manifest.KindResourceSetInputProvider)); count != depth+1 {
			t.Fatalf("provider count=%d, want %d", count, depth+1)
		}
		proofs := 0
		for _, doc := range outputByParent(res.Manifests)[testutil.KustomizationID("flux-system", "apps")] {
			if doc["kind"] == "Widget" {
				proofs++
			}
		}
		if proofs != depth {
			t.Fatalf("rendered provider proofs=%d, want %d", proofs, depth)
		}
		output, err := json.Marshal(outputByParent(res.Manifests)[testutil.KustomizationID("flux-system", "apps")])
		if err != nil {
			t.Fatal(err)
		}
		if previous != nil && !bytes.Equal(previous, output) {
			t.Fatal("repeated render bytes differ")
		}
		previous = output
	}
}

func TestDAG_SeedAdmissionPreservesSourceReaders(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	o, err := newTestOrchestrator(t, Tree{Path: root, RepoRoot: root}, Config{CacheDir: t.TempDir(), Concurrency: 2})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		o.stop()

	})
	source := &manifest.GitRepository{Name: "ignored", Namespace: "flux-system"}
	consumer := &manifest.Kustomization{Name: "consumer", Namespace: "flux-system"}
	o.store.AddObjects([]manifest.BaseManifest{source, consumer})
	index := &manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{consumer.Named(): {File: "consumer.yaml"}}}
	o.filter = change.NewFilter(change.NewSet([]string{"consumer.yaml"}), index, o.store, root)
	o.configureControllers()
	o.seedNodes()
	var reads store.ReadSet
	reads.Init(o.store, consumer.Named(), nil)
	defer reads.Close()
	if reads.GetObject(source.Named()) != source {
		t.Fatal("source metadata is unavailable")
	}
	var execution base.Execution
	execution.Init(o.src.Controller, source.Named())
	defer execution.Close()
	if execution.Reads.Admitted() {
		t.Fatal("unchanged source was admitted")
	}
	if !reads.Valid() {
		t.Fatal("initial source admission invalidated a consumer that already captured its metadata")
	}
}
