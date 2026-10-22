package orchestrator

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	kustomizev1 "github.com/fluxcd/kustomize-controller/api/v1"
	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/internal/testutil/controllertest"
	"github.com/home-operations/flate/pkg/change"
	"github.com/home-operations/flate/pkg/controllers/base"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/schedule"
	"github.com/home-operations/flate/pkg/source/gitrepo"
	"github.com/home-operations/flate/pkg/source/gittree"
	"github.com/home-operations/flate/pkg/store"
	"github.com/home-operations/flate/pkg/task"
)

type r4Fixture struct {
	o                        *Orchestrator
	reader, supplier, target manifest.NamedResource
}

func newR4Fixture(t *testing.T, readerKind, result string, required bool) r4Fixture {
	t.Helper()
	root := t.TempDir()
	testutil.WriteFile(t, root, "empty/kustomization.yaml", "resources: []\n")
	testutil.WriteFile(t, root, "supplier/kustomization.yaml", "resources: [input.yaml]\n")
	data := "  GREETING: exact\n"
	if readerKind == manifest.KindHelmRelease {
		data = "  values.yaml: 'greeting: exact'\n"
	}
	testutil.WriteFile(t, root, "supplier/input.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: input, namespace: flux-system}\ndata:\n"+data)
	if result == "empty" {
		testutil.WriteFile(t, root, "supplier/kustomization.yaml", "resources: []\n")
	}
	if result == "failed" {
		testutil.WriteFile(t, root, "supplier/kustomization.yaml", "resources: [missing.yaml]\n")
	}
	testutil.WriteFile(t, root, "reader/kustomization.yaml", "resources: [output.yaml]\n")
	testutil.WriteFile(t, root, "reader/output.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: output, namespace: flux-system}\ndata: {greeting: '${GREETING:=fallback}'}\n")
	testutil.WriteFile(t, root, "chart/Chart.yaml", "apiVersion: v2\nname: fixture\nversion: 1.0.0\n")
	testutil.WriteFile(t, root, "chart/templates/output.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: output, namespace: flux-system}\ndata: {greeting: {{ .Values.greeting | default \"fallback\" | quote }}}\n")
	o, err := New(t.Context(), Config{Path: root, RepoRoot: root, CacheDir: t.TempDir(), Concurrency: 2})
	assert.NoError(t, err)
	t.Cleanup(func() { assert.NoError(t, o.Stop()) })
	location, err := gitrepo.Locate(t.Context(), root, "")
	assert.NoError(t, err)
	input, err := gittree.AdmitDirectory(t.Context(), location)
	assert.NoError(t, err)
	o.store.SetArtifact(manifest.BootstrapSourceID, input)
	o.store.AddObject(&manifest.GitRepository{Name: manifest.BootstrapSourceID.Name, Namespace: manifest.BootstrapSourceID.Namespace})
	o.store.UpdateStatus(manifest.BootstrapSourceID, store.StatusInfo{Status: store.StatusReady, Reason: "", Message: ""})
	supplier := &manifest.Kustomization{Name: "supplier", Namespace: "flux-system", Path: "supplier"}
	target := manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: "flux-system", Name: "input"}
	var reader manifest.BaseManifest
	if readerKind == manifest.KindKustomization {
		ks := &manifest.Kustomization{Name: "reader", Namespace: "flux-system", Path: "reader", PostBuild: &kustomizev1.PostBuild{SubstituteStrategy: kustomizev1.SubstituteStrategyAlways, SubstituteFrom: []manifest.SubstituteReference{{Kind: target.Kind, Name: target.Name, Optional: true}}}}
		if required {
			ks.PostBuild.SubstituteFrom = append(ks.PostBuild.SubstituteFrom, manifest.SubstituteReference{Kind: target.Kind, Name: "cluster-vars"})
		}
		reader = ks
	} else {
		hr := &manifest.HelmRelease{Name: "reader", Namespace: "flux-system", Chart: manifest.HelmChart{Name: "chart", Source: manifest.NamedResource{Group: manifest.SourceDomain, Kind: manifest.KindGitRepository, Namespace: manifest.BootstrapSourceID.Namespace, Name: manifest.BootstrapSourceID.Name}}, ValuesFrom: []manifest.ValuesReference{{Kind: target.Kind, Name: target.Name, Optional: true}}}
		if required {
			hr.ValuesFrom = append(hr.ValuesFrom, manifest.ValuesReference{Kind: target.Kind, Name: "cluster-vars"})
		}
		reader = hr
	}
	o.index = &manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{
		target: {Productions: []manifest.Production{{Owner: supplier.Named()}}},
		{Kind: manifest.KindConfigMap, Namespace: "flux-system", Name: "output"}: {Productions: []manifest.Production{{Owner: reader.Named()}}},
		{Kind: manifest.KindConfigMap, Namespace: "flux-system", Name: "second"}: {Productions: []manifest.Production{{Owner: testutil.KustomizationID("flux-system", "extra")}}},
	}}
	o.store.AddObject(supplier)
	o.store.AddObject(reader)
	o.store.UpdateStatus(supplier.Named(), store.StatusInfo{Status: store.StatusReady, Reason: "", Message: "retained"})
	return r4Fixture{o: o, reader: reader.Named(), supplier: supplier.Named(), target: target}
}

type r4Tasks struct {
	tasks *task.Service
	after func(string)
}

func (r r4Tasks) Go(ctx context.Context, name string, fn func(context.Context)) {
	r.tasks.Go(ctx, name, func(ctx context.Context) {
		fn(ctx)
		if r.after != nil {
			r.after(name)
		}
	})
}
func (r r4Tasks) BlockTillDone() { r.tasks.BlockTillDone() }

func TestResolve_OptionalRunningReady(t *testing.T) {
	t.Parallel()
	for _, kind := range []string{manifest.KindKustomization, manifest.KindHelmRelease} {
		t.Run(kind, func(t *testing.T) { t.Parallel(); testR4Running(t, kind, "data", false) })
	}
}

func TestResolve_OptionalExhaustedSuccessor(t *testing.T) {
	t.Parallel()
	for _, kind := range []string{manifest.KindKustomization, manifest.KindHelmRelease} {
		for _, result := range []string{"failed", "empty", "before/failed", "before/empty"} {
			for _, required := range []bool{false, true} {
				t.Run(fmt.Sprintf("%s/%s/required=%t", kind, result, required), func(t *testing.T) { t.Parallel(); testR4Running(t, kind, result, required) })
			}
		}
	}
}

func testR4Running(t *testing.T, kind, result string, required bool) {
	t.Helper()
	result, before := strings.CutPrefix(result, "before/")
	f := newR4Fixture(t, kind, result, required)
	f.o.configureControllers()
	started, release, supplierDone := make(chan struct{}), make(chan struct{}), make(chan struct{})
	parked := make(chan struct{}, 1)
	var readerDispatches, publications atomic.Int64
	var first sync.Once
	d := fixtureDispatcher{store: f.o.store, dispatch: func(ctx context.Context, id schedule.NodeID, level manifest.DrainLevel) schedule.Result {
		if id == f.supplier {
			close(started)
			if !before {
				f.o.tasks.YieldSlot(func() { <-release })
			}
		}
		if id == f.reader {
			readerDispatches.Add(1)
			f.o.tasks.YieldSlot(func() {
				if before {
					<-supplierDone
				} else {
					<-started
				}
			})
		}
		assessment := (dagDispatcher{f.o}).Dispatch(ctx, id, level)
		if id == f.reader && !before {
			first.Do(func() {
				if assessment.Assessment.Outcome() != schedule.Blocked || f.o.store.GetArtifact(f.reader) != nil {
					t.Error("running Ready supplier admitted frozen absence")
				}
				parked <- struct{}{}
			})
		}
		return assessment
	}}
	unsubArtifact := f.o.store.AddListener(store.EventArtifactUpdated, func(id manifest.NamedResource, _ any) {
		if id == f.reader {
			publications.Add(1)
		}
	}, false)
	defer unsubArtifact()
	tasks := r4Tasks{tasks: f.o.tasks, after: func(name string) {
		if name == "schedule/"+f.supplier.String() {
			close(supplierDone)
		}
	}}
	sched := schedule.New(tasks, d)
	unsub := f.o.watchDAG(sched)
	defer unsub()
	sched.Seed([]schedule.NodeID{f.reader, f.supplier})
	done := make(chan error, 1)
	go func() { done <- sched.Run(t.Context()) }()
	if !before {
		select {
		case <-parked:
		case err := <-done:
			t.Fatalf("scheduler ended while supplier was running: %v", err)
		}
		if f.o.store.GetArtifact(f.reader) != nil {
			t.Error("reader published before supplier settled")
		}
		close(release)
	}
	assert.NoError(t, <-done)
	info, _ := f.o.store.GetStatus(f.reader)
	if required {
		missing := manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: "flux-system", Name: "cluster-vars"}
		assert.Equal(t, info.Status, store.StatusFailed)
		assert.Diff(t, info.BlockedBy(), []manifest.NamedResource{missing})
	} else {
		assert.Equal(t, info.Status, store.StatusReady)
		output, _ := f.o.store.Get[*manifest.ConfigMap](manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: f.reader.Namespace, Name: "output"})
		want := "fallback"
		if result == "data" {
			want = "exact"
		}
		if output == nil || output.Data["greeting"] != want {
			t.Fatalf("output=%+v want=%s", output, want)
		}
	}
	if readerDispatches.Load() > 4 || publications.Load() > 2 {
		t.Fatalf("unbounded replay: dispatches=%d publications=%d", readerDispatches.Load(), publications.Load())
	}
	for _, w := range f.o.store.Warnings() {
		if w.Resource == f.reader && slices.Contains([]string{manifest.WarnOmittedValues, manifest.WarnUnresolvedSubstitution}, w.Category) {
			t.Errorf("optional warning=%+v", w)
		}
	}
	assert.Equal(t, f.o.tasks.Failures(), int64(0))
}

func TestResolve_OptionalBindingArrival(t *testing.T) {
	t.Parallel()
	for _, kind := range []string{manifest.KindKustomization, manifest.KindHelmRelease} {
		t.Run(kind, func(t *testing.T) {
			t.Parallel()
			f := newR4Fixture(t, kind, "data", false)
			f.o.configureControllers()
			var sched *schedule.Scheduler
			var admitted atomic.Bool
			d := fixtureDispatcher{store: f.o.store, dispatch: func(ctx context.Context, id schedule.NodeID, level manifest.DrainLevel) schedule.Result {
				result := (dagDispatcher{f.o}).Dispatch(ctx, id, level)
				if id == f.reader && admitted.CompareAndSwap(false, true) {
					if result.Assessment.Outcome() != schedule.Blocked {
						t.Error("unadmitted supplier did not retain a wait")
					}
					sched.OnArrival(f.supplier, true)
				}
				return result
			}}
			sched = schedule.New(f.o.tasks, d)
			unsub := f.o.watchDAG(sched)
			defer unsub()
			sched.Seed([]schedule.NodeID{f.reader})
			assert.NoError(t, sched.Run(t.Context()))
			output, _ := f.o.store.Get[*manifest.ConfigMap](manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: f.reader.Namespace, Name: "output"})
			if output == nil || output.Data["greeting"] != "exact" {
				t.Fatalf("arrival output=%+v", output)
			}
		})
	}
}

func TestResolve_OptionalSuccessorBeforePark(t *testing.T) {
	t.Parallel()
	for _, kind := range []string{manifest.KindKustomization, manifest.KindHelmRelease} {
		t.Run(kind, func(t *testing.T) {
			t.Parallel()
			f := newR4Fixture(t, kind, "data", false)
			f.o.configureControllers()
			release, supplied := make(chan struct{}), make(chan struct{})
			var first sync.Once
			d := fixtureDispatcher{store: f.o.store, dispatch: func(ctx context.Context, id schedule.NodeID, level manifest.DrainLevel) schedule.Result {
				if id == f.supplier {
					f.o.tasks.YieldSlot(func() { <-release })
				}
				result := (dagDispatcher{f.o}).Dispatch(ctx, id, level)
				if id == f.reader {
					first.Do(func() {
						if result.Assessment.Outcome() != schedule.Blocked {
							t.Error("missing supplier did not park")
						}
						close(release)
						f.o.tasks.YieldSlot(func() { <-supplied })
						if result.Assessment.Valid() {
							t.Error("publication before park retained stale absence")
						}
					})
				}
				return result
			}}
			tasks := r4Tasks{tasks: f.o.tasks, after: func(name string) {
				if name == "schedule/"+f.supplier.String() {
					close(supplied)
				}
			}}
			sched := schedule.New(tasks, d)
			unsub := f.o.watchDAG(sched)
			defer unsub()
			sched.Seed([]schedule.NodeID{f.reader, f.supplier})
			assert.NoError(t, sched.Run(t.Context()))
			output, _ := f.o.store.Get[*manifest.ConfigMap](manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: f.reader.Namespace, Name: "output"})
			if output == nil || output.Data["greeting"] != "exact" {
				t.Fatalf("pre-registration arrival=%+v", output)
			}
		})
	}
}

func TestResolve_OptionalBindingSetChange(t *testing.T) {
	t.Parallel()
	for _, kind := range []string{manifest.KindKustomization, manifest.KindHelmRelease} {
		t.Run(kind, func(t *testing.T) {
			t.Parallel()
			f := newR4Fixture(t, kind, "data", false)
			f.o.configureControllers()
			var changed atomic.Bool
			d := fixtureDispatcher{store: f.o.store, dispatch: func(ctx context.Context, id schedule.NodeID, level manifest.DrainLevel) schedule.Result {
				result := (dagDispatcher{f.o}).Dispatch(ctx, id, level)
				if id == f.reader && changed.CompareAndSwap(false, true) {
					switch reader := f.o.store.GetObject(id).(type) {
					case *manifest.Kustomization:
						fresh := reader.Clone()
						fresh.PostBuild.SubstituteFrom = append(fresh.PostBuild.SubstituteFrom, manifest.SubstituteReference{Kind: manifest.KindConfigMap, Name: "second", Optional: true})
						f.o.store.AddObject(fresh)
					case *manifest.HelmRelease:
						fresh := reader.Clone()
						fresh.ValuesFrom = append(fresh.ValuesFrom, manifest.ValuesReference{Kind: manifest.KindConfigMap, Name: "second", Optional: true})
						f.o.store.AddObject(fresh)
					}
					if result.Assessment.Valid() {
						t.Error("changed reference set reused assessment")
					}
					f.o.store.AddObject(&manifest.Kustomization{Name: "extra", Namespace: f.reader.Namespace, Path: "empty"})
				}
				return result
			}}
			sched := schedule.New(f.o.tasks, d)
			unsub := f.o.watchDAG(sched)
			defer unsub()
			sched.Seed([]schedule.NodeID{f.reader, f.supplier})
			assert.NoError(t, sched.Run(t.Context()))
			output, _ := f.o.store.Get[*manifest.ConfigMap](manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: f.reader.Namespace, Name: "output"})
			if output == nil || output.Data["greeting"] != "exact" {
				t.Fatalf("reference-set output=%+v", output)
			}
		})
	}
}

func TestResolve_OptionalForceCut(t *testing.T) {
	t.Parallel()
	for _, kind := range []string{manifest.KindKustomization, manifest.KindHelmRelease} {
		t.Run(kind, func(t *testing.T) {
			t.Parallel()
			f := newR4Fixture(t, kind, "data", false)
			supplier, _ := f.o.store.Get[*manifest.Kustomization](f.supplier)
			supplier = supplier.Clone()
			supplier.PostBuild = &kustomizev1.PostBuild{SubstituteFrom: []manifest.SubstituteReference{{Kind: manifest.KindConfigMap, Name: "output", Optional: true}}}
			f.o.store.AddObject(supplier)
			f.o.configureControllers()
			sched := schedule.New(f.o.tasks, dagDispatcher{f.o})
			unsub := f.o.watchDAG(sched)
			defer unsub()
			sched.Seed([]schedule.NodeID{f.reader, f.supplier})
			assert.NoError(t, sched.Run(t.Context()))
			for _, id := range []manifest.NamedResource{f.reader, f.supplier} {
				info, _ := f.o.store.GetStatus(id)
				assert.Equal(t, info.Status, store.StatusReady)
			}
			output, _ := f.o.store.Get[*manifest.ConfigMap](manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: f.reader.Namespace, Name: "output"})
			if output == nil || output.Data["greeting"] != "exact" {
				t.Fatalf("cycle output=%+v", output)
			}
		})
	}
}

func TestResolve_OptionalDeterminism(t *testing.T) {
	t.Parallel()
	for _, kind := range []string{manifest.KindKustomization, manifest.KindHelmRelease} {
		t.Run(kind, func(t *testing.T) {
			t.Parallel()
			var previous []byte
			var warnings []manifest.Warning
			for run := range 2 {
				f := newR4Fixture(t, kind, "empty", false)
				f.o.configureControllers()
				sched := schedule.New(f.o.tasks, dagDispatcher{f.o})
				unsub := f.o.watchDAG(sched)
				sched.Seed([]schedule.NodeID{f.reader, f.supplier})
				assert.NoError(t, sched.Run(t.Context()))
				unsub()
				art, ok := f.o.store.GetArtifact(f.reader).(*store.Emission)
				if !ok {
					t.Fatal("reader did not complete")
				}
				output, err := json.Marshal(art.Manifests)
				assert.NoError(t, err)
				currentWarnings := f.o.store.Warnings()
				if run == 1 {
					assert.Diff(t, output, previous)
					assert.Diff(t, currentWarnings, warnings)
				}
				previous, warnings = output, currentWarnings
			}
		})
	}
}

func TestResolve_ResourceSetInput(t *testing.T) {
	t.Parallel()
	for _, kind := range []string{manifest.KindConfigMap, manifest.KindSecret} {
		for _, changed := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/changed=%t", kind, changed), func(t *testing.T) {
				t.Parallel()
				root := t.TempDir()
				testutil.WriteFile(t, root, "reader.yaml", testutil.KustomizationYAML("reader", "apps", "  path: ./reader\n  sourceRef: {kind: GitRepository, name: flux-system, namespace: flux-system}\n  postBuild:\n    substituteFrom: [{kind: "+kind+", name: input, optional: true}]\n"))
				testutil.WriteFile(t, root, "reader/kustomization.yaml", "resources: [output.yaml]\n")
				testutil.WriteFile(t, root, "reader/output.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: output, namespace: apps}\ndata: {greeting: '${GREETING:=fallback}'}\n")
				field := "data"
				if kind == manifest.KindSecret {
					field = "stringData"
				}
				testutil.WriteFile(t, root, "supplier.yaml", "apiVersion: fluxcd.controlplane.io/v1\nkind: ResourceSet\nmetadata: {name: supplier, namespace: apps}\nspec:\n  resources:\n    - apiVersion: v1\n      kind: "+kind+"\n      metadata: {name: input}\n      "+field+": {GREETING: exact}\n")
				var previous map[manifest.NamedResource][]map[string]any
				for range 2 {
					cfg := Config{Path: root, RepoRoot: root, CacheDir: t.TempDir(), Concurrency: 2}
					if changed {
						cfg.ExternalChanges = change.NewSet([]string{"reader/output.yaml"})
					}
					o, err := New(t.Context(), cfg)
					assert.NoError(t, err)
					t.Cleanup(func() { assert.NoError(t, o.Stop()) })
					reader := testutil.KustomizationID("apps", "reader")
					supplier := manifest.NamedResource{Group: manifest.FluxOperatorDomain, Kind: manifest.KindResourceSet, Namespace: "apps", Name: "supplier"}
					target := manifest.NamedResource{Kind: kind, Namespace: "apps", Name: "input"}
					for _, file := range []string{"reader.yaml", "supplier.yaml"} {
						var docs []map[string]any
						body, readErr := os.ReadFile(filepath.Join(root, file))
						assert.NoError(t, readErr)
						docs, err = manifest.DecodeDocs(strings.NewReader(string(body)))
						assert.NoError(t, err)
						obj, _, _, parseErr := manifest.ParseDoc(docs[0], manifest.ParseDocOptions{})
						assert.NoError(t, parseErr)
						o.store.AddObject(obj)
					}
					o.index = &manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{
						target: {Productions: []manifest.Production{{Owner: supplier, Source: manifest.BootstrapSourceID}}},
						reader: {File: "reader.yaml", Refs: []manifest.NamedResource{target}},
					}, Claims: map[string][]manifest.NamedResource{"reader/": {reader}}}
					o.store.SetDiscoveryIndex(o.index)
					o.filter = change.NewFilter(cfg.ExternalChanges, o.index, o.store)
					location, locateErr := gitrepo.Locate(t.Context(), root, "")
					assert.NoError(t, locateErr)
					input, admitErr := gittree.AdmitDirectory(t.Context(), location)
					assert.NoError(t, admitErr)
					o.store.SetArtifact(manifest.BootstrapSourceID, input)
					o.store.AddObject(&manifest.GitRepository{Name: manifest.BootstrapSourceID.Name, Namespace: manifest.BootstrapSourceID.Namespace})
					o.store.UpdateStatus(manifest.BootstrapSourceID, store.StatusInfo{Status: store.StatusReady, Reason: "", Message: ""})
					facts := o.store.ReferenceFacts(target, o.index)
					if !slices.ContainsFunc(facts.Productions, func(p manifest.Production) bool { return p.Owner == supplier }) || !o.filter.ShouldReconcile(supplier) {
						t.Fatalf("ResourceSet supplier unreachable: %+v", facts)
					}
					o.configureControllers()
					d := dagDispatcher{o}
					assessment := d.Dispatch(t.Context(), reader, manifest.DrainNone).Assessment
					defer assessment.Close()
					if assessment.Outcome() != schedule.Blocked {
						t.Fatal("reader did not wait for ResourceSet")
					}
					sched := schedule.New(d, d)
					unsubscribe := o.watchDAG(sched)
					sched.Seed(o.seedNodes())
					assert.NoError(t, sched.Run(t.Context()))
					unsubscribe()
					output, _ := o.store.Get[*manifest.ConfigMap](manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: "apps", Name: "output"})
					if output == nil || output.Data["greeting"] != "exact" || o.store.EmissionOwner(target) != supplier {
						t.Fatalf("ResourceSet input not consumed: %+v", output)
					}
					result := outputByParent(o.collectManifests())
					if previous != nil {
						assert.Diff(t, result, previous)
					}
					previous = result
				}
			})
		}
	}
}

func TestResolve_ArtifactPredictionSource(t *testing.T) {
	t.Parallel()
	for _, changed := range []bool{false, true} {
		t.Run(fmt.Sprintf("changed=%t", changed), func(t *testing.T) {
			t.Parallel()
			root := t.TempDir()
			testutil.WriteFile(t, root, "producer.yaml", testutil.KustomizationYAML("producer", "apps", "  path: ./producer\n  sourceRef: {kind: GitRepository, name: flux-system, namespace: flux-system}\n"))
			testutil.WriteFile(t, root, "producer/kustomization.yaml", "namespace: apps\nresources: [generator.yaml, reader.yaml]\n")
			testutil.WriteFile(t, root, "producer/generator.yaml", "apiVersion: source.extensions.fluxcd.io/v1beta1\nkind: ArtifactGenerator\nmetadata: {name: generator}\nspec:\n  pathPattern: '@repo/{app}'\n  artifacts: [{name: 'app-{app}'}]\n")
			testutil.WriteFile(t, root, "producer/reader.yaml", testutil.KustomizationYAML("reader", "apps", "  path: ./workload\n  sourceRef: {kind: ExternalArtifact, name: app-api}\n"))
			cfg := Config{Path: root, RepoRoot: root, CacheDir: t.TempDir(), Concurrency: 2}
			if changed {
				cfg.ExternalChanges = change.NewSet([]string{"producer/reader.yaml"})
			}
			o, err := New(t.Context(), cfg)
			assert.NoError(t, err)
			t.Cleanup(func() { assert.NoError(t, o.Stop()) })
			assert.NoError(t, o.Bootstrap(t.Context()))
			o.configureControllers()
			d := dagDispatcher{o}
			producer := testutil.KustomizationID("apps", "producer")
			target := manifest.NamedResource{Group: manifest.SourceDomain, Kind: manifest.KindExternalArtifact, Namespace: "apps", Name: "app-api"}
			if !o.filter.ShouldReconcile(producer) {
				t.Fatal("prediction owner not selected")
			}
			info := controllertest.DispatchToFixpoint(t.Context(), t, controllertest.Dispatch{Tasks: o.tasks, Store: o.store, ID: producer, ReconcileNode: o.ksc.ReconcileNode})
			if info.Status != store.StatusReady {
				t.Fatalf("producer did not settle: %+v", info)
			}
			facts := o.store.ReferenceFacts(target, o.index)
			if facts.Data != nil || facts.Accepted || !slices.ContainsFunc(facts.Productions, func(p manifest.Production) bool {
				return p.Owner == producer && p.Declaration.Kind == "ArtifactGenerator" && p.Source == manifest.BootstrapSourceID
			}) || o.store.GetArtifact(target) != nil {
				t.Fatalf("source-qualified prediction supplied content or disappeared: %+v", facts)
			}
			reader := d.Dispatch(t.Context(), testutil.KustomizationID("apps", "reader"), manifest.DrainNone).Assessment
			defer reader.Close()
			if reader.Outcome() != schedule.Blocked || !slices.Contains(reader.(*base.Execution).Blocked, target) {
				t.Fatalf("prediction bypassed ordinary source wait: outcome=%v blocked=%v status=%v selected=%t", reader.Outcome(), reader.(*base.Execution).Blocked, o.store.FailedResources(), o.filter.ShouldReconcile(testutil.KustomizationID("apps", "reader")))
			}
		})
	}
}

func TestResolve_OptionalForceCutInvalidation(t *testing.T) {
	t.Parallel()
	for _, kind := range []string{manifest.KindKustomization, manifest.KindHelmRelease} {
		for _, change := range []string{"declaration", "target", "supplier"} {
			t.Run(kind+"/"+change, func(t *testing.T) {
				t.Parallel()
				f := newR4Fixture(t, kind, "data", false)
				f.o.configureControllers()
				prior := (dagDispatcher{f.o}).Dispatch(t.Context(), f.reader, manifest.DrainNone).Assessment.(*base.Execution)
				defer prior.Close()
				if prior.Outcome() != schedule.Blocked || prior.Reads.GetObject(f.target) != nil {
					t.Fatal("optional input did not retain absence")
				}
				switch change {
				case "declaration":
					switch reader := f.o.store.GetObject(f.reader).(type) {
					case *manifest.Kustomization:
						fresh := reader.Clone()
						fresh.PostBuild.Substitute = map[string]string{"UNUSED": "changed"}
						f.o.store.AddObject(fresh)
					case *manifest.HelmRelease:
						fresh := reader.Clone()
						fresh.Values = map[string]any{"unused": "changed"}
						f.o.store.AddObject(fresh)
					}
				case "target", "supplier":
					owner := f.supplier
					if change == "supplier" {
						extra := &manifest.Kustomization{Name: "extra", Namespace: f.reader.Namespace, Path: "empty"}
						f.o.store.AddObject(extra)
						owner = extra.Named()
					}
					data := map[string]any{"GREETING": "exact"}
					if kind == manifest.KindHelmRelease {
						data = map[string]any{"values.yaml": "greeting: exact"}
					}
					f.o.ksc.Emit(owner, &store.Emission{Children: []store.Child{{Obj: &manifest.ConfigMap{Name: f.target.Name, Namespace: f.target.Namespace, Data: data}}}, Complete: true}, nil)
				}
				if prior.Valid() {
					t.Fatal("changed authority retained the prior assessment")
				}
				if prior.Reads.GetObject(f.target) != nil {
					t.Fatal("successor mutated the predecessor's frozen absence")
				}
				prior.Settle(t.Context(), manifest.DrainForce)
				if f.o.store.GetArtifact(f.reader) != nil {
					t.Fatal("stale force assessment published output")
				}
				sched := schedule.New(f.o.tasks, dagDispatcher{f.o})
				unsub := f.o.watchDAG(sched)
				defer unsub()
				sched.Seed([]schedule.NodeID{f.reader, f.supplier})
				assert.NoError(t, sched.Run(t.Context()))
				output, _ := f.o.store.Get[*manifest.ConfigMap](manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: f.reader.Namespace, Name: "output"})
				if output == nil || output.Data["greeting"] != "exact" {
					t.Fatalf("successor output=%+v", output)
				}
			})
		}
	}
}

func TestResolve_OptionalPairedSelection(t *testing.T) {
	t.Parallel()
	for _, kind := range []string{manifest.KindKustomization, manifest.KindHelmRelease} {
		t.Run(kind, func(t *testing.T) {
			t.Parallel()
			var runs []Rendered
			var reader manifest.NamedResource
			for _, result := range []string{"empty", "data"} {
				f := newR4Fixture(t, kind, result, false)
				reader = f.reader
				f.o.index.Records[reader] = manifest.DiscoveryRecord{File: "reader.yaml", Refs: []manifest.NamedResource{f.target}}
				f.o.index.Claims = map[string][]manifest.NamedResource{"reader/": {reader}}
				f.o.store.SetDiscoveryIndex(f.o.index)
				f.o.filter = change.NewFilter(change.NewSet([]string{"reader/output.yaml"}), f.o.index, f.o.store)
				if !f.o.filter.ShouldReconcile(reader) || !f.o.filter.ShouldReconcile(f.supplier) {
					t.Fatal("changed-only selection lost optional materializer")
				}
				f.o.configureControllers()
				sched := schedule.New(f.o.tasks, dagDispatcher{f.o})
				unsub := f.o.watchDAG(sched)
				sched.Seed([]schedule.NodeID{f.reader, f.supplier})
				assert.NoError(t, sched.Run(t.Context()))
				unsub()
				runs = append(runs, Rendered{Orchestrator: f.o, Result: &Result{Manifests: f.o.collectManifests(), Failed: f.o.store.FailedResources()}})
			}
			selected, err, _ := Select(runs, nil, []manifest.NamedResource{reader}, nil)
			assert.NoError(t, err)
			assert.Equal(t, len(selected), 2)
			for side, want := range []string{"fallback", "exact"} {
				docs := outputByParent(selected[side].Manifests)[reader]
				if len(docs) != 1 {
					t.Fatalf("side %d selected manifests=%v", side, docs)
				}
				data, _ := docs[0]["data"].(map[string]any)
				assert.Equal(t, data["greeting"], any(want))
				assert.Equal(t, len(selected[side].Failed), 0)
				assert.Equal(t, len(selected[side].Blocked), 0)
			}
		})
	}
}

func TestResolve_OptionalQueuedSuccessor(t *testing.T) {
	t.Parallel()
	for _, kind := range []string{manifest.KindKustomization, manifest.KindHelmRelease} {
		t.Run(kind, func(t *testing.T) {
			t.Parallel()
			f := newR4Fixture(t, kind, "data", false)
			f.o.configureControllers()
			keeper := testutil.KustomizationID("flux-system", "keeper")
			keeping, releaseKeeper := make(chan struct{}), make(chan struct{})
			var started, queued atomic.Bool
			var sched *schedule.Scheduler
			d := fixtureDispatcher{store: f.o.store, dispatch: func(ctx context.Context, id schedule.NodeID, level manifest.DrainLevel) schedule.Result {
				if id == keeper {
					close(keeping)
					<-releaseKeeper
					return schedule.Result{}
				}
				if id == f.supplier {
					started.Store(true)
				}
				if id == f.reader {
					<-keeping
				}
				result := (dagDispatcher{f.o}).Dispatch(ctx, id, level)
				if id == f.reader && queued.CompareAndSwap(false, true) {
					if result.Assessment.Outcome() != schedule.Blocked {
						t.Error("queued supplier did not retain optional absence")
					}
					sched.OnArrival(f.supplier, true)
					if started.Load() || f.o.store.GetArtifact(f.reader) != nil {
						t.Error("queued supplier started or admitted reader before a worker slot opened")
					}
					close(releaseKeeper)
				}
				return result
			}}
			sched = schedule.New(f.o.tasks, d)
			unsub := f.o.watchDAG(sched)
			defer unsub()
			sched.Seed([]schedule.NodeID{keeper, f.reader})
			assert.NoError(t, sched.Run(t.Context()))
			output, _ := f.o.store.Get[*manifest.ConfigMap](manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: f.reader.Namespace, Name: "output"})
			if output == nil || output.Data["greeting"] != "exact" {
				t.Fatalf("queued successor output=%+v", output)
			}
		})
	}
}
