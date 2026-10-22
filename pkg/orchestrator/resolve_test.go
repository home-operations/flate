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

	fluxopv1 "github.com/controlplaneio-fluxcd/flux-operator/api/v1"
	kustomizev1 "github.com/fluxcd/kustomize-controller/api/v1"
	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/change"
	"github.com/home-operations/flate/pkg/controllers/base"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/schedule"
	"github.com/home-operations/flate/pkg/source"
	sourcecache "github.com/home-operations/flate/pkg/source/cache"
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
	o.store.UpdateStatus(manifest.BootstrapSourceID, store.StatusReady, "", "")
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
		hr := &manifest.HelmRelease{Name: "reader", Namespace: "flux-system", Chart: manifest.HelmChart{Name: "chart", RepoGroup: manifest.SourceDomain, RepoKind: manifest.KindGitRepository, RepoNamespace: manifest.BootstrapSourceID.Namespace, RepoName: manifest.BootstrapSourceID.Name}, ValuesFrom: []manifest.ValuesReference{{Kind: target.Kind, Name: target.Name, Optional: true}}}
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
	o.store.UpdateStatus(supplier.Named(), store.StatusReady, "", "retained")
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

func TestResolve_OptionalQueuedSuccessor(t *testing.T) {
	t.Parallel()
	for _, kind := range []string{manifest.KindKustomization, manifest.KindHelmRelease} {
		t.Run(kind, func(t *testing.T) {
			t.Parallel()
			f := newR4Fixture(t, kind, "data", false)
			f.o.configureControllers()
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			keeper := testutil.KustomizationID("flux-system", "keeper")
			keeping, releaseKeeper := make(chan struct{}), make(chan struct{})
			observed, releaseReader := make(chan struct{}), make(chan struct{})
			observedOnce := sync.OnceFunc(func() { close(observed) })
			var started, requested atomic.Bool
			var sched *schedule.Scheduler
			d := withdrawalDispatcher(func(ctx context.Context, id schedule.NodeID, attempt manifest.ReconcileAttempt) (schedule.Outcome, manifest.ReconcileWait, bool) {
				if id == keeper {
					close(keeping)
					<-releaseKeeper
					return schedule.OutcomeTerminal, manifest.ReconcileWait{}, false
				}
				if id == f.supplier {
					started.Store(true)
				}
				queued := false
				if id == f.reader {
					<-keeping
					queued = (attempt.Content != nil && len(attempt.Content.Suppliers) > 0) && requested.CompareAndSwap(false, true)
					if queued {
						sched.OnArrival(f.supplier, true)
					}
				}
				out, wait, rerun := (dagDispatcher{f.o}).Dispatch(ctx, id, attempt)
				if id == f.reader && attempt.Content == nil && out != schedule.OutcomeBlocked {
					t.Error("unbound reader skipped content classification")
					observedOnce()
				}
				if queued {
					if out != schedule.OutcomeBlocked || wait.Content == nil || len(wait.Content.Suppliers) != 1 || wait.Content.Suppliers[0].Observed&1 == 0 {
						t.Errorf("queued supplier admitted absence: %v %+v", out, wait)
					}
					if f.o.store.GetArtifact(f.reader) != nil {
						t.Error("queued supplier allowed reader publication")
					}
					observedOnce()
					<-releaseReader
				}
				return out, wait, rerun
			})
			sched = schedule.New(f.o.tasks, d)
			unsub := f.o.watchDAG(sched)
			defer unsub()
			sched.Seed([]schedule.NodeID{keeper, f.reader})
			done := make(chan error, 1)
			go func() { done <- sched.Run(ctx) }()
			<-observed
			assert.Equal(t, started.Load(), false)
			info, _ := f.o.store.GetStatus(f.supplier)
			assert.Equal(t, info.Status, store.StatusReady)
			close(releaseReader)
			close(releaseKeeper)
			assert.NoError(t, <-done)
			output, _ := f.o.store.Get[*manifest.ConfigMap](manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: "flux-system", Name: "output"})
			if output == nil || output.Data["greeting"] != "exact" {
				t.Fatalf("queued output=%+v", output)
			}
			assert.Equal(t, f.o.tasks.Failures(), int64(0))
		})
	}
}

func TestResolve_OptionalRunningReady(t *testing.T) {
	t.Parallel()
	for _, kind := range []string{manifest.KindKustomization, manifest.KindHelmRelease} {
		t.Run(kind, func(t *testing.T) { t.Parallel(); testR4Running(t, kind, "data", false) })
	}
}

type r4Promotion func(manifest.NamedResource) bool

func (p r4Promotion) IsFileIndexed(manifest.NamedResource) bool { return false }
func (p r4Promotion) Promote(id manifest.NamedResource) bool    { return p(id) }

func TestResolve_OptionalSuccessorBeforePark(t *testing.T) {
	t.Parallel()
	for _, kind := range []string{manifest.KindKustomization, manifest.KindHelmRelease} {
		for _, boundary := range []string{"between samples", "before registration"} {
			t.Run(kind+"/"+boundary, func(t *testing.T) {
				t.Parallel()
				f := newR4Fixture(t, kind, "data", false)
				f.o.configureControllers()
				started, release := make(chan struct{}), make(chan struct{})
				supplierDone := make(chan struct{})
				var sched *schedule.Scheduler
				var changed, armed atomic.Bool
				var startOnce, doneOnce sync.Once
				promotion := r4Promotion(func(id manifest.NamedResource) bool {
					if id == f.target && armed.Load() && boundary == "between samples" && changed.CompareAndSwap(false, true) {
						sched.OnArrival(f.supplier, true)
					}
					return false
				})
				opts := base.Options{Index: f.o.index, Existence: promotion}
				f.o.ksc.Controller.Configure(opts)
				f.o.hrc.Controller.Configure(opts)
				var sawBound atomic.Bool
				d := withdrawalDispatcher(func(ctx context.Context, id schedule.NodeID, attempt manifest.ReconcileAttempt) (schedule.Outcome, manifest.ReconcileWait, bool) {
					if id == f.supplier {
						startOnce.Do(func() { close(started) })
						<-release
					}
					bound := id == f.reader && (attempt.Content != nil && len(attempt.Content.Suppliers) > 0) && sawBound.CompareAndSwap(false, true)
					if bound {
						<-started
						armed.Store(true)
					}
					out, wait, rerun := (dagDispatcher{f.o}).Dispatch(ctx, id, attempt)
					if bound {
						if out != schedule.OutcomeBlocked || f.o.store.GetArtifact(f.reader) != nil {
							t.Errorf("successor admitted reader before settlement: %v %+v", out, wait)
						}
						if boundary == "between samples" {
							if wait.Content.Suppliers[0].Observed == wait.Content.Suppliers[0].Word.Load() {
								t.Error("first sample did not precede successor intent")
							}
							close(release)
						} else {
							close(release)
							f.o.tasks.YieldSlot(func() { <-supplierDone })
						}
					} else if id == f.reader && sawBound.Load() && attempt.Drain != manifest.DrainNone {
						t.Errorf("missed epoch recheck forced drain %v", attempt.Drain)
					}
					return out, wait, rerun
				})
				tasks := r4Tasks{tasks: f.o.tasks, after: func(name string) {
					if name == "schedule/"+f.supplier.String() {
						doneOnce.Do(func() { close(supplierDone) })
					}
				}}
				sched = schedule.New(tasks, d)
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

func TestResolve_OptionalBindingArrival(t *testing.T) {
	t.Parallel()
	for _, kind := range []string{manifest.KindKustomization, manifest.KindHelmRelease} {
		t.Run(kind, func(t *testing.T) {
			t.Parallel()
			f := newR4Fixture(t, kind, "data", false)
			f.o.configureControllers()
			var sched *schedule.Scheduler
			var admitted atomic.Bool
			d := withdrawalDispatcher(func(ctx context.Context, id schedule.NodeID, attempt manifest.ReconcileAttempt) (schedule.Outcome, manifest.ReconcileWait, bool) {
				arrival := id == f.reader && (attempt.Content != nil && len(attempt.Content.Suppliers) > 0) && admitted.CompareAndSwap(false, true)
				if arrival {
					assert.Equal(t, attempt.Drain, manifest.DrainNone)
					if attempt.Content.Suppliers[0].Observed != 0 {
						t.Errorf("unadmitted supplier was not bound: %+v", attempt)
					}
					sched.OnArrival(f.supplier, true)
				}
				out, wait, rerun := (dagDispatcher{f.o}).Dispatch(ctx, id, attempt)
				if arrival && out != schedule.OutcomeBlocked {
					t.Errorf("arrival admitted absence: %v %+v", out, wait)
				}
				return out, wait, rerun
			})
			sched = schedule.New(f.o.tasks, d)
			unsub := f.o.watchDAG(sched)
			defer unsub()
			sched.Seed([]schedule.NodeID{f.reader})
			assert.NoError(t, sched.Run(t.Context()))
			assert.Equal(t, admitted.Load(), true)
			output, _ := f.o.store.Get[*manifest.ConfigMap](manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: f.reader.Namespace, Name: "output"})
			if output == nil || output.Data["greeting"] != "exact" {
				t.Fatalf("binding arrival output=%+v", output)
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
					o.store.UpdateStatus(manifest.BootstrapSourceID, store.StatusReady, "", "")
					facts := o.store.ReferenceFacts(target, o.index)
					if !slices.ContainsFunc(facts.Productions, func(p manifest.Production) bool { return p.Owner == supplier }) || !o.filter.ShouldReconcile(supplier) {
						t.Fatalf("ResourceSet supplier unreachable: %+v", facts)
					}
					o.configureControllers()
					d := dagDispatcher{o}
					out, wait, _ := d.Dispatch(t.Context(), reader, manifest.ReconcileAttempt{})
					if out != schedule.OutcomeBlocked || wait.Content == nil || !slices.Contains(wait.Content.Targets, target) {
						t.Fatalf("reader did not wait for ResourceSet: %v %+v", out, wait)
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
					result := o.collectManifests()
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
			out, wait, _ := d.Dispatch(t.Context(), producer, manifest.ReconcileAttempt{})
			if out != schedule.OutcomeTerminal || !wait.Empty() {
				t.Fatalf("producer did not render: %v %+v", out, wait)
			}
			facts := o.store.ReferenceFacts(target, o.index)
			if facts.Data != nil || facts.Accepted || !slices.ContainsFunc(facts.Productions, func(p manifest.Production) bool {
				return p.Owner == producer && p.Declaration.Kind == "ArtifactGenerator" && p.Source == manifest.BootstrapSourceID
			}) || o.store.GetArtifact(target) != nil {
				t.Fatalf("source-qualified prediction supplied content or disappeared: %+v", facts)
			}
			out, wait, _ = d.Dispatch(t.Context(), testutil.KustomizationID("apps", "reader"), manifest.ReconcileAttempt{})
			if out != schedule.OutcomeBlocked || wait.Content != nil || !slices.Contains(wait.Deps, target) {
				t.Fatalf("prediction bypassed ordinary source wait: %v %+v", out, wait)
			}
		})
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
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	started, release := make(chan struct{}), make(chan struct{})
	premature := make(chan struct{})
	supplierDone := make(chan struct{})
	var released atomic.Bool
	var drain atomic.Int64
	var lastSupplier manifest.Opportunity
	parked := make(chan struct{}, 8)
	d := withdrawalDispatcher(func(ctx context.Context, id schedule.NodeID, attempt manifest.ReconcileAttempt) (schedule.Outcome, manifest.ReconcileWait, bool) {
		if id == f.supplier {
			close(started)
			if !before {
				select {
				case <-release:
				case <-ctx.Done():
					close(premature)
					return schedule.OutcomeTerminal, manifest.ReconcileWait{}, false
				}
			}
		}
		if id == f.reader {
			drain.Store(int64(attempt.Drain))
			if !before && !released.Load() && attempt.Drain != manifest.DrainNone {
				t.Error("scheduler issued a cut while supplier body remained in flight")
				cancel()
				return schedule.OutcomeTerminal, manifest.ReconcileWait{}, false
			}
			if attempt.Content != nil && len(attempt.Content.Suppliers) > 0 {
				lastSupplier = attempt.Content.Suppliers[0]
			}
			if before {
				f.o.tasks.YieldSlot(func() { <-supplierDone })
			} else {
				select {
				case <-started:
				case <-ctx.Done():
					return schedule.OutcomeTerminal, manifest.ReconcileWait{}, false
				}
			}
		}
		out, wait, rerun := (dagDispatcher{f.o}).Dispatch(ctx, id, attempt)
		if id == f.reader && !before && !released.Load() && (out != schedule.OutcomeBlocked || wait.Content == nil) {
			t.Error("running supplier admitted absence before body completion")
			cancel()
		}
		return out, wait, rerun
	})
	tasks := r4Tasks{tasks: f.o.tasks, after: func(name string) {
		if name == "schedule/"+f.supplier.String() {
			close(supplierDone)
		}
		if name == "schedule/"+f.reader.String() {
			select {
			case parked <- struct{}{}:
			default:
				t.Error("supplier lifetime caused excessive reader redispatches")
				cancel()
			}
		}
	}}
	sched := schedule.New(tasks, d)
	unsub := f.o.watchDAG(sched)
	defer unsub()
	statusBarrier := f.o.store.AddListener(store.EventStatusUpdated, func(id manifest.NamedResource, payload any) {
		if id == f.supplier && result == "failed" && !before && payload.(store.StatusInfo).Status == store.StatusFailed {
			f.o.tasks.YieldSlot(func() { <-parked })
		}
	}, false)
	defer statusBarrier()
	sched.Seed([]schedule.NodeID{f.reader, f.supplier})
	done := make(chan error, 1)
	go func() { done <- sched.Run(ctx) }()
	if !before {
		for range 2 {
			select {
			case <-parked:
			case <-premature:
				t.Fatal("scheduler cut the running supplier opportunity before body completion")
			case err := <-done:
				t.Fatalf("scheduler ended while supplier body remained in flight: %v", err)
			}
		}
		if f.o.store.GetArtifact(f.reader) != nil {
			t.Error("running supplier allowed reader publication")
		}
		info, _ := f.o.store.GetStatus(f.supplier)
		assert.Equal(t, info.Status, store.StatusReady)
		released.Store(true)
		close(release)
	}
	assert.NoError(t, <-done)
	if !before && lastSupplier.Word == nil || lastSupplier.Word != nil && lastSupplier.Word.Load()&1 != 0 {
		t.Error("completed supplier opportunity remained open")
	}
	if !required {
		assert.Equal(t, manifest.DrainLevel(drain.Load()), manifest.DrainNone)
	}
	info, _ := f.o.store.GetStatus(f.reader)
	if required {
		missing := manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: "flux-system", Name: "cluster-vars"}
		assert.Equal(t, info.Status, store.StatusFailed)
		assert.Diff(t, info.BlockedBy(), []manifest.NamedResource{missing})
		if !strings.Contains(info.Message, "dependencies failed: ConfigMap/flux-system/cluster-vars: dependency not found") || strings.Contains(info.Message, f.supplier.Name) {
			t.Fatalf("required-only diagnostic=%q", info.Message)
		}
	} else {
		assert.Equal(t, info.Status, store.StatusReady)
		output, _ := f.o.store.Get[*manifest.ConfigMap](manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: "flux-system", Name: "output"})
		want := "fallback"
		if result == "data" {
			want = "exact"
		}
		if output == nil || output.Data["greeting"] != want {
			t.Fatalf("running output=%+v, want %s", output, want)
		}
	}
	for _, w := range f.o.store.Warnings() {
		if w.Resource == f.reader && slices.Contains([]string{manifest.WarnOmittedValues, manifest.WarnUnresolvedSubstitution}, w.Category) {
			t.Errorf("optional warning=%+v", w)
		}
	}
	assert.Equal(t, f.o.tasks.Failures(), int64(0))
}

func TestResolve_OptionalForceCut(t *testing.T) {
	t.Parallel()
	for _, kind := range []string{manifest.KindKustomization, manifest.KindHelmRelease} {
		for _, readerFirst := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/readerFirst=%t", kind, readerFirst), func(t *testing.T) {
				t.Parallel()
				f := newR4Fixture(t, kind, "data", false)
				f.o.configureControllers()
				supplier, _ := f.o.store.Get[*manifest.Kustomization](f.supplier)
				supplier = supplier.Clone()
				supplier.PostBuild = &kustomizev1.PostBuild{SubstituteFrom: []manifest.SubstituteReference{{Kind: manifest.KindConfigMap, Name: "output", Optional: true}}}
				f.o.store.AddObject(supplier)
				first := f.supplier
				if readerFirst {
					first = f.reader
				}
				completed := make(chan struct{})
				var cut atomic.Bool
				d := withdrawalDispatcher(func(ctx context.Context, id schedule.NodeID, attempt manifest.ReconcileAttempt) (schedule.Outcome, manifest.ReconcileWait, bool) {
					if attempt.Drain == manifest.DrainForce {
						cut.Store(true)
						if id != first {
							<-completed
						}
					}
					out, wait, rerun := (dagDispatcher{f.o}).Dispatch(ctx, id, attempt)
					if attempt.Drain == manifest.DrainForce && id == first {
						close(completed)
					}
					return out, wait, rerun
				})
				sched := schedule.New(f.o.tasks, d)
				unsub := f.o.watchDAG(sched)
				defer unsub()
				sched.Seed([]schedule.NodeID{f.reader, f.supplier})
				assert.NoError(t, sched.Run(t.Context()))
				if !cut.Load() {
					t.Fatal("cycle did not reach a structural force cut")
				}
				for _, id := range []manifest.NamedResource{f.reader, f.supplier} {
					info, _ := f.o.store.GetStatus(id)
					if info.Status != store.StatusReady || f.o.store.GetArtifact(id) == nil {
						t.Fatalf("force continuation did not render %s: %+v", id, info)
					}
				}
				assert.Equal(t, f.o.tasks.Failures(), int64(0))
			})
		}
	}
}

func TestResolve_OptionalForceCutInvalidation(t *testing.T) {
	t.Parallel()
	for _, kind := range []string{manifest.KindKustomization, manifest.KindHelmRelease} {
		for _, change := range []string{"successor", "declaration", "exhaustion between samples"} {
			t.Run(kind+"/"+change, func(t *testing.T) {
				t.Parallel()
				ctx, cancel := context.WithCancel(t.Context())
				defer cancel()
				f := newR4Fixture(t, kind, "data", false)
				f.o.configureControllers()
				supplier, _ := f.o.store.Get[*manifest.Kustomization](f.supplier)
				supplier = supplier.Clone()
				supplier.PostBuild = &kustomizev1.PostBuild{SubstituteFrom: []manifest.SubstituteReference{{Kind: manifest.KindConfigMap, Name: "output", Optional: true}}}
				f.o.store.AddObject(supplier)
				release, supplierDone := make(chan struct{}), make(chan struct{})
				var releaseOnce, doneOnce sync.Once
				var armed, rejected atomic.Bool
				promotion := r4Promotion(func(id manifest.NamedResource) bool {
					if id == f.target && armed.Load() && change == "exhaustion between samples" {
						releaseOnce.Do(func() { close(release) })
						f.o.tasks.YieldSlot(func() { <-supplierDone })
					}
					return false
				})
				opts := base.Options{Index: f.o.index, Existence: promotion}
				f.o.ksc.Controller.Configure(opts)
				f.o.hrc.Controller.Configure(opts)
				var sched *schedule.Scheduler
				d := withdrawalDispatcher(func(ctx context.Context, id schedule.NodeID, attempt manifest.ReconcileAttempt) (schedule.Outcome, manifest.ReconcileWait, bool) {
					if id == f.supplier && attempt.Drain == manifest.DrainForce {
						f.o.tasks.YieldSlot(func() { <-release })
					}
					check := id == f.reader && attempt.Drain == manifest.DrainForce && armed.CompareAndSwap(false, true)
					if check {
						if (attempt.Content == nil || len(attempt.Content.Suppliers) != 1) || attempt.Content.Suppliers[0].Cut == 0 {
							t.Fatal("scheduler did not issue a real force cut")
						}
						switch change {
						case "successor":
							sched.OnArrival(f.supplier, true)
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
						}
					}
					out, wait, rerun := (dagDispatcher{f.o}).Dispatch(ctx, id, attempt)
					if check {
						if out != schedule.OutcomeBlocked || f.o.store.GetArtifact(f.reader) != nil {
							t.Errorf("stale cut admitted reader: %v %+v", out, wait)
							cancel()
						}
						if wait.Content != nil {
							for _, o := range wait.Content.Suppliers {
								if o.Cut != 0 {
									t.Errorf("rejected cut survived: %+v", o)
								}
							}
						}
						rejected.Store(true)
						releaseOnce.Do(func() { close(release) })
					}
					return out, wait, rerun
				})
				tasks := r4Tasks{tasks: f.o.tasks, after: func(name string) {
					if name == "schedule/"+f.supplier.String() && armed.Load() {
						doneOnce.Do(func() { close(supplierDone) })
					}
				}}
				sched = schedule.New(tasks, d)
				unsub := f.o.watchDAG(sched)
				defer unsub()
				sched.Seed([]schedule.NodeID{f.reader, f.supplier})
				assert.NoError(t, sched.Run(ctx))
				assert.Equal(t, rejected.Load(), true)
				for _, id := range []manifest.NamedResource{f.reader, f.supplier} {
					info, _ := f.o.store.GetStatus(id)
					assert.Equal(t, info.Status, store.StatusReady)
				}
				assert.Equal(t, f.o.tasks.Failures(), int64(0))
			})
		}
	}
}

func TestResolve_OptionalBindingSetChange(t *testing.T) {
	t.Parallel()
	for _, kind := range []string{manifest.KindKustomization, manifest.KindHelmRelease} {
		t.Run(kind, func(t *testing.T) {
			t.Parallel()
			f := newR4Fixture(t, kind, "data", false)
			f.o.configureControllers()
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			extra := &manifest.Kustomization{Name: "extra", Namespace: f.reader.Namespace, Path: "empty"}
			release := make(chan struct{})
			releaseExtra := make(chan struct{})
			releaseSupplier := sync.OnceFunc(func() { close(release) })
			releaseExtraSupplier := sync.OnceFunc(func() { close(releaseExtra) })
			defer releaseSupplier()
			defer releaseExtraSupplier()
			extraDone := make(chan struct{})
			registered := make(chan struct{}, 8)
			var readerDispatches atomic.Int64
			var changed atomic.Bool
			var full atomic.Bool
			d := withdrawalDispatcher(func(ctx context.Context, id schedule.NodeID, attempt manifest.ReconcileAttempt) (schedule.Outcome, manifest.ReconcileWait, bool) {
				if id == f.reader {
					readerDispatches.Add(1)
				}
				if id == extra.Named() {
					f.o.tasks.YieldSlot(func() { <-releaseExtra })
				}
				if id == f.supplier {
					f.o.tasks.YieldSlot(func() { <-release })
				}
				if id == f.reader && (attempt.Content != nil && len(attempt.Content.Suppliers) > 0) && changed.CompareAndSwap(false, true) {
					switch reader := f.o.store.GetObject(f.reader).(type) {
					case *manifest.Kustomization:
						fresh := reader.Clone()
						fresh.PostBuild.SubstituteFrom = append(fresh.PostBuild.SubstituteFrom, manifest.SubstituteReference{Kind: manifest.KindConfigMap, Name: "second", Optional: true})
						f.o.store.AddObject(fresh)
					case *manifest.HelmRelease:
						fresh := reader.Clone()
						fresh.ValuesFrom = append(fresh.ValuesFrom, manifest.ValuesReference{Kind: manifest.KindConfigMap, Name: "second", Optional: true})
						f.o.store.AddObject(fresh)
					}
					f.o.store.AddObject(extra)
				}
				out, wait, rerun := (dagDispatcher{f.o}).Dispatch(ctx, id, attempt)
				if id == f.reader && changed.Load() && !full.Load() && (out != schedule.OutcomeBlocked || wait.Content == nil || len(wait.Content.Suppliers) != 2) {
					t.Errorf("binding lost current supplier set: %v %+v", out, wait)
					cancel()
					full.Store(true)
				}
				if id == f.reader && (attempt.Content != nil && len(attempt.Content.Suppliers) == 2) && full.CompareAndSwap(false, true) {
					assert.Equal(t, readerDispatches.Load(), int64(4))
					t.Logf("complete binding reached in %d reader dispatches", readerDispatches.Load())
					if out != schedule.OutcomeBlocked || wait.Content == nil || len(wait.Content.Suppliers) != 2 {
						t.Errorf("binding lost current supplier set: %v %+v", out, wait)
					}
				}
				return out, wait, rerun
			})
			tasks := r4Tasks{tasks: f.o.tasks, after: func(name string) {
				if name == "schedule/"+extra.Named().String() {
					close(extraDone)
				}
				if name == "schedule/"+f.reader.String() {
					registered <- struct{}{}
				}
			}}
			sched := schedule.New(tasks, d)
			unsub := f.o.watchDAG(sched)
			defer unsub()
			sched.Seed([]schedule.NodeID{f.reader, f.supplier})
			done := make(chan error, 1)
			go func() { done <- sched.Run(ctx) }()
			for dispatches := 0; !full.Load(); dispatches++ {
				if dispatches == cap(registered) {
					t.Fatal("complete supplier binding did not stabilize within the dispatch bound")
				}
				select {
				case <-registered:
				case err := <-done:
					t.Fatalf("scheduler ended before complete supplier binding: %v", err)
				}
			}
			releaseExtraSupplier()
			select {
			case <-extraDone:
			case <-ctx.Done():
				t.Fatal("complete supplier binding failed before extra supplier completion")
			}
			if f.o.store.GetArtifact(f.reader) != nil {
				t.Fatal("exhausted extra supplier allowed premature reader publication")
			}
			releaseSupplier()
			assert.NoError(t, <-done)
			output, _ := f.o.store.Get[*manifest.ConfigMap](manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: f.reader.Namespace, Name: "output"})
			if output == nil || output.Data["greeting"] != "exact" {
				t.Fatalf("binding-set output=%+v", output)
			}
		})
	}
}

func TestResolve_OptionalForceCutTargets(t *testing.T) {
	t.Parallel()
	for _, kind := range []string{manifest.KindKustomization, manifest.KindHelmRelease} {
		t.Run(kind, func(t *testing.T) {
			t.Parallel()
			f := newR4Fixture(t, kind, "data", false)
			second := &manifest.ConfigMap{Name: "second", Namespace: f.reader.Namespace, Data: map[string]any{"GREETING": "static", "values.yaml": "greeting: static"}}
			if kind == manifest.KindKustomization {
				delete(second.Data, "values.yaml")
			} else {
				delete(second.Data, "GREETING")
			}
			st := f.o.store
			staticOwner := &manifest.Kustomization{Name: "data-owner", Namespace: f.reader.Namespace, Path: "empty"}
			st.AddObject(staticOwner)
			index := &manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{
				f.target:       {Productions: []manifest.Production{{Owner: f.supplier}}},
				second.Named(): {Productions: []manifest.Production{{Owner: f.supplier}, {Owner: staticOwner.Named()}}},
				{Kind: manifest.KindConfigMap, Namespace: f.reader.Namespace, Name: "output"}: {Productions: []manifest.Production{{Owner: f.reader}}},
			}}
			switch reader := st.GetObject(f.reader).(type) {
			case *manifest.Kustomization:
				fresh := reader.Clone()
				fresh.PostBuild.SubstituteFrom = append(fresh.PostBuild.SubstituteFrom, manifest.SubstituteReference{Kind: second.Named().Kind, Name: second.Name, Optional: true})
				st.AddObject(fresh)
			case *manifest.HelmRelease:
				fresh := reader.Clone()
				fresh.ValuesFrom = append(fresh.ValuesFrom, manifest.ValuesReference{Kind: second.Named().Kind, Name: second.Name, Optional: true})
				st.AddObject(fresh)
			}
			supplier, _ := st.Get[*manifest.Kustomization](f.supplier)
			supplier = supplier.Clone()
			supplier.PostBuild = &kustomizev1.PostBuild{SubstituteFrom: []manifest.SubstituteReference{{Kind: manifest.KindConfigMap, Name: "output", Optional: true}}}
			st.AddObject(supplier)
			f.o.index = index
			f.o.configureControllers()
			f.o.ksc.Emit(staticOwner.Named(), &store.Emission{Children: []store.Child{{Obj: second}}, Complete: true})
			opts := base.Options{Index: index}
			f.o.ksc.Controller.Configure(opts)
			f.o.hrc.Controller.Configure(opts)
			release := make(chan struct{})
			var rejected atomic.Bool
			d := withdrawalDispatcher(func(ctx context.Context, id schedule.NodeID, attempt manifest.ReconcileAttempt) (schedule.Outcome, manifest.ReconcileWait, bool) {
				if id == f.supplier && attempt.Drain == manifest.DrainForce {
					f.o.tasks.YieldSlot(func() { <-release })
				}
				check := id == f.reader && attempt.Drain == manifest.DrainForce && rejected.CompareAndSwap(false, true)
				if check {
					if (attempt.Content == nil || len(attempt.Content.Suppliers) != 1) || attempt.Content.Suppliers[0].Cut == 0 || !slices.Equal(attempt.Content.Targets, []manifest.NamedResource{f.target}) {
						t.Errorf("unexpected scheduler cut: %+v", attempt)
					}
					f.o.ksc.Emit(staticOwner.Named(), &store.Emission{Children: []store.Child{{Obj: &manifest.ConfigMap{Name: f.target.Name, Namespace: f.target.Namespace, Data: second.Data}}}, Complete: true})
					f.o.ksc.Emit(f.supplier, &store.Emission{Children: []store.Child{{Obj: &manifest.RawObject{APIVersion: "v1", Kind: second.Named().Kind, Name: second.Name, Namespace: second.Namespace}}}})
				}
				out, wait, rerun := (dagDispatcher{f.o}).Dispatch(ctx, id, attempt)
				if check {
					if out != schedule.OutcomeBlocked || wait.Content == nil || !slices.Equal(wait.Content.Targets, []manifest.NamedResource{second.Named()}) || st.GetArtifact(f.reader) != nil {
						t.Errorf("changed target reused cut: %v %+v", out, wait)
					}
					close(release)
				}
				return out, wait, rerun
			})
			sched := schedule.New(f.o.tasks, d)
			unsub := f.o.watchDAG(sched)
			defer unsub()
			sched.Seed([]schedule.NodeID{f.reader, f.supplier})
			assert.NoError(t, sched.Run(t.Context()))
			assert.Equal(t, rejected.Load(), true)
			info, _ := st.GetStatus(f.reader)
			assert.Equal(t, info.Status, store.StatusReady)
		})
	}
}

func TestResolve_OptionalForceCutSuppliers(t *testing.T) {
	t.Parallel()
	for _, kind := range []string{manifest.KindKustomization, manifest.KindHelmRelease} {
		for _, change := range []string{"shrink", "equal-cardinality replacement", "binding order"} {
			t.Run(kind+"/"+change, func(t *testing.T) {
				t.Parallel()
				ctx, cancel := context.WithCancel(t.Context())
				defer cancel()
				f := newR4Fixture(t, kind, "data", false)
				extra := &manifest.Kustomization{Name: "z-extra", Namespace: f.reader.Namespace, Path: "empty", PostBuild: &kustomizev1.PostBuild{SubstituteFrom: []manifest.SubstituteReference{{Kind: manifest.KindConfigMap, Name: "output", Optional: true}}}}
				replacement := extra.Clone()
				replacement.Name = "zz-replacement"
				supplier, _ := f.o.store.Get[*manifest.Kustomization](f.supplier)
				supplier = supplier.Clone()
				supplier.PostBuild = extra.PostBuild
				st := f.o.store
				st.AddObject(supplier)
				st.AddObject(extra)
				st.AddObject(replacement)
				makeIndex := func(owners ...manifest.NamedResource) *manifest.DiscoveryIndex {
					var productions []manifest.Production
					for _, owner := range owners {
						productions = append(productions, manifest.Production{Owner: owner})
					}
					return &manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{
						f.target: {Productions: productions},
						{Kind: manifest.KindConfigMap, Namespace: f.reader.Namespace, Name: "output"}: {Productions: []manifest.Production{{Owner: f.reader}}},
					}}
				}
				f.o.index = makeIndex(f.supplier, extra.Named())
				f.o.configureControllers()
				opts := base.Options{Index: f.o.index}
				f.o.ksc.Controller.Configure(opts)
				f.o.hrc.Controller.Configure(opts)
				release := make(chan struct{})
				var rejected atomic.Bool
				d := withdrawalDispatcher(func(ctx context.Context, id schedule.NodeID, attempt manifest.ReconcileAttempt) (schedule.Outcome, manifest.ReconcileWait, bool) {
					if id != f.reader && attempt.Drain == manifest.DrainForce {
						f.o.tasks.YieldSlot(func() { <-release })
					}
					check := id == f.reader && attempt.Drain == manifest.DrainForce && rejected.CompareAndSwap(false, true)
					if check {
						if (attempt.Content == nil || len(attempt.Content.Suppliers) != 2) || attempt.Content.Suppliers[0].Cut == 0 || attempt.Content.Suppliers[1].Cut == 0 {
							t.Errorf("missing real supplier cuts: %+v", attempt)
						} else {
							switch change {
							case "shrink":
								f.o.ksc.Emit(extra.Named(), &store.Emission{Complete: true})
							case "equal-cardinality replacement":
								f.o.ksc.Emit(extra.Named(), &store.Emission{Complete: true})
								f.o.ksc.Emit(replacement.Named(), &store.Emission{Children: []store.Child{{Obj: &manifest.RawObject{APIVersion: "v1", Kind: f.target.Kind, Name: f.target.Name, Namespace: f.target.Namespace}}}, Complete: true})
							case "binding order":
								content := *attempt.Content
								attempt.Content = &content
								attempt.Content.Suppliers = slices.Clone(attempt.Content.Suppliers)
								attempt.Content.Suppliers[0], attempt.Content.Suppliers[1] = attempt.Content.Suppliers[1], attempt.Content.Suppliers[0]
							}
						}
					}
					out, wait, rerun := (dagDispatcher{f.o}).Dispatch(ctx, id, attempt)
					if check {
						if out != schedule.OutcomeBlocked || st.GetArtifact(f.reader) != nil {
							t.Errorf("changed suppliers reused cut: %v %+v", out, wait)
							cancel()
						}
						if wait.Content != nil {
							for _, o := range wait.Content.Suppliers {
								if o.Cut != 0 {
									t.Errorf("changed set retained cut: %+v", o)
								}
							}
						}
						close(release)
					}
					return out, wait, rerun
				})
				sched := schedule.New(f.o.tasks, d)
				unsub := f.o.watchDAG(sched)
				defer unsub()
				sched.Seed([]schedule.NodeID{f.reader, f.supplier, extra.Named(), replacement.Named()})
				assert.NoError(t, sched.Run(ctx))
				assert.Equal(t, rejected.Load(), true)
				assert.Equal(t, f.o.tasks.Failures(), int64(0))
				info, _ := st.GetStatus(f.reader)
				assert.Equal(t, info.Status, store.StatusReady)
			})
		}
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
				err := sched.Run(t.Context())
				unsub()
				assert.NoError(t, err)
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

type r4MissingFetcher struct{}

func (r4MissingFetcher) Fetch(_ context.Context, obj manifest.BaseManifest, _ sourcecache.Policy) (*store.SourceArtifact, error) {
	return nil, source.MissingSecretErr(obj.Named(), "input", "not found")
}

func TestReconcile_TypedWait(t *testing.T) {
	t.Parallel()
	for _, kind := range []string{manifest.KindKustomization, manifest.KindHelmRelease, manifest.KindResourceSet, manifest.KindOCIRepository} {
		t.Run(kind, func(t *testing.T) {
			t.Parallel()
			f := newR4Fixture(t, kind, "data", false)
			f.o.configureControllers()
			id := f.reader
			switch kind {
			case manifest.KindResourceSet:
				rs := &manifest.ResourceSet{Name: "typed", Namespace: "flux-system", ResourcesTemplate: "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: noop}\n"}
				gate := &manifest.Kustomization{Name: "gate", Namespace: rs.Namespace}
				f.o.store.AddObject(gate)
				f.o.store.UpdateStatus(gate.Named(), store.StatusReady, "", "")
				rs.DependsOn = []fluxopv1.Dependency{{APIVersion: manifest.FluxKustomizeDomain + "/v1", Kind: manifest.KindKustomization, Name: gate.Name, Namespace: gate.Namespace}}
				manifest.KindOf(&manifest.RawObject{APIVersion: rs.DependsOn[0].APIVersion, Kind: manifest.KindKustomization}).Namespaced()
				f.o.store.AddObject(rs)
				var out schedule.Outcome
				f.o.tasks.Go(t.Context(), "initial", func(ctx context.Context) {
					out, _, _ = (dagDispatcher{f.o}).Dispatch(ctx, rs.Named(), manifest.ReconcileAttempt{})
				})
				f.o.tasks.BlockTillDone()
				assert.Equal(t, out, schedule.OutcomeTerminal)
				f.o.store.UpdateStatus(gate.Named(), store.StatusPending, "", "successor queued")
				if f.o.store.GetArtifact(rs.Named()) == nil {
					t.Fatal("ResourceSet artifact was not retained")
				}
				id = rs.Named()
			case manifest.KindOCIRepository:
				repo := &manifest.OCIRepository{Name: "typed", Namespace: "flux-system"}
				f.o.WithFetcher(manifest.KindOCIRepository, r4MissingFetcher{})
				f.o.store.AddObject(repo)
				id = repo.Named()
			}
			before := f.o.store.GetArtifact(id)
			var out schedule.Outcome
			var wait manifest.ReconcileWait
			f.o.tasks.Go(t.Context(), "typed", func(ctx context.Context) {
				out, wait, _ = (dagDispatcher{f.o}).Dispatch(ctx, id, manifest.ReconcileAttempt{})
			})
			f.o.tasks.BlockTillDone()
			assert.Equal(t, out, schedule.OutcomeBlocked)
			assert.Equal(t, wait.Empty(), false)
			if kind == manifest.KindKustomization || kind == manifest.KindHelmRelease {
				assert.Equal(t, len(wait.Content.Suppliers), 1)
				assert.Diff(t, wait.Content.Targets, []manifest.NamedResource{f.target})
			} else {
				assert.Equal(t, len(wait.Deps), 1)
			}
			assert.Equal(t, f.o.store.GetArtifact(id), before)
			assert.Equal(t, f.o.tasks.Failures(), int64(0))
		})
	}
}

func TestResolve_OptionalInvocationSnapshot(t *testing.T) {
	t.Parallel()
	f := newR4Fixture(t, manifest.KindKustomization, "data", false)
	f.o.configureControllers()
	observer := &manifest.Kustomization{Name: "observer", Namespace: f.reader.Namespace, Path: "empty", PostBuild: &kustomizev1.PostBuild{SubstituteFrom: []manifest.SubstituteReference{{Kind: manifest.KindConfigMap, Name: "output", Optional: true}}}}
	f.o.store.AddObject(observer)
	release := make(chan struct{})
	var checked atomic.Bool
	var sched *schedule.Scheduler
	d := withdrawalDispatcher(func(ctx context.Context, id schedule.NodeID, attempt manifest.ReconcileAttempt) (schedule.Outcome, manifest.ReconcileWait, bool) {
		if id == f.supplier {
			f.o.tasks.YieldSlot(func() { <-release })
		}
		check := id == f.reader && attempt.Content != nil && checked.CompareAndSwap(false, true)
		var before []manifest.Opportunity
		if check {
			defer close(release)
			before = slices.Clone(attempt.Content.Suppliers)
			sched.OnArrival(f.supplier, true)
		}
		out, wait, rerun := (dagDispatcher{f.o}).Dispatch(ctx, id, attempt)
		if check {
			assert.Equal(t, slices.Equal(attempt.Content.Suppliers, before), true)
			assert.Equal(t, out, schedule.OutcomeBlocked)
		}
		return out, wait, rerun
	})
	sched = schedule.New(f.o.tasks, d)
	unsub := f.o.watchDAG(sched)
	defer unsub()
	sched.Seed([]schedule.NodeID{f.reader, f.supplier, observer.Named()})
	assert.NoError(t, sched.Run(t.Context()))
	assert.Equal(t, checked.Load(), true)
	assert.Equal(t, f.o.tasks.Failures(), int64(0))
	info, _ := f.o.store.GetStatus(f.reader)
	assert.Equal(t, info.Status, store.StatusReady)
}
