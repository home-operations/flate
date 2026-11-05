package orchestrator

import (
	"context"
	"fmt"
	"slices"
	"sync/atomic"
	"testing"

	"github.com/home-operations/flate/internal/admissiontest"
	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/internal/testutil/controllertest"
	"github.com/home-operations/flate/pkg/change"
	"github.com/home-operations/flate/pkg/controllers/base"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/schedule"
	"github.com/home-operations/flate/pkg/store"
)

//nolint:paralleltest // The test calls a helper that changes process-global state.
func TestResolve_ArtifactPredictionSource(t *testing.T) {
	for _, changed := range []bool{false, true} {
		t.Run(fmt.Sprintf("changed=%t", changed), func(t *testing.T) {
			root := t.TempDir()
			testutil.WriteFile(
				t,
				root,
				"producer.yaml",
				testutil.KustomizationYAML(
					"producer",
					"apps",
					"  path: ./producer\n  sourceRef: {kind: GitRepository, name: flux-system, namespace: flux-system}\n",
				),
			)
			testutil.WriteFile(
				t,
				root,
				"producer/kustomization.yaml",
				"namespace: apps\nresources: [generator.yaml, reader.yaml]\n",
			)
			testutil.WriteFile(
				t,
				root,
				"producer/generator.yaml",
				"apiVersion: source.extensions.fluxcd.io/v1beta1\nkind: ArtifactGenerator\nmetadata: {name: generator}\nspec:\n  pathPattern: '@repo/{app}'\n  artifacts: [{name: 'app-{app}'}]\n",
			)
			testutil.WriteFile(
				t,
				root,
				"producer/reader.yaml",
				testutil.KustomizationYAML(
					"reader",
					"apps",
					"  path: ./workload\n  sourceRef: {kind: ExternalArtifact, name: app-api}\n",
				),
			)
			cfg := treeConfig{CacheDir: t.TempDir(), Concurrency: 2, Path: root, RepoRoot: root}
			if changed {
				cfg.ExternalChanges = change.NewSet([]string{"producer/reader.yaml"})
			}
			o, err := newTestOrchestrator(t, cfg.Tree, cfg.Config)
			assert.NoError(t, err)
			t.Cleanup(func() { o.stop() })
			assert.NoError(t, o.bootstrap(t.Context()))
			reconcileEntryForTest(t, o)
			o.configureControllers()
			d := dagDispatcher{o}
			producer := testutil.KustomizationID("apps", "producer")
			target := manifest.NamedResource{
				Group:     manifest.SourceDomain,
				Kind:      manifest.KindExternalArtifact,
				Namespace: "apps",
				Name:      "app-api",
			}
			assert.FatalIf(t, !o.filter.ShouldReconcile(producer), "%v", "prediction owner not selected")
			info := controllertest.DispatchToFixpoint(
				t.Context(),
				t,
				controllertest.Dispatch{
					Tasks:         o.tasks,
					Store:         o.store,
					ID:            producer,
					ReconcileNode: o.ksc.ReconcileNode,
				},
			)

			assert.FatalIf(t, info.Status != store.StatusReady, "producer did not settle: %+v", info)

			facts := o.store.ReferenceFacts(target, o.index, manifest.NamedResource{})

			assert.FatalIf(t, facts.Data != nil || facts.Accepted ||
				!slices.ContainsFunc(facts.Productions, func(p manifest.Production) bool {
					return p.Owner == producer && p.Declaration.Kind == "ArtifactGenerator" &&
						p.Source == manifest.BootstrapSourceID
				}) ||
				o.store.GetArtifact(
					target,
					store.ArtifactContent,
				) != nil, "source-qualified prediction supplied content or disappeared: %+v", facts)

			reader := d.Dispatch(t.Context(), testutil.KustomizationID("apps", "reader"), manifest.DrainNone).Assessment
			defer reader.Close()
			assert.FatalIf(
				t,
				reader.Outcome() != schedule.Blocked || !slices.Contains(reader.(*base.Execution).Blocked, target),
				"prediction bypassed ordinary source wait: outcome=%v blocked=%v status=%v selected=%t",
				reader.Outcome(),
				reader.(*base.Execution).Blocked,
				o.store.FailedResources(),
				o.filter.ShouldReconcile(testutil.KustomizationID("apps", "reader")),
			)
		})
	}
}

//nolint:paralleltest // The test calls a helper that changes process-global state.
func TestResolve_OptionalForceCutInvalidation(t *testing.T) {
	for _, kind := range []string{manifest.KindKustomization, manifest.KindHelmRelease} {
		for _, change := range []string{"declaration", "target", "supplier"} {
			t.Run(kind+"/"+change, func(t *testing.T) {
				testResolveOptionalForceCutInvalidationCase(
					t,
					testResolveOptionalForceCutInvalidationCaseInputs{kind: &kind, change: &change},
				)
			},
			)
		}
	}
}

//nolint:paralleltest // The test calls a helper that changes process-global state.
func TestResolve_OptionalPairedSelection(t *testing.T) {
	for _, kind := range []string{manifest.KindKustomization, manifest.KindHelmRelease} {
		t.Run(kind, func(t *testing.T) {
			runs := make([]Rendered, 0, 2)
			var reader manifest.NamedResource
			for _, result := range []string{"empty", "data"} {
				f := newR4Fixture(t, kind, result, false)
				reader = f.reader
				f.o.index.Records[reader] = manifest.DiscoveryRecord{
					File: "reader.yaml",
					Refs: []manifest.NamedResource{f.target},
				}
				f.o.index.Claims = map[string][]manifest.NamedResource{"reader/": {reader}}
				f.o.store.SetDiscoveryIndex(f.o.index)
				f.o.loader = admissiontest.New(
					t,
					admissiontest.Config{
						Changes:   change.NewSet([]string{"reader/output.yaml"}),
						Index:     f.o.index,
						Inventory: f.o.store,
						Root:      "",
					},
				)
				admissiontest.Commit(t, f.o.loader, nil)
				f.o.filter = f.o.loader.Options.Filter
				assert.FatalIf(
					t,
					!f.o.filter.ShouldReconcile(reader) || !f.o.filter.ShouldReconcile(f.supplier),
					"%v",
					"changed-only selection lost optional materializer",
				)
				f.o.configureControllers()
				sched := schedule.New(f.o.tasks, dagDispatcher{f.o})
				unsub := f.o.watchDAG(sched)
				sched.Seed([]schedule.NodeID{f.reader, f.supplier})
				assert.NoError(t, sched.Run(t.Context()))
				unsub()
				runs = append(
					runs,
					Rendered{
						Orchestrator: f.o,
						Result:       &Result{Manifests: f.o.collectManifests(), Failed: f.o.store.FailedResources()},
					},
				)
			}
			selected, err, _ := Select(runs, nil, []manifest.NamedResource{reader}, nil)
			assert.NoError(t, err)
			assert.Equal(t, len(selected), 2)
			for side, want := range []string{"fallback", "exact"} {
				docs := outputByParent(selected[side].Manifests)[reader]
				assert.FatalIf(t, len(docs) != 1, "side %d selected manifests=%v", side, docs)
				data, _ := docs[0]["data"].(map[string]any)
				assert.Equal(t, data["greeting"], any(want))
				assert.Equal(t, len(selected[side].Failed), 0)
				assert.Equal(t, len(selected[side].Blocked), 0)
			}
		})
	}
}

//nolint:paralleltest // The test calls a helper that changes process-global state.
func TestResolve_OptionalQueuedSuccessor(t *testing.T) {
	for _, kind := range []string{manifest.KindKustomization, manifest.KindHelmRelease} {
		t.Run(kind, func(t *testing.T) {
			testResolveOptionalQueuedSuccessorCase(t, &kind)
		},
		)
	}
}

type testResolveOptionalForceCutInvalidationCaseInputs struct {
	kind   *string
	change *string
}

func testResolveOptionalForceCutInvalidationCase(
	t *testing.T,
	testCaseInput testResolveOptionalForceCutInvalidationCaseInputs,
) {
	t.Helper()

	f := newR4Fixture(t, (*testCaseInput.kind), "data", false)
	f.o.configureControllers()
	prior := (dagDispatcher{f.o}).Dispatch(t.Context(), f.reader, manifest.DrainNone).Assessment.(*base.Execution)
	defer prior.Close()
	assert.FatalIf(
		t,
		prior.Outcome() != schedule.Blocked || prior.Reads.GetObject(f.target) != nil,
		"%v",
		"optional input did not retain absence",
	)
	switch *testCaseInput.change {
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
		if (*testCaseInput.change) == "supplier" {
			extra := &manifest.Kustomization{Name: "extra", Namespace: f.reader.Namespace, Path: "empty"}
			f.o.store.AddObject(extra)
			owner = extra.Named()
		}
		data := map[string]any{"GREETING": "exact"}
		if (*testCaseInput.kind) == manifest.KindHelmRelease {
			data = map[string]any{"values.yaml": "greeting: exact"}
		}
		controllertest.Emit(
			t.Context(),
			f.o.ksc.Controller,
			owner,
			&store.Emission{
				Children: []store.Child{
					{
						Obj: &manifest.ConfigMap{
							Name:      f.target.Name,
							Namespace: f.target.Namespace,
							Data:      data,
						},
					},
				},
				Complete: true,
			},
		)
	}
	assert.FatalIf(t, prior.Valid(), "%v", "changed authority retained the prior assessment")
	assert.FatalIf(
		t,
		prior.Reads.GetObject(f.target) != nil,
		"%v",
		"successor mutated the predecessor's frozen absence",
	)
	prior.Settle(t.Context(), manifest.DrainForce)
	assert.FatalIf(t, f.rendered(), "%v", "stale force assessment published output")
	sched := schedule.New(f.o.tasks, dagDispatcher{f.o})
	unsub := f.o.watchDAG(sched)
	defer unsub()
	seeds := []schedule.NodeID{f.reader, f.supplier}
	_, _, inventory := f.o.store.GetEmission(f.reader)
	childValues := inventory.Children
	for childIndex := range childValues {
		child := childValues[childIndex]
		if child.Generated && manifest.KindOf(child.Obj).Runnable {
			seeds = append(seeds, child.Obj.Named())
		}
	}
	inventory.Finish()
	sched.Seed(seeds)
	assert.NoError(t, sched.Run(t.Context()))
	output, _ := f.o.store.Get[*manifest.ConfigMap](
		manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: f.reader.Namespace, Name: "output"},
	)
	assert.FatalIf(
		t,
		output == nil || output.Data["greeting"] != "exact",
		"successor output=%+v",
		output,
	)
}

func testResolveOptionalQueuedSuccessorCase(t *testing.T, testCaseInput *string,
) {
	t.Helper()

	f := newR4Fixture(t, (*testCaseInput), "data", false)
	f.o.configureControllers()
	keeper := testutil.KustomizationID("flux-system", "keeper")
	keeping, releaseKeeper := make(chan struct{}), make(chan struct{})
	var started, queued atomic.Bool
	var sched *schedule.Scheduler
	d := fixtureDispatcher{
		store: f.o.store,
		dispatch: func(ctx context.Context, id schedule.NodeID, level manifest.DrainLevel) schedule.Result {
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
				assert.ErrorIf(
					t,
					result.Assessment.Outcome() != schedule.Blocked,
					"%v",
					"queued supplier did not retain optional absence",
				)
				sched.OnArrival(f.supplier, true)
				assert.ErrorIf(
					t,
					started.Load() || f.rendered(),
					"%v",
					"queued supplier started or admitted reader before a worker slot opened",
				)
				close(releaseKeeper)
			}
			return result
		},
	}
	sched = schedule.New(f.o.tasks, d)
	unsub := f.o.watchDAG(sched)
	defer unsub()
	sched.Seed([]schedule.NodeID{keeper, f.reader})
	assert.NoError(t, sched.Run(t.Context()))
	output, _ := f.o.store.Get[*manifest.ConfigMap](
		manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: f.reader.Namespace, Name: "output"},
	)
	assert.FatalIf(
		t,
		output == nil || output.Data["greeting"] != "exact",
		"queued successor output=%+v",
		output,
	)
}
