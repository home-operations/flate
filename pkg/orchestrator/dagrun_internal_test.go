package orchestrator

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/home-operations/flate/pkg/selection"

	"github.com/home-operations/flate/internal/admissiontest"
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
			testDAGKeepBeforeLateDataPublicationCase(
				t,
				&workers,
			)
		},
		)
	}
}

//nolint:paralleltest // The test calls a helper that changes process-global state.
func TestDAG_BoundedPublications(t *testing.T) {
	for _, workers := range []int{2, 16} {
		t.Run(fmt.Sprintf("workers=%d", workers), func(t *testing.T) {
			first := renderScalePublications(t, workers, 160)
			second := renderScalePublications(t, workers, 160)
			assert.Equal(t, bytes.Equal(first, second), true)
		})
	}
}

//nolint:paralleltest // The test calls a helper that changes process-global state.
func TestDAG_ScalePublications(t *testing.T) {
	if os.Getenv("FLATE_E8_SLOW") != "1" {
		t.Skip("set FLATE_E8_SLOW=1 for 1000 producers and 10000 publications")
	}
	for _, workers := range []int{2, 16} {
		t.Run(fmt.Sprintf("workers=%d", workers), func(t *testing.T) {
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
		testutil.WriteFile(
			t,
			root,
			"shared/"+name+".yaml",
			"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: "+name+", namespace: apps}\ndata: {value: shared}\n",
		)
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
		testutil.WriteFile(
			t,
			root,
			"apps/"+name+"/kustomization.yaml",
			"resources: [../../shared]\nnamePrefix: "+name+"-\n",
		)
		testutil.WriteFile(
			t,
			root,
			"flux/"+name+".yaml",
			testutil.KustomizationYAML(
				name,
				"flux-system",
				"  path: ./apps/"+name+"\n"+deps.String()+"  sourceRef: {kind: GitRepository, name: flux-system}\n",
			),
		)
	}
	return renderScalePublicationsTail(
		t,
		&renderScalePublicationsTailInputs{
			root:      &root,
			workers:   &workers,
			roots:     &roots,
			ids:       &ids,
			producers: &producers,
		},
	)
}

func renderScalePublicationsTail(t *testing.T, tailCaseInput *renderScalePublicationsTailInputs) []byte {
	t.Helper()
	o, err := newTestOrchestrator(
		t,
		Tree{Path: filepath.Join((*tailCaseInput.root), "flux"), RepoRoot: (*tailCaseInput.root)},
		Config{Concurrency: (*tailCaseInput.workers)},
	)
	assert.NoError(t, err)
	defer o.stop()
	assert.NoError(t, o.bootstrap(t.Context()))
	reconcileEntryForTest(t, o)
	o.configureControllers()
	o.src.StartLifecycle()
	o.ksc.StartLifecycle()
	firstWave := min((*tailCaseInput.workers), len((*tailCaseInput.roots)))
	started, release := make(chan struct{}, firstWave), make(chan struct{})
	var active, overBound, rootStarts, publications atomic.Int64
	d := fixtureDispatcher{
		store: o.store,
		dispatch: func(ctx context.Context, id schedule.NodeID, level manifest.DrainLevel, _ schedule.Result) schedule.Result {
			// Input acquisition yields its slot; only admission holds it continuously.
			func() {
				current := active.Add(1)
				defer active.Add(-1)
				if current > int64((*tailCaseInput.workers)) {
					overBound.Add(1)
				}
				if (*tailCaseInput.roots)[id] && rootStarts.Add(1) <= int64(firstWave) {
					started <- struct{}{}
					<-release
				}
			}()
			return (dagDispatcher{o}).Dispatch(ctx, id, level, schedule.Result{})
		},
	}
	s := schedule.New(o.tasks, d)
	unsubscribe := o.watchDAG(s)
	defer unsubscribe()
	unsubPublications := o.store.AddListener(store.EventObjectAdded, func(id manifest.NamedResource, payload any) {
		if id.Kind == manifest.KindConfigMap && payload != nil {
			publications.Add(1)
		}
	}, false)
	defer unsubPublications()
	seeds, err := o.seedNodes()
	assert.NoError(t, err)
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
	for _, id := range *tailCaseInput.ids {
		status, ok := o.store.GetStatus(id)
		assert.Equal(t, ok, true)
		assert.Equal(t, status.Status, store.StatusReady)
		for n := range 10 {
			child := manifest.NamedResource{
				Kind:      manifest.KindConfigMap,
				Name:      fmt.Sprintf("%s-object-%02d", id.Name, n),
				Namespace: "apps",
			}
			assert.Equal(t, o.store.GetObject(child) != nil, true)
		}
	}
	assert.Equal(t, publications.Load(), int64((*tailCaseInput.producers)*10))
	assert.Equal(t, len(o.store.ListObjects("", manifest.KindConfigMap)), (*tailCaseInput.producers)*10)
	assert.Equal(t, len(o.store.FailedResources()), 0)
	output, err := json.Marshal(o.collectManifests())
	assert.NoError(t, err)
	return output
}

//nolint:paralleltest // The test calls a helper that changes process-global state.
func TestDAG_SuspensionAfterRefresh(t *testing.T) {
	for _, changedOnly := range []bool{false, true} {
		for _, retained := range []bool{false, true} {
			var previous []byte
			for _, workers := range []int{2, 8} {
				if workers == 8 && (!changedOnly || !retained) {
					continue
				}
				t.Run(
					fmt.Sprintf("changed_%t/retained_%t/workers_%d", changedOnly, retained, workers),
					func(t *testing.T) {
						testDAGSuspensionAfterRefreshCase(
							t,
							testDAGSuspensionAfterRefreshCaseInputs{
								changedOnly: &changedOnly,
								retained:    &retained,
								previous:    &previous,
								workers:     &workers,
							},
						)
					},
				)
			}
		}
	}
}

func testDAGKeepBeforeLateDataPublicationCase(
	t *testing.T,
	testCaseInput *int,
) {
	t.Helper()

	st, tasks := store.New(), task.NewBounded((*testCaseInput))
	parent := &manifest.Kustomization{Name: "parent", Namespace: "flux-system", TargetNamespace: "tenant"}
	provider := &manifest.Kustomization{Name: "provider", Namespace: "tenant"}
	sibling := &manifest.Kustomization{Name: "sibling", Namespace: "tenant"}
	child := testutil.KustomizationID("tenant", "child")
	data := manifest.NamedResource{Kind: manifest.KindConfigMap, Name: "settings", Namespace: "tenant"}
	st.AddObjects([]manifest.BaseManifest{parent, provider, sibling})
	index := &manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{
		parent.Named(): {
			File: "parent.yaml",
		},
		provider.Named(): {File: "provider.yaml"},
		sibling.Named():  {File: "sibling.yaml"},
		data: {
			Productions: []manifest.Production{
				{
					Owner:    provider.Named(),
					Authored: data,
					File:     "settings.yaml",
					Source:   manifest.BootstrapSourceID,
				},
			},
		},
	}}
	st.SetDiscoveryIndex(index)
	filterLoader, session := admissiontest.New(
		t,
		admissiontest.Config{
			Changes:   change.NewSet([]string{"parent.yaml"}),
			Index:     index,
			Inventory: st,
			Root:      "",
		},
	)
	admissiontest.Commit(t, session, nil)
	c := base.New(st, tasks)
	c.Configure(base.Options{Session: session, Loader: filterLoader, Index: index})
	c.StartLifecycle()
	childDocs := []map[string]any{
		{
			"apiVersion": "kustomize.toolkit.fluxcd.io/v1",
			"kind":       "Kustomization",
			"metadata":   map[string]any{"name": child.Name},
			"spec": map[string]any{
				"path": "./child",
				"postBuild": map[string]any{
					"substituteFrom": []any{map[string]any{"kind": manifest.KindConfigMap, "name": data.Name}},
				},
			},
		},
	}
	parseChildrenResult1 := (&base.Execution{Controller: c}).ParseChildren(
		parent,
		testutil.DocumentOccurrences(childDocs),
		manifest.ParseDocOptions{},
	)
	children, retained, failures := parseChildrenResult1.Children, parseChildrenResult1.Visible, parseChildrenResult1.Failures
	assert.Equal(t, len(failures), 0)
	assert.Equal(t, len(children), 1)
	assert.Equal(t, children[0].Obj.Named(), child)
	blocked, release := make(chan []manifest.NamedResource, 1), make(chan struct{})
	var childRuns, siblingRuns atomic.Int64
	arrivals := make(chan bool, 2)
	unsubArrival := st.AddListener(store.EventObjectAdded, func(id manifest.NamedResource, payload any) {
		if payload != nil && (id == child || id == data) {
			arrivals <- session.Snapshot().Reconcile(id)
		}
	}, false)
	defer unsubArrival()
	d := fixtureDispatcher{
		store: st,
		dispatch: func(ctx context.Context, id schedule.NodeID, level manifest.DrainLevel, _ schedule.Result) schedule.Result {
			if id == provider.Named() {
				tasks.YieldSlot(func() { <-release })
			}
			e := controllertest.NewExecution(c, id)
			e.Dispatch(
				ctx,
				level,
				e,
				func(e *base.Execution, _ context.Context, _ *manifest.Kustomization, _ manifest.DrainLevel) error {
					_, _, next := e.Reads.GetEmission(id)
					defer next.Finish()
					switch id {
					case parent.Named():
						next.Children, next.Manifests = children, retained
					case provider.Named():
						next.Children = []store.Child{
							{
								Obj: &manifest.ConfigMap{
									Name:      data.Name,
									Namespace: data.Namespace,
									Data:      map[string]any{"VALUE": "ready"},
								},
							},
						}
					case child:
						childRuns.Add(1)
						obj := e.Reads.GetObject(data)
						if obj == nil {
							return &depwait.ErrBlocked{Deps: []manifest.NamedResource{data}}
						}
						next.Children = []store.Child{
							{
								Obj: &manifest.ConfigMap{
									Name:      "output",
									Namespace: data.Namespace,
									Data:      map[string]any{"VALUE": obj.(*manifest.ConfigMap).Data["VALUE"]},
								},
							},
						}
					case sibling.Named():
						siblingRuns.Add(1)
					}
					next.Complete = true
					return e.Emit(t.Context(), id, &next, nil)
				},
			)
			if id == child && e.Outcome() == schedule.Blocked {
				blocked <- slices.Clone(e.Blocked)
			}
			return schedule.Result{Assessment: e}
		},
	}
	o := &Orchestrator{store: st}
	s := schedule.New(tasks, d)
	unsubscribe := o.watchDAG(s)
	defer unsubscribe()
	s.Seed([]schedule.NodeID{parent.Named(), provider.Named(), sibling.Named()})
	done := make(chan error, 1)
	go func() { done <- s.Run(t.Context()) }()
	assert.Diff(t, <-blocked, []manifest.NamedResource{data})
	assert.Equal(t, st.GetObject(data), nil)
	assert.Equal(t, session.Snapshot().Reconcile(child), true)
	assert.Equal(t, session.Snapshot().Reconcile(provider.Named()), true)
	assert.Equal(t, session.Snapshot().Reconcile(sibling.Named()), false)
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
}

type testDAGSuspensionAfterRefreshCaseInputs struct {
	changedOnly *bool
	retained    *bool
	previous    *[]byte
	workers     *int
}

func testDAGSuspensionAfterRefreshCase(t *testing.T, testCaseInput testDAGSuspensionAfterRefreshCaseInputs) {
	t.Helper()

	root := t.TempDir()
	gate, child := testutil.KustomizationID(
		"flux-system",
		"gate",
	), testutil.KustomizationID(
		"flux-system",
		"child",
	)
	testutil.WriteFile(
		t,
		root,
		"kustomization.yaml",
		"resources: [flux/gate.yaml, unused/child.yaml, flux/dependent.yaml, flux/independent.yaml]\n",
	)
	testutil.WriteFile(t, root, "unused/kustomization.yaml", "resources: [child.yaml]\n")
	testutil.WriteFile(
		t,
		root,
		"flux/gate.yaml",
		strings.Replace(ksYAML("gate", "unused", ""), "spec:\n", "spec:\n  suspend: true\n", 1),
	)
	writeSuspensionWorkloads(t, root)
	cfg := treeConfig{CacheDir: t.TempDir(), Concurrency: (*testCaseInput.workers), Path: root, RepoRoot: root}
	if *testCaseInput.changedOnly {
		cfg.ExternalChanges = change.NewSet(
			[]string{
				"flux/gate.yaml",
				"unused/child.yaml",
				"dependent/cm.yaml",
				"independent/cm.yaml",
			},
		)
	}
	o, err := newTestOrchestrator(t, cfg.Tree, cfg.Config)
	assert.FatalIf(t, err != nil, "%v", err)
	t.Cleanup(func() {
		o.stop()
	})
	{
		localErr := o.bootstrap(t.Context())
		assert.FatalIf(t, localErr != nil, "%v", localErr)
	}
	assert.FatalIf(
		t, (*testCaseInput.changedOnly) &&
			o.store.GetObject(child) != nil, "%v",
		"producer-owned child hydrated before publication",
	)
	reconcileEntryForTest(t, o)
	original, ok := o.store.Get[*manifest.Kustomization](child)
	assert.FatalIf(t, !ok || original.Suspend, "%v", "active child missing")
	o.store.UpdateStatus(
		gate,
		store.StatusInfo{
			Status:  store.StatusReady,
			Reason:  store.ReasonSuspended,
			Message: "suspended",
		},
	)
	o.store.UpdateStatus(
		child,
		store.StatusInfo{Status: store.StatusPending, Reason: "", Message: "queued"},
	)
	o.configureControllers()
	assert.NoError(
		t,
		controllertest.Emit(
			t.Context(),
			o.ksc.Controller,
			gate,
			&store.Emission{Children: []store.Child{{Obj: original}}, Complete: true},
		),
	)
	var result schedule.Result
	o.tasks.Go(t.Context(), "dependent", func(ctx context.Context) {
		result = (dagDispatcher{o}).Dispatch(
			ctx,
			testutil.KustomizationID("flux-system", "dependent"),
			manifest.DrainNone,
			schedule.Result{},
		)
	})
	o.tasks.BlockTillDone()
	assert.FatalIf(t, result.Assessment.Outcome() != schedule.Blocked ||
		!slices.Contains(
			result.Assessment.(*base.Execution).Blocked,
			child,
		), "dependent must first wait on child: %+v", result.Assessment)
	retainedID := manifest.NamedResource{
		Kind:      manifest.KindConfigMap,
		Namespace: child.Namespace,
		Name:      "retained",
	}
	artifact := retainedSuspensionArtifact(testCaseInput, o, child, retainedID)
	var publication, arrivals atomic.Int64
	unsub := o.store.AddListener(
		store.EventStatusUpdated,
		func(id manifest.NamedResource, payload any) {
			info, ok := payload.(store.StatusInfo)
			if id != child || !ok || info.String() != "resolving dependencies" ||
				!publication.CompareAndSwap(0, 1) {
				return
			}
			publishSuspendedChild(t, o, gate)
		},
		false,
	)
	t.Cleanup(unsub)
	unsubArrivals := o.store.AddListener(
		store.EventObjectAdded,
		func(id manifest.NamedResource, _ any) {
			if id == child {
				testCaseInput.assertArrival(t, o, child, gate)
			}
			if id == retainedID ||
				id == (manifest.NamedResource{Kind: manifest.KindConfigMap, Name: child.Name}) {
				arrivals.Add(1)
			}
		},
		false,
	)
	t.Cleanup(unsubArrivals)
	res, err := o.collectResult(t.Context(), o.run(t.Context()))
	t.Cleanup(res.Close)
	assert.FatalIf(t, err != nil || len(res.Failed) != 0 || len(res.Blocked) != 0 || o.tasks.Failures() != 0,

		"scheduler failed to drain: error=%v failures=%v blocked=%v",
		err,
		res.Failed,
		res.Blocked)

	info, _ := o.store.GetStatus(child)
	fresh, _ := o.store.Get[*manifest.Kustomization](child)
	assert.FatalIf(t, publication.Load() != 1 || arrivals.Load() != 0 ||
		info.Outcome() != store.OutcomeSuspended ||
		!fresh.Suspend ||
		original.Suspend,

		"late suspension: publications=%d arrivals=%d status=%+v original=%+v fresh=%+v",
		publication.Load(),
		arrivals.Load(),
		info,
		original,
		fresh)

	{
		owners := testutil.Owners(o.store.Productions(child, nil))
		assert.FatalIf(
			t,
			!slices.Contains(owners, gate),
			"missing independent contributor: %v",
			owners,
		)
	}
	assert.FatalIf(t, !slices.ContainsFunc(
		slices.Collect(o.index.ReadersOfFile("child/cm.yaml")),
		func(r manifest.FileReader) bool { return r.Reader == child },
	), "%v", "suspension lost the child's file read tracking")
	_, _, inventory := o.store.GetEmission(child)
	if *testCaseInput.retained {
		assert.FatalIf(t, len(artifact.Manifests) != 1 ||
			o.store.GetArtifact(child, store.ArtifactContent) != nil ||
			o.store.GetObject(retainedID) == nil ||
			len(
				inventory.Children,
			) != 1, "%v", "suspension pruned retained artifact or emission")
	} else {
		assert.FatalIf(
			t,
			o.store.GetArtifact(child, store.ArtifactContent) != nil ||
				len(inventory.Children) != 0,
			"%v",
			"suspended child produced output",
		)
	}
	assertSuspensionActiveOutputs(t, o, res)
	assertSuspensionOutputBytes(t, res, testCaseInput)
}

type renderScalePublicationsTailInputs struct {
	root *string

	workers *int

	roots *map[manifest.
		NamedResource]bool

	ids *[]manifest.
		NamedResource

	producers *int
}

func writeSuspensionWorkloads(t *testing.T, root string) {
	t.Helper()
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
		testutil.WriteFile(
			t,
			root,
			name+"/cm.yaml",
			"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: "+name+"}\ndata: {value: active}\n",
		)
	}
}

func publishSuspendedChild(t *testing.T, o *Orchestrator, gate manifest.NamedResource) {
	t.Helper()
	docs, localErr := testutil.DecodeDocs(
		strings.NewReader(
			strings.Replace(
				ksYAML("child", "child", "gate"),
				"spec:\n",
				"spec:\n  suspend: true\n",
				1,
			),
		),
	)
	if localErr != nil {
		t.Error(localErr)
		return
	}
	parseChildrenForTestResult1 := parseChildrenForTest(
		gate,
		docs,
		manifest.ParseDocOptions{DefaultNamespace: gate.Namespace},
	)
	children, retained := parseChildrenForTestResult1.Children, parseChildrenForTestResult1.Documents
	emittedValues := children
	for emittedIndex := range emittedValues {
		emitted := emittedValues[emittedIndex]
		if o.selection != nil {
			if _, innerLocalErr := o.ksc.KeepEmitted(
				t.Context(),
				gate,
				selection.PrepareRequest{Generation: []store.Child{emitted}, Reads: nil},
			); innerLocalErr != nil {
				t.Error(innerLocalErr)
				return
			}
		}
	}
	_, _, next := o.store.GetEmission(gate)
	defer next.Finish()
	next.Children, next.Manifests, next.Complete = children, testutil.DocumentOccurrences(
		retained,
	), true
	o.store.RecordEmitted(gate, &next, nil)
}

func retainedSuspensionArtifact(
	testCaseInput testDAGSuspensionAfterRefreshCaseInputs,
	o *Orchestrator,
	child, retainedID manifest.NamedResource,
) *store.Emission {
	var artifact *store.Emission
	if *testCaseInput.retained {
		doc := map[string]any{
			"apiVersion": "v1",
			"kind":       manifest.KindConfigMap,
			"metadata": map[string]any{
				"name":      retainedID.Name,
				"namespace": retainedID.Namespace,
			},
			"data": map[string]any{"value": "prior"},
		}
		publishDocuments(
			o.ksc.Controller,
			child,
			[]map[string]any{doc},
			manifest.ParseDocOptions{DefaultNamespace: child.Namespace},
		)
		artifact = o.store.GetArtifact(child, store.ArtifactContent).(*store.Emission)
	}
	return artifact
}

func assertSuspensionActiveOutputs(t *testing.T, o *Orchestrator, res *Result) {
	t.Helper()
	for _, name := range []string{"dependent", "independent"} {
		id := manifest.NamedResource{Kind: manifest.KindConfigMap, Name: name}
		assert.FatalIf(t, o.store.GetObject(id) == nil ||
			len(
				outputByParent(res.Manifests)[testutil.KustomizationID("flux-system", name)],
			) != 1, "active output missing: %s", id)
	}
}

func assertSuspensionOutputBytes(t *testing.T, res *Result, testCaseInput testDAGSuspensionAfterRefreshCaseInputs) {
	t.Helper()
	output, err := json.Marshal(canonicalBatches(res))
	assert.FatalIf(t, err != nil, "%v", err)
	assert.FatalIf(
		t, (*testCaseInput.previous) !=
			nil && !bytes.Equal((*testCaseInput.previous), output), "%v",
		"output bytes differ across worker counts",
	)
	(*testCaseInput.previous) = output
}

func (in testDAGSuspensionAfterRefreshCaseInputs) assertArrival(
	t *testing.T,
	o *Orchestrator,
	child, gate manifest.NamedResource,
) {
	t.Helper()
	owners := testutil.Owners(o.store.Productions(child, nil))
	assert.ErrorIf(t, !slices.Contains(owners, gate) || (*in.changedOnly) &&
		!o.selection.Snapshot().Reconcile(
			child,
		), "child arrived before evidence/selection: %v", owners)
}
