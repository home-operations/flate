package orchestrator

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"testing"

	helmv2 "github.com/fluxcd/helm-controller/api/v2"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/internal/testutil/controllertest"
	"github.com/home-operations/flate/pkg/change"
	"github.com/home-operations/flate/pkg/controllers/base"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
	"github.com/home-operations/flate/pkg/task"
)

func TestOrchestrator_ParentOf(t *testing.T) {
	t.Parallel()
	id := testutil.HelmReleaseID("apps", "child")
	structural, rendered := testutil.KustomizationID("flux-system", "structural"), testutil.KustomizationID("flux-system", "rendered")
	for _, tc := range []struct {
		name                       string
		structural, rendered, want bool
	}{
		{name: "none"}, {name: "structural", structural: true, want: true},
		{name: "rendered", rendered: true, want: true},
		{name: "precedence", structural: true, rendered: true, want: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			o := &Orchestrator{store: store.New(), index: indexForTest(nil)}
			if tc.structural {
				o.index.Records[id] = manifest.DiscoveryRecord{Parent: structural}
			}
			if tc.rendered {
				publishFixture(t, o.store, rendered, []manifest.NamedResource{id})
			}
			got, ok := o.parentOf(id)
			want := rendered
			if tc.structural {
				want = structural
			}
			if ok != tc.want || (ok && got != want) {
				t.Fatalf("parent = %s, %t, want %s, %t", got, ok, want, tc.want)
			}
		})
	}
	t.Run("minimum_accepted_owner", func(t *testing.T) {
		t.Parallel()
		o := &Orchestrator{store: store.New(), index: indexForTest(nil)}
		a, z := testutil.KustomizationID("apps", "a"), testutil.KustomizationID("apps", "z")
		for _, owner := range []manifest.NamedResource{z, a} {
			publishFixture(t, o.store, owner, []manifest.NamedResource{id})
		}
		parent, ok := o.parentOf(id)
		if !ok || parent != a || o.store.EmissionOwner(id) != z {
			t.Fatalf("parent=%v/%t content owner=%v, want %v/true and %v", parent, ok, o.store.EmissionOwner(id), a, z)
		}
		assert.Diff(t, o.store.Productions(id, nil), []manifest.Production{{Owner: a}, {Owner: z}})
		o.store.RecordEmitted(a, &store.Emission{Complete: true}, nil)
		parent, ok = o.parentOf(id)
		if !ok || parent != z {
			t.Fatalf("withdrawn parent=%v/%t, want %v/true", parent, ok, z)
		}
	})
	t.Run("empty_does_not_erase_owner", func(t *testing.T) {
		t.Parallel()
		o := &Orchestrator{store: store.New(), index: indexForTest(nil)}
		o.index.Records[id] = manifest.DiscoveryRecord{Productions: []manifest.Production{{Owner: rendered}, {}}}
		parent, ok := o.parentOf(id)
		if !ok || parent != rendered {
			t.Fatalf("parent=%v/%t, want %v/true", parent, ok, rendered)
		}
		assert.Diff(t, o.store.Productions(id, o.index), []manifest.Production{{}, {Owner: rendered}})
	})

	for _, proof := range []struct {
		name       string
		production manifest.Production
	}{
		{"empty", manifest.Production{}},
		{"self", manifest.Production{Owner: id}},
		{"authored", manifest.Production{Owner: rendered, Authored: id}},
		{"declaration", manifest.Production{Owner: rendered, Declaration: id}},
	} {
		t.Run(proof.name, func(t *testing.T) {
			t.Parallel()
			o := &Orchestrator{store: store.New(), index: indexForTest(nil)}
			o.index.Records[id] = manifest.DiscoveryRecord{Productions: []manifest.Production{proof.production}}
			parent, ok := o.parentOf(id)
			if ok || parent != (manifest.NamedResource{}) {
				t.Fatalf("ineligible parent=%v/%t", parent, ok)
			}
			assert.Diff(t, o.store.Productions(id, o.index), []manifest.Production{proof.production})
		})
	}

}

func TestOrchestrator_ParentOfAllocations(t *testing.T) {
	o := &Orchestrator{store: store.New(), index: indexForTest(nil)}
	owner := testutil.KustomizationID("apps", "parent")
	child := testutil.HelmReleaseID("apps", "child")
	publishFixture(t, o.store, owner, []manifest.NamedResource{child})
	var parent manifest.NamedResource
	var ok bool
	allocs := testing.AllocsPerRun(10, func() { parent, ok = o.parentOf(child) })
	if allocs != 0 || !ok || parent != owner {
		t.Fatalf("parent=%v/%t allocations=%v", parent, ok, allocs)
	}
}

func TestOrchestrator_ChildrenByParent(t *testing.T) {
	o, err := New(t.Context(), Config{Path: t.TempDir(), CacheDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	id := func(name string) manifest.NamedResource {
		return testutil.KustomizationID("apps", name)
	}
	parent, other, first, second := id("parent"), id("other"), id("a"), id("b")
	setParentsForTest(o, map[manifest.NamedResource]manifest.NamedResource{first: parent, second: parent})
	publishFixture(t, o.store, parent, []manifest.NamedResource{second})
	publishFixture(t, o.store, other, []manifest.NamedResource{first})
	got := o.childrenByParent()
	if !slices.Equal(got[parent], []manifest.NamedResource{first, second}) ||
		!slices.Equal(got[other], []manifest.NamedResource{first}) {
		t.Fatalf("ownership snapshot = %v", got)
	}
	got[parent][0] = other
	delete(got, other)
	fresh := o.childrenByParent()
	if !slices.Equal(fresh[parent], []manifest.NamedResource{first, second}) ||
		!slices.Equal(fresh[other], []manifest.NamedResource{first}) {
		t.Fatalf("caller mutated ownership: %v", fresh)
	}
}

func acceptInputsForTest(t *testing.T, st *store.Store, id manifest.NamedResource, required []manifest.NamedResource) {
	t.Helper()
	reads := new(store.ReadSet)
	reads.Init(st, id, nil)
	reads.GetObject(id)
	for _, dep := range required {
		reads.GetObject(dep)
		reads.RequireData(dep)
	}
	reads.Accept()
	assert.Equal(t, reads.Valid(), true)
	reads.Retire()
	for range st.Sweep() {
	}
}

func TestSelection_CompletedReadClosure(t *testing.T) {
	t.Parallel()
	selected := &manifest.HelmRelease{Name: "selected", Namespace: "apps"}
	selected.ValuesFrom = []helmv2.ValuesReference{{Kind: manifest.KindConfigMap, Name: "optional", Optional: true}, {Kind: manifest.KindSecret, Name: "unconsumed"}}
	input := manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: "apps", Name: "required"}
	owner, optional, sibling := testutil.KustomizationID("apps", "required-owner"), testutil.KustomizationID("apps", "optional-owner"), testutil.KustomizationID("apps", "sibling")
	run := selectedRun(&Result{Failed: make(map[manifest.NamedResource]store.StatusInfo)})
	run.store.AddObject(selected)
	publishFixture(t, run.store, owner, []manifest.NamedResource{input})
	for _, id := range []manifest.NamedResource{selected.Named(), owner, optional, sibling} {
		if run.store.GetObject(id) == nil {
			run.store.AddObject(&manifest.Kustomization{Name: id.Name, Namespace: id.Namespace})
		}
		info := store.StatusInfo{Status: store.StatusFailed, Message: "failed " + id.Name}
		run.store.UpdateStatus(id, store.StatusInfo{Status: info.Status, Reason: info.Reason, Message: info.Message})
		run.Result.Failed[id] = info
	}
	setParentsForTest(run.Orchestrator, map[manifest.NamedResource]manifest.NamedResource{{Kind: manifest.KindConfigMap, Namespace: "apps", Name: "optional"}: optional})
	acceptInputsForTest(t, run.store, selected.Named(), []manifest.NamedResource{input})
	results, err, remainder := Select([]Rendered{run}, &FailuresError{Message: "raw failures"}, []manifest.NamedResource{selected.Named()}, nil)
	assert.Equal(t, err != nil, true)
	assert.NoError(t, remainder)
	assert.Equal(t, len(results[0].Failed), 2)
	for _, id := range []manifest.NamedResource{selected.Named(), owner} {
		_, failed := results[0].Failed[id]
		assert.Equal(t, failed, true)
	}
	for _, id := range []manifest.NamedResource{optional, sibling} {
		_, failed := results[0].Failed[id]
		assert.Equal(t, failed, false)
	}
	assert.Equal(t, len(results[0].Unrelated), 2)
}

func TestSelection_AcceptedFootprintReplacement(t *testing.T) {
	t.Parallel()
	st := store.New()
	obj := &manifest.HelmRelease{Name: "selected", Namespace: "apps"}
	st.AddObject(obj)
	first, second := manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: "apps", Name: "first"}, manifest.NamedResource{Kind: manifest.KindSecret, Namespace: "apps", Name: "second"}
	acceptInputsForTest(t, st, obj.Named(), []manifest.NamedResource{first})
	before := slices.Collect(st.RequiredInputs(obj.Named()))
	acceptInputsForTest(t, st, obj.Named(), []manifest.NamedResource{second})
	assert.Diff(t, slices.Collect(st.RequiredInputs(obj.Named())), []manifest.NamedResource{second})
	assert.Diff(t, before, []manifest.NamedResource{first})
	assert.Equal(t, len(slices.Collect(st.RequiredInputs(testutil.HelmReleaseID("apps", "absent")))), 0)
}

func TestSelection_SkippedHasNoConsumedPrerequisites(t *testing.T) {
	t.Parallel()
	st, tasks := store.New(), task.NewBounded(2)
	obj := &manifest.HelmRelease{Name: "selected", Namespace: "apps"}
	obj.Suspend = true
	obj.ValuesFrom = []helmv2.ValuesReference{{Kind: manifest.KindConfigMap, Name: "unconsumed"}}
	st.AddObject(obj)
	e := controllertest.NewExecution(base.New(st, tasks), obj.Named())
	e.Dispatch[*manifest.HelmRelease](t.Context(), manifest.DrainNone, t, func(t *testing.T, _ context.Context, _ *manifest.HelmRelease, _ manifest.DrainLevel) error {
		t.Helper()
		t.Fatal("suspended declaration reconciled")
		return nil
	})
	e.Close()
	for range st.Sweep() {
	}
	assert.Equal(t, len(slices.Collect(st.RequiredInputs(obj.Named()))), 0)
	info, _ := st.GetStatus(obj.Named())
	assert.Equal(t, info.Outcome() == store.OutcomeSuspended, true)
}

func TestSelection_WithdrawsStaleRequiredFootprint(t *testing.T) {
	t.Parallel()
	st := store.New()
	obj := &manifest.Kustomization{Name: "reader", Namespace: "apps"}
	st.AddObject(obj)
	var reads store.ReadSet
	reads.Init(st, obj.Named(), nil)
	reads.GetObject(obj.Named())
	dependency := manifest.NamedResource{Kind: manifest.KindConfigMap, Name: "required", Namespace: "apps"}
	reads.RequireData(dependency)
	reads.Accept()
	defer reads.Retire()
	assert.Diff(t, slices.Collect(st.RequiredInputs(obj.Named())), []manifest.NamedResource{dependency})
	changed := obj.Clone()
	changed.Path = "changed"
	st.AddObject(changed)
	assert.Equal(t, len(slices.Collect(st.RequiredInputs(obj.Named()))), 0)
}

func TestSelection_AcceptedWarningWithdrawal(t *testing.T) {
	t.Parallel()
	obj := &manifest.Kustomization{Name: "selected", Namespace: "apps"}
	run := selectedRun(&Result{})
	run.store.AddObject(obj)
	first := new(store.ReadSet)
	first.Init(run.store, obj.Named(), nil)
	first.GetObject(obj.Named())
	first.AddWarning(obj.Named(), manifest.Warning{Resource: obj.Named(), Category: manifest.WarnUnresolvedSubstitution, Message: "missing key"})
	first.Accept()
	run.Result.Warnings = run.store.Warnings()
	selected, err, _ := Select([]Rendered{run}, nil, []manifest.NamedResource{obj.Named()}, nil)
	assert.NoError(t, err)
	assert.Equal(t, len(selected[0].Warnings), 1)
	acceptInputsForTest(t, run.store, obj.Named(), nil)
	first.Retire()
	for range run.store.Sweep() {
	}
	run.Result.Warnings = run.store.Warnings()
	selected, err, _ = Select([]Rendered{run}, nil, []manifest.NamedResource{obj.Named()}, nil)
	assert.NoError(t, err)
	assert.Equal(t, len(selected[0].Warnings), 0)
}

func TestDAGSweep_ClassifiesBeforeTermination(t *testing.T) {
	t.Parallel()
	st := store.New()
	parent, child, reader := &manifest.Kustomization{Name: "parent", Namespace: "apps", Path: "./apps"}, &manifest.Kustomization{Name: "child", Namespace: "apps"}, &manifest.Kustomization{Name: "reader", Namespace: "apps"}
	st.AddObjects([]manifest.BaseManifest{parent, child, reader})
	st.UpdateStatus(child.Named(), store.StatusInfo{Status: store.StatusFailed, Reason: "", Message: "unwired"})
	o := &Orchestrator{store: st, index: indexForTest(map[manifest.NamedResource]string{parent.Named(): "root.yaml", child.Named(): "apps/child.yaml"})}
	d := dagDispatcher{o}
	assert.Equal(t, len(d.Sweep(t.Context(), true)), 0)
	info, _ := st.GetStatus(child.Named())
	assert.Equal(t, info.Status, store.StatusFailed)
	st.UpdateStatus(parent.Named(), store.StatusInfo{Status: store.StatusReady, Reason: "", Message: ""})
	var reads store.ReadSet
	reads.Init(st, reader.Named(), nil)
	reads.GetStatus(child.Named())
	publications := 0
	u := st.AddListener(store.EventStatusUpdated, func(manifest.NamedResource, any) { publications++ }, false)
	defer u()
	assert.Diff(t, d.Sweep(t.Context(), true), []manifest.NamedResource{reader.Named()})
	info, _ = st.GetStatus(child.Named())
	assert.Equal(t, info.Status, store.StatusReady)
	assert.Equal(t, o.orphans[child.Named()], "unwired")
	assert.Equal(t, len(d.Sweep(t.Context(), true)), 0)
	assert.Equal(t, publications, 1)
	reads.Retire()
	for range st.Sweep() {
	}
}

func TestFinalize_InspectionDoesNotChangeStatus(t *testing.T) {
	t.Parallel()
	st := store.New()
	parent, child := &manifest.Kustomization{Name: "parent", Namespace: "apps", Path: "./apps"}, &manifest.Kustomization{Name: "child", Namespace: "apps"}
	st.AddObjects([]manifest.BaseManifest{parent, child})
	st.UpdateStatus(parent.Named(), store.StatusInfo{Status: store.StatusReady, Reason: "", Message: ""})
	st.UpdateStatus(child.Named(), store.StatusInfo{Status: store.StatusFailed, Reason: "", Message: "unwired"})
	o := &Orchestrator{store: st, tasks: task.NewBounded(2), index: indexForTest(map[manifest.NamedResource]string{parent.Named(): "root.yaml", child.Named(): "apps/child.yaml"})}
	publications := 0
	u := st.AddListener(store.EventStatusUpdated, func(manifest.NamedResource, any) { publications++ }, false)
	defer u()
	assert.Equal(t, o.finalize() != nil, true)
	info, _ := st.GetStatus(child.Named())
	assert.Equal(t, info.Status, store.StatusFailed)
	assert.Equal(t, publications, 0)
}

func TestDAGSweep_DecodeFailureAttribution(t *testing.T) {
	t.Parallel()
	st := store.New()
	producer, consumer := &manifest.Kustomization{Name: "producer", Namespace: "apps", Path: "./apps"}, &manifest.Kustomization{Name: "consumer", Namespace: "apps"}
	st.AddObjects([]manifest.BaseManifest{producer, consumer})
	st.UpdateStatus(producer.Named(), store.StatusInfo{Status: store.StatusReady, Reason: "", Message: ""})
	failure := fmt.Errorf("%w: invalid yaml", manifest.ErrInput)
	index := indexForTest(map[manifest.NamedResource]string{producer.Named(): "root.yaml"})
	index.Errors = map[string]error{"apps/invalid.yaml": failure}
	index.Readers = map[string][]manifest.FileReader{"apps/invalid.yaml": {{Reader: producer.Named(), Source: manifest.BootstrapSourceID}}}
	o := &Orchestrator{store: st, index: index}
	var reads store.ReadSet
	reads.Init(st, consumer.Named(), nil)
	reads.GetStatus(producer.Named())
	publications := 0
	u := st.AddListener(store.EventStatusUpdated, func(manifest.NamedResource, any) { publications++ }, false)
	defer u()
	d := dagDispatcher{o}
	assert.Diff(t, d.Sweep(t.Context(), true), []manifest.NamedResource{consumer.Named()})
	info, _ := st.GetStatus(producer.Named())
	assert.Equal(t, info.Status, store.StatusFailed)
	assert.Equal(t, info.Message, failure.Error())
	assert.Equal(t, len(d.Sweep(t.Context(), true)), 0)
	assert.Equal(t, publications, 1)
	reads.Retire()
	for range st.Sweep() {
	}
}

func TestDecodeFailures_ExactInputs(t *testing.T) {
	t.Parallel()
	for _, relation := range []string{"source values", "chart", "graph"} {
		for _, selected := range []bool{false, true} {
			for _, unowned := range []bool{false, true} {
				t.Run(fmt.Sprintf("%s/selected=%t/unowned=%t", relation, selected, unowned), func(t *testing.T) {
					t.Parallel()
					st := store.New()
					var reader manifest.BaseManifest = &manifest.HelmRelease{Name: "reader", Namespace: "apps"}
					if relation == "graph" {
						reader = &manifest.Kustomization{Name: "reader", Namespace: "apps", Path: "unrelated"}
					}
					st.AddObject(reader)
					index := indexForTest(map[manifest.NamedResource]string{reader.Named(): "reader.yaml"})
					files := []string{"inputs/a.yaml", "inputs/z.yaml"}
					if relation == "chart" {
						files = []string{"charts/demo/a.yaml", "charts/demo/z.yaml"}
					}
					first, second := fmt.Errorf("%w: first", manifest.ErrInput), fmt.Errorf("%w: second", manifest.ErrInput)
					index.Errors = map[string]error{files[0]: first, files[1]: second}
					index.Readers = map[string][]manifest.FileReader{files[0]: {{Reader: reader.Named(), Source: manifest.BootstrapSourceID, Exact: relation != "graph"}}, files[1]: {{Reader: reader.Named(), Source: manifest.BootstrapSourceID, Exact: relation != "graph"}}}
					unownedErr := fmt.Errorf("%w: unowned", manifest.ErrInput)
					if unowned {
						index.Errors["charts/demo/not-read.yaml"] = unownedErr
					}
					var changes []string
					if selected {
						changes = []string{"reader.yaml"}
					}
					o := &Orchestrator{store: st, index: index, filter: change.NewFilter(change.NewSet(changes), index, st, ""), tasks: task.NewBounded(2)}
					attributed, other := o.decodeFailures(nil)
					assert.Equal(t, len(attributed) != 0, selected)
					if selected {
						assert.Equal(t, attributed[reader.Named()], first)
					}
					if unowned {
						assert.Equal(t, len(other), 1)
						assert.Equal(t, other[0], unownedErr)
					} else {
						assert.Equal(t, len(other), 0)
					}
					o.classifyCompleted()
					assert.Equal(t, o.finalize() != nil, selected || unowned)
					assert.Equal(t, len(st.Warnings()), 0)
				})
			}
		}
	}
}

func TestDAGSweep_OrphanDecodeAttributionStable(t *testing.T) {
	t.Parallel()
	st := store.New()
	parent := &manifest.Kustomization{Name: "parent", Namespace: "apps", Path: "./apps"}
	orphan := &manifest.Kustomization{Name: "orphan", Namespace: "apps", Path: "./apps/orphan"}
	consumer := &manifest.Kustomization{Name: "consumer", Namespace: "apps"}
	st.AddObjects([]manifest.BaseManifest{parent, orphan, consumer})
	st.UpdateStatus(parent.Named(), store.StatusInfo{Status: store.StatusReady, Reason: "", Message: ""})
	failure := fmt.Errorf("%w: invalid yaml", manifest.ErrInput)
	st.UpdateFailure(orphan.Named(), failure)
	index := indexForTest(map[manifest.NamedResource]string{parent.Named(): "root.yaml", orphan.Named(): "apps/orphan/ks.yaml"})
	index.Errors = map[string]error{"apps/orphan/invalid.yaml": failure}
	index.Readers = map[string][]manifest.FileReader{"apps/orphan/invalid.yaml": {{Reader: orphan.Named(), Source: manifest.BootstrapSourceID}}}
	o := &Orchestrator{store: st, index: index}
	publications := 0
	u := st.AddListener(store.EventStatusUpdated, func(manifest.NamedResource, any) { publications++ }, false)
	defer u()
	d := dagDispatcher{o}
	assert.Equal(t, len(d.Sweep(t.Context(), true)), 0)
	info, _ := st.GetStatus(orphan.Named())
	assert.Equal(t, info.Status, store.StatusReady)
	var reads store.ReadSet
	reads.Init(st, consumer.Named(), nil)
	reads.GetStatus(orphan.Named())
	assert.Equal(t, len(d.Sweep(t.Context(), true)), 0)
	assert.Equal(t, publications, 1)
	assert.Equal(t, reads.Valid(), true)
	reads.Retire()
	for range st.Sweep() {
	}
}

func TestRenderTrees_ChangedOnlyAndDiff_GeneratedInputs(t *testing.T) {
	t.Parallel()
	for _, scenario := range []string{"edit", "move", "delete", "target", "key", "optional", "unrelated"} {
		t.Run(scenario, func(t *testing.T) {
			t.Parallel()
			dirs := [2]string{t.TempDir(), t.TempDir()}
			values := [2]string{"before", "after"}
			if scenario == "move" || scenario == "delete" || scenario == "unrelated" {
				values[1] = "before"
			}
			if scenario == "key" {
				values[1] = "fallback"
			}
			for side, root := range dirs {
				target, key, file := "settings", "VALUE", "generator.env"
				if side == 1 {
					switch scenario {
					case "move":
						file = "moved.env"
					case "target":
						target = "renamed-settings"
					case "key":
						key = "OTHER"
					}
				}
				generator := "  envs: [" + file + "]\n"
				if side == 1 && scenario == "delete" {
					generator = "  literals: [VALUE=before]\n"
				} else {
					testutil.WriteFile(t, root, "producer/"+file, key+"="+values[side]+"\n")
				}
				testutil.WriteFile(t, root, "producer/kustomization.yaml", "namespace: flux-system\ngeneratorOptions: {disableNameSuffixHash: true}\nconfigMapGenerator:\n- name: "+target+"\n"+generator)
				for _, name := range []string{"producer", "consumer", "sibling"} {
					postBuild := ""
					if name == "consumer" {
						postBuild = fmt.Sprintf("  postBuild:\n    substituteFrom: [{kind: ConfigMap, name: %s, optional: %t}]\n", target, scenario == "optional")
					}
					testutil.WriteFile(t, root, "flux/"+name+".yaml", testutil.KustomizationYAML(name, "flux-system", "  path: ./"+name+"\n  sourceRef: {kind: GitRepository, name: flux-system}\n"+postBuild))
				}
				testutil.WriteFile(t, root, "consumer/kustomization.yaml", "resources: [result.yaml]\n")
				testutil.WriteFile(t, root, "consumer/result.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: result}\ndata: {value: '${VALUE:=fallback}'}\n")
				testutil.WriteFile(t, root, "sibling/kustomization.yaml", "resources: [result.yaml]\n")
				testutil.WriteFile(t, root, "sibling/result.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: untouched}\n")
				if scenario == "unrelated" {
					testutil.WriteFile(t, root, "notes.txt", fmt.Sprintf("revision %d\n", side))
				}
			}
			cache := t.TempDir()
			var previous [2]string
			for run, state := range []string{"cold", "warm"} {
				cfg := Config{CacheDir: cache, Concurrency: 2}
				base, head, err := RenderTrees(t.Context(), Tree{Path: dirs[0], RepoRoot: dirs[0]}, Tree{Path: dirs[1], RepoRoot: dirs[1]}, cfg)
				assert.NoError(t, err)
				for side, rendered := range []Rendered{base, head} {
					producer := testutil.KustomizationID("flux-system", "producer")
					consumer := testutil.KustomizationID("flux-system", "consumer")
					sibling := testutil.KustomizationID("flux-system", "sibling")
					want := []manifest.NamedResource{producer, consumer}
					if scenario == "unrelated" {
						want = nil
					}
					var got []manifest.NamedResource
					for id, docs := range outputByParent(rendered.Result.Manifests) {
						if len(docs) > 0 {
							got = append(got, id)
						}
					}
					slices.SortFunc(got, manifest.NamedResource.Compare)
					slices.SortFunc(want, manifest.NamedResource.Compare)
					assert.Diff(t, got, want)
					assert.Equal(t, rendered.filter.ShouldReconcile(sibling), false)
					if scenario == "unrelated" {
						continue
					}
					body, err := json.Marshal(outputByParent(rendered.Result.Manifests)[consumer])
					assert.NoError(t, err)
					wantBody := fmt.Sprintf(`[{"apiVersion":"v1","data":{"value":%q},"kind":"ConfigMap","metadata":{"labels":{"kustomize.toolkit.fluxcd.io/name":"consumer","kustomize.toolkit.fluxcd.io/namespace":"flux-system"},"name":"result"}}]`, values[side])
					assert.Equal(t, string(body), wantBody)
					if run > 0 {
						assert.Equal(t, string(body), previous[side])
					}
					previous[side] = string(body)
					target, key := "settings", "VALUE"
					if side == 1 && scenario == "target" {
						target = "renamed-settings"
					}
					if side == 1 && scenario == "key" {
						key = "OTHER"
					}
					producerBody, err := json.Marshal(outputByParent(rendered.Result.Manifests)[producer])
					assert.NoError(t, err)
					wantProducer := fmt.Sprintf(`[{"apiVersion":"v1","data":{%q:%q},"kind":"ConfigMap","metadata":{"annotations":{"config.kubernetes.io/origin":"configuredIn: kustomization.yaml\nconfiguredBy:\n  apiVersion: builtin\n  kind: ConfigMapGenerator\n"},"labels":{"kustomize.toolkit.fluxcd.io/name":"producer","kustomize.toolkit.fluxcd.io/namespace":"flux-system"},"name":%q,"namespace":"flux-system"}}]`, key, values[side], target)
					assert.Equal(t, string(producerBody), wantProducer)
					t.Logf("state=%s side=%d affected=%v producer=%s consumer=%s", state, side, want, producerBody, body)
				}
			}
		})
	}
}
