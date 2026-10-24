package orchestrator

import (
	"context"
	"fmt"
	"slices"
	"testing"

	helmv2 "github.com/fluxcd/helm-controller/api/v2"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/internal/testutil/controllertest"
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
		o.store.RecordEmitted(a, &store.Emission{Complete: true})
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
		run.store.UpdateStatus(id, info.Status, info.Reason, info.Message)
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
	e := controllertest.NewExecution(base.New(st, tasks, "test"), obj.Named())
	e.Dispatch[*manifest.HelmRelease](t.Context(), manifest.DrainNone, func(_ context.Context, _ *manifest.HelmRelease, _ manifest.DrainLevel) error {
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
	st.UpdateStatus(child.Named(), store.StatusFailed, "", "unwired")
	o := &Orchestrator{store: st, index: indexForTest(map[manifest.NamedResource]string{parent.Named(): "root.yaml", child.Named(): "apps/child.yaml"})}
	d := dagDispatcher{o}
	assert.Equal(t, len(d.Sweep(t.Context(), true)), 0)
	info, _ := st.GetStatus(child.Named())
	assert.Equal(t, info.Status, store.StatusFailed)
	st.UpdateStatus(parent.Named(), store.StatusReady, "", "")
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
	st.UpdateStatus(parent.Named(), store.StatusReady, "", "")
	st.UpdateStatus(child.Named(), store.StatusFailed, "", "unwired")
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
	st.UpdateStatus(producer.Named(), store.StatusReady, "", "")
	failure := fmt.Errorf("%w: invalid yaml", manifest.ErrInput)
	index := indexForTest(map[manifest.NamedResource]string{producer.Named(): "root.yaml"})
	index.Errors = map[string]error{"apps/invalid.yaml": failure}
	index.Readers = map[string][]manifest.NamedResource{"apps/invalid.yaml": {producer.Named()}}
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

func TestDAGSweep_OrphanDecodeAttributionStable(t *testing.T) {
	t.Parallel()
	st := store.New()
	parent := &manifest.Kustomization{Name: "parent", Namespace: "apps", Path: "./apps"}
	orphan := &manifest.Kustomization{Name: "orphan", Namespace: "apps", Path: "./apps/orphan"}
	consumer := &manifest.Kustomization{Name: "consumer", Namespace: "apps"}
	st.AddObjects([]manifest.BaseManifest{parent, orphan, consumer})
	st.UpdateStatus(parent.Named(), store.StatusReady, "", "")
	failure := fmt.Errorf("%w: invalid yaml", manifest.ErrInput)
	st.UpdateFailure(orphan.Named(), failure)
	index := indexForTest(map[manifest.NamedResource]string{parent.Named(): "root.yaml", orphan.Named(): "apps/orphan/ks.yaml"})
	index.Errors = map[string]error{"apps/orphan/invalid.yaml": failure}
	index.Readers = map[string][]manifest.NamedResource{"apps/orphan/invalid.yaml": {orphan.Named()}}
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
