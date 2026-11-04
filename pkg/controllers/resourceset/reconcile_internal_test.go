package resourceset

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/home-operations/flate/internal/testutil"

	fluxopv1 "github.com/controlplaneio-fluxcd/flux-operator/api/v1"

	"github.com/home-operations/flate/internal/admissiontest"
	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil/controllertest"
	"github.com/home-operations/flate/pkg/change"
	"github.com/home-operations/flate/pkg/controllers/base"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/resourceset"
	"github.com/home-operations/flate/pkg/schedule"
	"github.com/home-operations/flate/pkg/store"
	"github.com/home-operations/flate/pkg/task"
)

func TestReconcile_ParentUpdateDuringRender(t *testing.T) {
	t.Parallel()
	for _, boundary := range []string{"capture", "render"} {
		t.Run(boundary, func(t *testing.T) {
			t.Parallel()
			testReconcileParentUpdateDuringRenderCase(
				t,
				&boundary,
			)
		},
		)
	}
}

func TestReconcile_ProviderInventorySettlement(t *testing.T) {
	t.Parallel()
	for _, workers := range []int{2, 4} {
		t.Run(fmt.Sprintf("workers_%d", workers), func(t *testing.T) {
			t.Parallel()
			testReconcileProviderInventorySettlementCase(
				t,
				&workers,
			)
		},
		)
	}
}

func testReconcileParentUpdateDuringRenderCase(
	t *testing.T,
	testCaseInput *string,
) {
	t.Helper()

	st := store.New()
	tasks := task.NewBounded(2)
	c := New(st, tasks, nil)
	started, resume, rendered := make(chan struct{}), make(chan struct{}), make(chan struct{})
	release := sync.OnceFunc(func() { close(resume) })
	t.Cleanup(func() { release(); tasks.BlockTillDone() })
	parent := &manifest.Kustomization{Name: "parent", Namespace: "apps"}
	st.AddObject(parent)
	provider := &manifest.ResourceSetInputProvider{
		Name:      "dynamic",
		Namespace: "apps",
		Type:      fluxopv1.InputProviderGitHubBranch,
	}
	st.AddObject(provider)
	st.UpdateStatus(provider.Named(), store.StatusInfo{Status: store.StatusReady, Reason: "", Message: ""})
	docA := map[string]any{
		"apiVersion": "fluxcd.controlplane.io/v1", "kind": manifest.KindResourceSet,
		"metadata": map[string]any{"name": "binding", "namespace": "apps"},
		"spec": map[string]any{
			"inputs": []any{map[string]any{"name": "from-a"}},
			"inputsFrom": []any{
				map[string]any{"kind": manifest.KindResourceSetInputProvider, "name": provider.Name},
			},
			"resourcesTemplate": "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: << inputs.name >>, namespace: apps}\n",
		},
	}
	docB := manifest.DeepCopyMap(docA)
	docB["spec"].(map[string]any)["inputs"].([]any)[0].(map[string]any)["name"] = "from-b"
	publish := func(doc map[string]any) {
		publishDocuments(
			c,
			parent.Named(),
			[]map[string]any{doc},
			manifest.ParseDocOptions{DefaultNamespace: "apps"},
		)
	}
	publish(docA)
	st.UpdateStatus(parent.Named(), store.StatusInfo{Status: store.StatusReady, Reason: "", Message: ""})
	id := manifest.NamedResource{
		Group:     manifest.FluxOperatorDomain,
		Kind:      manifest.KindResourceSet,
		Namespace: "apps",
		Name:      "binding",
	}
	rs, ok := st.Get[*manifest.ResourceSet](id)
	assert.FatalIf(t, !ok, "%v", "parent did not emit input A")
	renderWithAdmissionResult1, renderWithAdmissionResult1Err := renderWithAdmission(
		rs,
		resourceset.StoreResolver(st),
		"",
	)
	_, wantFingerprint, err := renderWithAdmissionResult1.documents, renderWithAdmissionResult1.fingerprint, renderWithAdmissionResult1Err
	assert.FatalIf(t, err != nil, "%v", err)
	if (*testCaseInput) == "capture" {
		pause := sync.OnceFunc(func() { close(started); tasks.YieldSlot(func() { <-resume }) })
		c.Configure(
			base.Options{
				PreflightFailure: func(manifest.NamedResource) (string, bool) { pause(); return "", false },
			},
		)
	} else {
		c.Configure(base.Options{})
	}
	c.StartLifecycle()
	var runState *execution
	var accepted atomic.Pointer[store.Emission]
	unsubscribe := st.AddListener(store.EventArtifactUpdated, func(other manifest.NamedResource, payload any) {
		if emission, localOk := payload.(*store.Emission); other == id && localOk && emission.Complete {
			accepted.Store(emission)
		}
	}, false)
	t.Cleanup(unsubscribe)
	tasks.Go(t.Context(), "parent", func(context.Context) {
		defer release()
		reached := false
		tasks.YieldSlot(func() {
			select {
			case <-started:
				reached = true
			case <-rendered:
			}
		})
		if !reached {
			t.Error("render never reached admission")
			return
		}
		publish(docB)
	})
	startReplacementRender(
		t,
		&replacementRenderInputs{
			testCaseInput: &testCaseInput,
			tasks:         &tasks,
			c:             &c,
			started:       &started,
			resume:        &resume,
			rendered:      &rendered,
			id:            &id,
			rs:            &rs,
			runState:      &runState,
		},
	)
	tasks.BlockTillDone()
	assert.FatalIf(t, tasks.Failures() != 0,
		"task failures = %d", tasks.Failures())

	if (*testCaseInput) == "render" {
		assert.FatalIf(t, runState.run.Reads.Valid() || accepted.Load() != nil ||
			st.GetArtifact(
				id,
				store.ArtifactContent,
			) != nil, "%v", "replaced input A published stale output")
	} else {
		{
			emission := accepted.Load()
			assert.FatalIf(
				t,
				emission == nil || !hasRenderedDoc(emission, manifest.KindConfigMap, "from-b"),
				"capture did not use replacement B: %+v",
				emission,
			)
		}
	}
	current, ok := st.Get[*manifest.ResourceSet](id)
	assert.FatalIf(
		t,
		!ok || string(current.Inputs[0]["name"].Raw) != `"from-b"`,
		"current input is not B: %+v",
		current,
	)
	run := c.ReconcileNode(t.Context(), id, manifest.DrainNone)
	assert.FatalIf(t, run.Outcome() == schedule.Blocked,
		"replacement blocked: %v", run.Blocked)

	emission := accepted.Load()
	assert.FatalIf(t, emission == nil || !emission.Complete || emission.Fingerprint == wantFingerprint ||
		!hasRenderedDoc(emission, manifest.KindConfigMap, "from-b") ||
		hasRenderedDoc(
			emission,
			manifest.KindConfigMap,
			"from-a",
		), "replacement B did not render: %+v", emission)
	assert.FatalIf(t, string(rs.Inputs[0]["name"].Raw) != `"from-a"`, "%v", "render mutated input A")
}

func testReconcileProviderInventorySettlementCase(
	t *testing.T,
	testCaseInput *int,
) {
	t.Helper()

	st := store.New()
	tasks := task.NewBounded((*testCaseInput))
	rs := selectorFixture("reader", "blue")
	rs.ResourcesTemplate = "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: '<< inputs.provider.name >>', namespace: ns}\ndata: {value: '<< inputs.value >>'}"
	owner := &manifest.Kustomization{Name: "providers", Namespace: "ns"}
	st.AddObjects([]manifest.BaseManifest{rs, owner})
	st.UpdateStatus(owner.Named(), store.StatusInfo{Status: store.StatusReady, Reason: "", Message: ""})
	a := staticRSIP("a", "ns", map[string]string{"color": "blue"}, map[string]string{"value": "first"})
	b := staticRSIP("b", "ns", map[string]string{"color": "blue"}, map[string]string{"value": "second"})
	publish := func(providers ...*manifest.ResourceSetInputProvider) {
		t.Helper()
		children := make([]store.Child, 0, len(providers))
		for _, p := range providers {
			children = append(children, store.Child{Obj: p})
		}
		st.RecordEmitted(owner.Named(), &store.Emission{Children: children, Complete: true}, nil)
	}
	check := func(count int, status store.Status) *store.Emission {
		t.Helper()
		index := &manifest.DiscoveryIndex{
			Records: map[manifest.NamedResource]manifest.DiscoveryRecord{rs.Named(): {File: "reader.yaml"}},
		}
		filterLoader := admissiontest.New(
			t,
			admissiontest.Config{
				Changes:   change.NewSet([]string{"reader.yaml"}),
				Index:     index,
				Inventory: st,
				Root:      "",
			},
		)
		admissiontest.Commit(t, filterLoader, nil)
		filter := filterLoader.Options.Filter
		_, _, before := st.GetEmission(rs.Named())
		for _, child := range before.Children {
			assert.FatalIf(
				t,
				!filter.ShouldReconcile(child.Obj.Named()),
				"%v",
				"accepted child missing before replay",
			)
		}
		before.Finish()
		c := New(st, tasks, nil)
		c.Configure(base.Options{Filter: filter, Index: index, Loader: filterLoader})
		c.StartLifecycle()

		info := controllertest.DispatchToFixpoint(
			t.Context(),
			t,
			controllertest.Dispatch{Tasks: tasks, Store: st, ID: rs.Named(), ReconcileNode: c.ReconcileNode},
		)
		assert.FatalIf(t, info.Status != status, "status=%+v, want %s", info, status)
		_, _, current := st.GetEmission(rs.Named())
		defer current.Finish()
		assert.FatalIf(
			t,
			len(current.Manifests) != count || len(current.Children) != count,
			"inventory=%+v, want %d documents and children",
			current,
			count,
		)
		if status == store.StatusReady {
			for _, child := range current.Children {
				assert.FatalIf(t, !filter.ShouldReconcile(child.Obj.Named()),
					"accepted child %s missing from changed-only selection", child.Obj.Named())
			}
		}
		accepted, _ := st.GetArtifact(rs.Named(), store.ArtifactContent).(*store.Emission)
		return accepted
	}
	publish(a)
	initial := check(1, store.StatusReady)
	replayed := check(1, store.StatusReady)
	assert.FatalIf(
		t,
		initial.Fingerprint != replayed.Fingerprint ||
			!hasRenderedDoc(replayed, manifest.KindConfigMap, "a"),
		"%v",
		"replay lost accepted inventory",
	)
	publish(b, a)
	accepted := check(2, store.StatusReady)
	changed := *a
	changed.Status.ExportedInputs = []fluxopv1.ResourceSetInput{{"value": {Raw: []byte(`"current"`)}}}
	publish(&changed, b)
	updated := check(2, store.StatusReady)
	assert.FatalIf(
		t,
		updated.Fingerprint == accepted.Fingerprint,
		"%v",
		"changed provider replayed stale content",
	)
	assert.Diff(t, updated.Manifests[0].Document["data"], map[string]any{"value": "current"})
	accepted = updated
	broken := *a
	broken.DefaultValues = fluxopv1.ResourceSetInput{"value": {Raw: []byte("invalid")}}
	publish(&broken, b)
	check(0, store.StatusFailed)
	{
		failed, ok := st.GetArtifact(rs.Named(), store.ArtifactContent).(*store.Emission)
		assert.FatalIf(t, ok &&
			(len(failed.Manifests) != 0 || len(failed.Children) != 0), "%v", "failed successor exposed output with changed provider inputs")
	}
	publish(&changed, b)
	recovered := check(2, store.StatusReady)
	assert.Diff(t, recovered.Manifests, accepted.Manifests)
	publish()
	check(0, store.StatusReady)
	assert.FatalIf(t, st.GetObject(a.Named()) != nil ||
		st.GetObject(
			manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: "ns", Name: "a"},
		) != nil, "%v", "complete empty inventory retained a provider or output")
	publish(&broken)
	check(0, store.StatusFailed)
}

type replacementRenderInputs struct {
	testCaseInput **string
	tasks         **task.Service
	c             **Controller
	started       *chan struct{}
	resume        *chan struct{}
	rendered      *chan struct{}
	id            *manifest.NamedResource
	rs            **manifest.ResourceSet
	runState      **execution
}

func startReplacementRender(t *testing.T, scope *replacementRenderInputs) {
	t.Helper()
	(*scope.tasks).Go(t.Context(), "render", func(context.Context) {
		defer close((*scope.rendered))
		(*scope.runState) = (*scope.c).newExecution((*scope.id))
		defer (*scope.runState).run.Close()
		if (*(*scope.testCaseInput)) == "capture" {
			(*scope.runState).run.Dispatch(t.Context(), manifest.DrainNone, (*scope.runState), (*execution).reconcile)
			return
		}
		_, _, attempt := (*scope.runState).run.Reads.GetEmission((*scope.id))
		docs, warnings, err := renderProviders(
			(*scope.rs),
			resourceset.StoreResolver(&(*scope.runState).run.Reads),
			func(content any) (bool, error) {
				fingerprintDedupResult1, fingerprintDedupResult1Err := (*scope.runState).run.FingerprintDedup(
					t.Context(),
					(*scope.id),
					content,
					&store.Emission{},
				)
				fp, hit, err := fingerprintDedupResult1.Fingerprint, fingerprintDedupResult1.Reused, fingerprintDedupResult1Err
				if hit || err != nil {
					return hit, err
				}
				attempt.Fingerprint = fp
				close((*scope.started))
				(*scope.tasks).YieldSlot(func() { <-(*scope.resume) })
				return false, nil
			},
		)
		defer attempt.Finish()
		if err != nil {
			t.Error(err)
			return
		}
		{
			warningValues := warnings
			for warningIndex := range warningValues {
				warning := warningValues[warningIndex]
				(*scope.runState).run.Reads.AddWarning(warning)
			}
		}
		parsed := (&base.Execution{Controller: (*scope.c).Controller}).ParseChildren(
			(*scope.c).Store.GetObject((*scope.id)),
			testutil.DocumentOccurrences(testutil.DocumentMaps(docs)),
			manifest.ParseDocOptions{},
		)
		if failure := manifest.DocumentFailure(parsed.Failures); failure != nil {
			panic(failure)
		}
		children, retained := parsed.Children, testutil.DocumentMaps(parsed.Visible)
		attempt.Children, attempt.Manifests = children, testutil.DocumentOccurrences(retained)
		attempt.Complete = true
		(*scope.runState).run.Emit(t.Context(), (*scope.id), &attempt, nil)
	})
}
