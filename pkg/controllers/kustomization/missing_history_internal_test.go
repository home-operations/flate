package kustomization

import (
	"context"
	"errors"
	"slices"
	"sync/atomic"
	"testing"

	kustomizev1 "github.com/fluxcd/kustomize-controller/api/v1"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/internal/testutil/cachetest"
	"github.com/home-operations/flate/internal/testutil/controllertest"
	"github.com/home-operations/flate/pkg/controllers/base"
	"github.com/home-operations/flate/pkg/depwait"
	"github.com/home-operations/flate/pkg/kustomize"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/schedule"
	"github.com/home-operations/flate/pkg/selection"
	sourcecache "github.com/home-operations/flate/pkg/source/cache"
	"github.com/home-operations/flate/pkg/store"
)

type historyInputPromotion struct {
	store    *store.Store
	input    *manifest.ConfigMap
	indexed  []manifest.NamedResource
	promoted []manifest.NamedResource
}

func (p *historyInputPromotion) IsFileIndexed(id manifest.NamedResource) bool {
	p.indexed = append(p.indexed, id)
	return id == p.input.Named()
}

//nolint:gocritic // The admission contract requires a request value.
func (p *historyInputPromotion) Promote(
	_ context.Context,
	_ selection.PrepareRequest,
	id manifest.NamedResource,
) (bool, error) {
	p.promoted = append(p.promoted, id)
	if id != p.input.Named() {
		return false, nil
	}
	p.store.AddObject(p.input)
	return true, nil
}

func TestPreviousRender_IndexedInputRequiresNormalRender(t *testing.T) {
	t.Parallel()
	fixture, st, root := newControllerWithFixture(t)
	const document = "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: output, namespace: apps}\ndata: {value: '${VALUE:=fallback}'}\n"
	testutil.WriteFile(t, root, "apps/cm.yaml", document)
	input := &manifest.ConfigMap{Name: "input", Namespace: "flux-system", Data: map[string]any{"VALUE": "promoted"}}
	promotion := &historyInputPromotion{store: st, input: input}
	trees := kustomize.NewTreeCache(cachetest.New(t, t.TempDir(), sourcecache.New), nil, fixture.Tasks)
	t.Cleanup(trees.Close)
	c := New(st, fixture.Tasks, trees)
	c.Configure(Options{Existence: promotion}, sourcecache.RetryConfig{})
	c.StartLifecycle()
	owner := &manifest.Kustomization{
		Name: "apps", Namespace: "flux-system", Path: "./apps",
		PostBuild: &kustomizev1.PostBuild{SubstituteFrom: []manifest.SubstituteReference{
			{Kind: manifest.KindConfigMap, Name: input.Name, Optional: true},
		}},
	}
	freezeSourceForTest(t, st)
	warm, warmStore, _ := newControllerWithFixture(t)
	warm.Trees = c.Trees
	warmStore.SetArtifact(
		manifest.BootstrapSourceID,
		st.GetArtifact(manifest.BootstrapSourceID, store.ArtifactContent).(*store.SourceArtifact),
	)
	warmStore.AddObject(owner)
	warmInfo := controllertest.DispatchToFixpoint(t.Context(), t, controllertest.Dispatch{
		Tasks: warm.Tasks, Store: warmStore, ID: owner.Named(), ReconcileNode: warm.ReconcileNode,
	})
	assert.Equal(t, warmInfo.Status, store.StatusReady)
	warmHistory := warm.PreviousRender(t.Context(), owner, schedule.Result{})
	assert.Equal(t, warmHistory.Complete, true)
	assert.FatalIf(t, warmHistory.Assessment == nil, "missing warm assessment")
	warmHistory.Assessment.Close()
	st.AddObject(owner)
	st.SetIncluded(owner.Named(), false, false)
	objects, warnings := st.Objects(), st.Warnings()
	status, statusExists := st.GetStatus(owner.Named())
	var events atomic.Int32
	for _, kind := range []store.EventKind{store.EventObjectAdded, store.EventStatusUpdated, store.EventArtifactUpdated} {
		t.Cleanup(st.AddListener(kind, func(manifest.NamedResource, any) { events.Add(1) }, false))
	}
	history := c.PreviousRender(t.Context(), owner, schedule.Result{})
	assert.FatalIf(t, history.Assessment == nil, "missing probe assessment")
	assert.Equal(t, history.Complete, false)
	assert.Equal(t, history.Records == nil, true)
	assert.Diff(t, promotion.indexed, []manifest.NamedResource{input.Named()})
	assert.Equal(t, len(promotion.promoted), 0)
	assert.Equal(t, events.Load(), int32(0))
	assert.Equal(t, slices.Equal(st.Objects(), objects), true)
	assert.Diff(t, st.Warnings(), warnings)
	afterStatus, afterExists := st.GetStatus(owner.Named())
	assert.Equal(t, afterStatus, status)
	assert.Equal(t, afterExists, statusExists)
	assert.Equal(t, history.Assessment.Reads.Admitted(), false)
	assert.Equal(t, history.Assessment.Reads.Valid(), true)
	assert.Equal(t, st.GetObject(input.Named()) == nil, true)
	assert.Equal(t, st.GetArtifact(owner.Named(), store.ArtifactContent) == nil, true)
	assert.Equal(t, c.Tasks.Failures(), int64(0))

	history.Assessment.Close()
	st.SetIncluded(owner.Named(), true, true)
	info := controllertest.DispatchToFixpoint(t.Context(), t, controllertest.Dispatch{
		Tasks: c.Tasks, Store: st, ID: owner.Named(), ReconcileNode: c.ReconcileNode,
	})
	assert.Equal(t, info.Status, store.StatusReady)
	assert.Equal(t, slices.Contains(promotion.promoted, input.Named()), true)
	actual := st.GetArtifact(owner.Named(), store.ArtifactContent).(*store.Emission)
	actualBytes, err := manifest.EncodeRender(actual.Manifests)
	assert.NoError(t, err)
	assert.Equal(t, actual.Manifests[0].Document["data"].(map[string]any)["value"], any("promoted"))

	full, fullStore, fullRoot := newControllerWithFixture(t)
	testutil.WriteFile(t, fullRoot, "apps/cm.yaml", document)
	fullStore.AddObject(owner)
	fullStore.AddObject(input)
	fullInfo := controllertest.DispatchToFixpoint(t.Context(), t, controllertest.Dispatch{
		Tasks: full.Tasks, Store: fullStore, ID: owner.Named(), ReconcileNode: full.ReconcileNode,
	})
	assert.Equal(t, fullInfo.Status, store.StatusReady)
	fullOutput := fullStore.GetArtifact(owner.Named(), store.ArtifactContent).(*store.Emission)
	fullBytes, err := manifest.EncodeRender(fullOutput.Manifests)
	assert.NoError(t, err)
	assert.Equal(t, string(actualBytes), string(fullBytes))
}

func TestPreviousRender_UnreadableSecretMatchesNormalCapture(t *testing.T) {
	t.Parallel()
	for _, present := range []bool{false, true} {
		name := "absent"
		if present {
			name = "empty"
		}
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			c, st, _ := newControllerWithFixture(t)
			owner := &manifest.Kustomization{
				Name: "apps", Namespace: "flux-system", Path: "./apps",
				PostBuild: &kustomizev1.PostBuild{SubstituteFrom: []manifest.SubstituteReference{
					{Kind: manifest.KindSecret, Name: "unreadable"},
				}},
			}
			input := &manifest.Secret{Name: "unreadable", Namespace: owner.Namespace}
			st.AddObject(owner)
			if present {
				st.AddObject(input)
			}
			history := c.PreviousRender(t.Context(), owner, schedule.Result{})
			assert.FatalIf(t, history.Assessment == nil, "missing probe assessment")
			defer history.Assessment.Close()
			normal := c.newExecution(owner.Named())
			defer normal.run.Close()
			_, err := normal.run.DataInputs(t.Context(), owner, normal.data)
			assert.NoError(t, err)
			assert.Diff(t, history.Assessment.Unreadable, []manifest.NamedResource{input.Named()})
			assert.Diff(t, history.Assessment.Unreadable, normal.run.Unreadable)
			assert.Equal(t, len(st.Warnings()), 0)
			assert.Equal(t, st.GetArtifact(owner.Named(), store.ArtifactContent) == nil, true)
		})
	}
}

func TestPreviousRender_DataAssessmentHasNoRuntimeEffects(t *testing.T) {
	t.Parallel()
	for _, inherited := range []bool{false, true} {
		name := "fresh"
		if inherited {
			name = "inherited"
		}
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			fixture, st, _ := newControllerWithFixture(t)
			promotion := &historyInputPromotion{
				store: st,
				input: &manifest.ConfigMap{Name: "not-indexed", Namespace: "flux-system"},
			}
			c := New(st, fixture.Tasks, fixture.Trees)
			c.Configure(Options{Existence: promotion}, sourcecache.RetryConfig{})
			c.StartLifecycle()
			owner := &manifest.Kustomization{
				Name:      "apps",
				Namespace: "flux-system",
				Path:      "./apps",
				PostBuild: &kustomizev1.PostBuild{
					SubstituteFrom: []manifest.SubstituteReference{{Kind: manifest.KindSecret, Name: "input"}},
				},
			}
			producer := (&manifest.Kustomization{Name: "producer", Namespace: owner.Namespace}).Named()
			input := manifest.NamedResource{Kind: manifest.KindSecret, Name: "input", Namespace: owner.Namespace}
			st.AddObject(owner)
			st.SetDiscoveryIndex(&manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{
				input: {Productions: []manifest.Production{{Owner: producer, Declaration: producer}}},
			}})
			st.SetIncluded(owner.Named(), false, false)
			prior := c.newExecution(owner.Named())
			defer prior.run.Close()
			var previous schedule.Result
			if inherited {
				prior.run.AdmissionPreparation = &selection.Preparation{}
				previous.Assessment = &prior.run
			}
			preparation := prior.run.AdmissionPreparation
			objects, warnings := st.Objects(), st.Warnings()
			status, known := st.GetStatus(owner.Named())
			var events atomic.Int32
			for _, event := range []store.EventKind{store.EventObjectAdded, store.EventStatusUpdated, store.EventArtifactUpdated} {
				t.Cleanup(st.AddListener(event, func(manifest.NamedResource, any) { events.Add(1) }, false))
			}
			history := c.PreviousRender(t.Context(), owner, previous)
			assert.FatalIf(t, history.Assessment == nil, "missing assessment")
			defer history.Assessment.Close()
			assert.Equal(t, history.Complete, false)
			assert.Equal(t, history.Assessment.Reads.Valid(), true)
			assert.Equal(t, history.Assessment.Reads.Admitted(), false)
			assert.Equal(t, history.Assessment.AdmissionPreparation, preparation)
			assert.Equal(t, prior.run.AdmissionPreparation == nil, true)
			assert.Equal(t, len(promotion.promoted), 0)
			assert.Equal(t, events.Load(), int32(0))
			assert.Equal(t, slices.Equal(st.Objects(), objects), true)
			assert.Diff(t, st.Warnings(), warnings)
			after, afterKnown := st.GetStatus(owner.Named())
			assert.Equal(t, after, status)
			assert.Equal(t, afterKnown, known)
			assert.Equal(t, st.GetDeclaration(owner.Named()), manifest.BaseManifest(owner))
			assert.Equal(t, st.GetDeclaration(input) == nil, true)
			assert.Equal(t, st.GetDeclaration(producer) == nil, true)
			assert.Equal(t, st.GetArtifact(owner.Named(), store.ArtifactContent) == nil, true)
			assert.Equal(t, c.Tasks.Failures(), int64(0))
			normal := c.newExecution(owner.Named())
			defer normal.run.Close()
			inputs, inputErr := normal.run.DataInputs(t.Context(), owner, normal.data)
			assert.NoError(t, inputErr)
			resumed := false
			waitErr := normal.run.AwaitData(t.Context(), base.DataWait{
				Inputs: inputs,
				Resume: func(context.Context, manifest.DrainLevel, []manifest.NamedResource) error {
					resumed = true
					return nil
				},
			}, manifest.DrainNone)
			blocked, isBlocked := errors.AsType[*depwait.ErrBlocked](waitErr)
			assert.Equal(t, isBlocked, true)
			assert.Diff(t, blocked.Deps, []manifest.NamedResource{producer})
			assert.Equal(t, resumed, false)
		})
	}
}

func TestPreviousRender_CapturedSourceReplacementInvalidatesHistory(t *testing.T) {
	t.Parallel()
	fixture, st, root := newControllerWithFixture(t)
	trees := kustomize.NewTreeCache(cachetest.New(t, t.TempDir(), sourcecache.New), nil, fixture.Tasks)
	t.Cleanup(trees.Close)
	c := New(st, fixture.Tasks, trees)
	c.Configure(Options{}, sourcecache.RetryConfig{})
	c.StartLifecycle()
	testutil.WriteFile(
		t,
		root,
		"apps/cm.yaml",
		"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: output}\ndata: {value: '${VALUE}'}\n",
	)
	freezeSourceForTest(t, st)
	owner := &manifest.Kustomization{
		Name:      "apps",
		Namespace: "flux-system",
		Path:      "./apps",
		PostBuild: &kustomizev1.PostBuild{
			SubstituteFrom: []manifest.SubstituteReference{{Kind: manifest.KindConfigMap, Name: "input"}},
		},
	}
	input := &manifest.ConfigMap{Name: "input", Namespace: owner.Namespace, Data: map[string]any{"VALUE": "stable"}}
	producer := &manifest.Kustomization{Name: "producer", Namespace: owner.Namespace}
	source := (&manifest.GitRepository{Name: "inputs", Namespace: owner.Namespace}).Named()
	st.AddObjects([]manifest.BaseManifest{owner, input, producer})
	st.AddDeclaration(store.Declaration{Obj: input, Origin: manifest.SuppliedDeclaration})
	st.SetArtifact(source, &store.SourceArtifact{Content: "before"})
	st.SetDiscoveryIndex(&manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{
		input.Named(): {
			Productions: []manifest.Production{
				{Owner: producer.Named(), Declaration: producer.Named(), Source: source},
			},
		},
	}})
	info := controllertest.DispatchToFixpoint(
		t.Context(),
		t,
		controllertest.Dispatch{Tasks: c.Tasks, Store: st, ID: owner.Named(), ReconcileNode: c.ReconcileNode},
	)
	assert.Equal(t, info.Status, store.StatusReady)
	history := c.PreviousRender(t.Context(), owner, schedule.Result{})
	assert.FatalIf(t, history.Assessment == nil, "missing assessment")
	defer history.Assessment.Close()
	assert.Equal(t, history.Complete, true)
	assert.Equal(t, history.Assessment.Reads.Valid(), true)
	beforeInput, beforeProducer := st.GetObject(input.Named()), st.GetDeclaration(producer.Named())
	replacement := &store.SourceArtifact{Content: "after"}
	st.SetArtifact(source, replacement)
	assert.Equal(t, history.Assessment.Reads.Valid(), false)
	assert.Equal(t, st.GetObject(input.Named()), beforeInput)
	assert.Equal(t, st.GetDeclaration(producer.Named()), beforeProducer)
	fresh := c.PreviousRender(t.Context(), owner, schedule.Result{})
	assert.FatalIf(t, fresh.Assessment == nil, "missing fresh assessment")
	defer fresh.Assessment.Close()
	assert.Equal(t, fresh.Complete, true)
	assert.Equal(t, fresh.Assessment.Reads.Valid(), true)
	normal := c.newExecution(owner.Named())
	defer normal.run.Close()
	inputs, err := normal.run.DataInputs(t.Context(), owner, normal.data)
	assert.NoError(t, err)
	resumed := false
	assert.NoError(
		t,
		normal.run.AwaitData(
			t.Context(),
			base.DataWait{
				Inputs: inputs,
				Resume: func(context.Context, manifest.DrainLevel, []manifest.NamedResource) error { resumed = true; return nil },
			},
			manifest.DrainNone,
		),
	)
	assert.Equal(t, resumed, true)
	assert.Diff(
		t,
		fresh.Assessment.Reads.ReferenceFacts(input.Named(), nil, owner.Named()),
		normal.data.Lookup(input.Named()),
	)
}
