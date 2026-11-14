package kustomization

import (
	"context"
	"errors"
	"fmt"
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
	"github.com/home-operations/flate/pkg/loader"
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
	persistent := sourcecache.RenderPersistenceDisabledReason() == ""
	for _, tc := range []struct {
		name     string
		optional bool
		allowed  []string
	}{
		{name: "optional", optional: true},
		{name: "required_permitted", allowed: []string{manifest.KindConfigMap}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			fixture, st, root := newControllerWithFixture(t)
			const document = "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: output, namespace: apps}\ndata: {value: '${VALUE:=fallback}'}\n"
			testutil.WriteFile(t, root, "apps/cm.yaml", document)
			input := &manifest.ConfigMap{
				Name:      "input",
				Namespace: "flux-system",
				Data:      map[string]any{"VALUE": "promoted"},
			}
			promotion := &historyInputPromotion{store: st, input: input}
			trees := kustomize.NewTreeCache(cachetest.New(t, t.TempDir(), sourcecache.New), nil, fixture.Tasks)
			t.Cleanup(trees.Close)
			c := New(st, fixture.Tasks, trees)
			c.Configure(
				Options{Existence: promotion, Missing: depwait.NewMissingPolicy(tc.allowed)},
				sourcecache.RetryConfig{},
			)
			c.StartLifecycle()
			owner := &manifest.Kustomization{
				Name: "apps", Namespace: "flux-system", Path: "./apps",
				PostBuild: &kustomizev1.PostBuild{SubstituteFrom: []manifest.SubstituteReference{
					{Kind: manifest.KindConfigMap, Name: input.Name, Optional: tc.optional},
				}},
			}
			freezeSourceForTest(t, st)
			warmFixture, warmStore, _ := newControllerWithFixture(t)
			warm := New(warmStore, warmFixture.Tasks, c.Trees)
			warm.Configure(Options{Missing: depwait.NewMissingPolicy(tc.allowed)}, sourcecache.RetryConfig{})
			warm.StartLifecycle()
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
			assert.Equal(t, warmHistory.Complete, persistent)
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

			fullFixture, fullStore, fullRoot := newControllerWithFixture(t)
			full := New(fullStore, fullFixture.Tasks, fullFixture.Trees)
			full.Configure(Options{Missing: depwait.NewMissingPolicy(tc.allowed)}, sourcecache.RetryConfig{})
			full.StartLifecycle()
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
		})
	}
}

type unreadableSecretCase struct {
	name     string
	kind     string
	object   manifest.BaseManifest
	mixed    bool
	optional bool
	allowed  []string
	value    string
	warnings int
}

func TestPreviousRender_UnreadableSecretMatchesNormalCapture(t *testing.T) {
	t.Parallel()
	persistent := sourcecache.RenderPersistenceDisabledReason() == ""
	for _, tc := range []unreadableSecretCase{
		{name: "absent", kind: manifest.KindSecret},
		{
			name: "empty", kind: manifest.KindSecret,
			object: &manifest.Secret{Name: "unreadable", Namespace: "flux-system"},
		},
		{
			name: "permitted_absent_secret", kind: manifest.KindSecret,
			allowed: []string{manifest.KindSecret}, value: "fallback",
		},
		{
			name: "permitted_absent_configmap", kind: manifest.KindConfigMap,
			allowed: []string{manifest.KindConfigMap}, value: "fallback",
		},
		{
			name: "permitted_mixed_secret", kind: manifest.KindSecret,
			object: &manifest.Secret{
				Name: "unreadable", Namespace: "flux-system",
				StringData: map[string]any{
					"PLAIN": "readable", "VALUE": "ENC[AES256_GCM,data:abc,iv:def,tag:ghi,type:str]",
				},
			},
			mixed: true, allowed: []string{manifest.KindSecret}, value: "readable", warnings: 1,
		},
		{
			name: "permitted_mixed_configmap", kind: manifest.KindConfigMap,
			object: &manifest.ConfigMap{
				Name: "unreadable", Namespace: "flux-system",
				Data: map[string]any{
					"PLAIN": "readable", "VALUE": "ENC[AES256_GCM,data:abc,iv:def,tag:ghi,type:str]",
				},
			},
			mixed: true, allowed: []string{manifest.KindConfigMap}, value: "readable", warnings: 1,
		},
		{
			name: "required_mixed_secret", kind: manifest.KindSecret,
			object: &manifest.Secret{
				Name: "unreadable", Namespace: "flux-system",
				StringData: map[string]any{
					"PLAIN": "readable", "VALUE": "ENC[AES256_GCM,data:abc,iv:def,tag:ghi,type:str]",
					"OTHER": "ENC[AES256_GCM,data:abc,iv:def,tag:ghi,type:str]",
				},
			},
			mixed: true, value: "readable", warnings: 1,
		},
		{
			name: "required_mixed_configmap", kind: manifest.KindConfigMap,
			object: &manifest.ConfigMap{
				Name: "unreadable", Namespace: "flux-system",
				Data: map[string]any{
					"PLAIN": "readable", "VALUE": "ENC[AES256_GCM,data:abc,iv:def,tag:ghi,type:str]",
					"OTHER": "ENC[AES256_GCM,data:abc,iv:def,tag:ghi,type:str]",
				},
			},
			mixed: true, value: "readable", warnings: 1,
		},
		{
			name: "optional_mixed_secret", kind: manifest.KindSecret,
			object: &manifest.Secret{
				Name: "unreadable", Namespace: "flux-system",
				StringData: map[string]any{
					"PLAIN": "readable", "VALUE": "ENC[AES256_GCM,data:abc,iv:def,tag:ghi,type:str]",
				},
			},
			mixed: true, optional: true, value: "readable", warnings: 1,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			fixture, st, root := newControllerWithFixture(t)
			trees := kustomize.NewTreeCache(cachetest.New(t, t.TempDir(), sourcecache.New), nil, fixture.Tasks)
			t.Cleanup(trees.Close)
			c := New(st, fixture.Tasks, trees)
			c.Configure(Options{Missing: depwait.NewMissingPolicy(tc.allowed)}, sourcecache.RetryConfig{})
			c.StartLifecycle()
			testutil.WriteFile(
				t,
				root,
				"apps/cm.yaml",
				"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: output}\ndata: {value: '${PLAIN:=fallback}', missing: '${VALUE:=default}'}\n",
			)
			freezeSourceForTest(t, st)
			owner := &manifest.Kustomization{
				Name: "apps", Namespace: "flux-system", Path: "./apps",
				PostBuild: &kustomizev1.PostBuild{SubstituteFrom: []manifest.SubstituteReference{
					{Kind: tc.kind, Name: "unreadable", Optional: tc.optional},
				}, Substitute: map[string]string{"ACTIVE": "yes"}},
			}
			input := manifest.NamedResource{Kind: tc.kind, Name: "unreadable", Namespace: owner.Namespace}
			st.AddObject(owner)
			if tc.object != nil {
				child, outcome, _ := loader.Prepare(
					manifest.DocOccurrence{Document: testutil.ObjectDocument(tc.object)},
					manifest.ParseDocOptions{RequiredIdentity: true},
				)
				assert.NoError(t, outcome.Err)
				st.AddDeclaration(store.Declaration{
					Obj: child.Obj, Doc: child.Doc, Origin: manifest.RepositoryDeclaration, Warnings: child.Warnings,
				})
			}
			beforeWarnings := st.Warnings()
			history := c.PreviousRender(t.Context(), owner, schedule.Result{})
			assert.FatalIf(t, history.Assessment == nil, "missing probe assessment")
			normal := c.newExecution(owner.Named())
			defer normal.run.Close()
			_, err := normal.run.DataInputs(t.Context(), owner, normal.data)
			assert.NoError(t, err)
			var unreadable []manifest.NamedResource
			if !tc.optional && (tc.object == nil || tc.mixed && len(tc.allowed) != 0) {
				unreadable = []manifest.NamedResource{input}
			}
			assert.Diff(t, history.Assessment.Unreadable, unreadable)
			assert.Diff(t, history.Assessment.Unreadable, normal.run.Unreadable)
			assert.Diff(t, st.Warnings(), beforeWarnings)
			assert.Equal(t, len(st.Warnings()), tc.warnings)
			assert.Equal(t, st.GetArtifact(owner.Named(), store.ArtifactContent) == nil, true)
			history.Assessment.Close()
			if len(tc.allowed) == 0 && !tc.mixed {
				return
			}
			for _, regime := range []string{"cold", "reused"} {
				info := controllertest.DispatchToFixpoint(t.Context(), t, controllertest.Dispatch{
					Tasks: c.Tasks, Store: st, ID: owner.Named(), ReconcileNode: c.ReconcileNode,
				})
				assert.Equal(t, info.Status, store.StatusReady)
				output := st.GetArtifact(owner.Named(), store.ArtifactContent).(*store.Emission)
				assert.Diff(
					t,
					output.Manifests[0].Document["data"],
					map[string]any{"value": tc.value, "missing": "default"},
				)
				assert.Diff(t, output.Unreadable, unreadable)
				tc.assertWarnings(t, st.Warnings(), beforeWarnings, input)
				replay := c.PreviousRender(t.Context(), owner, schedule.Result{})
				assert.FatalIf(t, replay.Assessment == nil, "missing %s assessment", regime)
				assert.Equal(t, replay.Complete, persistent)
				assert.Diff(t, replay.Assessment.Unreadable, normal.run.Unreadable)
				assert.Diff(
					t,
					replay.Assessment.Reads.ReferenceFacts(input, nil, owner.Named()),
					normal.data.Lookup(input),
				)
				replay.Assessment.Close()
				t.Logf("%s history: complete=%t, output and warning assertions evaluated", regime, replay.Complete)
			}
		})
	}
}

func (tc *unreadableSecretCase) assertWarnings(
	t *testing.T, warnings, beforeWarnings []manifest.Warning, input manifest.NamedResource,
) {
	t.Helper()
	if tc.optional {
		assert.Diff(t, warnings, beforeWarnings)
	} else {
		assert.Equal(t, len(warnings), len(beforeWarnings)+1)
		unresolved := warnings[len(warnings)-1]
		assert.Equal(t, unresolved.Category, manifest.WarnUnresolvedSubstitution)
		assert.Equal(t, unresolved.Count, 1)
		if tc.mixed {
			key := "VALUE"
			var detail []string
			if len(tc.allowed) == 0 {
				key, detail = "OTHER", []string{"VALUE"}
			}
			assert.Equal(t, unresolved.Message, fmt.Sprintf(
				`substituteFrom %s "flux-system/unreadable" could not be read offline; its variables are unavailable, so their ${VAR} references may remain unsubstituted or resolve to defaults or empty values: key %q is SOPS-encrypted and unreadable; supply it with --supply`,
				tc.kind,
				key,
			))
			assert.Diff(t, unresolved.Detail, detail)
			assert.Diff(t, unresolved.Related, []manifest.NamedResource{input})
		}
	}
	if tc.mixed {
		assert.Diff(t, warnings[0], beforeWarnings[0])
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
				Resume: func(context.Context, manifest.DrainLevel, []depwait.DataDependency) error {
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
	persistent := sourcecache.RenderPersistenceDisabledReason() == ""
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
	assert.Equal(t, history.Complete, persistent)
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
	assert.Equal(t, fresh.Complete, persistent)
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
				Resume: func(context.Context, manifest.DrainLevel, []depwait.DataDependency) error { resumed = true; return nil },
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
