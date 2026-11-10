package helmrelease

import (
	"context"
	"errors"
	"os"
	"slices"
	"sync/atomic"
	"testing"

	helmv2 "github.com/fluxcd/helm-controller/api/v2"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/internal/testutil/cachetest"
	"github.com/home-operations/flate/pkg/controllers/base"
	"github.com/home-operations/flate/pkg/depwait"
	"github.com/home-operations/flate/pkg/helm"
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
	input := &manifest.ConfigMap{
		Name: "input", Namespace: "apps", Data: map[string]any{"values.yaml": "value: promoted\n"},
	}
	promotion := &historyInputPromotion{input: input}
	c, st := newTestControllerWithOptions(t, &base.Options{Existence: promotion})
	promotion.store = st
	root := t.TempDir()
	testutil.WriteFile(t, root, "chart/Chart.yaml", "apiVersion: v2\nname: fixture\nversion: 1.0.0\n")
	testutil.WriteFile(t, root, "chart/templates/cm.yaml", `apiVersion: v1
kind: ConfigMap
metadata: {name: output, namespace: apps}
data:
  value: {{ .Values.value | default "fallback" | quote }}
`)
	source := &manifest.GitRepository{Name: "charts", Namespace: "apps"}
	st.AddObject(source)
	st.SetArtifact(source.Named(), &store.SourceArtifact{
		Kind: manifest.KindGitRepository, FS: os.DirFS(root), LocalPath: root,
	})
	st.UpdateStatus(source.Named(), store.StatusInfo{Status: store.StatusReady})
	owner := &manifest.HelmRelease{
		Name: "consumer", Namespace: "apps", Chart: manifest.HelmChart{Name: "chart", Source: source.Named()},
		HelmReleaseSpec: helmv2.HelmReleaseSpec{
			Chart: &helmv2.HelmChartTemplate{Spec: helmv2.HelmChartTemplateSpec{
				Chart: "chart", SourceRef: &helmv2.CrossNamespaceObjectReference{
					Kind: manifest.KindGitRepository, Name: source.Name, Namespace: source.Namespace,
					APIVersion: manifest.SourceDomain + "/v1",
				},
			}},
			ValuesFrom: []manifest.ValuesReference{{Kind: manifest.KindConfigMap, Name: input.Name, Optional: true}},
		},
	}
	c.Helm = helm.NewClient(cachetest.New(t, t.TempDir(), sourcecache.New))
	warm, warmStore := newTestController(t, nil)
	warm.Helm = c.Helm
	warmStore.AddObject(source)
	warmStore.SetArtifact(source.Named(), st.GetArtifact(source.Named(), store.ArtifactContent).(*store.SourceArtifact))
	warmStore.UpdateStatus(source.Named(), store.StatusInfo{Status: store.StatusReady})
	warmStore.AddObject(owner)
	assert.Equal(t, reconcileFixture(t, warm, owner.Named()).Status, store.StatusReady)
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
	info := reconcileFixture(t, c, owner.Named())
	assert.Equal(t, info.Status, store.StatusReady)
	assert.Equal(t, slices.Contains(promotion.promoted, input.Named()), true)
	actual := st.GetArtifact(owner.Named(), store.ArtifactContent).(*store.Emission)
	actualBytes, err := manifest.EncodeRender(actual.Manifests)
	assert.NoError(t, err)
	assert.Equal(t, actual.Manifests[0].Document["data"].(map[string]any)["value"], any("promoted"))

	full, fullStore := newTestController(t, nil)
	fullStore.AddObject(source)
	fullStore.SetArtifact(source.Named(), &store.SourceArtifact{
		Kind: manifest.KindGitRepository, FS: os.DirFS(root), LocalPath: root,
	})
	fullStore.UpdateStatus(source.Named(), store.StatusInfo{Status: store.StatusReady})
	fullStore.AddObject(owner)
	fullStore.AddObject(input)
	fullInfo := reconcileFixture(t, full, owner.Named())
	assert.Equal(t, fullInfo.Status, store.StatusReady)
	fullOutput := fullStore.GetArtifact(owner.Named(), store.ArtifactContent).(*store.Emission)
	fullBytes, err := manifest.EncodeRender(fullOutput.Manifests)
	assert.NoError(t, err)
	assert.Equal(t, string(actualBytes), string(fullBytes))
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
			promotion := &historyInputPromotion{input: &manifest.ConfigMap{Name: "not-indexed", Namespace: "apps"}}
			c, st := newTestControllerWithOptions(t, &base.Options{
				Existence: promotion, Missing: depwait.NewMissingPolicy([]string{manifest.KindConfigMap}),
			})
			promotion.store = st
			owner := &manifest.HelmRelease{
				Name:       "consumer",
				Namespace:  "apps",
				ValuesFrom: []manifest.ValuesReference{{Kind: manifest.KindConfigMap, Name: "input"}},
			}
			producer := (&manifest.Kustomization{Name: "producer", Namespace: "apps"}).Named()
			input := manifest.NamedResource{Kind: manifest.KindConfigMap, Name: "input", Namespace: "apps"}
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
	c, st := newTestController(t, nil)
	c.Helm = helm.NewClient(cachetest.New(t, t.TempDir(), sourcecache.New))
	root := t.TempDir()
	testutil.WriteFile(t, root, "chart/Chart.yaml", "apiVersion: v2\nname: fixture\nversion: 1.0.0\n")
	testutil.WriteFile(
		t,
		root,
		"chart/templates/cm.yaml",
		"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: output}\ndata: {value: '{{ .Values.value }}'}\n",
	)
	charts := &manifest.GitRepository{Name: "charts", Namespace: "apps"}
	st.AddObject(charts)
	st.SetArtifact(charts.Named(), &store.SourceArtifact{FS: os.DirFS(root), LocalPath: root, Content: "chart-tree"})
	st.UpdateStatus(charts.Named(), store.StatusInfo{Status: store.StatusReady})
	owner := &manifest.HelmRelease{
		Name: "consumer", Namespace: "apps", Chart: manifest.HelmChart{Name: "chart", Source: charts.Named()},
		HelmReleaseSpec: helmv2.HelmReleaseSpec{
			Chart: &helmv2.HelmChartTemplate{
				Spec: helmv2.HelmChartTemplateSpec{
					Chart: "chart",
					SourceRef: &helmv2.CrossNamespaceObjectReference{
						Kind: manifest.KindGitRepository,
						Name: charts.Name,
					},
				},
			},
			ValuesFrom: []manifest.ValuesReference{{Kind: manifest.KindConfigMap, Name: "input"}},
		},
	}
	input := &manifest.ConfigMap{
		Name:      "input",
		Namespace: "apps",
		Data:      map[string]any{"values.yaml": "value: stable\n"},
	}
	producer := &manifest.Kustomization{Name: "producer", Namespace: "apps"}
	source := (&manifest.GitRepository{Name: "inputs", Namespace: "apps"}).Named()
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
	assert.Equal(t, reconcileFixture(t, c, owner.Named()).Status, store.StatusReady)
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
