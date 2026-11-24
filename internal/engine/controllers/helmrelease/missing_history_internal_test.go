package helmrelease

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"slices"
	"sync/atomic"
	"testing"
	"testing/fstest"

	helmv2 "github.com/fluxcd/helm-controller/api/v2"

	"github.com/home-operations/flate/internal/engine/controllers/base"
	"github.com/home-operations/flate/internal/engine/depwait"
	"github.com/home-operations/flate/internal/engine/helm"
	"github.com/home-operations/flate/internal/engine/loader"
	"github.com/home-operations/flate/internal/engine/schedule"
	"github.com/home-operations/flate/internal/engine/selection"
	"github.com/home-operations/flate/internal/engine/source"
	sourcecache "github.com/home-operations/flate/internal/engine/source/cache"
	"github.com/home-operations/flate/internal/engine/source/helmchart"
	"github.com/home-operations/flate/internal/engine/store"
	"github.com/home-operations/flate/internal/engine/values"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/internal/testutil/assert"
	"github.com/home-operations/flate/internal/testutil/cachetest"
	"github.com/home-operations/flate/pkg/manifest"
)

type historyInputPromotion struct {
	store    *store.Store
	input    *manifest.ConfigMap
	indexed  []manifest.NamedResource
	promoted []manifest.NamedResource
}

func TestPreviousRender_ChartDependencyHasNoRuntimeEffects(t *testing.T) {
	t.Parallel()
	for _, state := range []string{
		"indexed_repository", "missing_secret", "remote_content", "rotated_secret", "unavailable_source", "unreadable_source",
	} {
		t.Run(state, func(t *testing.T) {
			t.Parallel()
			var requests atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				requests.Add(1)
				w.WriteHeader(http.StatusInternalServerError)
			}))
			t.Cleanup(server.Close)
			repository := &manifest.HelmRepository{Name: "declared", Namespace: "charts"}
			repository.URL = server.URL
			repository.SecretRef = &manifest.LocalObjectReference{Name: "auth"}
			index := &manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{
				repository.Named(): {Declaration: repository},
			}}
			promotion := &historyInputPromotion{input: &manifest.ConfigMap{Name: "unused", Namespace: "apps"}}
			c, st := newTestControllerWithOptions(t, &base.Options{Index: index, Existence: promotion})
			promotion.store = st
			c.index = index
			c.Cache = cachetest.New(t, t.TempDir(), sourcecache.New)
			network := new(source.Network)
			t.Cleanup(network.Close)
			c.Helm = helm.NewClient(
				c.Cache,
				helmchart.New(nil, helmchart.Config{Cache: c.Cache, Network: network, Concurrency: 2}),
			)
			chartSource := &manifest.GitRepository{Name: "input", Namespace: "charts"}
			st.AddObject(chartSource)
			artifact := &store.SourceArtifact{Kind: manifest.KindGitRepository, FS: fstest.MapFS{
				"parent/Chart.yaml": {
					Data: []byte(
						"apiVersion: v2\nname: parent\nversion: 1.0.0\ndependencies:\n- name: child\n  version: 1.0.0\n  repository: " + server.URL + "\n",
					),
				},
			}}
			if state == "unreadable_source" {
				artifact.FS = nil
			}
			if state != "unavailable_source" {
				st.SetArtifact(chartSource.Named(), artifact)
			}
			st.UpdateStatus(chartSource.Named(), store.StatusInfo{Status: store.StatusReady})
			owner := &manifest.HelmRelease{
				Name:      "consumer",
				Namespace: "apps",
				Chart:     manifest.HelmChart{Name: "parent", Source: chartSource.Named()},
				HelmReleaseSpec: helmv2.HelmReleaseSpec{
					Chart: &helmv2.HelmChartTemplate{Spec: helmv2.HelmChartTemplateSpec{
						Chart: "parent",
						SourceRef: &helmv2.CrossNamespaceObjectReference{
							Kind:      manifest.KindGitRepository,
							Name:      chartSource.Name,
							Namespace: chartSource.Namespace,
						},
					}},
				},
			}
			st.AddObject(owner)
			st.SetIncluded(owner.Named(), false, false, false)
			if state != "indexed_repository" {
				st.AddObject(repository)
			}
			secret := &manifest.Secret{
				Name:       "auth",
				Namespace:  "charts",
				StringData: map[string]any{"username": "user", "password": "password"},
			}
			if state == "remote_content" || state == "rotated_secret" {
				st.AddObject(secret)
			}
			objects, warnings := st.Objects(), st.Warnings()
			status, known := st.GetStatus(owner.Named())
			var events atomic.Int32
			for _, event := range []store.EventKind{store.EventObjectAdded, store.EventStatusUpdated, store.EventArtifactUpdated} {
				t.Cleanup(st.AddListener(event, func(manifest.NamedResource, any) { events.Add(1) }, false))
			}
			history := c.PreviousRender(t.Context(), owner, schedule.Result{})
			assert.FatalIf(t, history.Assessment == nil, "missing dependency assessment")
			defer history.Assessment.Close()
			assert.Equal(t, history.Complete, false)
			assert.Equal(t, history.Assessment.Reads.Admitted(), false)
			assert.Equal(t, requests.Load(), int32(0))
			assert.Equal(t, events.Load(), int32(0))
			assert.Equal(t, len(promotion.promoted), 0)
			assert.Equal(t, slices.Equal(st.Objects(), objects), true)
			assert.Diff(t, st.Warnings(), warnings)
			after, afterKnown := st.GetStatus(owner.Named())
			assert.Equal(t, after, status)
			assert.Equal(t, afterKnown, known)
			assert.Equal(t, st.GetArtifact(owner.Named(), store.ArtifactContent) == nil, true)
			assert.Equal(t, c.Tasks.Failures(), int64(0))

			if !c.Cache.CanPersistRenders() {
				return
			}
			inputs := chartDependencyHistoryInputs{store: st, repository: repository, secret: secret}
			inputs.assertReads(t, &history.Assessment.Reads, state)
		})
	}
}

type chartDependencyHistoryInputs struct {
	store      *store.Store
	repository *manifest.HelmRepository
	secret     *manifest.Secret
}

func (in chartDependencyHistoryInputs) assertReads(t *testing.T, reads *store.ReadSet, state string) {
	t.Helper()
	captured := slices.Collect(reads.Inputs())
	sourceReadable := state != "unavailable_source" && state != "unreadable_source"
	assert.Equal(t, slices.Contains(captured, in.repository.Named()), sourceReadable)
	assert.Equal(t, slices.Contains(captured, in.secret.Named()), sourceReadable && state != "indexed_repository")
	assert.Equal(t, reads.Valid(), true)
	switch state {
	case "indexed_repository":
		in.store.AddObject(in.repository)
	case "remote_content":
		replacement := *in.repository
		replacement.URL += "/changed"
		in.store.AddObject(&replacement)
	case "missing_secret":
		in.store.AddObject(in.secret)
	case "rotated_secret":
		replacement := *in.secret
		replacement.StringData = map[string]any{"username": "user", "password": "rotated"}
		in.store.AddObject(&replacement)
	default:
		return
	}
	assert.Equal(t, reads.Valid(), false)
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
	chartSourceObject := &manifest.GitRepository{Name: "charts", Namespace: "apps"}
	st.AddObject(chartSourceObject)
	st.SetArtifact(chartSourceObject.Named(), &store.SourceArtifact{
		Kind: manifest.KindGitRepository, FS: os.DirFS(root), LocalPath: root,
	})
	st.UpdateStatus(chartSourceObject.Named(), store.StatusInfo{Status: store.StatusReady})
	owner := &manifest.HelmRelease{
		Name:      "consumer",
		Namespace: "apps",
		Chart:     manifest.HelmChart{Name: "chart", Source: chartSourceObject.Named()},
		HelmReleaseSpec: helmv2.HelmReleaseSpec{
			Chart: &helmv2.HelmChartTemplate{Spec: helmv2.HelmChartTemplateSpec{
				Chart: "chart", SourceRef: &helmv2.CrossNamespaceObjectReference{
					Kind:       manifest.KindGitRepository,
					Name:       chartSourceObject.Name,
					Namespace:  chartSourceObject.Namespace,
					APIVersion: manifest.SourceDomain + "/v1",
				},
			}},
			ValuesFrom: []manifest.ValuesReference{{Kind: manifest.KindConfigMap, Name: input.Name, Optional: true}},
		},
	}
	c.Cache = cachetest.New(t, t.TempDir(), sourcecache.New)
	c.Helm = helm.NewClient(c.Cache, nil)
	warm, warmStore := newTestController(t, nil)
	warm.Cache = c.Cache
	warm.Helm = c.Helm
	warmStore.AddObject(chartSourceObject)
	warmStore.SetArtifact(
		chartSourceObject.Named(),
		st.GetArtifact(chartSourceObject.Named(), store.ArtifactContent).(*store.SourceArtifact),
	)
	warmStore.UpdateStatus(chartSourceObject.Named(), store.StatusInfo{Status: store.StatusReady})
	warmStore.AddObject(owner)
	assert.Equal(t, reconcileFixture(t, warm, owner.Named()).Status, store.StatusReady)
	warmHistory := warm.PreviousRender(t.Context(), owner, schedule.Result{})
	assert.Equal(t, warmHistory.Complete, persistent)
	assert.FatalIf(t, warmHistory.Assessment == nil, "missing warm assessment")
	warmHistory.Assessment.Close()
	st.AddObject(owner)
	st.SetIncluded(owner.Named(), false, false, false)
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
	st.SetIncluded(owner.Named(), true, true, false)
	info := reconcileFixture(t, c, owner.Named())
	assert.Equal(t, info.Status, store.StatusReady)
	assert.Equal(t, slices.Contains(promotion.promoted, input.Named()), true)
	actual := st.GetArtifact(owner.Named(), store.ArtifactContent).(*store.Emission)
	actualBytes, err := manifest.EncodeRender(actual.Manifests)
	assert.NoError(t, err)
	assert.Equal(t, actual.Manifests[0].Document["data"].(map[string]any)["value"], any("promoted"))

	full, fullStore := newTestController(t, nil)
	fullStore.AddObject(chartSourceObject)
	fullStore.SetArtifact(chartSourceObject.Named(), &store.SourceArtifact{
		Kind: manifest.KindGitRepository, FS: os.DirFS(root), LocalPath: root,
	})
	fullStore.UpdateStatus(chartSourceObject.Named(), store.StatusInfo{Status: store.StatusReady})
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
			st.SetIncluded(owner.Named(), false, false, false)
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
			inputs, inputErr := normal.run.DataInputs(t.Context(), owner, normal.run.Data())
			assert.NoError(t, inputErr)
			resumed := false
			waitErr := normal.run.AwaitData(t.Context(), base.DataWait{
				Inputs: inputs,
				Resume: func(context.Context, manifest.DrainLevel, []values.Result) error {
					resumed = true
					return nil
				},
			}, manifest.DrainNone)
			blocked, isBlocked := errors.AsType[*depwait.BlockedError](waitErr)
			assert.Equal(t, isBlocked, true)
			assert.Diff(t, blocked.Deps, []manifest.NamedResource{producer})
			assert.Equal(t, resumed, false)
		})
	}
}

func TestPreviousRender_CapturedSourceReplacementInvalidatesHistory(t *testing.T) {
	t.Parallel()
	persistent := sourcecache.RenderPersistenceDisabledReason() == ""
	st := store.New()
	l := loader.New(st)
	t.Cleanup(l.Retire)
	session, err := selection.New(st, l, selection.Request{})
	assert.NoError(t, err)
	l.Options.Session = session
	c, _ := newTestController(t, l)
	var accounting manifest.WorkRecorder
	st.SetWorkTree(manifest.TreeID{}, accounting.Scope())
	c.Cache = cachetest.New(t, t.TempDir(), sourcecache.New)
	c.Helm = helm.NewClient(c.Cache, nil)
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
	chartSourceObject := (&manifest.GitRepository{Name: "inputs", Namespace: "apps"}).Named()
	for _, object := range []manifest.BaseManifest{owner, input, producer} {
		st.AddObject(object)
	}
	st.AddDeclaration(store.Declaration{Obj: input, Origin: manifest.SuppliedDeclaration})
	st.SetArtifact(chartSourceObject, &store.SourceArtifact{Content: "before"})
	st.SetDiscoveryIndex(&manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{
		input.Named(): {
			Productions: []manifest.Production{
				{Owner: producer.Named(), Declaration: producer.Named(), Source: chartSourceObject},
			},
		},
	}})
	assert.Equal(t, reconcileFixture(t, c, owner.Named()).Status, store.StatusReady)
	history := c.PreviousRender(t.Context(), owner, schedule.Result{})
	assert.FatalIf(t, history.Assessment == nil, "missing assessment")
	defer history.Assessment.Close()
	assert.Equal(t, history.Complete, persistent)
	assert.Equal(t, history.Assessment.Reads.Valid(), true)
	beforeInput, beforeProducer := st.GetObject(input.Named()), st.GetDeclaration(producer.Named())
	replacement := &store.SourceArtifact{Content: "after"}
	st.SetArtifact(chartSourceObject, replacement)
	assert.Equal(t, history.Assessment.Reads.Valid(), false)
	assert.Equal(t, st.GetObject(input.Named()), beforeInput)
	assert.Equal(t, st.GetDeclaration(producer.Named()), beforeProducer)
	fresh := c.PreviousRender(t.Context(), owner, schedule.Result{})
	assert.FatalIf(t, fresh.Assessment == nil, "missing fresh assessment")
	defer fresh.Assessment.Close()
	assert.Equal(t, fresh.Complete, persistent)
	assert.Equal(t, fresh.Assessment.Reads.Valid(), true)
	assert.Equal(t, fresh.Replay == nil, true)
	counts := fresh.Assessment.Reads.WorkScope().Record(manifest.WorkPreparation, "")
	before := counts.Counts.OwnerPrepares
	assert.Equal(t, before > 0, true)
	session.PublishEntry(&selection.EntryRoute{Replay: owner.Named()})
	replay := c.PreviousRender(t.Context(), owner, schedule.Result{Assessment: fresh.Assessment})
	assert.FatalIf(t, replay.Assessment == nil || replay.Replay == nil, "history did not establish replay")
	defer replay.Assessment.Close()
	assert.Equal(t, replay.Complete, persistent)
	assert.Equal(t, counts.Counts.OwnerPrepares, before)
	if persistent {
		execution := replay.Replay(t.Context(), manifest.DrainNone)
		assert.Equal(t, execution, replay.Assessment)
		assert.Equal(t, execution.Reads.Valid(), true)
		assert.Equal(t, counts.Counts.OwnerPrepares, before)
	}
	normal := c.newExecution(owner.Named())
	defer normal.run.Close()
	inputs, err := normal.run.DataInputs(t.Context(), owner, normal.run.Data())
	assert.NoError(t, err)
	resumed := false
	assert.NoError(
		t,
		normal.run.AwaitData(
			t.Context(),
			base.DataWait{
				Inputs: inputs,
				Resume: func(context.Context, manifest.DrainLevel, []values.Result) error { resumed = true; return nil },
			},
			manifest.DrainNone,
		),
	)
	assert.Equal(t, resumed, true)
	captured := replay.Assessment.Reads.ReferenceFacts(input.Named(), nil, owner.Named())
	expected := normal.run.Data().Lookup(input.Named())
	assert.Diff(t, captured.Data.Snapshot(), expected.Data.Snapshot())
	assert.Equal(t, captured.Data.KeysKnown(), expected.Data.KeysKnown())
	if captured.Original != nil && expected.Original != nil {
		assert.Diff(t, captured.Original.Snapshot(), expected.Original.Snapshot())
		assert.Equal(t, captured.Original.KeysKnown(), expected.Original.KeysKnown())
	} else {
		assert.Equal(t, captured.Original == nil, expected.Original == nil)
	}
	captured.Data, expected.Data, captured.Original, expected.Original = nil, nil, nil, nil
	assert.Diff(t, captured, expected)
}
