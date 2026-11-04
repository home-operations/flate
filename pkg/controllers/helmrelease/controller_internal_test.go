package helmrelease

import (
	"context"
	"errors"
	"fmt"
	"os"
	"slices"
	"strings"
	"sync"
	"testing"
	"testing/fstest"
	"testing/synctest"
	"time"

	"github.com/home-operations/flate/internal/filtertest"

	helmv2 "github.com/fluxcd/helm-controller/api/v2"
	sourcev1 "github.com/fluxcd/source-controller/api/v1"
	"github.com/google/go-cmp/cmp"

	"github.com/home-operations/flate/internal/admissiontest"
	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/internal/testutil/cachetest"
	"github.com/home-operations/flate/internal/testutil/controllertest"
	"github.com/home-operations/flate/pkg/change"
	"github.com/home-operations/flate/pkg/controllers/base"
	sourcecontroller "github.com/home-operations/flate/pkg/controllers/source"
	"github.com/home-operations/flate/pkg/depwait"
	"github.com/home-operations/flate/pkg/helm"
	"github.com/home-operations/flate/pkg/loader"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/schedule"
	sourcecache "github.com/home-operations/flate/pkg/source/cache"
	"github.com/home-operations/flate/pkg/store"
	"github.com/home-operations/flate/pkg/task"
)

func newTestController(t *testing.T, l *loader.Loader) (*Controller, *store.Store) {
	t.Helper()
	opts := base.Options{}
	if l != nil {
		opts.Filter, opts.Loader = l.Options.Filter, l
	}
	return newTestControllerWithOptions(t, &opts)
}

func newTestControllerWithParentOf(
	t *testing.T,
	parentOf map[manifest.NamedResource]manifest.NamedResource,
) (*Controller, *store.Store) {
	t.Helper()
	return newTestControllerWithOptions(t, &base.Options{Index: parentIndexForTest(parentOf)})
}

func newTestControllerWithOptions(t *testing.T, opts *base.Options) (*Controller, *store.Store) {
	t.Helper()
	st := store.New()
	if opts.Loader != nil {
		st = opts.Loader.Store
	}
	ts := task.NewBounded(2)
	owner := cachetest.New(t, "", sourcecache.New)
	hc := helm.NewClient(owner)
	controller := base.New(st, ts)
	c := New(controller, hc, helm.Options{})
	assert.FatalIf(t, c.Controller != controller, "%v", "constructor replaced the supplied base controller")
	c.Configure(*opts)
	c.StartLifecycle()
	t.Cleanup(func() {
		ts.BlockTillDone()
	})
	return c, st
}

func reconcileFixture(t *testing.T, c *Controller, id manifest.NamedResource) store.StatusInfo {
	t.Helper()
	d := controllertest.Dispatch{Tasks: c.Tasks, Store: c.Store, ReconcileNode: c.reconcileNodeForTest}
	scheduler := schedule.New(c.Tasks, d)
	stop := c.Store.AddListener(store.EventObjectAdded, func(id manifest.NamedResource, _ any) {
		if c.Owns(id) || id.Group == manifest.SourceDomain && id.Kind == manifest.KindHelmChart {
			scheduler.OnArrival(id, true)
		}
	}, true)
	defer stop()
	scheduler.Seed([]schedule.NodeID{id})
	{
		err := scheduler.Run(t.Context())
		assert.FatalIf(t, err != nil, "%v", err)
	}
	assert.FatalIf(t, c.Tasks.Failures() != 0, "%v", "fixture task failed")
	info, _ := c.Store.GetStatus(id)
	return info
}

type chartSourceDispatcher struct {
	store    *store.Store
	dispatch func(context.Context, schedule.NodeID, manifest.DrainLevel) schedule.Result
}

func (d chartSourceDispatcher) Dispatch(
	ctx context.Context,
	id schedule.NodeID,
	level manifest.DrainLevel,
) schedule.Result {
	return d.dispatch(ctx, id, level)
}

func (d chartSourceDispatcher) Sweep(context.Context, bool) []schedule.NodeID {
	return slices.Collect(d.store.Sweep())
}

func TestReconcile_SharedChartSourceCompletes(t *testing.T) {
	t.Parallel()

	for _, workers := range []int{2, 4} {
		for _, tc := range []struct {
			name   string
			status store.Status
		}{
			{"ready", store.StatusReady},
			{"failed", store.StatusFailed},
		} {
			t.Run(fmt.Sprintf("%s/workers=%d", tc.name, workers), func(t *testing.T) {
				t.Parallel()
				testReconcileSharedChartSourceCompletesCase(
					t,
					testReconcileSharedChartSourceCompletesCaseInputs{workers: &workers, tc: &tc},
				)
			},
			)
		}
	}
}

func (c *Controller) reconcileNodeForTest(
	ctx context.Context,
	id manifest.NamedResource,
	level manifest.DrainLevel,
) *base.Execution {
	if chart, ok := c.Store.GetObject(id).(*manifest.HelmChartSource); ok && chart.ExistenceOnly() {
		return sourcecontroller.New(c.Store, c.Tasks).ReconcileNode(ctx, id, level)
	}
	return c.ReconcileNode(ctx, id, level)
}

func TestController_ShortCircuitsToReady(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name    string
		suspend bool
	}{
		{name: "suspended", suspend: true},
		{name: "unchanged"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var filterLoader *loader.Loader
			if !tc.suspend {
				filterLoader = admissiontest.New(
					t,
					admissiontest.Config{
						Changes: change.NewSet(nil),
						Index: &manifest.DiscoveryIndex{
							Records: map[manifest.NamedResource]manifest.DiscoveryRecord{},
						},
						Inventory: filtertest.MapLister{},
						Root:      "",
					},
				)
				admissiontest.Commit(t, filterLoader, nil)
			}
			c, st := newTestController(t, filterLoader)
			hr := &manifest.HelmRelease{Name: "demo", Namespace: "default", Suspend: tc.suspend}
			st.AddObject(hr)
			info := reconcileFixture(t, c, hr.Named())
			assert.FatalIf(
				t,
				info.Status != store.StatusReady || info.Message != tc.name,
				"status = %+v, want Ready/%s",
				info,
				tc.name,
			)
		})
	}
}

func TestController_AllowMissingSecretsOmitsUnavailableValuesFrom(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	testutil.WriteFile(t, dir, "mychart/values.yaml", "required: valid\n")
	testutil.WriteFile(t, dir, "mychart/Chart.yaml", `apiVersion: v2
name: mychart
version: 0.1.0
`)
	testutil.WriteFile(t, dir, "mychart/templates/configmap.yaml", `apiVersion: v1
kind: ConfigMap
metadata:
  name: {{ .Release.Name }}-cm
data:
  required: {{ required "missing required value" .Values.required | quote }}
  kept: {{ .Values.kept | quote }}
  fallback: {{ .Values.missing | default "fallback" | quote }}
`)

	c, st := newTestControllerWithOptions(
		t,
		&base.Options{AllowMissingKinds: []string{manifest.KindSecret, manifest.KindConfigMap}},
	)
	src := &manifest.GitRepository{Name: "charts", Namespace: "flux-system"}
	st.AddObject(src)
	st.SetArtifact(src.Named(), &store.SourceArtifact{
		Kind: manifest.KindGitRepository, URL: "file://" + dir, FS: os.DirFS(dir), LocalPath: dir,
	})
	st.UpdateStatus(src.Named(), store.StatusInfo{Status: store.StatusReady, Reason: "", Message: ""})
	st.AddObject(&manifest.ConfigMap{
		Name:      "present-values",
		Namespace: "default",
		Data:      map[string]any{"values.yaml": "kept: kept-value\n"},
	})
	publishRawFixture(t, c, &manifest.RawObject{
		Kind:       "ExternalSecret",
		APIVersion: "external-secrets.io/v1",
		Name:       "app-creds",
		Namespace:  "default",
		Spec: map[string]any{
			"target": map[string]any{"name": "app-values"},
		},
	})
	hr := &manifest.HelmRelease{
		Name:      "demo",
		Namespace: "default",
		Chart: manifest.HelmChart{
			Name: "mychart",
			Source: manifest.NamedResource{
				Group:     manifest.SourceDomain,
				Kind:      manifest.KindGitRepository,
				Namespace: "flux-system",
				Name:      "charts",
			},
		},
		HelmReleaseSpec: helmv2.HelmReleaseSpec{
			Chart: &helmv2.HelmChartTemplate{
				Spec: helmv2.HelmChartTemplateSpec{
					Chart:   "mychart",
					Version: "",
					SourceRef: &helmv2.CrossNamespaceObjectReference{
						Kind:       manifest.KindGitRepository,
						Name:       "charts",
						Namespace:  "flux-system",
						APIVersion: manifest.SourceDomain + "/v1",
					},
				},
			},
			Interval: metav1Duration(time.Hour),
			ValuesFrom: []manifest.ValuesReference{
				{Kind: manifest.KindConfigMap, Name: "present-values"},
				{Kind: manifest.KindSecret, Name: "app-values"},
				{Kind: manifest.KindConfigMap, Name: "runtime-values"},
			},
		},
	}
	st.AddObject(hr)

	info := reconcileFixture(t, c, hr.Named())
	assert.FatalIf(t, info.Status != store.StatusReady, "status = %+v, want StatusReady", info)
	assert.FatalIf(
		t,
		info.Outcome() == store.OutcomeSkipped,
		"generated valuesFrom refs should be omitted, not skip the HelmRelease: %+v",
		info,
	)
	art, ok := st.GetArtifact(hr.Named(), store.ArtifactContent).(*store.Emission)
	assert.FatalIf(t, !ok, "%v", "HelmRelease artifact was not written")
	{
		got := renderedConfigMapValue(testutil.DocumentMaps(art.Manifests), "kept")
		assert.FatalIf(t, got != "kept-value", "rendered kept = %q, want kept-value", got)
	}
	{
		got := renderedConfigMapValue(testutil.DocumentMaps(art.Manifests), "fallback")
		assert.FatalIf(t, got != "fallback", "rendered fallback = %q, want fallback", got)
	}
	assert.Diff(t, st.Warnings(), []manifest.Warning{
		{
			Resource: hr.Named(),
			Subject:  manifest.DiagnosticFor(hr.Named(), hr),
			Category: manifest.WarnOmittedValues,
			Message:  "required valuesFrom ConfigMap/default/runtime-values omitted from offline render",
			Count:    1,
		},
		{
			Resource: hr.Named(),
			Subject:  manifest.DiagnosticFor(hr.Named(), hr),
			Category: manifest.WarnOmittedValues,
			Message:  "required valuesFrom Secret/default/app-values omitted from offline render",
			Count:    1,
		},
	})
	warnings := st.Warnings()
	hr = hr.Clone()
	hr.Labels = map[string]string{"owner": "re-emitted"}
	st.AddObject(hr)
	assert.Equal(t, reconcileFixture(t, c, hr.Named()).Status, store.StatusReady)
	latest, _ := st.GetArtifact(hr.Named(), store.ArtifactContent).(*store.Emission)
	assert.FatalIf(
		t,
		latest == nil || latest.Fingerprint != art.Fingerprint,
		"%v",
		"metadata-only update rendered different effective inputs",
	)
	assert.Diff(t, st.Warnings(), warnings)
	hr = hr.Clone()
	hr.ValuesFrom = append(hr.ValuesFrom, manifest.ValuesReference{Kind: manifest.KindSecret, Name: "another-values"})
	st.AddObject(hr)
	assert.Equal(t, reconcileFixture(t, c, hr.Named()).Status, store.StatusReady)
	assert.Diff(t, st.Warnings(), []manifest.Warning{
		{
			Resource: hr.Named(),
			Subject:  manifest.DiagnosticFor(hr.Named(), hr),
			Category: manifest.WarnOmittedValues,
			Message:  "required valuesFrom ConfigMap/default/runtime-values omitted from offline render",
			Count:    1,
		},
		{
			Resource: hr.Named(),
			Subject:  manifest.DiagnosticFor(hr.Named(), hr),
			Category: manifest.WarnOmittedValues,
			Message:  "required valuesFrom Secret/default/another-values omitted from offline render",
			Count:    1,
		},
		{
			Resource: hr.Named(),
			Subject:  manifest.DiagnosticFor(hr.Named(), hr),
			Category: manifest.WarnOmittedValues,
			Message:  "required valuesFrom Secret/default/app-values omitted from offline render",
			Count:    1,
		},
	})
	invalid := hr.Clone()
	invalid.Name = "invalid"
	invalid.Values = map[string]any{"required": ""}
	st.AddObject(invalid)
	info = reconcileFixture(t, c, invalid.Named())
	assert.FatalIf(
		t,
		info.Status != store.StatusFailed || !strings.Contains(info.Message, "missing required value"),
		"permitted values omission hid required chart failure: %+v",
		info,
	)
	assert.FatalIf(t, !slices.ContainsFunc(st.Warnings(), func(w manifest.Warning) bool {
		return w.Resource == invalid.Named() && w.Category == manifest.WarnOmittedValues &&
			strings.Contains(w.Message, "Secret/default/app-values")
	}),
		"failed chart lost its omission warning: %+v", st.Warnings())
}

func TestController_ValuesOmissionPreservesRequiredChartFailure(t *testing.T) {
	t.Parallel()
	c, st := newTestControllerWithOptions(
		t,
		&base.Options{AllowMissingKinds: []string{manifest.KindSecret, manifest.KindConfigMap}},
	)
	chart := &manifest.HelmChartSource{Name: "chart", Namespace: "apps"}
	st.AddObject(chart)
	st.UpdateStatus(chart.Named(), store.StatusInfo{Status: store.StatusFailed, Reason: "", Message: "chart failed"})
	hr := &manifest.HelmRelease{
		Name:      "consumer",
		Namespace: "apps",
		Chart: manifest.HelmChart{
			Source: manifest.NamedResource{
				Group:     manifest.SourceDomain,
				Kind:      manifest.KindHelmChart,
				Namespace: "apps",
				Name:      chart.Name,
			},
		},
		ChartRef: &helmv2.CrossNamespaceSourceReference{
			Kind:      manifest.KindHelmChart,
			Name:      chart.Name,
			Namespace: "apps",
		},
		ValuesFrom: []manifest.ValuesReference{{Kind: manifest.KindSecret, Name: "values"}},
	}
	st.AddObject(hr)
	values := manifest.NamedResource{Kind: manifest.KindSecret, Namespace: "apps", Name: "values"}
	for _, level := range []manifest.DrainLevel{manifest.DrainNone, manifest.DrainCascade} {
		err := c.newExecution(hr.Named()).reconcile(t.Context(), hr, level)
		failed, ok := errors.AsType[*manifest.DependencyFailedError](err)
		assert.FatalIf(t, !ok || !slices.Equal(failed.Failed, []manifest.NamedResource{chart.Named()}) ||
			failed.Reasons[chart.Named()].Message != "chart failed", "required chart failure changed: %v", err)
	}

	assert.FatalIf(
		t,
		len(hr.ValuesFrom) != 1 || hr.ValuesFrom[0].Name != values.Name,
		"%v",
		"omission mutated stored valuesFrom",
	)
}

type testReconcileSharedChartSourceCompletesCaseInputs struct {
	workers *int
	tc      *struct {
		name string

		status store.Status
	}
}

func testReconcileSharedChartSourceCompletesCase(
	t *testing.T,
	testCaseInput testReconcileSharedChartSourceCompletesCaseInputs,
) {
	t.Helper()

	synctest.Test(t, func(t *testing.T) {
		runSharedChartSource(t, &testCaseInput)
	})
}

func runSharedChartSource(t *testing.T, inputs *testReconcileSharedChartSourceCompletesCaseInputs) {
	t.Helper()
	c, st := newTestController(t, nil)
	tasks := task.NewBounded((*inputs.workers))
	c.Tasks = tasks
	repository := &manifest.HelmRepository{
		Name:      "bitnami",
		Namespace: "flux-system",
		URL:       "https://charts.example/bitnami",
	}
	st.AddObject(repository)
	st.UpdateStatus(
		repository.Named(),
		store.StatusInfo{Status: store.StatusReady, Reason: "", Message: ""},
	)
	consumers := make([]*manifest.HelmRelease, 40)
	for i := range consumers {
		consumers[i] = &manifest.HelmRelease{Name: fmt.Sprintf("redis-%02d", i), Namespace: "apps"}
	}
	source := manifest.NamedResource{
		Group:     manifest.SourceDomain,
		Kind:      manifest.KindHelmChart,
		Namespace: "flux-system",
		Name:      "shared",
	}
	st.AddObject(
		&manifest.HelmChartSource{
			Name:      source.Name,
			Namespace: source.Namespace,
			Chart:     "redis",
			Version:   "1.0.0",
			SourceRef: sourcev1.LocalHelmChartSourceReference{
				Kind: manifest.KindHelmRepository,
				Name: repository.Name,
			},
		},
	)
	for _, hr := range consumers {
		hr.ChartRef = &helmv2.CrossNamespaceSourceReference{
			Kind:      manifest.KindHelmChart,
			Name:      source.Name,
			Namespace: source.Namespace,
		}
		st.AddObject(hr)
	}
	sourceStarted := make(chan struct{})
	sourceRelease := make(chan struct{})
	releaseSource := sync.OnceFunc(func() { close(sourceRelease) })
	defer releaseSource()
	parked := make(chan manifest.NamedResource, len(consumers))
	dispatch := chartSourceDispatcher{
		store: st,
		dispatch: func(ctx context.Context, id schedule.NodeID, level manifest.DrainLevel) schedule.Result {
			if id == source {
				close(sourceStarted)
				tasks.YieldSlot(func() { <-sourceRelease })
				if inputs.tc.status == store.StatusReady {
					st.SetArtifact(id, &store.SourceArtifact{FS: fstest.MapFS{
						"chart/Chart.yaml": &fstest.MapFile{
							Data: []byte("apiVersion: v2\nname: redis\nversion: 1.0.0\n"),
						},
						"chart/templates/output.yaml": &fstest.MapFile{
							Data: []byte(
								"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: '{{ .Release.Name }}'}\ndata: {value: complete}\n",
							),
						},
						"chart/templates/authored.yaml": &fstest.MapFile{
							Data: []byte(
								"apiVersion: source.toolkit.fluxcd.io/v1\nkind: HelmChart\nmetadata: {name: 'authored-{{ .Release.Name }}', namespace: flux-system}\nspec: {chart: redis, version: 1.0.0, interval: 1m, sourceRef: {kind: HelmRepository, name: bitnami}}\n",
							),
						},
					}})
				}
				st.UpdateStatus(
					id,
					store.StatusInfo{Status: inputs.tc.status, Reason: "", Message: "source finished"},
				)
				return schedule.Result{}
			}
			ex := c.newExecution(id)
			ex.run.Dispatch(
				ctx,
				level,
				ex,
				func(ex *execution, ctx context.Context, hr *manifest.HelmRelease, level manifest.DrainLevel) error {
					_, err := ex.run.RequireRefresh(ctx, hr, manifest.GateArtifacts, level)
					if _, ok := errors.AsType[*depwait.ErrBlocked](err); ok {
						parked <- id
					}
					return err
				},
			)
			return schedule.Result{Assessment: &ex.run}
		},
	}

	runSharedChartSchedule(
		t,
		&sharedChartScheduleInputs{
			inputs:        &inputs,
			c:             &c,
			st:            &st,
			tasks:         &tasks,
			consumers:     &consumers,
			source:        &source,
			sourceStarted: &sourceStarted,
			releaseSource: &releaseSource,
			parked:        &parked,
			dispatch:      &dispatch,
		},
	)
}

type sharedChartScheduleInputs struct {
	inputs        **testReconcileSharedChartSourceCompletesCaseInputs
	c             **Controller
	st            **store.Store
	tasks         **task.Service
	consumers     *[]*manifest.HelmRelease
	source        *manifest.NamedResource
	sourceStarted *chan struct{}
	releaseSource *func()
	parked        *chan manifest.NamedResource
	dispatch      *chartSourceDispatcher
}

func runSharedChartSchedule(t *testing.T, scope *sharedChartScheduleInputs) {
	t.Helper()
	s := schedule.New((*scope.tasks), (*scope.dispatch))
	unsub := (*scope.st).AddListener(store.EventObjectAdded, func(id manifest.NamedResource, _ any) {
		if id == (*scope.source) {
			s.OnArrival(id, true)
		}
	}, false)
	defer unsub()
	s.Seed([]schedule.NodeID{(*scope.source)})
	done := make(chan error, 1)
	go func() { done <- s.Run(t.Context()) }()
	<-(*scope.sourceStarted)
	for _, hr := range *scope.consumers {
		s.OnArrival(hr.Named(), true)
	}
	{
		info, ok := (*scope.st).GetStatus((*scope.source))
		assert.ErrorIf(
			t,
			ok,
			"consumer wrote synthetic status = %+v, want no Ready condition",
			info,
		)
	}
	for range *scope.consumers {
		<-(*scope.parked)
	}
	synctest.Wait()
	{
		_, ok := (*scope.st).GetObject((*scope.source)).(*manifest.HelmChartSource)
		assert.FatalIf(t, !ok, "%v", "typed synthetic child unavailable while H is incomplete")
	}
	for _, hr := range *scope.consumers {
		_, _, partial := (*scope.st).GetEmission(hr.Named())
		assert.FatalIf(t, len(partial.Children) != 0 || len(partial.Manifests) != 0,

			"synthetic child lost incomplete inventory semantics for %s: children=%+v manifests=%v",
			hr.Named(),
			partial.Children,
			partial.Manifests)
	}
	(*scope.releaseSource)()
	{
		err := <-done
		assert.FatalIf(t, err != nil, "%v", err)
	}
	{
		info, ok := (*scope.st).GetStatus(
			(*scope.source),
		)
		assert.ErrorIf(t, !ok || info.Status != (*scope.inputs).tc.status ||
			info.Message != "source finished", "source status = %+v (ok=%v), want preserved %v", info, ok, (*scope.inputs).tc.status)
	}
	checkSharedChartConsumers(t, scope)
}

func checkSharedChartConsumers(t *testing.T, scope *sharedChartScheduleInputs) {
	t.Helper()
	var closeReplays []func()
	defer func() {
		for _, closeReplay := range slices.Backward(closeReplays) {
			closeReplay()
		}
	}()
	for _, hr := range *scope.consumers {
		if (*scope.inputs).tc.status == store.StatusReady {
			replay := prepareSharedChartRerun(t, (*scope.c), (*scope.st), hr)
			closeReplays = append(closeReplays, replay.run.Close)
			assert.FatalIf(t, replay.run.Outcome() == schedule.Blocked || !replay.valid || replay.arrivals != 0 ||
				replay.artifacts != 0 ||
				replay.statuses != 0 ||
				replay.renders != 1,

				"equal H rerun: unresolved=%t authority=%t object/artifact/status events=%d/%d/%d renders=%d",
				replay.run.Outcome() == schedule.Blocked,
				replay.valid,
				replay.arrivals,
				replay.artifacts,
				replay.statuses,
				replay.renders)

			assert.FatalIf(t, !replay.after.Complete || replay.before.Fingerprint != replay.after.Fingerprint ||
				!cmp.Equal(
					replay.before.Manifests,
					replay.after.Manifests,
				), "%v", "equal H rerun changed accepted content or completeness")
		} else {
			(*scope.st).UpdateFailure(
				hr.Named(),
				manifest.NewError(manifest.ErrInput, nil, "H failed after partial work"),
			)
		}
		scope.checkInventory(t, hr)
	}
}

type sharedChartRerun struct {
	run                                    *base.Execution
	before, after                          *store.Emission
	valid                                  bool
	arrivals, artifacts, statuses, renders int
}

func prepareSharedChartRerun(t *testing.T, c *Controller, st *store.Store, hr *manifest.HelmRelease) sharedChartRerun {
	t.Helper()
	var renders int
	stopRender := st.AddListener(
		store.EventStatusUpdated,
		func(id manifest.NamedResource, payload any) {
			if info, ok := payload.(store.StatusInfo); id == hr.Named() && ok &&
				info.Message == "rendering" {
				renders++
			}
		},
		false,
	)
	initialRun := c.ReconcileNode(t.Context(), hr.Named(), manifest.DrainNone)
	assert.FatalIf(t, initialRun.Outcome() == schedule.Blocked, "%s", initialRun.Blocked)

	before := st.GetArtifact(hr.Named(), store.ArtifactContent).(*store.Emission)
	var reader store.ReadSet
	reader.Init(st, manifest.NamedResource{Kind: "Observer", Name: hr.Name}, nil)
	reader.GetArtifact(hr.Named(), store.ArtifactContent)
	var arrivals, artifacts, statuses int
	stopObjects := st.AddListener(
		store.EventObjectAdded,
		func(manifest.NamedResource, any) { arrivals++ },
		false,
	)
	stopArtifacts := st.AddListener(
		store.EventArtifactUpdated,
		func(id manifest.NamedResource, _ any) {
			if id == hr.Named() {
				artifacts++
			}
		},
		false,
	)
	stopStatus := st.AddListener(
		store.EventStatusUpdated,
		func(id manifest.NamedResource, _ any) {
			if id == hr.Named() {
				statuses++
			}
		},
		false,
	)
	rerun := c.ReconcileNode(t.Context(), hr.Named(), manifest.DrainNone)
	initialRun.Retire()
	stopObjects()
	stopArtifacts()
	stopStatus()
	stopRender()
	after := st.GetArtifact(hr.Named(), store.ArtifactContent).(*store.Emission)
	valid := reader.Valid()
	reader.Retire()
	return sharedChartRerun{
		run:       rerun,
		before:    before,
		after:     after,
		valid:     valid,
		arrivals:  arrivals,
		artifacts: artifacts,
		statuses:  statuses,
		renders:   renders,
	}
}

func (s *sharedChartScheduleInputs) checkInventory(t *testing.T, hr *manifest.HelmRelease) {
	t.Helper()
	_, _, retained := (*s.st).GetEmission(hr.Named())
	wantDocs := 0
	if (*s.inputs).tc.status == store.StatusReady {
		wantDocs = 2
	}
	assert.FatalIf(
		t, (*s.st).GetObject((*s.source)) == nil || len(retained.Manifests) != wantDocs, "%v",
		"actual H completion lost C or output",
	)
	authored := false
	for _, doc := range retained.Manifests {
		if manifest.DocKind(doc.Document) != manifest.KindHelmChart {
			continue
		}
		metadata := manifest.DocMetadata(doc.Document)
		name, namespace := metadata.Name, metadata.Namespace
		assert.FatalIf(t, name == s.source.Name, "%v", "synthetic C leaked to output")
		authored = true
		{
			_, ok := (*s.st).GetObject(manifest.NamedResource{Group: manifest.SourceDomain, Kind: manifest.KindHelmChart, Namespace: namespace, Name: name}).(*manifest.HelmChartSource)
			assert.FatalIf(t, !ok, "%v", "authored C lost typed admission")
		}
	}
	assert.FatalIf(
		t,
		authored != ((*s.inputs).tc.status == store.StatusReady),
		"%v",
		"authored C document was dropped",
	)
	assert.FatalIf(
		t, (*s.st).GetObject((*s.source)) == nil, "%v",
		"synthetic C unavailable after H completes",
	)
	if info, ok := (*s.st).GetStatus(hr.Named()); !ok || info.Status != (*s.inputs).tc.status {
		t.Errorf("consumer %s status = %+v (ok=%v), want %v", hr.Named(), info, ok, (*s.inputs).tc.status)
	}
}
