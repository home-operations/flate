package helmrelease

import (
	"context"
	"fmt"
	"io/fs"
	"os"
	"slices"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/google/go-cmp/cmp/cmpopts"

	helmv2 "github.com/fluxcd/helm-controller/api/v2"
	"github.com/google/go-cmp/cmp"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/internal/testutil/controllertest"
	"github.com/home-operations/flate/pkg/controllers/base"
	"github.com/home-operations/flate/pkg/helm"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/schedule"
	sourcecache "github.com/home-operations/flate/pkg/source/cache"
	"github.com/home-operations/flate/pkg/store"
	"github.com/home-operations/flate/pkg/task"
)

func TestReconcile_SuspensionAfterRefresh(t *testing.T) {
	t.Parallel()
	for _, site := range []struct {
		name, message    string
		parent, retained bool
	}{
		{"parent", "resolving dependencies", true, false},
		{"dependencies_after_parent", "resolving dependencies", true, false},
		{"dependencies_without_parent", "resolving dependencies", false, false},
		{"pre_render", "resolving values inputs", false, false},
		{"pre_render_retained", "resolving values inputs", true, true},
	} {
		t.Run(site.name, func(t *testing.T) {
			t.Parallel()
			testReconcileSuspensionAfterRefreshCase(t, &site)
		},
		)
	}
}

type renderBarrierFS struct {
	fs.FS

	tasks   *task.Service
	once    sync.Once
	started chan struct{}
	resume  <-chan struct{}
}

func (f *renderBarrierFS) Open(name string) (fs.File, error) {
	if name == "chart/Chart.yaml" {
		f.once.Do(func() {
			close(f.started)
			f.tasks.YieldSlot(func() { <-f.resume })
		})
	}
	return f.FS.Open(name)
}

func TestReconcile_ParentUpdateDuringRender(t *testing.T) {
	t.Parallel()

	for _, workers := range []int{2, 4} {
		t.Run(fmt.Sprintf("workers_%d", workers), func(t *testing.T) {
			t.Parallel()

			st := store.New()
			tasks := task.NewBounded(workers)
			owner, err := sourcecache.New(t.Context(), "")
			{
				assert.FatalIf(t, err != nil, "%v", err)
			}
			t.Cleanup(func() { _ = owner.Close() })
			client := helm.NewClient(owner)
			c := New(base.New(st, tasks), client, helm.Options{}, false)
			c.Configure(base.Options{})
			c.StartLifecycle()
			started, resume, rendered := make(chan struct{}), make(chan struct{}), make(chan struct{})
			release := sync.OnceFunc(func() { close(resume) })
			t.Cleanup(func() { release(); tasks.BlockTillDone(); c.StartLifecycle() })
			parent := &manifest.Kustomization{Name: "parent", Namespace: "apps"}
			st.AddObject(parent)
			dir := t.TempDir()
			testutil.WriteFile(t, dir, "chart/Chart.yaml", "apiVersion: v2\nname: binding\nversion: 1.0.0\n")
			testutil.WriteFile(t, dir, "chart/templates/cm.yaml", `apiVersion: v1
kind: ConfigMap
metadata: {name: binding}
data:
  output: {{ .Values.output | quote }}
  required: {{ required "required value" .Values.required | quote }}
`)
			src := &manifest.OCIRepository{Name: "chart", Namespace: "apps", URL: "oci://example.test/chart"}
			st.AddObject(src)
			source := &store.SourceArtifact{
				Kind:      manifest.KindOCIRepository,
				FS:        &renderBarrierFS{FS: os.DirFS(dir), tasks: tasks, started: started, resume: resume},
				LocalPath: dir,
				Revision:  "1.0.0@sha256:abcdef12345675476ed4d43c14f80d52d81d36bc16906843463d464c6146f0d8",
				Digest:    "sha256:abcdef12345675476ed4d43c14f80d52d81d36bc16906843463d464c6146f0d8",
			}
			st.SetArtifact(src.Named(), source)
			st.UpdateStatus(src.Named(), store.StatusInfo{Status: store.StatusReady, Reason: "", Message: ""})
			required := "present"
			docA := map[string]any{
				"apiVersion": "helm.toolkit.fluxcd.io/v2", "kind": manifest.KindHelmRelease,
				"metadata": map[string]any{"name": "release", "namespace": "apps"},
				"spec": map[string]any{
					"chartRef": map[string]any{"kind": manifest.KindOCIRepository, "name": src.Name},
					"values":   map[string]any{"output": "A", "required": required},
				},
			}
			docB := manifest.DeepCopyMap(docA)
			docB["spec"].(map[string]any)["values"].(map[string]any)["output"] = "B"
			publish := func(doc map[string]any) {
				_, _, next := st.GetEmission(parent.Named())
				defer next.Finish()
				children, retained := testutil.ParseChildren(
					c,
					parent,
					[]map[string]any{doc},
					manifest.ParseDocOptions{DefaultNamespace: "apps"},
				)
				next.Children, next.Manifests = children, testutil.DocumentOccurrences(retained)
				next.Complete = true
				controllertest.Emit(t.Context(), c.Controller, parent.Named(), &next)
			}
			publish(docA)
			st.UpdateStatus(parent.Named(), store.StatusInfo{Status: store.StatusReady, Reason: "", Message: ""})
			id := testutil.HelmReleaseID("apps", "release")
			hr, ok := st.Get[*manifest.HelmRelease](id)
			{
				assert.FatalIf(t, !ok, "%v", "parent did not emit input A")
			}
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
					t.Error("render never reached chart filesystem")
					return
				}
				publish(docB)
			})
			markRendered := sync.OnceFunc(func() { close(rendered) })
			info := controllertest.DispatchToFixpoint(t.Context(), t, controllertest.Dispatch{
				Tasks: tasks,
				Store: st,
				ID:    id,
				ReconcileNode: func(ctx context.Context, id manifest.NamedResource, level manifest.DrainLevel) *base.Execution {
					defer markRendered()
					return c.ReconcileNode(ctx, id, level)
				},
			})
			{
				assert.FatalIf(t, info.Status != store.StatusReady, "canonical replacement status = %+v", info)
			}

			if tasks.Failures() != 0 {
				t.Fatalf("task failures = %d", tasks.Failures())
			}
			emission := accepted.Load()
			{
				assert.FatalIf(
					t,
					emission == nil || !emission.Complete || len(emission.Manifests) != 1 || emission.Fingerprint == "" ||
						!emission.OutputReads.Valid(),
					"accepted B emission = %+v; want complete output with current provenance and a content key",
					emission,
				)
			}
			{
				{
					got := renderedConfigMapValue(testutil.DocumentMaps(emission.Manifests), "output")
					assert.FatalIf(t, got != "B", "rendered output = %q, want B", got)
				}
			}
			current, ok := st.Get[*manifest.HelmRelease](id)
			{
				assert.FatalIf(t, !ok || current.Values["output"] != "B", "current input is not B: %+v", current)
			}
			{
				assert.ErrorIf(
					t,
					st.GetArtifact(id, store.ArtifactContent) != emission,
					"%v",
					"canonical B does not expose its accepted output",
				)
			}
			output, ok := st.Get[*manifest.ConfigMap](
				manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: "apps", Name: "binding"},
			)
			{
				assert.FatalIf(
					t,
					!ok || output.Data["output"] != "B",
					"canonical emitted child = %+v, want output B",
					output,
				)
			}

			{
				assert.FatalIf(
					t,
					hr.Values["output"] != "A" || hr.Values["required"] != required,
					"%v",
					"render mutated input A",
				)
			}
		})
	}
}

func TestReconcile_SyntheticChartReplayContent(t *testing.T) {
	t.Parallel()
	c, st := newTestController(t, nil)
	repository := &manifest.HelmRepository{Name: "charts", Namespace: "apps"}
	repository.URL = "https://charts.invalid"
	st.AddObject(repository)
	st.UpdateStatus(repository.Named(), store.StatusInfo{Status: store.StatusReady, Reason: "", Message: ""})
	input := &manifest.ConfigMap{
		Name:      "values",
		Namespace: "apps",
		Data:      map[string]any{"values.yaml": "value: first\n"},
	}
	st.AddObject(input)
	hr := &manifest.HelmRelease{
		Name:      "app",
		Namespace: "apps",
		Chart: manifest.HelmChart{
			Name:    "fixture",
			Version: "1.0.0",
			Source: manifest.NamedResource{
				Group:     manifest.SourceDomain,
				Kind:      manifest.KindHelmRepository,
				Namespace: repository.Namespace,
				Name:      repository.Name,
			},
		},
		HelmReleaseSpec: helmv2.HelmReleaseSpec{
			Chart: &helmv2.HelmChartTemplate{
				Spec: helmv2.HelmChartTemplateSpec{
					Chart:   "fixture",
					Version: "1.0.0",
					SourceRef: &helmv2.CrossNamespaceObjectReference{
						Kind:       manifest.KindHelmRepository,
						Name:       repository.Name,
						Namespace:  repository.Namespace,
						APIVersion: manifest.SourceDomain + "/v1",
					},
				},
			},
			ValuesFrom: []manifest.ValuesReference{{Kind: manifest.KindConfigMap, Name: input.Name}},
		},
	}
	st.AddObject(hr)
	var executions []*base.Execution
	t.Cleanup(func() {
		for _, execution := range executions {
			execution.Close()
		}
		for range st.Sweep() {
		}
	})
	reconcile := func() *base.Execution {
		t.Helper()
		var next *base.Execution
		c.Tasks.Go(t.Context(), "reconcile", func(ctx context.Context) {
			next = c.ReconcileNode(ctx, hr.Named(), manifest.DrainNone)
		})
		c.Tasks.BlockTillDone()
		{
			assert.FatalIf(t, c.Tasks.Failures() != 0 || next == nil, "%v", "reconciliation task failed")
		}
		if len(executions) != 0 {
			executions[len(executions)-1].Retire()
		}
		executions = append(executions, next)
		for range st.Sweep() {
		}
		return next
	}
	{
		{
			first := reconcile()
			assert.FatalIf(t, first.Outcome() != schedule.Blocked, "%v", "missing synthetic chart did not block")
		}
	}
	charts := st.ListObjects(manifest.SourceDomain, manifest.KindHelmChart)
	{
		assert.FatalIf(t, len(charts) != 1, "synthetic charts = %d, want 1", len(charts))
	}
	chart := charts[0].Named()
	firstDir, secondDir := t.TempDir(), t.TempDir()
	for _, fixture := range []struct{ dir, value string }{{firstDir, "one"}, {secondDir, "two"}} {
		testutil.WriteFile(t, fixture.dir, "chart/Chart.yaml", "apiVersion: v2\nname: fixture\nversion: 1.0.0\n")
		testutil.WriteFile(
			t,
			fixture.dir,
			"chart/templates/output.yaml",
			"apiVersion: v1\nkind: ConfigMapList\nitems:\n- metadata: {name: '{{ .Release.Name }}-first'}\n  data: {value: '{{ .Values.value }}', source: "+fixture.value+"}\n- metadata: {name: '{{ .Release.Name }}-second'}\n  data: {value: '{{ .Values.value }}', source: "+fixture.value+"}\n",
		)
	}
	st.SetArtifact(
		chart,
		&store.SourceArtifact{
			Kind:      manifest.KindHelmChart,
			Key:       "chart-one",
			FS:        os.DirFS(firstDir),
			LocalPath: firstDir,
		},
	)
	st.UpdateStatus(chart, store.StatusInfo{Status: store.StatusReady, Reason: "", Message: ""})
	initialRun := reconcile()
	initial, ok := st.GetArtifact(hr.Named(), store.ArtifactContent).(*store.Emission)
	{
		assert.FatalIf(
			t,
			!ok || initialRun.Outcome() == schedule.Blocked || initialRun.Reused || len(initial.Manifests) != 2,
			"%v",
			"initial two-document render did not settle",
		)
	}
	for i, doc := range initial.Manifests {
		name, namespace := manifest.DocMetadata(doc.Document)
		want := []string{"app-first", "app-second"}[i]
		labels := doc.Document["metadata"].(map[string]any)["labels"].(map[string]any)
		assert.Equal(t, labels["helm.toolkit.fluxcd.io/name"], any(hr.Name))
		if name != want || namespace != hr.Namespace ||
			st.GetObject(
				manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: namespace, Name: name},
			) == nil {
			t.Fatalf("rendered identity = %s/%s, want %s/%s", namespace, name, hr.Namespace, want)
		}
	}
	testReconcileSyntheticChartReplayContentTail(
		t,
		&testReconcileSyntheticChartReplayContentTailInputs{
			hr:        &hr,
			st:        &st,
			initial:   &initial,
			reconcile: &reconcile,
			input:     &input,
			chart:     &chart,
			secondDir: &secondDir,
		},
	)
}

func testReconcileSyntheticChartReplayContentTail(
	t *testing.T,
	tailCaseInput *testReconcileSyntheticChartReplayContentTailInputs,
) {
	t.Helper()
	stamped := (*tailCaseInput.hr).Clone()
	stamped.Labels = map[string]string{"kustomize.toolkit.fluxcd.io/name": "parent"}
	(*tailCaseInput.st).
		AddObject(stamped)
	{
		assert.FatalIf(
			t,
			(*tailCaseInput.initial).OutputReads.Valid(),
			"%v",
			"metadata change did not invalidate the prior assessment",
		)
	}
	var pending atomic.Bool
	stop := (*tailCaseInput.st).AddListener(store.EventStatusUpdated, func(id manifest.NamedResource, payload any) {
		if info, ok := payload.(store.StatusInfo); id == (*tailCaseInput.hr).Named() && ok &&
			info.Status == store.StatusPending {
			pending.Store(true)
		}
	}, false)
	t.Cleanup(stop)
	replay := (*tailCaseInput.reconcile)()
	after := (*tailCaseInput.st).GetArtifact((*tailCaseInput.hr).Named(), store.ArtifactContent).(*store.Emission)
	{
		assert.ErrorIf(t, pending.Load(), "%v", "equal synthetic-chart replay transiently became Pending")
	}
	if replay.Outcome() == schedule.Blocked || !replay.Reused ||
		after.Fingerprint != (*tailCaseInput.initial).Fingerprint {
		t.Errorf(
			"same content after synthetic publication: unresolved=%t reused=%t fingerprint=%q, want %q",
			replay.Outcome() == schedule.Blocked,
			replay.Reused,
			after.Fingerprint, (*tailCaseInput.initial).
				Fingerprint,
		)
	}
	{
		{
			diff := cmp.Diff((*tailCaseInput.initial).Manifests, after.Manifests)
			assert.FatalIf(t, diff != "", "same content changed output (-want +got): %s", diff)
		}
	}
	fresh := *(*tailCaseInput.input)
	fresh.Data = map[string]any{"values.yaml": "value: second\n"}
	(*tailCaseInput.st).
		AddObject(&fresh)
	valuesRun := (*tailCaseInput.reconcile)()
	withValues := (*tailCaseInput.st).GetArtifact((*tailCaseInput.hr).Named(), store.ArtifactContent).(*store.Emission)
	{
		assert.FatalIf(
			t,
			valuesRun.Outcome() == schedule.Blocked || valuesRun.Reused || withValues.Fingerprint == after.Fingerprint ||
				renderedConfigMapValue(testutil.DocumentMaps(withValues.Manifests), "value") != "second" ||
				len(withValues.Manifests) != 2,
			"%v",
			"changed consumed values reused stale output",
		)
	}
	(*tailCaseInput.st).
		SetArtifact((*tailCaseInput.chart), &store.SourceArtifact{
			Kind:      manifest.KindHelmChart,
			Key:       "chart-two",
			FS:        os.DirFS((*tailCaseInput.secondDir)),
			LocalPath: (*tailCaseInput.secondDir),
		},
		)
	sourceRun := (*tailCaseInput.reconcile)()
	withSource := (*tailCaseInput.st).GetArtifact((*tailCaseInput.hr).Named(), store.ArtifactContent).(*store.Emission)
	if sourceRun.Outcome() == schedule.Blocked || sourceRun.Reused ||
		withSource.Fingerprint == withValues.Fingerprint ||
		renderedConfigMapValue(testutil.DocumentMaps(withSource.Manifests), "source") != "two" ||
		len(withSource.Manifests) != 2 {
		t.Fatalf(
			"changed chart content: unresolved=%t reused=%t fingerprint=%q previous=%q source=%q documents=%d",
			sourceRun.Outcome() == schedule.Blocked,
			sourceRun.Reused,
			withSource.Fingerprint,
			withValues.Fingerprint,
			renderedConfigMapValue(testutil.DocumentMaps(withSource.Manifests), "source"),
			len(withSource.Manifests),
		)
	}
}

type chartCaptureFS struct {
	fs.FS

	captures atomic.Int64
}

func (f *chartCaptureFS) Open(name string) (fs.File, error) {
	if name == "chart/Chart.yaml" {
		f.captures.Add(1)
	}
	return f.FS.Open(name)
}

func testReconcileSuspensionAfterRefreshCase(
	t *testing.T,
	testCaseInput *struct {
		name string

		message  string
		parent   bool
		retained bool
	},
) {
	t.Helper()

	for _, suspended := range []bool{false, true} {
		t.Run(fmt.Sprintf("suspended=%t", suspended), func(t *testing.T) {
			t.Parallel()
			testReconcileSuspensionAfterRefreshCaseCase(
				t,
				testReconcileSuspensionAfterRefreshCaseCaseInputs{testCaseInput: &testCaseInput, suspended: &suspended},
			)
		},
		)
	}
}

type testReconcileSuspensionAfterRefreshCaseCaseInputs struct {
	testCaseInput **struct {
		name     string
		message  string
		parent   bool
		retained bool
	}
	suspended *bool
}

func testReconcileSuspensionAfterRefreshCaseCase(
	t *testing.T,
	testCaseInput testReconcileSuspensionAfterRefreshCaseCaseInputs,
) {
	t.Helper()

	parent := &manifest.Kustomization{Name: "parent", Namespace: "apps"}
	parents := map[manifest.NamedResource]manifest.NamedResource{}
	id := testutil.HelmReleaseID("apps", "release")
	if (*testCaseInput.testCaseInput).parent {
		parents[id] = parent.Named()
	}
	c, st := newTestControllerWithParentOf(t, parents)
	st.AddObject(parent)
	st.UpdateStatus(
		parent.Named(),
		store.StatusInfo{Status: store.StatusReady, Reason: "", Message: ""},
	)
	dep := &manifest.HelmRelease{
		Name:      "dependency",
		Namespace: "apps",
		Suspend:   true,
		ChartRef: &helmv2.CrossNamespaceSourceReference{
			Kind: manifest.KindOCIRepository,
			Name: "chart",
		},
	}
	st.AddObject(dep)
	st.UpdateStatus(
		dep.Named(),
		store.StatusInfo{
			Status:  store.StatusReady,
			Reason:  store.ReasonSuspended,
			Message: "suspended",
		},
	)
	dir := t.TempDir()
	testutil.WriteFile(t, dir, "chart/Chart.yaml", "apiVersion: v2\nname: refresh\nversion: 1.0.0\n")
	testutil.WriteFile(t, dir, "chart/templates/cm.yaml", `apiVersion: v1
kind: ConfigMap
metadata: {name: {{ .Values.output }}, namespace: apps}
data: {greeting: hi}
`)
	src := &manifest.OCIRepository{Name: "chart", Namespace: "apps", URL: "oci://example.test/chart"}
	st.AddObject(src)
	st.SetArtifact(
		src.Named(),
		&store.SourceArtifact{
			Kind:      manifest.KindOCIRepository,
			FS:        os.DirFS(dir),
			LocalPath: dir,
			Revision:  "1.0.0@sha256:abcdef12345675476ed4d43c14f80d52d81d36bc16906843463d464c6146f0d8",
			Digest:    "sha256:abcdef12345675476ed4d43c14f80d52d81d36bc16906843463d464c6146f0d8",
		},
	)
	st.UpdateStatus(src.Named(), store.StatusInfo{Status: store.StatusReady, Reason: "", Message: ""})
	doc := map[string]any{
		"apiVersion": "helm.toolkit.fluxcd.io/v2", "kind": manifest.KindHelmRelease,
		"metadata": map[string]any{"name": id.Name, "namespace": id.Namespace},
		"spec": map[string]any{
			"chartRef":  map[string]any{"kind": manifest.KindOCIRepository, "name": src.Name},
			"dependsOn": []any{map[string]any{"name": dep.Name}},
			"values":    map[string]any{"output": "stale-output"},
		},
	}
	if (*testCaseInput.testCaseInput).message == "resolving values inputs" {
		input := &manifest.ConfigMap{
			Name:      "values",
			Namespace: "apps",
			Data:      map[string]any{"values.yaml": "{}"},
		}
		st.AddObject(input)
		doc["spec"].(map[string]any)["valuesFrom"] = []any{
			map[string]any{"kind": manifest.KindConfigMap, "name": input.Name},
		}
	}
	obj, _, _, err := manifest.ParseDoc(doc, manifest.ParseDocOptions{DefaultNamespace: ""})
	{
		assert.FatalIf(t, err != nil, "%v", err)
	}
	hr := obj.(*manifest.HelmRelease)
	original := hr.Clone()
	st.AddObject(hr)
	if (*testCaseInput.testCaseInput).parent {
		controllertest.Emit(
			t.Context(),
			c.Controller,
			parent.Named(),
			&store.Emission{Children: []store.Child{{Obj: hr}}, Complete: true},
		)
	}
	st.UpdateStatus(id, store.StatusInfo{Status: store.StatusPending, Reason: "", Message: ""})
	var retained *store.Emission
	if (*testCaseInput.testCaseInput).retained {
		retained = &store.Emission{
			Fingerprint: "prior",
			Manifests: testutil.DocumentOccurrences(
				[]map[string]any{
					{
						"apiVersion": "v1",
						"kind":       "ConfigMap",
						"metadata":   map[string]any{"name": "prior"},
					},
				},
			),
		}
		st.RecordEmitted(id, retained, nil)
		retained, _ = st.GetArtifact(id, store.ArtifactContent).(*store.Emission)
	}
	replacement := manifest.DeepCopyMap(doc)
	spec := replacement["spec"].(map[string]any)
	spec["suspend"] = (*testCaseInput.suspended)
	spec["values"].(map[string]any)["output"] = "hr-refresh-output"
	var observations, parentEvents, renders atomic.Int64
	var fired, published atomic.Bool
	unsub := st.AddListener(store.EventStatusUpdated, func(other manifest.NamedResource, payload any) {
		info, ok := payload.(store.StatusInfo)
		if other != id || !ok {
			return
		}
		if info.Message == "rendering" {
			renders.Add(1)
		}
		if info.Message == "waiting for structural parents" {
			parentEvents.Add(1)
		}
		if info.Message != (*testCaseInput.testCaseInput).message {
			return
		}
		observations.Add(1)
		if fired.Swap(true) {
			return
		}
		if (*testCaseInput.testCaseInput).parent {
			publishDocuments(
				c,
				parent.Named(),
				testutil.DocumentOccurrences([]map[string]any{replacement}),
				manifest.ParseDocOptions{WipeSecrets: false, DefaultNamespace: ""},
			)
		} else {
			fresh := hr.Clone()
			fresh.Suspend = (*testCaseInput.suspended)
			fresh.Values["output"] = "hr-refresh-output"
			st.AddObject(fresh)
		}
		published.Store(true)
	}, false)
	t.Cleanup(unsub)
	reconcileFixture(t, c, id)
	if observations.Load() < 1 || !published.Load() || c.Tasks.Failures() != 0 {
		t.Fatalf(
			"observations=%d published=%t panics=%d",
			observations.Load(),
			published.Load(),
			c.Tasks.Failures(),
		)
	}
	if parentEvents.Load() != 0 {
		t.Fatalf("parent refresh events = %d", parentEvents.Load())
	}
	fresh, ok := st.Get[*manifest.HelmRelease](id)
	{
		assert.FatalIf(t, !ok || fresh == hr || fresh.Suspend != (*testCaseInput.suspended) ||
			fresh.Values["output"] != "hr-refresh-output", "canonical replacement = %+v", fresh)
	}
	{
		{
			diff := cmp.Diff(original, hr, cmpopts.IgnoreUnexported(manifest.HelmRelease{}))
			assert.FatalIf(t, diff != "", "original mutated (-want +got):\n%s", diff)
		}
	}
	if (*testCaseInput.testCaseInput).parent {
		{
			{
				owners := testutil.Owners(
					st.Productions(id, nil),
				)
				assert.FatalIf(t, !slices.Contains(
					owners,
					parent.Named(),
				), "missing independent contributor: %v", owners)
			}
		}
	}
	info, _ := st.GetStatus(id)
	art, _ := st.GetArtifact(id, store.ArtifactContent).(*store.Emission)
	output := manifest.NamedResource{
		Kind:      manifest.KindConfigMap,
		Namespace: "apps",
		Name:      "hr-refresh-output",
	}
	if *testCaseInput.suspended {
		{
			assert.ErrorIf(
				t,
				info.Outcome() != store.OutcomeSuspended || info.Message != "suspended",
				"status = %+v, want Ready/suspended",
				info,
			)
		}
		_, _, inventory := st.GetEmission(id)
		if art != retained || renders.Load() != 0 || st.GetObject(output) != nil ||
			len(inventory.Children) != 0 {
			t.Errorf(
				"suspended replacement rendered: artifact=%+v output=%v renders=%d",
				art,
				st.GetObject(output),
				renders.Load(),
			)
		}
	} else if info.Status != store.StatusReady || art == nil || art == retained || renders.Load() != 1 || len(art.Manifests) != 1 || st.GetObject(output) == nil {
		t.Fatalf(
			"active replacement did not render %s: status=%+v artifact=%+v renders=%d",
			output,
			info,
			art,
			renders.Load(),
		)
	}
}

type testReconcileSyntheticChartReplayContentTailInputs struct {
	hr        **manifest.HelmRelease
	st        **store.Store
	initial   **store.Emission
	reconcile *func() *base.Execution
	input     **manifest.ConfigMap
	chart     *manifest.NamedResource
	secondDir *string
}
