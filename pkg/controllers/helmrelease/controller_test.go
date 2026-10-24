package helmrelease

import (
	"context"
	"errors"
	"fmt"
	"github.com/home-operations/flate/internal/filtertest"
	"os"
	"slices"
	"sync"
	"testing"
	"testing/fstest"
	"testing/synctest"
	"time"

	helmv2 "github.com/fluxcd/helm-controller/api/v2"
	kustomizev1 "github.com/fluxcd/kustomize-controller/api/v1"
	sourcev1 "github.com/fluxcd/source-controller/api/v1"
	"github.com/google/go-cmp/cmp"
	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/internal/testutil/cachetest"
	"github.com/home-operations/flate/internal/testutil/controllertest"
	"github.com/home-operations/flate/pkg/change"
	"github.com/home-operations/flate/pkg/controllers/base"
	"github.com/home-operations/flate/pkg/depwait"
	"github.com/home-operations/flate/pkg/helm"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/schedule"
	sourcecache "github.com/home-operations/flate/pkg/source/cache"
	"github.com/home-operations/flate/pkg/store"
	"github.com/home-operations/flate/pkg/task"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func newTestController(t *testing.T, filter *change.Filter) (*Controller, *store.Store) {
	t.Helper()
	return newTestControllerWithOptions(t, base.Options{Filter: filter})
}

func newTestControllerWithParentOf(t *testing.T, parentOf map[manifest.NamedResource]manifest.NamedResource) (*Controller, *store.Store) {
	t.Helper()
	return newTestControllerWithOptions(t, base.Options{Index: parentIndexForTest(parentOf)})
}

func newTestControllerWithOptions(t *testing.T, opts base.Options) (*Controller, *store.Store) {
	t.Helper()
	st := store.New()
	ts := task.NewBounded(2)
	owner := cachetest.New(t, "", sourcecache.New)
	hc := helm.NewClient(owner)
	controller := base.New(st, ts)
	c := New(controller, hc, helm.Options{}, false)
	if c.Controller != controller {
		t.Fatal("constructor replaced the supplied base controller")
	}
	c.Configure(opts)
	c.StartLifecycle()
	t.Cleanup(func() {

		ts.BlockTillDone()
	})
	return c, st
}

func TestMaterializeHelmChartSource_RepointsAndRegisters(t *testing.T) {
	hr := &manifest.HelmRelease{
		Name: "kromgo", Namespace: "apps",
		Chart: manifest.HelmChart{Name: "kromgo", Version: "3.0.0", Source: manifest.NamedResource{Group: manifest.SourceDomain, Kind: manifest.KindHelmRepository, Namespace: "flux-system", Name: "truecharts"}},
	}

	filter := change.NewFilter(change.NewSet([]string{"release.yaml"}), &manifest.DiscoveryIndex{
		Records: map[manifest.NamedResource]manifest.DiscoveryRecord{hr.Named(): {File: "release.yaml"}},
	}, filtertest.MapLister{}, "")
	c, st := newTestController(t, filter)
	st.AddObject(&manifest.HelmRepository{
		Name: "truecharts", Namespace: "flux-system",
		URL:  "oci://oci.trueforge.org/truecharts",
		Type: sourcev1.HelmRepositoryTypeOCI,
	})
	st.AddListener(store.EventObjectAdded, func(id manifest.NamedResource, _ any) {
		if id.Kind == manifest.KindHelmChart && !filter.ShouldReconcile(id) {
			t.Error("synthetic HelmChart was published before entering the changed-only keep set")
		}
	}, false)

	for _, group := range []string{"", "example.test"} {
		t.Run("group="+group, func(t *testing.T) {
			foreign := hr.Clone()
			foreign.Chart.Source.Group = group
			if _, repointed, err := c.newExecution(foreign.Named()).materializeHelmChartSource(foreign.Named(), foreign); repointed || err != nil {
				t.Fatal("foreign chart source borrowed the real HelmRepository")
			}
		})
	}

	st.AddObject(hr)
	got, repointed, materializeErr := c.newExecution(hr.Named()).materializeHelmChartSource(hr.Named(), hr)
	assert.NoError(t, materializeErr)
	if !repointed {
		t.Fatal("materialize did not report a repoint for a HelmRepository-sourced chart")
	}

	// Chart repointed to a synthetic HelmChart; name/version kept.
	if got.Chart.Source.Kind != manifest.KindHelmChart {
		t.Fatalf("RepoKind = %q, want HelmChart", got.Chart.Source.Kind)
	}
	if got.Chart.Source.Namespace != "flux-system" {
		t.Errorf("RepoNamespace = %q, want flux-system", got.Chart.Source.Namespace)
	}
	if got.Chart.Name != "kromgo" || got.Chart.Version != "3.0.0" {
		t.Errorf("chart name/version = %q/%q, want kromgo/3.0.0", got.Chart.Name, got.Chart.Version)
	}

	synID := manifest.NamedResource{Group: manifest.SourceDomain, Kind: manifest.KindHelmChart, Namespace: got.Chart.Source.Namespace, Name: got.Chart.Source.Name}
	obj := st.GetObject(synID)
	if obj == nil {
		t.Fatalf("synthetic HelmChart %s not added to store", synID)
	}
	if hc, ok := obj.(*manifest.HelmChartSource); !ok {
		t.Fatalf("stored object is %T, want *manifest.HelmChartSource", obj)
	} else if hc.Chart != "kromgo" || hc.Version != "3.0.0" ||
		hc.SourceRef.Kind != manifest.KindHelmRepository || hc.SourceRef.Name != "truecharts" {
		t.Errorf("synthetic HelmChart = %+v, want chart kromgo@3.0.0 sourceRef HelmRepository/truecharts", hc)
	}
	if owners := testutil.Owners(st.Productions(synID, nil)); !slices.Contains(owners, hr.Named()) {
		t.Fatalf("missing independent contributor: %v", owners)
	}
	if info, ok := st.GetStatus(synID); ok {
		t.Errorf("consumer wrote synthetic status = %+v, want no Ready condition", info)
	}
}

type chartSourceDispatcher struct {
	store    *store.Store
	dispatch func(context.Context, schedule.NodeID, manifest.DrainLevel) schedule.Result
}

func (d chartSourceDispatcher) Dispatch(ctx context.Context, id schedule.NodeID, level manifest.DrainLevel) schedule.Result {
	return d.dispatch(ctx, id, level)
}

func (d chartSourceDispatcher) Sweep(context.Context, bool) []schedule.NodeID {
	return slices.Collect(d.store.Sweep())
}

func TestMaterializeHelmChartSource_SharedSourceCompletes(t *testing.T) {
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

				synctest.Test(t, func(t *testing.T) {
					c, st := newTestController(t, nil)
					tasks := task.NewBounded(workers)
					c.Tasks = tasks
					repository := &manifest.HelmRepository{Name: "bitnami", Namespace: "flux-system", URL: "https://charts.example/bitnami"}
					st.AddObject(repository)
					st.UpdateStatus(repository.Named(), store.StatusInfo{Status: store.StatusReady, Reason: "", Message: ""})
					consumers := make([]*manifest.HelmRelease, 40)
					for i := range consumers {
						consumers[i] = &manifest.HelmRelease{Name: fmt.Sprintf("redis-%02d", i), Namespace: "apps"}
					}
					chart := manifest.HelmChart{Name: "redis", Version: "1.0.0", Source: manifest.NamedResource{Group: manifest.SourceDomain, Kind: manifest.KindHelmRepository, Namespace: "flux-system", Name: "bitnami"}}
					for _, hr := range consumers {
						hr.Chart = chart
						st.AddObject(hr)
					}
					first := consumers[0].Clone()
					var materializeErr error
					first, _, materializeErr = c.newExecution(first.Named()).materializeHelmChartSource(first.Named(), first)
					assert.NoError(t, materializeErr)
					source := (first.ChartSource())
					sourceStarted := make(chan struct{})
					sourceRelease := make(chan struct{})
					releaseSource := sync.OnceFunc(func() { close(sourceRelease) })
					defer releaseSource()
					parked := make(chan manifest.NamedResource, len(consumers))
					dispatch := chartSourceDispatcher{store: st, dispatch: func(ctx context.Context, id schedule.NodeID, level manifest.DrainLevel) schedule.Result {
						if id == source {
							close(sourceStarted)
							tasks.YieldSlot(func() { <-sourceRelease })
							if tc.status == store.StatusReady {
								st.SetArtifact(id, &store.SourceArtifact{FS: fstest.MapFS{
									"Chart.yaml":              &fstest.MapFile{Data: []byte("apiVersion: v2\nname: redis\nversion: 1.0.0\n")},
									"templates/output.yaml":   &fstest.MapFile{Data: []byte("apiVersion: v1\nkind: ConfigMap\nmetadata: {name: '{{ .Release.Name }}'}\ndata: {value: complete}\n")},
									"templates/authored.yaml": &fstest.MapFile{Data: []byte("apiVersion: source.toolkit.fluxcd.io/v1\nkind: HelmChart\nmetadata: {name: 'authored-{{ .Release.Name }}', namespace: flux-system}\nspec: {chart: redis, version: 1.0.0, interval: 1m, sourceRef: {kind: HelmRepository, name: bitnami}}\n")},
								}})
							}
							st.UpdateStatus(id, store.StatusInfo{Status: tc.status, Reason: "", Message: "source finished"})
							return schedule.Result{}
						}
						ex := c.newExecution(id)
						ex.run.Dispatch(ctx, level, ex, func(ex *execution, ctx context.Context, hr *manifest.HelmRelease, level manifest.DrainLevel) error {
							hr, _, materializeErr := ex.materializeHelmChartSource(id, hr.Clone())
							assert.NoError(t, materializeErr)
							_, err := ex.run.RequireRefresh(ctx, hr, manifest.GateArtifacts, level)
							if _, ok := errors.AsType[*depwait.ErrBlocked](err); ok {
								parked <- id
							}
							return err
						})
						return schedule.Result{Assessment: &ex.run}
					}}

					s := schedule.New(tasks, dispatch)
					unsub := st.AddListener(store.EventObjectAdded, func(id manifest.NamedResource, _ any) {
						if id == source {
							s.OnArrival(id, true)
						}
					}, false)
					defer unsub()
					s.Seed([]schedule.NodeID{source})
					done := make(chan error, 1)
					go func() { done <- s.Run(t.Context()) }()
					<-sourceStarted
					for _, hr := range consumers {
						s.OnArrival(hr.Named(), true)
					}
					second := consumers[1].Clone()

					second, _, materializeErr = c.newExecution(second.Named()).materializeHelmChartSource(second.Named(), second)
					assert.NoError(t, materializeErr)
					if (second.ChartSource()) != source {
						t.Errorf("second source = %s, want shared %s", (second.ChartSource()), source)
					}
					if info, ok := st.GetStatus(source); ok {
						t.Errorf("consumer wrote synthetic status = %+v, want no Ready condition", info)
					}
					for range consumers {
						<-parked
					}
					synctest.Wait()
					if _, ok := st.GetObject(source).(*manifest.HelmChartSource); !ok {
						t.Fatal("typed synthetic child unavailable while H is incomplete")
					}
					for _, hr := range consumers {
						_, _, partial := st.GetEmission(hr.Named())
						if len(partial.Children) != 1 || partial.Children[0].Doc != nil || len(partial.Manifests) != 0 {
							t.Fatalf("synthetic child lost incomplete inventory semantics for %s: children=%+v manifests=%v", hr.Named(), partial.Children, partial.Manifests)
						}
					}
					releaseSource()
					if err := <-done; err != nil {
						t.Fatal(err)
					}
					if info, ok := st.GetStatus(source); !ok || info.Status != tc.status || info.Message != "source finished" {
						t.Errorf("source status = %+v (ok=%v), want preserved %v", info, ok, tc.status)
					}
					for _, hr := range consumers {
						if tc.status == store.StatusReady {
							var renders int
							stopRender := st.AddListener(store.EventStatusUpdated, func(id manifest.NamedResource, payload any) {
								if info, ok := payload.(store.StatusInfo); id == hr.Named() && ok && info.Message == "rendering" {
									renders++
								}
							}, false)
							initialRun := c.ReconcileNode(t.Context(), hr.Named(), manifest.DrainNone)
							if initialRun.Outcome() == schedule.Blocked {
								t.Fatal(initialRun.Blocked)
							}
							before := st.GetArtifact(hr.Named()).(*store.Emission)
							var reader store.ReadSet
							reader.Init(st, manifest.NamedResource{Kind: "Observer", Name: hr.Name}, nil)
							reader.GetArtifact(hr.Named())
							var arrivals, artifacts, statuses int
							stopObjects := st.AddListener(store.EventObjectAdded, func(manifest.NamedResource, any) { arrivals++ }, false)
							stopArtifacts := st.AddListener(store.EventArtifactUpdated, func(id manifest.NamedResource, _ any) {
								if id == hr.Named() {
									artifacts++
								}
							}, false)
							stopStatus := st.AddListener(store.EventStatusUpdated, func(id manifest.NamedResource, _ any) {
								if id == hr.Named() {
									statuses++
								}
							}, false)
							rerun := c.ReconcileNode(t.Context(), hr.Named(), manifest.DrainNone)
							initialRun.Retire()
							stopObjects()
							stopArtifacts()
							stopStatus()
							stopRender()
							after := st.GetArtifact(hr.Named()).(*store.Emission)
							valid := reader.Valid()
							reader.Retire()
							defer rerun.Close()
							if rerun.Outcome() == schedule.Blocked || !valid || arrivals != 0 || artifacts != 0 || statuses != 0 || renders != 1 {
								t.Fatalf("equal H rerun: unresolved=%t authority=%t object/artifact/status events=%d/%d/%d renders=%d", rerun.Outcome() == schedule.Blocked, valid, arrivals, artifacts, statuses, renders)
							}
							if !after.Complete || before.Fingerprint != after.Fingerprint || !cmp.Equal(before.Manifests, after.Manifests) {
								t.Fatal("equal H rerun changed accepted content or completeness")
							}
						} else {
							st.UpdateFailure(hr.Named(), fmt.Errorf("%w: H failed after partial work", manifest.ErrInput))
						}
						_, _, retained := st.GetEmission(hr.Named())
						found := false
						for _, child := range retained.Children {
							if child.Obj.Named() == source {
								found = true
								if child.Doc != nil {
									t.Fatal("synthetic C gained a document")
								}
							}
						}
						wantDocs := 0
						if tc.status == store.StatusReady {
							wantDocs = 2
						}
						if !found || len(retained.Manifests) != wantDocs {
							t.Fatal("actual H completion lost C or output")
						}
						authored := false
						for _, doc := range retained.Manifests {
							if manifest.DocKind(doc) == manifest.KindHelmChart {
								name, namespace := manifest.DocMetadata(doc)
								if name == source.Name {
									t.Fatal("synthetic C leaked to output")
								}
								authored = true
								if _, ok := st.GetObject(manifest.NamedResource{Group: manifest.SourceDomain, Kind: manifest.KindHelmChart, Namespace: namespace, Name: name}).(*manifest.HelmChartSource); !ok {
									t.Fatal("authored C lost typed admission")
								}
							}
						}
						if authored != (tc.status == store.StatusReady) {
							t.Fatal("authored C document was dropped")
						}
						if st.GetObject(source) == nil {
							t.Fatal("synthetic C unavailable after H completes")
						}
						if info, ok := st.GetStatus(hr.Named()); !ok || info.Status != tc.status {
							t.Errorf("consumer %s status = %+v (ok=%v), want %v", hr.Named(), info, ok, tc.status)
						}
					}
				})
			})
		}
	}
}

// TestMaterializeHelmChartSource_LateArrival pins the contract reconcile relies
// on: materialize is a no-op while the HelmRepository is absent from the Store
// (e.g. render-emitted by a sibling and not yet landed) and repoints only once
// it's present. reconcile awaits the declared source to Ready before calling
// materialize, so this present-then-repoint path is the one it always takes —
// no loop needed.
func TestMaterializeHelmChartSource_LateArrival(t *testing.T) {
	t.Parallel()
	c, st := newTestController(t, nil)
	hr := &manifest.HelmRelease{
		Name: "kromgo", Namespace: "apps",
		Chart: manifest.HelmChart{Name: "kromgo", Version: "3.0.0", Source: manifest.NamedResource{Group: manifest.SourceDomain, Kind: manifest.KindHelmRepository, Namespace: "flux-system", Name: "truecharts"}},
	}

	// Pass 1: HelmRepository not in the Store yet — must not repoint.
	if _, repointed, err := c.newExecution(hr.Named()).materializeHelmChartSource(hr.Named(), hr); err != nil || repointed {
		t.Fatal("repointed while the HelmRepository was absent (must be a no-op)")
	}

	// HelmRepository arrives mid-run (render-emitted / lazily promoted).
	st.AddObject(&manifest.HelmRepository{
		Name: "truecharts", Namespace: "flux-system",
		URL:  "oci://oci.trueforge.org/truecharts",
		Type: sourcev1.HelmRepositoryTypeOCI,
	})

	// Pass 2: now present — must repoint to the synthetic HelmChart.
	st.AddObject(hr)
	if _, repointed, err := c.newExecution(hr.Named()).materializeHelmChartSource(hr.Named(), hr); err != nil || !repointed {
		t.Fatal("did not repoint once the HelmRepository was present")
	}
}

// TestMaterializeHelmChartSource_HTTPRepoMaterializes pins the behavior flip:
// a type=default (HTTP) HelmRepository chart is now ALSO materialized into a
// synthetic HelmChart (previously it was left to an inline helm.Client pull).
// The oci-vs-http decision is the fetcher's; the controller repoints uniformly.
func TestMaterializeHelmChartSource_HTTPRepoMaterializes(t *testing.T) {
	t.Parallel()
	c, st := newTestController(t, nil)
	st.AddObject(&manifest.HelmRepository{
		Name: "bitnami", Namespace: "flux-system",
		URL: "https://charts.example/bitnami",
	})
	hr := &manifest.HelmRelease{
		Name: "redis", Namespace: "apps",
		Chart: manifest.HelmChart{Name: "redis", Version: "1.0.0", Source: manifest.NamedResource{Group: manifest.SourceDomain, Kind: manifest.KindHelmRepository, Namespace: "flux-system", Name: "bitnami"}},
	}
	st.AddObject(hr)
	got, repointed, materializeErr := c.newExecution(hr.Named()).materializeHelmChartSource(hr.Named(), hr)
	assert.NoError(t, materializeErr)
	if !repointed || got.Chart.Source.Kind != manifest.KindHelmChart {
		t.Fatalf("HTTP HelmRepository was not materialized: %+v (repointed=%v)", got.Chart, repointed)
	}
	if hc, ok := st.GetObject(got.ChartSource()).(*manifest.HelmChartSource); !ok || hc.SourceRef.Name != "bitnami" {
		t.Errorf("synthetic HelmChart does not reference the bitnami repo: %v", got.Chart)
	}
}

// TestMaterializeHelmChartSource_EmptyVersion guards the HelmChart prerequisite
// placeholder gotcha: a HelmRepository chart with no version still
// materializes into a (versionless) HelmChart rather than being mistaken for
// an unresolved chartRef:HelmChart placeholder.
func TestMaterializeHelmChartSource_EmptyVersion(t *testing.T) {
	t.Parallel()
	c, st := newTestController(t, nil)
	st.AddObject(&manifest.HelmRepository{
		Name: "bitnami", Namespace: "flux-system",
		URL: "https://charts.example/bitnami",
	})
	hr := &manifest.HelmRelease{
		Name: "redis", Namespace: "apps",
		Chart: manifest.HelmChart{Name: "redis", Source: manifest.NamedResource{Group: manifest.SourceDomain, Kind: manifest.KindHelmRepository, Namespace: "flux-system", Name: "bitnami"}},
	}
	st.AddObject(hr)
	got, repointed, materializeErr := c.newExecution(hr.Named()).materializeHelmChartSource(hr.Named(), hr)
	assert.NoError(t, materializeErr)
	if !repointed || got.Chart.Source.Kind != manifest.KindHelmChart {
		t.Fatalf("empty-version HelmRepository chart not materialized: %+v (repointed=%v)", got.Chart, repointed)
	}
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
			var filter *change.Filter
			if !tc.suspend {
				filter = change.NewFilter(change.NewSet(nil), &manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{}}, filtertest.MapLister{}, "")
			}
			c, st := newTestController(t, filter)
			hr := &manifest.HelmRelease{Name: "demo", Namespace: "default", Suspend: tc.suspend}
			st.AddObject(hr)
			info := controllertest.DispatchToFixpoint(t.Context(), t, controllertest.Dispatch{Tasks: c.Tasks, Store: st, ID: hr.Named(), ReconcileNode: c.ReconcileNode})
			if info.Status != store.StatusReady || info.Message != tc.name {
				t.Fatalf("status = %+v, want Ready/%s", info, tc.name)
			}
		})
	}
}

func TestController_AllowMissingSecretsOmitsUnavailableValuesFrom(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	testutil.WriteFile(t, dir, "mychart/Chart.yaml", `apiVersion: v2
name: mychart
version: 0.1.0
`)
	testutil.WriteFile(t, dir, "mychart/templates/configmap.yaml", `apiVersion: v1
kind: ConfigMap
metadata:
  name: {{ .Release.Name }}-cm
data:
  kept: {{ .Values.kept | quote }}
  fallback: {{ .Values.missing | default "fallback" | quote }}
`)

	c, st := newTestControllerWithOptions(t, base.Options{AllowMissingSecrets: true})
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
		Name: "demo", Namespace: "default",
		Interval: metav1Duration(time.Hour),
		ValuesFrom: []manifest.ValuesReference{
			{Kind: manifest.KindConfigMap, Name: "present-values"},
			{Kind: manifest.KindSecret, Name: "app-values"},
			{Kind: manifest.KindConfigMap, Name: "runtime-values"},
		},
		Chart: manifest.HelmChart{Name: "mychart", Source: manifest.NamedResource{Group: manifest.SourceDomain, Kind: manifest.KindGitRepository, Namespace: "flux-system", Name: "charts"}},
	}
	st.AddObject(hr)

	info := controllertest.DispatchToFixpoint(t.Context(), t, controllertest.Dispatch{Tasks: c.Tasks, Store: st, ID: hr.Named(), ReconcileNode: c.ReconcileNode})
	if info.Status != store.StatusReady {
		t.Fatalf("status = %+v, want StatusReady", info)
	}
	if info.Outcome() == store.OutcomeSkipped {
		t.Fatalf("generated valuesFrom refs should be omitted, not skip the HelmRelease: %+v", info)
	}
	art, ok := st.GetArtifact(hr.Named()).(*store.Emission)
	if !ok {
		t.Fatal("HelmRelease artifact was not written")
	}
	if got := renderedConfigMapValue(art.Manifests, "kept"); got != "kept-value" {
		t.Fatalf("rendered kept = %q, want kept-value", got)
	}
	if got := renderedConfigMapValue(art.Manifests, "fallback"); got != "fallback" {
		t.Fatalf("rendered fallback = %q, want fallback", got)
	}
	assert.Diff(t, st.Warnings(), []manifest.Warning{
		{Resource: hr.Named(), Category: manifest.WarnOmittedValues, Message: "required valuesFrom ConfigMap/default/runtime-values omitted from offline render", Count: 1},
		{Resource: hr.Named(), Category: manifest.WarnOmittedValues, Message: "required valuesFrom Secret/default/app-values omitted from offline render", Count: 1},
	})
	warnings := st.Warnings()
	hr = hr.Clone()
	hr.Labels = map[string]string{"owner": "re-emitted"}
	st.AddObject(hr)
	assert.Equal(t, controllertest.DispatchToFixpoint(t.Context(), t, controllertest.Dispatch{Tasks: c.Tasks, Store: st, ID: hr.Named(), ReconcileNode: c.ReconcileNode}).Status, store.StatusReady)
	latest, _ := st.GetArtifact(hr.Named()).(*store.Emission)
	if latest == nil || latest.Fingerprint != art.Fingerprint {
		t.Fatal("metadata-only update rendered different effective inputs")
	}
	assert.Diff(t, st.Warnings(), warnings)
	hr = hr.Clone()
	hr.ValuesFrom = append(hr.ValuesFrom, manifest.ValuesReference{Kind: manifest.KindSecret, Name: "another-values"})
	st.AddObject(hr)
	assert.Equal(t, controllertest.DispatchToFixpoint(t.Context(), t, controllertest.Dispatch{Tasks: c.Tasks, Store: st, ID: hr.Named(), ReconcileNode: c.ReconcileNode}).Status, store.StatusReady)
	if !slices.ContainsFunc(st.Warnings(), func(w manifest.Warning) bool {
		return w.Message == "required valuesFrom Secret/default/another-values omitted from offline render" && w.Count == 1
	}) {
		t.Fatalf("new omitted reference did not produce a Count 1 warning: %+v", st.Warnings())
	}
}

func TestController_ValuesOmissionPreservesRequiredChartFailure(t *testing.T) {
	t.Parallel()
	c, st := newTestControllerWithOptions(t, base.Options{AllowMissingSecrets: true})
	chart := &manifest.HelmChartSource{Name: "chart", Namespace: "apps"}
	st.AddObject(chart)
	st.UpdateStatus(chart.Named(), store.StatusInfo{Status: store.StatusFailed, Reason: "", Message: "chart failed"})
	hr := &manifest.HelmRelease{
		Name: "consumer", Namespace: "apps",
		Chart:      manifest.HelmChart{Source: manifest.NamedResource{Group: manifest.SourceDomain, Kind: manifest.KindHelmChart, Namespace: "apps", Name: chart.Name}},
		ValuesFrom: []manifest.ValuesReference{{Kind: manifest.KindSecret, Name: "values"}},
	}
	st.AddObject(hr)
	values := manifest.NamedResource{Kind: manifest.KindSecret, Namespace: "apps", Name: "values"}
	for _, level := range []manifest.DrainLevel{manifest.DrainNone, manifest.DrainCascade} {
		err := c.newExecution(hr.Named()).reconcile(t.Context(), hr, level)
		failed, ok := errors.AsType[*manifest.DependencyFailedError](err)
		if !ok || !slices.Equal(failed.Failed, []manifest.NamedResource{chart.Named()}) || failed.Reasons[chart.Named()] != "chart failed" {
			t.Fatalf("required chart failure changed: %v", err)
		}
	}

	if len(hr.ValuesFrom) != 1 || hr.ValuesFrom[0].Name != values.Name {
		t.Fatal("omission mutated stored valuesFrom")
	}
}

// Without --allow-missing-secrets, a valuesFrom Secret whose only declaration
// is an in-repo ExternalSecret (a producer) is omitted so the render proceeds —
// the producer is positive evidence the Secret materializes live. This is the
// flag-decoupled path: producer-backed refs are dropped regardless of the flag.
func TestController_ProducerBackedValuesFromOmittedWithoutFlag(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	testutil.WriteFile(t, dir, "mychart/Chart.yaml", "apiVersion: v2\nname: mychart\nversion: 0.1.0\n")
	testutil.WriteFile(t, dir, "mychart/templates/configmap.yaml", `apiVersion: v1
kind: ConfigMap
metadata:
  name: {{ .Release.Name }}-cm
data:
  kept: {{ .Values.kept | quote }}
  fallback: {{ .Values.missing | default "fallback" | quote }}
`)

	// No flag — the producer index is the only reason the ref is dropped.
	c, st := newTestControllerWithOptions(t, base.Options{})
	src := &manifest.GitRepository{Name: "charts", Namespace: "flux-system"}
	st.AddObject(src)
	st.SetArtifact(src.Named(), &store.SourceArtifact{
		Kind: manifest.KindGitRepository, URL: "file://" + dir, FS: os.DirFS(dir), LocalPath: dir,
	})
	st.UpdateStatus(src.Named(), store.StatusInfo{Status: store.StatusReady, Reason: "", Message: ""})
	st.AddObject(&manifest.ConfigMap{
		Name: "present-values", Namespace: "default",
		Data: map[string]any{"values.yaml": "kept: kept-value\n"},
	})
	// The in-repo ExternalSecret declaring the (absent) app-values Secret —
	// AddObject fires the producer listener, seeding the shared index.
	publishRawFixture(t, c, &manifest.RawObject{
		Kind: "ExternalSecret", APIVersion: "external-secrets.io/v1",
		Name: "app-creds", Namespace: "default",
		Spec: map[string]any{"target": map[string]any{"name": "app-values"}},
	})
	hr := &manifest.HelmRelease{
		Name: "demo", Namespace: "default",
		Interval: metav1Duration(time.Hour),
		ValuesFrom: []manifest.ValuesReference{
			{Kind: manifest.KindConfigMap, Name: "present-values"},
			{Kind: manifest.KindSecret, Name: "app-values"},
		},
		Chart: manifest.HelmChart{Name: "mychart", Source: manifest.NamedResource{Group: manifest.SourceDomain, Kind: manifest.KindGitRepository, Namespace: "flux-system", Name: "charts"}},
	}
	st.AddObject(hr)

	info := controllertest.DispatchToFixpoint(t.Context(), t, controllertest.Dispatch{Tasks: c.Tasks, Store: st, ID: hr.Named(), ReconcileNode: c.ReconcileNode})
	if info.Status != store.StatusReady || info.Outcome() == store.OutcomeSkipped {
		t.Fatalf("status = %+v, want StatusReady (producer-backed ref omitted, not skipped/failed)", info)
	}
	art, ok := st.GetArtifact(hr.Named()).(*store.Emission)
	if !ok {
		t.Fatal("HelmRelease artifact was not written")
	}
	if got := renderedConfigMapValue(art.Manifests, "kept"); got != "kept-value" {
		t.Fatalf("rendered kept = %q, want kept-value", got)
	}
	if got := renderedConfigMapValue(art.Manifests, "fallback"); got != "fallback" {
		t.Fatalf("rendered fallback = %q, want fallback", got)
	}
}

// Fail-loud safety: without the flag, a missing valuesFrom Secret that has NO
// declared producer is kept, waited on, and ultimately fails — producer
// inference must not silently swallow a genuine typo / misconfiguration.
func TestController_MissingValuesFromNoProducerFailsWithoutFlag(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	testutil.WriteFile(t, dir, "mychart/Chart.yaml", "apiVersion: v2\nname: mychart\nversion: 0.1.0\n")
	testutil.WriteFile(t, dir, "mychart/templates/configmap.yaml",
		"apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: {{ .Release.Name }}-cm\n")

	c, st := newTestControllerWithOptions(t, base.Options{})
	src := &manifest.GitRepository{Name: "charts", Namespace: "flux-system"}
	st.AddObject(src)
	st.SetArtifact(src.Named(), &store.SourceArtifact{
		Kind: manifest.KindGitRepository, URL: "file://" + dir, FS: os.DirFS(dir), LocalPath: dir,
	})
	st.UpdateStatus(src.Named(), store.StatusInfo{Status: store.StatusReady, Reason: "", Message: ""})
	hr := &manifest.HelmRelease{
		Name: "demo", Namespace: "default",
		Interval:   metav1Duration(time.Hour),
		ValuesFrom: []manifest.ValuesReference{{Kind: manifest.KindSecret, Name: "no-producer"}},
		Chart:      manifest.HelmChart{Name: "mychart", Source: manifest.NamedResource{Group: manifest.SourceDomain, Kind: manifest.KindGitRepository, Namespace: "flux-system", Name: "charts"}},
	}
	st.AddObject(hr)

	info := controllertest.DispatchToFixpoint(t.Context(), t, controllertest.Dispatch{Tasks: c.Tasks, Store: st, ID: hr.Named(), ReconcileNode: c.ReconcileNode})
	if info.Status == store.StatusReady {
		t.Fatalf("status = %+v, want failure (missing no-producer Secret must not be silently omitted)", info)
	}
}

// TestController_HelmChartSourceResolvedViaResolver locks the iter-16
// contract: the HR controller no longer maintains a chartSources
// push-registry; HelmChartSource lookups go through
// helm.Client.Resolver().HelmChart against the canonical Store. After
// AddObject lands a HelmChartSource the resolver MUST surface it
// immediately — that's what helm.Prepare relies on at reconcile time.
func TestController_HelmChartSourceResolvedViaResolver(t *testing.T) {
	t.Parallel()
	c, st := newTestController(t, nil)
	src := &manifest.HelmChartSource{
		Name: "podinfo", Namespace: "flux-system",
		Chart:     "podinfo",
		Version:   "6.3.2",
		SourceRef: sourcev1.LocalHelmChartSourceReference{Name: "podinfo", Kind: manifest.KindHelmRepository},
	}
	st.AddObject(src)

	resolver := helm.NewSources(c.Store)
	got := resolver.HelmChart(src.Named())
	if got == nil {
		t.Fatalf("resolver.HelmChart(%q, %q) returned nil; expected the just-added source", src.Namespace, src.Name)
	}
	if got.Chart != "podinfo" {
		t.Errorf("resolver returned wrong source: %+v", got)
	}
}

// TestHelmReleaseFingerprint_StableAcrossLabelStamping locks the
// dedup contract: an HR re-AddObject'd with kustomize ownership
// labels (the typical pattern when the parent KS emits a
// re-stamped copy) must produce the same fingerprint as the file-
// loaded original — otherwise the dedup short-circuit can't fire
// and helm.Template runs twice for one logical release.
func TestHelmReleaseFingerprint_StableAcrossLabelStamping(t *testing.T) {
	t.Parallel()
	base := &manifest.HelmRelease{
		Name: "demo", Namespace: "default",
		Interval: metav1Duration(time.Hour),
		Chart:    manifest.HelmChart{Name: "podinfo", Source: manifest.NamedResource{Group: manifest.SourceDomain, Kind: manifest.KindHelmRepository, Namespace: "flux-system", Name: "podinfo"}},
		Values:   map[string]any{"replicas": 2},
	}
	stamped := base.Clone()
	stamped.Labels = map[string]string{
		"kustomize.toolkit.fluxcd.io/name":      "parent-ks",
		"kustomize.toolkit.fluxcd.io/namespace": "flux-system",
	}
	stamped.Annotations = map[string]string{"reconcile.fluxcd.io/requestedAt": "now"}

	if got, want := replayKey(stamped.Content(nil, nil)), replayKey(base.Content(nil, nil)); got != want {
		t.Errorf("fingerprint changed under label/annotation stamping; got %q want %q", got, want)
	}
}

// TestHelmReleaseFingerprint_DifferentOnSpecChange flips the
// invariant: if patches mutate spec (tholinka's cluster KS pattern
// — driftDetection, install.crds, upgrade.* injected via
// kustomize), the fingerprint MUST differ so the controller
// renders the canonical post-patch values.
func TestHelmReleaseFingerprint_DifferentOnSpecChange(t *testing.T) {
	t.Parallel()
	base := &manifest.HelmRelease{
		Name: "demo", Namespace: "default",
		Interval: metav1Duration(time.Hour),
		Chart:    manifest.HelmChart{Name: "podinfo"},
	}
	patched := base.Clone()
	patched.DriftDetection = &helmv2.DriftDetection{Mode: helmv2.DriftDetectionEnabled}

	if got := replayKey(base.Content(nil, nil)); got == replayKey(patched.Content(nil, nil)) {
		t.Errorf("fingerprint should differ when spec.driftDetection mutates; both = %q", got)
	}
}

func TestHelmReleaseFingerprint_SourceIdentity(t *testing.T) {
	t.Parallel()
	hr := &manifest.HelmRelease{Name: "demo", Namespace: "apps"}
	const digest = "sha256:ff3d3e14728f75476ed4d43c14f80d52d81d36bc16906843463d464c6146f0d8"
	art := &store.SourceArtifact{Digest: digest, Revision: "6.15.0@" + digest, LocalPath: "/chart"}
	fp := replayKey(hr.Content(struct{ Digest, Revision string }{art.Digest, art.Revision}, nil))
	for _, tc := range []struct {
		name     string
		edit     func(*store.SourceArtifact)
		wantSame bool
	}{
		{"digest", func(a *store.SourceArtifact) { a.Digest = digest[:len(digest)-1] + "9" }, false},
		{"revision", func(a *store.SourceArtifact) { a.Revision = "invalid" }, false},
		{"local path", func(a *store.SourceArtifact) { a.LocalPath = "/other" }, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			changed := *art
			tc.edit(&changed)
			if same := replayKey(hr.Content(struct{ Digest, Revision string }{changed.Digest, changed.Revision}, nil)) == fp; same != tc.wantSame {
				t.Fatalf("fingerprint unchanged = %v, want %v", same, tc.wantSame)
			}
		})
	}
}

func metav1Duration(d time.Duration) metav1.Duration { return metav1.Duration{Duration: d} }

func renderedConfigMapValue(docs []map[string]any, key string) string {
	for _, doc := range docs {
		if manifest.DocKind(doc) != manifest.KindConfigMap {
			continue
		}
		data, _ := doc["data"].(map[string]any)
		value, _ := data[key].(string)
		if value != "" {
			return value
		}
	}
	return ""
}

func TestController_AlreadyReady_NoTransientPending(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	testutil.WriteFile(t, dir, "mychart/Chart.yaml", "apiVersion: v2\nname: mychart\nversion: 0.1.0\n")
	testutil.WriteFile(t, dir, "mychart/templates/configmap.yaml",
		"apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: {{ .Release.Name }}-cm\ndata:\n  k: v\n")

	c, st := newTestController(t, nil)
	src := &manifest.GitRepository{Name: "charts", Namespace: "flux-system"}
	st.AddObject(src)
	st.SetArtifact(src.Named(), &store.SourceArtifact{
		Kind: manifest.KindGitRepository, URL: "file://" + dir, FS: os.DirFS(dir), LocalPath: dir,
	})
	st.UpdateStatus(src.Named(), store.StatusInfo{Status: store.StatusReady, Reason: "", Message: ""})

	hr := &manifest.HelmRelease{
		Name: "demo", Namespace: "default",
		Interval: metav1Duration(time.Hour),
		Chart:    manifest.HelmChart{Name: "mychart", Source: manifest.NamedResource{Group: manifest.SourceDomain, Kind: manifest.KindGitRepository, Namespace: "flux-system", Name: "charts"}},
	}
	st.AddObject(hr)
	if info := controllertest.DispatchToFixpoint(t.Context(), t, controllertest.Dispatch{Tasks: c.Tasks, Store: st, ID: hr.Named(), ReconcileNode: c.ReconcileNode}); info.Status != store.StatusReady {
		t.Fatalf("status = %+v, want StatusReady", info)
	}

	var mu sync.Mutex
	var sawPending bool
	st.AddListener(store.EventStatusUpdated, func(id manifest.NamedResource, payload any) {
		if id != hr.Named() {
			return
		}
		if info, ok := payload.(store.StatusInfo); ok && info.Status == store.StatusPending {
			mu.Lock()
			sawPending = true
			mu.Unlock()
		}
	}, false)

	// Re-emit with stamped ownership labels: HR retains labels so the re-read
	// body re-runs, but the label-insensitive fingerprint matches → dedup-skip
	// no-op. The second ReconcileNode drives that no-op re-run synchronously.
	stamped := hr.Clone()
	stamped.Labels = map[string]string{
		"kustomize.toolkit.fluxcd.io/name":      "parent",
		"kustomize.toolkit.fluxcd.io/namespace": "flux-system",
	}
	st.AddObject(stamped)
	c.ReconcileNode(context.Background(), hr.Named(), 0)

	mu.Lock()
	pending := sawPending
	mu.Unlock()
	if pending {
		t.Error("no-op re-reconcile of an already-Ready HelmRelease transiently downgraded it to Pending (the quiescence-race window)")
	}
	if info, _ := st.GetStatus(hr.Named()); info.Status != store.StatusReady {
		t.Errorf("HR not Ready after no-op re-run: %+v", info)
	}
}

func TestController_ParentReadsUnreadableSecret_ReferencePositions(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name              string
		reverse, supplied bool
	}{
		{name: "forward"},
		{name: "reverse", reverse: true},
		{name: "forward supplied", supplied: true},
		{name: "reverse supplied", reverse: true, supplied: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			var supplied map[manifest.NamedResource]manifest.BaseManifest
			if tc.supplied {
				supplied = map[manifest.NamedResource]manifest.BaseManifest{
					{Kind: manifest.KindSecret, Namespace: "apps", Name: "owned"}:      &manifest.Secret{},
					{Kind: manifest.KindSecret, Namespace: "apps", Name: "unreadable"}: &manifest.Secret{},
				}
			}
			c, st := newTestControllerWithOptions(t, base.Options{SubstituteFrom: supplied})
			readable := &manifest.Kustomization{Name: "a-readable", Namespace: "apps"}
			unreadable := &manifest.Kustomization{Name: "z-unreadable", Namespace: "apps"}
			unreadable.PostBuild = &kustomizev1.PostBuild{SubstituteFrom: []manifest.SubstituteReference{{Kind: "Unsupported"}, {Kind: manifest.KindSecret, Name: "owned"}, {Kind: manifest.KindSecret, Name: "unreadable"}}}
			st.AddObjects([]manifest.BaseManifest{readable, unreadable})
			hr := &manifest.HelmRelease{Name: "app", Namespace: "apps"}
			parents := []*manifest.Kustomization{readable, unreadable}
			if tc.reverse {
				slices.Reverse(parents)
			}
			for _, parent := range parents {
				var unavailable []manifest.NamedResource
				if parent == unreadable {
					unavailable = []manifest.NamedResource{{Kind: manifest.KindSecret, Namespace: "apps", Name: "owned"}, {Kind: manifest.KindSecret, Namespace: "apps", Name: "unreadable"}}
				}
				c.Emit(parent.Named(), &store.Emission{Children: []store.Child{{Obj: hr}}, Complete: true, Unreadable: unavailable}, nil)
			}
			if got := c.newExecution(hr.Named()).parentReadsUnreadableSecret(hr.Named()); got == tc.supplied {
				t.Fatalf("unreadable parent secret = %t, supplied = %t", got, tc.supplied)
			}
			assert.Equal(t, len(unreadable.PostBuild.SubstituteFrom), 3)
		})
	}
}

func TestController_ChartAndValuesPrerequisites(t *testing.T) {
	t.Parallel()
	t.Run("repository", func(t *testing.T) {
		t.Parallel()
		c, st := newTestController(t, nil)
		repo := &manifest.HelmRepository{Name: "charts", Namespace: "apps"}
		st.AddObject(repo)
		st.UpdateStatus(repo.Named(), store.StatusInfo{Status: store.StatusReady, Reason: "", Message: "resolved"})
		hr := &manifest.HelmRelease{
			Name: "app", Namespace: "apps",
			HelmReleaseSpec: helmv2.HelmReleaseSpec{Chart: &helmv2.HelmChartTemplate{
				Spec: helmv2.HelmChartTemplateSpec{Chart: "app", SourceRef: helmv2.CrossNamespaceObjectReference{Kind: manifest.KindHelmRepository, Name: repo.Name}},
			}},
			Chart: manifest.HelmChart{Source: manifest.NamedResource{Group: manifest.SourceDomain, Kind: manifest.KindHelmRepository, Namespace: repo.Namespace, Name: repo.Name}},
		}
		for _, level := range []manifest.DrainLevel{manifest.DrainNone, manifest.DrainForce} {
			if err := c.newExecution((hr).Named()).requireChartForTest(t.Context(), hr, level); err != nil {
				t.Fatalf("repository required content: %v", err)
			}
		}
		for _, group := range []string{"", "example.test"} {
			apiVersion := "v1"
			if group != "" {
				apiVersion = group + "/v1"
			}
			publishRawFixture(t, c, &manifest.RawObject{APIVersion: apiVersion, Kind: manifest.KindHelmRepository, Name: repo.Name, Namespace: repo.Namespace})
			foreign := &manifest.HelmRelease{Name: hr.Name, Namespace: hr.Namespace, Chart: manifest.HelmChart{Source: manifest.NamedResource{Group: group, Kind: manifest.KindHelmRepository, Namespace: repo.Namespace, Name: repo.Name}}}
			if err := c.newExecution((foreign).Named()).requireChartForTest(t.Context(), foreign, manifest.DrainForce); !errors.Is(err, manifest.ErrObjectNotFound) {
				t.Fatalf("foreign repository bypassed content gate: %v", err)
			}
		}

	})
	c, st := newTestController(t, nil)
	hr := &manifest.HelmRelease{Name: "app", Namespace: "apps", ChartRef: &helmv2.CrossNamespaceSourceReference{Kind: manifest.KindOCIRepository, Name: "chart"}}
	st.AddObject(&manifest.OCIRepository{Name: "chart", Namespace: "apps"})
	id := manifest.NamedResource{Group: manifest.SourceDomain, Kind: manifest.KindOCIRepository, Name: "chart", Namespace: "apps"}
	st.UpdateStatus(id, store.StatusInfo{Status: store.StatusReady, Reason: "", Message: "released"})
	st.SetArtifact(id, &store.SourceArtifact{Key: "pin"})
	if blocked, ok := errors.AsType[*depwait.ErrBlocked](c.newExecution((hr).Named()).requireChartForTest(t.Context(), hr, manifest.DrainNone)); !ok || len(blocked.Deps) != 1 || blocked.Deps[0] != id {
		t.Fatalf("released source did not block on acquisition: %v", blocked)
	}
	if err := c.newExecution((hr).Named()).requireChartForTest(t.Context(), hr, manifest.DrainForce); !errors.Is(err, manifest.ErrObjectNotFound) {
		t.Fatalf("released source at fixpoint: %v", err)
	}
	st.UpdateStatus(id, store.StatusInfo{Status: store.StatusReady, Reason: store.ReasonSkipped, Message: "missing credentials"})
	assert.Equal(t, errors.Is(c.newExecution((hr).Named()).requireChartForTest(t.Context(), hr, manifest.DrainNone), manifest.ErrSourceSkipped), true)
	st.SetArtifact(id, &store.SourceArtifact{Key: "pin", FS: fstest.MapFS{}})
	st.UpdateStatus(id, store.StatusInfo{Status: store.StatusReady, Reason: "", Message: "reacquired"})
	assert.Equal(t, c.newExecution((hr).Named()).requireChartForTest(t.Context(), hr, manifest.DrainNone), nil)
	st.UpdateStatus(manifest.NamedResource{Group: manifest.SourceDomain, Kind: manifest.KindOCIRepository, Name: "chart", Namespace: "apps"}, store.StatusInfo{Status: store.StatusPending, Reason: "", Message: "fetching"})
	_, blocked := errors.AsType[*depwait.ErrBlocked](c.newExecution((hr).Named()).requireChartForTest(t.Context(), hr, manifest.DrainNone))
	assert.Equal(t, blocked, true)
	hr.ChartRef.Kind = manifest.KindHelmChart
	hr.ValuesFrom = []manifest.ValuesReference{{Kind: manifest.KindConfigMap, Name: "optional", Optional: true}, {Kind: manifest.KindSecret, Name: "required"}}
	for _, version := range []string{"", "1.0.0"} {
		hr.Chart.Version = version
		want := []manifest.NamedResource{{Kind: manifest.KindSecret, Namespace: "apps", Name: "required"}}
		if version == "" {
			want = []manifest.NamedResource{{Group: manifest.SourceDomain, Kind: manifest.KindHelmChart, Namespace: "apps", Name: "chart"}}
		}
		st.AddObject(hr.Clone())
		err := c.newExecution(hr.Named()).reconcile(t.Context(), hr, manifest.DrainNone)
		blocked, ok := errors.AsType[*depwait.ErrBlocked](err)
		if !ok {
			t.Fatalf("pre-render gate=%v", err)
		}
		assert.Diff(t, blocked.Deps, want)
	}
}

func replayKey(content any) string {
	c := base.New(store.New(), nil)
	fp, _, _ := controllertest.NewExecution(c, manifest.NamedResource{}).FingerprintDedup(manifest.NamedResource{}, content)
	return fp
}

func TestRawProducerIndex_StoreListener(t *testing.T) {
	t.Parallel()

	c, st := newTestControllerWithOptions(t, base.Options{})
	target := manifest.NamedResource{Kind: manifest.KindSecret, Namespace: "default", Name: "app-values"}
	if got := st.Productions(target, nil); len(got) != 0 {
		t.Fatalf("empty store reported productions: %v", got)
	}
	producer := &manifest.RawObject{
		Kind: "ExternalSecret", APIVersion: "external-secrets.io/v1",
		Name: "app-creds", Namespace: "default",
		Spec: map[string]any{"target": map[string]any{"name": target.Name}},
	}
	publishRawFixture(t, c, producer)
	if got := st.Productions(target, nil); len(got) != 1 || got[0].Declaration != producer.Named() {
		t.Fatalf("productions = %v, want declaration %v", got, producer.Named())
	}
}

func (c *execution) requireChartForTest(ctx context.Context, hr *manifest.HelmRelease, level manifest.DrainLevel) error {
	c.Store.AddObject(hr)
	_, err := c.run.RequireRefresh(ctx, hr, manifest.GateArtifacts, level)
	return err
}

func TestMaterializeHelmChartSource_RequiresCapturedInput(t *testing.T) {
	t.Parallel()
	for _, state := range []string{"replacement", "absent", "suspended"} {
		t.Run(state, func(t *testing.T) {
			t.Parallel()
			c, st := newTestController(t, nil)
			input := &manifest.HelmRelease{Name: "input", Namespace: "apps", Chart: manifest.HelmChart{Name: "chart", Source: manifest.NamedResource{Group: manifest.SourceDomain, Kind: manifest.KindHelmRepository, Name: "absent"}}}
			st.AddObject(input)
			run := c.newExecution(input.Named())
			run.run.Reads.GetDeclaration(input.Named())
			attempt := captureEmission(st, input.Named(), &store.Emission{})
			defer attempt.Finish()
			replacement := input.Clone()
			replacement.Chart.Name, replacement.Suspend = "replacement", state == "suspended"
			if state == "absent" {
				st.DeleteObject(input.Named())
			} else {
				st.AddObject(replacement)
			}
			var entered bool
			run.run.Dispatch(t.Context(), manifest.DrainNone, struct{}{}, func(_ struct{}, _ context.Context, hr *manifest.HelmRelease, _ manifest.DrainLevel) error {
				entered = true
				run.materializeHelmChartSource(hr.Named(), hr.Clone())
				return nil
			})
			if entered || run.run.Valid() || st.GetArtifact(input.Named()) != nil {
				t.Fatalf("stale synthesis reached body: entered=%t valid=%t", entered, run.run.Valid())
			}

		})
	}
}
