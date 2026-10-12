package helmrelease

import (
	"context"
	"errors"
	"slices"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	helmv2 "github.com/fluxcd/helm-controller/api/v2"
	sourcev1 "github.com/fluxcd/source-controller/api/v1"
	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/change"
	"github.com/home-operations/flate/pkg/depwait"
	"github.com/home-operations/flate/pkg/helm"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/schedule"
	"github.com/home-operations/flate/pkg/source/cacheroot"
	"github.com/home-operations/flate/pkg/store"
	"github.com/home-operations/flate/pkg/task"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func newTestController(t *testing.T, filter *change.Filter) (*Controller, *store.Store) {
	t.Helper()
	return newTestControllerWithOptions(t, ReconcileOptions{Filter: filter})
}

type indexedOrderingDeps map[manifest.NamedResource]bool

func (i indexedOrderingDeps) IsFileIndexed(id manifest.NamedResource) bool { return i[id] }
func (indexedOrderingDeps) Promote(manifest.NamedResource) bool            { return false }

func TestController_CollectHRDepsOrderingFilter(t *testing.T) {
	for _, tc := range []struct {
		name                      string
		active, selected, indexed bool
		present                   bool
		status                    store.Status
		retain                    bool
	}{
		{name: "unfiltered missing", retain: true},
		{name: "known file excluded", active: true, indexed: true},
		{name: "render generated excluded", active: true, present: true},
		{name: "unknown retained", active: true, retain: true},
		{name: "selected ready", active: true, selected: true, present: true, status: store.StatusReady, retain: true},
		{name: "selected failed", active: true, selected: true, present: true, status: store.StatusFailed, retain: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			target := &manifest.HelmRelease{Name: "target", Namespace: "apps"}
			var changes *change.Set
			if tc.active {
				files := []string{"consumer.yaml"}
				if tc.selected {
					files = append(files, "target.yaml")
				}
				changes = change.NewSet(files)
			}
			hr := &manifest.HelmRelease{Name: "consumer", Namespace: "apps", DependsOn: []manifest.DependencyRef{{NamedResource: target.Named(), ReadyExpr: "dep.spec.suspend == false"}}}
			filter := change.NewFilter(changes, &manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{hr.Named(): {File: "consumer.yaml"}, target.Named(): {File: "target.yaml"}}}, testutil.MapLister{})
			c, st := newTestControllerWithOptions(t, ReconcileOptions{Filter: filter, Existence: indexedOrderingDeps{target.Named(): tc.indexed}})
			if tc.present {
				st.AddObject(target)
				st.UpdateStatus(target.Named(), tc.status, "fixture")
			}
			size := len(filter.KeepNames())
			deps := c.OrderingDeps(hr.DependsOn)
			if got := slices.Contains(deps, hr.DependsOn[0]); got != tc.retain {
				t.Errorf("ordering edge retained = %v, want %v; deps = %v", got, tc.retain, deps)
			}
			if len(filter.KeepNames()) != size || len(hr.DependsOn) != 1 {
				t.Error("ordering collection must preserve keep and input edges")
			}
		})
	}
}

func newTestControllerWithParentOf(t *testing.T, parentOf map[manifest.NamedResource]manifest.NamedResource) (*Controller, *store.Store) {
	t.Helper()
	resolver := func(id manifest.NamedResource) (manifest.NamedResource, bool) {
		parent, ok := parentOf[id]
		return parent, ok
	}
	return newTestControllerWithOptions(t, ReconcileOptions{ParentOf: resolver})
}

func newTestControllerWithOptions(t *testing.T, opts ReconcileOptions) (*Controller, *store.Store) {
	t.Helper()
	st := store.New()
	ts := task.NewBounded(0)
	hc, err := helm.NewClient(cacheroot.New(t.TempDir()))
	if err != nil {
		t.Fatalf("helm.NewClient: %v", err)
	}
	// Mirror the orchestrator's production wiring: the HR controller
	// resolves source CRs (HelmRepository, OCIRepository, HelmChart)
	// through the Store via the helm client's SourceResolver.
	hc.SetSourceResolver(helm.NewStoreSourceResolver(st))
	// Mirror the orchestrator, which always injects a producer index; an
	// absent one is nil-safe but would silently record nothing, so the
	// producer-index tests need a live instance.
	if opts.Producers == nil {
		opts.Producers = &manifest.ProducerIndex{}
	}
	c := New(st, ts, hc, helm.Options{}, false)
	c.Configure(opts)
	c.Start(context.Background())
	t.Cleanup(func() {
		c.Close()
		ts.BlockTillDone()
	})
	return c, st
}

// dispatchToFixpoint drives id through ReconcileNode with escalating drain
// levels (none→cascade→force), mirroring the scheduler's structural fixpoint,
// until the node terminalizes (no blocked deps) or drain is exhausted, then
// returns the final store status. Synchronous — replaces the event engine's
// AddObject→listener→status-poll dispatch in unit tests.
func dispatchToFixpoint(t *testing.T, c *Controller, st *store.Store, id manifest.NamedResource) store.StatusInfo {
	t.Helper()
	for _, drain := range []manifest.DrainLevel{manifest.DrainNone, manifest.DrainCascade, manifest.DrainForce} {
		if blocked := c.ReconcileNode(context.Background(), id, drain); len(blocked) == 0 {
			break
		}
	}
	info, _ := st.GetStatus(id)
	return info
}

func TestMaterializeHelmChartSource_RepointsAndRegisters(t *testing.T) {
	hr := &manifest.HelmRelease{
		Name: "kromgo", Namespace: "apps",
		Chart: manifest.HelmChart{
			RepoKind:      manifest.KindHelmRepository,
			RepoNamespace: "flux-system",
			RepoName:      "truecharts",
			Name:          "kromgo",
			Version:       "3.0.0",
		},
	}

	filter := change.NewFilter(change.NewSet([]string{"release.yaml"}), &manifest.DiscoveryIndex{
		Records: map[manifest.NamedResource]manifest.DiscoveryRecord{hr.Named(): {File: "release.yaml"}},
	}, testutil.MapLister{})
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

	got, repointed := c.materializeHelmChartSource(hr.Named(), hr)
	if !repointed {
		t.Fatal("materialize did not report a repoint for a HelmRepository-sourced chart")
	}

	// Chart repointed to a synthetic HelmChart; name/version kept.
	if got.Chart.RepoKind != manifest.KindHelmChart {
		t.Fatalf("RepoKind = %q, want HelmChart", got.Chart.RepoKind)
	}
	if got.Chart.RepoNamespace != "flux-system" {
		t.Errorf("RepoNamespace = %q, want flux-system", got.Chart.RepoNamespace)
	}
	if got.Chart.Name != "kromgo" || got.Chart.Version != "3.0.0" {
		t.Errorf("chart name/version = %q/%q, want kromgo/3.0.0", got.Chart.Name, got.Chart.Version)
	}

	synID := manifest.NamedResource{Kind: manifest.KindHelmChart, Namespace: got.Chart.RepoNamespace, Name: got.Chart.RepoName}
	obj := st.GetObjectByName(manifest.KindHelmChart, synID.Namespace, synID.Name)
	if obj == nil {
		t.Fatalf("synthetic HelmChart %s not added to store", synID)
	}
	if hc, ok := obj.(*manifest.HelmChartSource); !ok {
		t.Fatalf("stored object is %T, want *manifest.HelmChartSource", obj)
	} else if hc.Chart != "kromgo" || hc.Version != "3.0.0" ||
		hc.SourceRef.Kind != manifest.KindHelmRepository || hc.SourceRef.Name != "truecharts" {
		t.Errorf("synthetic HelmChart = %+v, want chart kromgo@3.0.0 sourceRef HelmRepository/truecharts", hc)
	}
	if owner, ok := st.EmittedParent(synID); ok {
		t.Errorf("synthetic chart has render owner %v", owner)
	}
	if info, ok := st.GetStatus(synID); ok {
		t.Errorf("consumer wrote synthetic status = %+v, want no Ready condition", info)
	}
}

type chartSourceDispatcher func(context.Context, schedule.NodeID, manifest.DrainLevel) (schedule.Outcome, []schedule.NodeID, bool)

func (d chartSourceDispatcher) Dispatch(ctx context.Context, id schedule.NodeID, level manifest.DrainLevel) (schedule.Outcome, []schedule.NodeID, bool) {
	return d(ctx, id, level)
}

func TestMaterializeHelmChartSource_SharedSourceCompletes(t *testing.T) {
	for _, tc := range []struct {
		name   string
		status store.Status
	}{
		{"ready", store.StatusReady},
		{"failed", store.StatusFailed},
	} {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				c, st := newTestController(t, nil)
				st.AddObject(&manifest.HelmRepository{
					Name: "bitnami", Namespace: "flux-system", URL: "https://charts.example/bitnami",
				})
				consumers := []*manifest.HelmRelease{
					{Name: "redis-a", Namespace: "apps"},
					{Name: "redis-b", Namespace: "apps"},
				}
				chart := manifest.HelmChart{
					RepoKind: manifest.KindHelmRepository, RepoNamespace: "flux-system", RepoName: "bitnami",
					Name: "redis", Version: "1.0.0",
				}
				for _, hr := range consumers {
					hr.Chart = chart
					st.AddObject(hr)
				}
				first := *consumers[0]
				c.materializeHelmChartSource(first.Named(), &first)
				source := chartSourceID(&first)
				sourceStarted := make(chan struct{})
				sourceRelease := make(chan struct{})
				parked := make(chan manifest.NamedResource, len(consumers))
				dispatch := chartSourceDispatcher(func(ctx context.Context, id schedule.NodeID, level manifest.DrainLevel) (schedule.Outcome, []schedule.NodeID, bool) {
					if id == source {
						close(sourceStarted)
						<-sourceRelease
						if tc.status == store.StatusReady {
							st.SetArtifact(id, &store.SourceArtifact{})
						}
						st.UpdateStatus(id, tc.status, "source finished")
						return schedule.OutcomeTerminal, nil, false
					}
					hr := *st.GetObject(id).(*manifest.HelmRelease)
					c.materializeHelmChartSource(id, &hr)
					err := c.awaitChartSource(ctx, &hr, level)
					if blocked, ok := errors.AsType[*depwait.ErrBlocked](err); ok {
						parked <- id
						return schedule.OutcomeBlocked, blocked.Deps, false
					}
					if err != nil {
						st.UpdateStatus(id, store.StatusFailed, err.Error())
						return schedule.OutcomeDependencyFailed, []schedule.NodeID{source}, false
					}
					st.UpdateStatus(id, store.StatusReady, "")
					return schedule.OutcomeTerminal, nil, false
				})
				s := schedule.New(task.NewBounded(2), dispatch)
				s.Seed([]schedule.NodeID{source})
				done := make(chan error, 1)
				go func() { done <- s.Run(t.Context()) }()
				<-sourceStarted
				for _, hr := range consumers {
					s.OnArrival(hr.Named(), true)
				}
				second := *consumers[1]
				c.materializeHelmChartSource(second.Named(), &second)
				if chartSourceID(&second) != source {
					t.Errorf("second source = %s, want shared %s", chartSourceID(&second), source)
				}
				if info, ok := st.GetStatus(source); ok {
					t.Errorf("consumer wrote synthetic status = %+v, want no Ready condition", info)
				}
				for range consumers {
					<-parked
				}
				synctest.Wait()
				close(sourceRelease)
				if err := <-done; err != nil {
					t.Fatal(err)
				}
				if info, ok := st.GetStatus(source); !ok || info.Status != tc.status || info.Message != "source finished" {
					t.Errorf("source status = %+v (ok=%v), want preserved %v", info, ok, tc.status)
				}
				for _, hr := range consumers {
					if info, ok := st.GetStatus(hr.Named()); !ok || info.Status != tc.status {
						t.Errorf("consumer %s status = %+v (ok=%v), want %v", hr.Named(), info, ok, tc.status)
					}
				}
			})
		})
	}
}

// TestMaterializeHelmChartSource_LateArrival pins the contract reconcile relies
// on: materialize is a no-op while the HelmRepository is absent from the Store
// (e.g. render-emitted by a sibling and not yet landed) and repoints only once
// it's present. reconcile awaits the declared source to Ready before calling
// materialize, so this present-then-repoint path is the one it always takes —
// no loop needed.
func TestMaterializeHelmChartSource_LateArrival(t *testing.T) {
	c, st := newTestController(t, nil)
	hr := &manifest.HelmRelease{
		Name: "kromgo", Namespace: "apps",
		Chart: manifest.HelmChart{
			RepoKind: manifest.KindHelmRepository, RepoNamespace: "flux-system", RepoName: "truecharts",
			Name: "kromgo", Version: "3.0.0",
		},
	}

	// Pass 1: HelmRepository not in the Store yet — must not repoint.
	if _, repointed := c.materializeHelmChartSource(hr.Named(), hr); repointed {
		t.Fatal("repointed while the HelmRepository was absent (must be a no-op)")
	}

	// HelmRepository arrives mid-run (render-emitted / lazily promoted).
	st.AddObject(&manifest.HelmRepository{
		Name: "truecharts", Namespace: "flux-system",
		URL:  "oci://oci.trueforge.org/truecharts",
		Type: sourcev1.HelmRepositoryTypeOCI,
	})

	// Pass 2: now present — must repoint to the synthetic HelmChart.
	if _, repointed := c.materializeHelmChartSource(hr.Named(), hr); !repointed {
		t.Fatal("did not repoint once the HelmRepository was present")
	}
}

// TestMaterializeHelmChartSource_HTTPRepoMaterializes pins the behavior flip:
// a type=default (HTTP) HelmRepository chart is now ALSO materialized into a
// synthetic HelmChart (previously it was left to an inline helm.Client pull).
// The oci-vs-http decision is the fetcher's; the controller repoints uniformly.
func TestMaterializeHelmChartSource_HTTPRepoMaterializes(t *testing.T) {
	c, st := newTestController(t, nil)
	st.AddObject(&manifest.HelmRepository{
		Name: "bitnami", Namespace: "flux-system",
		URL: "https://charts.example/bitnami",
	})
	hr := &manifest.HelmRelease{
		Name: "redis", Namespace: "apps",
		Chart: manifest.HelmChart{
			RepoKind: manifest.KindHelmRepository, RepoNamespace: "flux-system", RepoName: "bitnami",
			Name: "redis", Version: "1.0.0",
		},
	}
	got, repointed := c.materializeHelmChartSource(hr.Named(), hr)
	if !repointed || got.Chart.RepoKind != manifest.KindHelmChart {
		t.Fatalf("HTTP HelmRepository was not materialized: %+v (repointed=%v)", got.Chart, repointed)
	}
	if hc, ok := st.GetObjectByName(manifest.KindHelmChart, got.Chart.RepoNamespace, got.Chart.RepoName).(*manifest.HelmChartSource); !ok || hc.SourceRef.Name != "bitnami" {
		t.Errorf("synthetic HelmChart does not reference the bitnami repo: %v", got.Chart)
	}
}

// TestMaterializeHelmChartSource_EmptyVersion guards the preparePrereqs
// placeholder gotcha: a HelmRepository chart with no version still
// materializes into a (versionless) HelmChart rather than being mistaken for
// an unresolved chartRef:HelmChart placeholder.
func TestMaterializeHelmChartSource_EmptyVersion(t *testing.T) {
	c, st := newTestController(t, nil)
	st.AddObject(&manifest.HelmRepository{
		Name: "bitnami", Namespace: "flux-system",
		URL: "https://charts.example/bitnami",
	})
	hr := &manifest.HelmRelease{
		Name: "redis", Namespace: "apps",
		Chart: manifest.HelmChart{
			RepoKind: manifest.KindHelmRepository, RepoNamespace: "flux-system", RepoName: "bitnami",
			Name: "redis", // Version omitted
		},
	}
	got, repointed := c.materializeHelmChartSource(hr.Named(), hr)
	if !repointed || got.Chart.RepoKind != manifest.KindHelmChart {
		t.Fatalf("empty-version HelmRepository chart not materialized: %+v (repointed=%v)", got.Chart, repointed)
	}
}

func TestController_SuspendedShortCircuitsToReady(t *testing.T) {
	c, st := newTestController(t, nil)
	hr := &manifest.HelmRelease{
		Name: "demo", Namespace: "default",
		Suspend: true,
	}
	st.AddObject(hr)

	info := dispatchToFixpoint(t, c, st, hr.Named())
	if info.Status != store.StatusReady {
		t.Fatalf("status = %+v, want StatusReady", info)
	}
	if info.Message != "suspended" {
		t.Errorf("expected suspended; got %q", info.Message)
	}
}

func TestController_FilterUnchangedShortCircuitsToReady(t *testing.T) {
	filter := change.NewFilter(change.NewSet(nil), &manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{}}, testutil.MapLister{})
	c, st := newTestController(t, filter)
	hr := &manifest.HelmRelease{Name: "demo", Namespace: "default"}
	st.AddObject(hr)

	info := dispatchToFixpoint(t, c, st, hr.Named())
	if info.Status != store.StatusReady {
		t.Fatalf("status = %+v, want StatusReady", info)
	}
	if info.Message != "unchanged" {
		t.Errorf("expected unchanged short-circuit; got %q", info.Message)
	}
}

func TestController_AllowMissingSecretsOmitsUnavailableValuesFrom(t *testing.T) {
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

	c, st := newTestControllerWithOptions(t, ReconcileOptions{AllowMissingSecrets: true})
	src := &manifest.GitRepository{Name: "charts", Namespace: "flux-system"}
	st.AddObject(src)
	st.SetArtifact(src.Named(), &store.SourceArtifact{
		Kind: manifest.KindGitRepository, URL: "file://" + dir, LocalPath: dir,
	})
	st.UpdateStatus(src.Named(), store.StatusReady, "")
	st.AddObject(&manifest.ConfigMap{
		Name:      "present-values",
		Namespace: "default",
		Data:      map[string]any{"values.yaml": "kept: kept-value\n"},
	})
	st.AddObject(&manifest.RawObject{
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
		Chart: manifest.HelmChart{
			Name:          "mychart",
			RepoName:      "charts",
			RepoNamespace: "flux-system",
			RepoKind:      manifest.KindGitRepository,
		},
	}
	st.AddObject(hr)

	info := dispatchToFixpoint(t, c, st, hr.Named())
	if info.Status != store.StatusReady {
		t.Fatalf("status = %+v, want StatusReady", info)
	}
	if store.IsSkipped(info) {
		t.Fatalf("generated valuesFrom refs should be omitted, not skip the HelmRelease: %+v", info)
	}
	art, ok := st.GetArtifact(hr.Named()).(*store.HelmReleaseArtifact)
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
	assert.Equal(t, dispatchToFixpoint(t, c, st, hr.Named()).Status, store.StatusReady)
	assert.Equal(t, st.GetArtifact(hr.Named()), store.Artifact(art))
	assert.Diff(t, st.Warnings(), warnings)
	hr = hr.Clone()
	hr.ValuesFrom = append(hr.ValuesFrom, manifest.ValuesReference{Kind: manifest.KindSecret, Name: "another-values"})
	st.AddObject(hr)
	assert.Equal(t, dispatchToFixpoint(t, c, st, hr.Named()).Status, store.StatusReady)
	if !slices.ContainsFunc(st.Warnings(), func(w manifest.Warning) bool {
		return w.Message == "required valuesFrom Secret/default/another-values omitted from offline render" && w.Count == 1
	}) {
		t.Fatalf("new omitted reference did not produce a Count 1 warning: %+v", st.Warnings())
	}
}

func TestController_ValuesOmissionWaitsForBlockedReferences(t *testing.T) {
	c, st := newTestControllerWithOptions(t, ReconcileOptions{AllowMissingSecrets: true})
	chart := &manifest.HelmChartSource{Name: "chart", Namespace: "apps"}
	st.AddObject(chart)
	st.UpdateStatus(chart.Named(), store.StatusFailed, "chart failed")
	hr := &manifest.HelmRelease{
		Name: "consumer", Namespace: "apps",
		Chart:      manifest.HelmChart{RepoKind: manifest.KindHelmChart, RepoNamespace: "apps", RepoName: chart.Name},
		ValuesFrom: []manifest.ValuesReference{{Kind: manifest.KindSecret, Name: "values"}},
	}
	st.AddObject(hr)
	values := manifest.NamedResource{Kind: manifest.KindSecret, Namespace: "apps", Name: "values"}
	_, _, err := c.resolvePreRenderValuesFrom(t.Context(), hr, manifest.DrainNone)
	blocked, ok := errors.AsType[*depwait.ErrBlocked](err)
	if !ok || !slices.Equal(blocked.Deps, []manifest.NamedResource{values}) {
		t.Fatalf("mixed gate = %v, want blocked values reference", err)
	}
	_, _, err = c.resolvePreRenderValuesFrom(t.Context(), hr, manifest.DrainCascade)
	failed, ok := errors.AsType[*manifest.DependencyFailedError](err)
	if !ok || !slices.Equal(failed.Failed, []manifest.NamedResource{chart.Named()}) || failed.Reasons[chart.Named()] != "chart failed" {
		t.Fatalf("drained gate = %v, want chart failure after values omission", err)
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
	c, st := newTestControllerWithOptions(t, ReconcileOptions{})
	src := &manifest.GitRepository{Name: "charts", Namespace: "flux-system"}
	st.AddObject(src)
	st.SetArtifact(src.Named(), &store.SourceArtifact{
		Kind: manifest.KindGitRepository, URL: "file://" + dir, LocalPath: dir,
	})
	st.UpdateStatus(src.Named(), store.StatusReady, "")
	st.AddObject(&manifest.ConfigMap{
		Name: "present-values", Namespace: "default",
		Data: map[string]any{"values.yaml": "kept: kept-value\n"},
	})
	// The in-repo ExternalSecret declaring the (absent) app-values Secret —
	// AddObject fires the producer listener, seeding the shared index.
	st.AddObject(&manifest.RawObject{
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
		Chart: manifest.HelmChart{
			Name: "mychart", RepoName: "charts",
			RepoNamespace: "flux-system", RepoKind: manifest.KindGitRepository,
		},
	}
	st.AddObject(hr)

	info := dispatchToFixpoint(t, c, st, hr.Named())
	if info.Status != store.StatusReady || store.IsSkipped(info) {
		t.Fatalf("status = %+v, want StatusReady (producer-backed ref omitted, not skipped/failed)", info)
	}
	art, ok := st.GetArtifact(hr.Named()).(*store.HelmReleaseArtifact)
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
	dir := t.TempDir()
	testutil.WriteFile(t, dir, "mychart/Chart.yaml", "apiVersion: v2\nname: mychart\nversion: 0.1.0\n")
	testutil.WriteFile(t, dir, "mychart/templates/configmap.yaml",
		"apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: {{ .Release.Name }}-cm\n")

	c, st := newTestControllerWithOptions(t, ReconcileOptions{})
	src := &manifest.GitRepository{Name: "charts", Namespace: "flux-system"}
	st.AddObject(src)
	st.SetArtifact(src.Named(), &store.SourceArtifact{
		Kind: manifest.KindGitRepository, URL: "file://" + dir, LocalPath: dir,
	})
	st.UpdateStatus(src.Named(), store.StatusReady, "")
	hr := &manifest.HelmRelease{
		Name: "demo", Namespace: "default",
		Interval:   metav1Duration(time.Hour),
		ValuesFrom: []manifest.ValuesReference{{Kind: manifest.KindSecret, Name: "no-producer"}},
		Chart: manifest.HelmChart{
			Name: "mychart", RepoName: "charts",
			RepoNamespace: "flux-system", RepoKind: manifest.KindGitRepository,
		},
	}
	st.AddObject(hr)

	info := dispatchToFixpoint(t, c, st, hr.Named())
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
	c, st := newTestController(t, nil)
	src := &manifest.HelmChartSource{
		Name: "podinfo", Namespace: "flux-system",
		Chart:     "podinfo",
		Version:   "6.3.2",
		SourceRef: sourcev1.LocalHelmChartSourceReference{Name: "podinfo", Kind: manifest.KindHelmRepository},
	}
	st.AddObject(src)

	resolver := c.Helm.Resolver()
	if resolver == nil {
		t.Fatal("HelmClient has no resolver wired")
	}
	got := resolver.HelmChart(src.Namespace, src.Name)
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
	base := &manifest.HelmRelease{
		Name: "demo", Namespace: "default",
		Interval: metav1Duration(time.Hour),
		Chart:    manifest.HelmChart{Name: "podinfo", RepoName: "podinfo", RepoNamespace: "flux-system", RepoKind: manifest.KindHelmRepository},
		Values:   map[string]any{"replicas": 2},
	}
	stamped := base.Clone()
	stamped.Labels = map[string]string{
		"kustomize.toolkit.fluxcd.io/name":      "parent-ks",
		"kustomize.toolkit.fluxcd.io/namespace": "flux-system",
	}
	stamped.Annotations = map[string]string{"reconcile.fluxcd.io/requestedAt": "now"}

	if got, want := helmReleaseFingerprint(stamped, "", nil), helmReleaseFingerprint(base, "", nil); got != want {
		t.Errorf("fingerprint changed under label/annotation stamping; got %q want %q", got, want)
	}
}

// TestHelmReleaseFingerprint_DifferentOnSpecChange flips the
// invariant: if patches mutate spec (tholinka's cluster KS pattern
// — driftDetection, install.crds, upgrade.* injected via
// kustomize), the fingerprint MUST differ so the controller
// renders the canonical post-patch values.
func TestHelmReleaseFingerprint_DifferentOnSpecChange(t *testing.T) {
	base := &manifest.HelmRelease{
		Name: "demo", Namespace: "default",
		Interval: metav1Duration(time.Hour),
		Chart:    manifest.HelmChart{Name: "podinfo"},
	}
	patched := base.Clone()
	patched.DriftDetection = &helmv2.DriftDetection{Mode: helmv2.DriftDetectionEnabled}

	if got := helmReleaseFingerprint(base, "", nil); got == helmReleaseFingerprint(patched, "", nil) {
		t.Errorf("fingerprint should differ when spec.driftDetection mutates; both = %q", got)
	}
}

func TestHelmReleaseFingerprint_SourceIdentity(t *testing.T) {
	hr := &manifest.HelmRelease{Name: "demo", Namespace: "apps"}
	const digest = "sha256:ff3d3e14728f75476ed4d43c14f80d52d81d36bc16906843463d464c6146f0d8"
	art := &store.SourceArtifact{Digest: digest, Revision: "6.15.0@" + digest, LocalPath: "/chart"}
	fp := helmReleaseFingerprint(hr, art.Digest+"\x00"+art.Revision, nil)
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
			changed := *art
			tc.edit(&changed)
			if same := helmReleaseFingerprint(hr, changed.Digest+"\x00"+changed.Revision, nil) == fp; same != tc.wantSame {
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

func TestEmit_SourceOwnershipBeforeArrivals(t *testing.T) {
	st := store.New()
	ts := task.NewBounded(0)
	hc, err := helm.NewClient(cacheroot.New(t.TempDir()))
	if err != nil {
		t.Fatalf("helm.NewClient: %v", err)
	}
	hc.SetSourceResolver(helm.NewStoreSourceResolver(st))

	// Build a minimal filter with a non-nil keep set (so Enabled() == true
	// and AddEmitted is exercised). Use a parent HR in the keep set as a
	// "primary" emitter — Filter.AddEmitted propagates keep when the emitter
	// is primary.
	parent := manifest.NamedResource{
		Kind:      manifest.KindHelmRelease,
		Namespace: "flux-system",
		Name:      "tofu",
	}
	sourceFiles := map[manifest.NamedResource]string{
		parent: "apps/tofu/helmrelease.yaml",
	}
	filter := change.NewFilter(change.NewSet([]string{"apps/tofu/helmrelease.yaml"}), &manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{parent: {File: sourceFiles[parent]}}}, testutil.MapLister{})

	c := New(st, ts, hc, helm.Options{}, false)
	c.Configure(ReconcileOptions{Filter: filter})
	c.Start(context.Background())
	t.Cleanup(func() { c.Close(); ts.BlockTillDone() })

	// One source-kind doc (OCIRepository) and one non-source-kind (ConfigMap).
	// Emit must extend keep and ownership for the source
	// only, and route the ConfigMap through AddObject without either call.
	ociDoc := map[string]any{
		"apiVersion": "source.toolkit.fluxcd.io/v1beta2",
		"kind":       "OCIRepository",
		"metadata":   map[string]any{"name": "tofu-oci", "namespace": "flux-system"},
		"spec":       map[string]any{"url": "oci://ghcr.io/tofu/tofu"},
	}
	cmDoc := map[string]any{
		"apiVersion": "v1",
		"kind":       "ConfigMap",
		"metadata":   map[string]any{"name": "tofu-config", "namespace": "flux-system"},
		"data":       map[string]any{"key": "value"},
	}

	var arrivals int
	st.AddListener(store.EventObjectAdded, func(id manifest.NamedResource, _ any) {
		arrivals++
		source := manifest.NamedResource{Kind: manifest.KindOCIRepository, Namespace: "flux-system", Name: "tofu-oci"}
		if got, ok := st.EmittedParent(source); !ok || got != parent {
			t.Errorf("source owner at arrival of %s = (%v, %v), want %s", id, got, ok, parent)
		}
	}, false)
	c.Emit(parent, []map[string]any{cmDoc, ociDoc}, c.WipeSecrets, true)
	if arrivals != 2 {
		t.Fatalf("arrivals = %d, want 2", arrivals)
	}

	// The OCIRepository must have been added to the store (AddObject path).
	ociID := manifest.NamedResource{
		Kind:      manifest.KindOCIRepository,
		Namespace: "flux-system",
		Name:      "tofu-oci",
	}
	if obj := st.GetObject(ociID); obj == nil {
		t.Error("OCIRepository was not added to store via AddObject")
	}

	// keepEmitted: the OCIRepository child should now be in the filter's keep
	// set because the parent HR was a primary emitter (its file changed).
	if !filter.ShouldReconcile(ociID) {
		t.Error("keepEmitted did not add OCIRepository to filter keep set")
	}

	if got, ok := st.EmittedParent(ociID); !ok || got != parent {
		t.Errorf("source owner = (%v, %v), want (%v, true)", got, ok, parent)
	}
	cmID := manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: "flux-system", Name: "tofu-config"}
	if _, ok := st.EmittedParent(cmID); ok {
		t.Error("ConfigMap must not receive HelmRelease render ownership")
	}
}

// TestEmit_DedupReplay_NoStoreWrites pins the publish gate
// (mirrors the KS TestEmitRenderedChildren_DedupSkip_NoStoreWrites): the
// fingerprint-dedup replay (publish=false) must NOT re-AddObject the
// chart-emitted source CRs — re-emit is a DeepEqual no-op anyway, and gating it
// makes that explicit. The idempotent side-effects the replay exists for —
// KeepEmitted (keep-set) + Store.RecordEmitted (provenance) — MUST still fire.
func TestEmit_DedupReplay_NoStoreWrites(t *testing.T) {
	st := store.New()
	ts := task.NewBounded(0)
	hc, err := helm.NewClient(cacheroot.New(t.TempDir()))
	if err != nil {
		t.Fatalf("helm.NewClient: %v", err)
	}
	hc.SetSourceResolver(helm.NewStoreSourceResolver(st))

	parent := manifest.NamedResource{Kind: manifest.KindHelmRelease, Namespace: "flux-system", Name: "tofu"}
	filter := change.NewFilter(change.NewSet([]string{"apps/tofu/helmrelease.yaml"}), &manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{parent: {File: "apps/tofu/helmrelease.yaml"}}}, testutil.MapLister{})
	c := New(st, ts, hc, helm.Options{}, false)
	c.Configure(ReconcileOptions{Filter: filter})
	c.Start(context.Background())
	t.Cleanup(func() { c.Close(); ts.BlockTillDone() })

	ociDoc := map[string]any{
		"apiVersion": "source.toolkit.fluxcd.io/v1beta2",
		"kind":       "OCIRepository",
		"metadata":   map[string]any{"name": "tofu-oci", "namespace": "flux-system"},
		"spec":       map[string]any{"url": "oci://ghcr.io/tofu/tofu"},
	}
	ociID := manifest.NamedResource{Kind: manifest.KindOCIRepository, Namespace: "flux-system", Name: "tofu-oci"}

	var events int
	st.AddListener(store.EventObjectAdded, func(_ manifest.NamedResource, _ any) { events++ }, false)

	// Dedup replay: no AddObject, no event, store untouched — but the keep-set
	// and provenance side-effects still fire.
	c.Emit(parent, []map[string]any{ociDoc}, c.WipeSecrets, false)

	if events != 0 {
		t.Errorf("publish=false fired %d EventObjectAdded; want 0 (no source re-emit)", events)
	}
	if obj := st.GetObject(ociID); obj != nil {
		t.Error("publish=false AddObject'd a source CR; want the store left untouched")
	}
	if got, ok := st.EmittedParent(ociID); !ok || got != parent {
		t.Errorf("publish=false owner = (%v, %v), want (%v, true)", got, ok, parent)
	}
	if !filter.ShouldReconcile(ociID) {
		t.Error("publish=false did not extend the keep set (KeepEmitted must still run)")
	}

	// Fresh render publishes: event fires and the source lands in the store.
	c.Emit(parent, []map[string]any{ociDoc}, c.WipeSecrets, true)
	if events != 1 {
		t.Errorf("publish=true fired %d EventObjectAdded; want 1", events)
	}
	if obj := st.GetObject(ociID); obj == nil {
		t.Error("publish=true did not store the source CR")
	}
}

// TestRawProducerIndex_ExternalSecretWithExplicitTarget verifies that an
// ExternalSecret with spec.target.name set is indexed under that explicit
// target name, and that Producer returns the producer identity
// for a matching Secret ref.
func TestRawProducerIndex_ExternalSecretWithExplicitTarget(t *testing.T) {
	c, st := newTestControllerWithOptions(t, ReconcileOptions{})

	producer := &manifest.RawObject{
		Kind:       "ExternalSecret",
		APIVersion: "external-secrets.io/v1",
		Name:       "app-creds",
		Namespace:  "default",
		Spec: map[string]any{
			"target": map[string]any{"name": "app-values"},
		},
	}
	st.AddObject(producer)

	target := manifest.NamedResource{
		Kind:      manifest.KindSecret,
		Namespace: "default",
		Name:      "app-values",
	}
	got, ok := c.producers.Producer(target)
	if !ok {
		t.Fatal("Producer returned false; expected producer to be indexed")
	}
	if got.Kind != "ExternalSecret" || got.Namespace != "default" || got.Name != "app-creds" {
		t.Errorf("Producer returned %v; want ExternalSecret/default/app-creds", got)
	}
}

// TestRawProducerIndex_ExternalSecretFallsBackToOwnName verifies that an
// ExternalSecret with no spec.target.name is indexed under its own .metadata.name.
func TestRawProducerIndex_ExternalSecretFallsBackToOwnName(t *testing.T) {
	c, st := newTestControllerWithOptions(t, ReconcileOptions{})

	producer := &manifest.RawObject{
		Kind:       "ExternalSecret",
		APIVersion: "external-secrets.io/v1",
		Name:       "my-secret",
		Namespace:  "staging",
		Spec:       map[string]any{},
	}
	st.AddObject(producer)

	target := manifest.NamedResource{
		Kind:      manifest.KindSecret,
		Namespace: "staging",
		Name:      "my-secret",
	}
	got, ok := c.producers.Producer(target)
	if !ok {
		t.Fatal("Producer returned false; expected producer to be indexed via own name")
	}
	if got.Name != "my-secret" || got.Namespace != "staging" {
		t.Errorf("Producer returned %v; want ExternalSecret/staging/my-secret", got)
	}
}

// TestRawProducerIndex_SealedSecretWithTemplateMetadataName verifies that a
// SealedSecret with spec.template.metadata.name is indexed under that name.
func TestRawProducerIndex_SealedSecretWithTemplateMetadataName(t *testing.T) {
	c, st := newTestControllerWithOptions(t, ReconcileOptions{})

	producer := &manifest.RawObject{
		Kind:       "SealedSecret",
		APIVersion: "bitnami.com/v1alpha1",
		Name:       "sealed-db",
		Namespace:  "prod",
		Spec: map[string]any{
			"template": map[string]any{
				"metadata": map[string]any{"name": "db-password"},
			},
		},
	}
	st.AddObject(producer)

	target := manifest.NamedResource{
		Kind:      manifest.KindSecret,
		Namespace: "prod",
		Name:      "db-password",
	}
	got, ok := c.producers.Producer(target)
	if !ok {
		t.Fatal("Producer returned false for SealedSecret with template.metadata.name")
	}
	if got.Name != "sealed-db" || got.Namespace != "prod" {
		t.Errorf("Producer returned %v; want SealedSecret/prod/sealed-db", got)
	}
}

// TestRawProducerIndex_Missing verifies that Producer returns
// false for a Secret that has no matching producer in the index.
func TestRawProducerIndex_Missing(t *testing.T) {
	c, _ := newTestControllerWithOptions(t, ReconcileOptions{})

	target := manifest.NamedResource{
		Kind:      manifest.KindSecret,
		Namespace: "default",
		Name:      "no-such-secret",
	}
	if _, ok := c.producers.Producer(target); ok {
		t.Error("Producer returned true for a non-existent producer; want false")
	}
}

// TestRawProducerIndex_NamespaceIsolation verifies that a producer in
// namespace A does not match a query for the same name in namespace B.
func TestRawProducerIndex_NamespaceIsolation(t *testing.T) {
	c, st := newTestControllerWithOptions(t, ReconcileOptions{})

	st.AddObject(&manifest.RawObject{
		Kind:       "ExternalSecret",
		APIVersion: "external-secrets.io/v1",
		Name:       "svc-creds",
		Namespace:  "team-a",
		Spec:       map[string]any{},
	})

	// Same name, different namespace — must not match.
	crossNS := manifest.NamedResource{
		Kind:      manifest.KindSecret,
		Namespace: "team-b",
		Name:      "svc-creds",
	}
	if _, ok := c.producers.Producer(crossNS); ok {
		t.Error("Producer matched a producer from a different namespace; want false")
	}

	// Same namespace — must match.
	sameNS := manifest.NamedResource{
		Kind:      manifest.KindSecret,
		Namespace: "team-a",
		Name:      "svc-creds",
	}
	if _, ok := c.producers.Producer(sameNS); !ok {
		t.Error("Producer did not match producer in same namespace; want true")
	}
}

func TestController_CollectHRDepsClone(t *testing.T) {
	c, _ := newTestController(t, nil)
	hr := &manifest.HelmRelease{
		Name: "demo", Namespace: "default",
		DependsOn: []manifest.DependencyRef{
			{Kind: manifest.KindHelmRelease, Namespace: "default", Name: "other"},
		},
	}
	got := c.OrderingDeps(hr.DependsOn)
	if len(got) != 1 || got[0].Name != "other" {
		t.Fatalf("OrderingDeps = %+v", got)
	}
	// Mutating the returned slice must not affect hr.DependsOn.
	got[0].Name = "mutated"
	if hr.DependsOn[0].Name == "mutated" {
		t.Errorf("OrderingDeps did not return a defensive copy")
	}
}

func TestController_CollectHRDepsPrunesUnchangedDeps(t *testing.T) {
	// #517: in changed-only mode a changed HR's dependsOn on an UNCHANGED
	// dep must be pruned. The unchanged dep's producing Kustomization is
	// skipped, so the dep HR is never render-emitted into the Store and
	// depwait would spuriously fail "dependency not found". Build a filter
	// whose keep-set contains qbittorrent (its file changed) but not
	// prowlarr (unchanged): only the in-keep dep survives.
	qbit := manifest.NamedResource{Kind: manifest.KindHelmRelease, Namespace: "downloads", Name: "qbittorrent"}
	prowlarr := manifest.NamedResource{Kind: manifest.KindHelmRelease, Namespace: "downloads", Name: "prowlarr"}
	filter := change.NewFilter(change.NewSet([]string{"qbittorrent.yaml"}), &manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{qbit: {File: "qbittorrent.yaml"}, prowlarr: {File: "prowlarr.yaml"}}}, testutil.MapLister{})
	c, st := newTestController(t, filter)
	st.AddObject(&manifest.HelmRelease{Name: prowlarr.Name, Namespace: prowlarr.Namespace})
	hr := &manifest.HelmRelease{
		Name: "lidarr", Namespace: "downloads",
		DependsOn: []manifest.DependencyRef{{NamedResource: qbit}, {NamedResource: prowlarr}},
	}
	got := c.OrderingDeps(hr.DependsOn)
	if len(got) != 1 || got[0].NamedResource != qbit {
		t.Fatalf("OrderingDeps = %+v; want only the in-keep qbittorrent dep", got)
	}

	// Sanity: a disabled filter (full mode) prunes nothing.
	cFull, _ := newTestController(t, nil)
	if got := cFull.OrderingDeps(hr.DependsOn); len(got) != 2 {
		t.Errorf("disabled filter must not prune; OrderingDeps = %+v", got)
	}
}

// TestController_AlreadyReady_NoTransientPending pins the base
// SetPendingUnlessReady guard at the HelmRelease level (the #657/#658 dedup
// closing the analogous HR gap): a no-op re-reconcile of an already-Ready HR
// (re-emitted by its parent KS render with stamped ownership labels — HR
// retains labels, so a stamped re-emit re-runs and the label-insensitive
// fingerprint dedup-skips) must NOT transiently flip Ready→Pending at the
// pre-dedup "resolving chart" write. An HR that dependsOn this one waits
// quiescence-bound and could re-read that transient Pending at a transient pool
// drain and drop. The genuine-render downgrade after the dedup check is
// unaffected.
func TestController_AlreadyReady_NoTransientPending(t *testing.T) {
	dir := t.TempDir()
	testutil.WriteFile(t, dir, "mychart/Chart.yaml", "apiVersion: v2\nname: mychart\nversion: 0.1.0\n")
	testutil.WriteFile(t, dir, "mychart/templates/configmap.yaml",
		"apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: {{ .Release.Name }}-cm\ndata:\n  k: v\n")

	c, st := newTestController(t, nil)
	src := &manifest.GitRepository{Name: "charts", Namespace: "flux-system"}
	st.AddObject(src)
	st.SetArtifact(src.Named(), &store.SourceArtifact{
		Kind: manifest.KindGitRepository, URL: "file://" + dir, LocalPath: dir,
	})
	st.UpdateStatus(src.Named(), store.StatusReady, "")

	hr := &manifest.HelmRelease{
		Name: "demo", Namespace: "default",
		Interval: metav1Duration(time.Hour),
		Chart: manifest.HelmChart{
			Name: "mychart", RepoName: "charts", RepoNamespace: "flux-system",
			RepoKind: manifest.KindGitRepository,
		},
	}
	st.AddObject(hr)
	if info := dispatchToFixpoint(t, c, st, hr.Named()); info.Status != store.StatusReady {
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
	defer mu.Unlock()
	if sawPending {
		t.Error("no-op re-reconcile of an already-Ready HelmRelease transiently downgraded it to Pending (the quiescence-race window)")
	}
	if info, _ := st.GetStatus(hr.Named()); info.Status != store.StatusReady {
		t.Errorf("HR not Ready after no-op re-run: %+v", info)
	}
}
