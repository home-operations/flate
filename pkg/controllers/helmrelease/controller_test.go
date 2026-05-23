package helmrelease

import (
	"context"
	"testing"
	"time"

	helmv2 "github.com/fluxcd/helm-controller/api/v2"
	sourcev1 "github.com/fluxcd/source-controller/api/v1"

	"github.com/home-operations/flate/pkg/change"
	"github.com/home-operations/flate/pkg/helm"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
	"github.com/home-operations/flate/pkg/task"
)

func newTestController(t *testing.T, filter *change.Filter) (*Controller, *store.Store) {
	t.Helper()
	st := store.New()
	ts := task.New()
	hc, err := helm.NewClient(t.TempDir(), t.TempDir())
	if err != nil {
		t.Fatalf("helm.NewClient: %v", err)
	}
	// Mirror the orchestrator's production wiring: the HR controller
	// resolves source CRs (HelmRepository, OCIRepository, HelmChart)
	// through the Store via the helm client's SourceResolver.
	hc.SetSourceResolver(helm.NewStoreSourceResolver(st))
	c := New(st, ts, hc, helm.Options{}, false)
	c.Configure(ReconcileOptions{Filter: filter})
	c.Start(context.Background())
	t.Cleanup(func() {
		c.Close()
		ts.BlockTillDone()
	})
	return c, st
}

func waitForStatus(t *testing.T, st *store.Store, id manifest.NamedResource, want store.Status) store.StatusInfo {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if info, ok := st.GetStatus(id); ok && info.Status == want {
			return info
		}
		time.Sleep(5 * time.Millisecond)
	}
	info, _ := st.GetStatus(id)
	t.Fatalf("status %v not reached; last=%+v", want, info)
	return info
}

func TestController_SuspendedShortCircuitsToReady(t *testing.T) {
	_, st := newTestController(t, nil)
	hr := &manifest.HelmRelease{
		Name: "demo", Namespace: "default",
		HelmReleaseSpec: helmv2.HelmReleaseSpec{Suspend: true},
	}
	st.AddObject(hr)

	info := waitForStatus(t, st, hr.Named(), store.StatusReady)
	if info.Message != "suspended" {
		t.Errorf("expected suspended; got %q", info.Message)
	}
}

func TestController_FilterUnchangedShortCircuitsToReady(t *testing.T) {
	filter := change.NewFilter(
		change.NewSet(nil),
		map[manifest.NamedResource]string{},
		"",
		mapLister{},
	)
	_, st := newTestController(t, filter)
	hr := &manifest.HelmRelease{Name: "demo", Namespace: "default"}
	st.AddObject(hr)

	info := waitForStatus(t, st, hr.Named(), store.StatusReady)
	if info.Message != "unchanged" {
		t.Errorf("expected unchanged short-circuit; got %q", info.Message)
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
		HelmChartSpec: sourcev1.HelmChartSpec{
			Chart:     "podinfo",
			Version:   "6.3.2",
			SourceRef: sourcev1.LocalHelmChartSourceReference{Name: "podinfo", Kind: manifest.KindHelmRepository},
		},
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

func TestController_CollectHRDepsClone(t *testing.T) {
	c, _ := newTestController(t, nil)
	hr := &manifest.HelmRelease{
		Name: "demo", Namespace: "default",
		DependsOn: []manifest.DependencyRef{
			{NamedResource: manifest.NamedResource{Kind: manifest.KindHelmRelease, Namespace: "default", Name: "other"}},
		},
	}
	got := c.collectHRDeps(hr)
	if len(got) != 1 || got[0].Name != "other" {
		t.Fatalf("collectHRDeps = %+v", got)
	}
	// Mutating the returned slice must not affect hr.DependsOn.
	got[0].Name = "mutated"
	if hr.DependsOn[0].Name == "mutated" {
		t.Errorf("collectHRDeps did not return a defensive copy")
	}
}

type mapLister map[manifest.NamedResource]manifest.BaseManifest

func (m mapLister) GetObject(id manifest.NamedResource) manifest.BaseManifest { return m[id] }
func (m mapLister) ListObjects(kind string) []manifest.BaseManifest {
	var out []manifest.BaseManifest
	for id, obj := range m {
		if id.Kind == kind {
			out = append(out, obj)
		}
	}
	return out
}
