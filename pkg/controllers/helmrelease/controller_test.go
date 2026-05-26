package helmrelease

import (
	"context"
	"testing"
	"time"

	helmv2 "github.com/fluxcd/helm-controller/api/v2"
	sourcev1 "github.com/fluxcd/source-controller/api/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/change"
	"github.com/home-operations/flate/pkg/helm"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source/cacheroot"
	"github.com/home-operations/flate/pkg/store"
	"github.com/home-operations/flate/pkg/task"
)

func newTestController(t *testing.T, filter *change.Filter) (*Controller, *store.Store) {
	t.Helper()
	return newTestControllerWithOptions(t, ReconcileOptions{Filter: filter})
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
	ts := task.New()
	hc, err := helm.NewClient(cacheroot.New(t.TempDir()))
	if err != nil {
		t.Fatalf("helm.NewClient: %v", err)
	}
	// Mirror the orchestrator's production wiring: the HR controller
	// resolves source CRs (HelmRepository, OCIRepository, HelmChart)
	// through the Store via the helm client's SourceResolver.
	hc.SetSourceResolver(helm.NewStoreSourceResolver(st))
	c := New(st, ts, hc, helm.Options{}, false)
	c.Configure(opts)
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

	_, st := newTestControllerWithOptions(t, ReconcileOptions{AllowMissingSecrets: true})
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
		HelmReleaseSpec: helmv2.HelmReleaseSpec{
			Interval: metav1Duration(time.Hour),
			Timeout:  ptrDuration(100 * time.Millisecond),
			ValuesFrom: []manifest.ValuesReference{
				{Kind: manifest.KindConfigMap, Name: "present-values"},
				{Kind: manifest.KindSecret, Name: "app-values"},
				{Kind: manifest.KindConfigMap, Name: "runtime-values"},
			},
		},
		Chart: manifest.HelmChart{
			Name:          "mychart",
			RepoName:      "charts",
			RepoNamespace: "flux-system",
			RepoKind:      manifest.KindGitRepository,
		},
	}
	st.AddObject(hr)

	info := waitForStatus(t, st, hr.Named(), store.StatusReady)
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

// TestHelmReleaseFingerprint_StableAcrossLabelStamping locks the
// dedup contract: an HR re-AddObject'd with kustomize ownership
// labels (the typical pattern when the parent KS emits a
// re-stamped copy) must produce the same fingerprint as the file-
// loaded original — otherwise the dedup short-circuit can't fire
// and helm.Template runs twice for one logical release.
func TestHelmReleaseFingerprint_StableAcrossLabelStamping(t *testing.T) {
	base := &manifest.HelmRelease{
		Name: "demo", Namespace: "default",
		HelmReleaseSpec: helmv2.HelmReleaseSpec{
			Interval: metav1Duration(time.Hour),
		},
		Chart:  manifest.HelmChart{Name: "podinfo", RepoName: "podinfo", RepoNamespace: "flux-system", RepoKind: manifest.KindHelmRepository},
		Values: map[string]any{"replicas": 2},
	}
	stamped := base.Clone()
	stamped.Labels = map[string]string{
		"kustomize.toolkit.fluxcd.io/name":      "parent-ks",
		"kustomize.toolkit.fluxcd.io/namespace": "flux-system",
	}
	stamped.Annotations = map[string]string{"reconcile.fluxcd.io/requestedAt": "now"}

	if got, want := helmReleaseFingerprint(stamped), helmReleaseFingerprint(base); got != want {
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
		HelmReleaseSpec: helmv2.HelmReleaseSpec{Interval: metav1Duration(time.Hour)},
		Chart:           manifest.HelmChart{Name: "podinfo"},
	}
	patched := base.Clone()
	patched.DriftDetection = &helmv2.DriftDetection{Mode: helmv2.DriftDetectionEnabled}

	if got := helmReleaseFingerprint(base); got == helmReleaseFingerprint(patched) {
		t.Errorf("fingerprint should differ when spec.driftDetection mutates; both = %q", got)
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
