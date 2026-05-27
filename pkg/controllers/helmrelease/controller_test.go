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

func TestController_SuspendedShortCircuitsToReady(t *testing.T) {
	_, st := newTestController(t, nil)
	hr := &manifest.HelmRelease{
		Name: "demo", Namespace: "default",
		HelmReleaseSpec: helmv2.HelmReleaseSpec{Suspend: true},
	}
	st.AddObject(hr)

	info := testutil.WaitForStatus(t, st, hr.Named(), store.StatusReady)
	if info.Message != "suspended" {
		t.Errorf("expected suspended; got %q", info.Message)
	}
}

func TestController_FilterUnchangedShortCircuitsToReady(t *testing.T) {
	filter := change.NewFilter(
		change.NewSet(nil),
		map[manifest.NamedResource]string{},
		"",
		testutil.MapLister{},
	)
	_, st := newTestController(t, filter)
	hr := &manifest.HelmRelease{Name: "demo", Namespace: "default"}
	st.AddObject(hr)

	info := testutil.WaitForStatus(t, st, hr.Named(), store.StatusReady)
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

	info := testutil.WaitForStatus(t, st, hr.Named(), store.StatusReady)
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

// spyTracker records every MarkRendered call for inspection in tests.
type spyTracker struct {
	calls []struct{ parent, child manifest.NamedResource }
}

func (s *spyTracker) MarkRendered(parent, child manifest.NamedResource) {
	s.calls = append(s.calls, struct{ parent, child manifest.NamedResource }{parent, child})
}

// TestEmitRenderedChildren_SourceKindGetsKeepEmittedAndMarkRendered asserts
// that emitRenderedChildren calls keepEmitted (via Filter.AddEmitted) and
// markRendered for every source-kind child (isFluxSourceKind == true), and
// does NOT call them for non-source kinds (which go through AddRendered).
// This is the correctness contract for iter-11: HR-rendered source CRs
// (tofu-controller's OCIRepository pattern) must get parent-provenance
// tracking and filter keep-set extension, matching KS behavior.
func TestEmitRenderedChildren_SourceKindGetsKeepEmittedAndMarkRendered(t *testing.T) {
	st := store.New()
	ts := task.New()
	hc, err := helm.NewClient(cacheroot.New(t.TempDir()))
	if err != nil {
		t.Fatalf("helm.NewClient: %v", err)
	}
	hc.SetSourceResolver(helm.NewStoreSourceResolver(st))

	tracker := &spyTracker{}

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
	filter := change.NewFilter(
		change.NewSet([]string{"apps/tofu/helmrelease.yaml"}),
		sourceFiles,
		"",
		testutil.MapLister{},
	)

	c := New(st, ts, hc, helm.Options{}, false)
	c.Configure(ReconcileOptions{
		Filter:        filter,
		RenderTracker: tracker,
	})
	c.Start(context.Background())
	t.Cleanup(func() { c.Close(); ts.BlockTillDone() })

	// One source-kind doc (OCIRepository) and one non-source-kind (ConfigMap).
	// emitRenderedChildren should call keepEmitted+markRendered for the source
	// only, and route the ConfigMap through AddRendered without either call.
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

	c.emitRenderedChildren(parent, []map[string]any{ociDoc, cmDoc})

	// markRendered must have been called exactly once, for the OCIRepository.
	if got := len(tracker.calls); got != 1 {
		t.Fatalf("MarkRendered called %d times, want 1", got)
	}
	call := tracker.calls[0]
	if call.parent != parent {
		t.Errorf("MarkRendered parent = %v, want %v", call.parent, parent)
	}
	if call.child.Kind != manifest.KindOCIRepository || call.child.Name != "tofu-oci" {
		t.Errorf("MarkRendered child = %v, want OCIRepository/flux-system/tofu-oci", call.child)
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

	// The ConfigMap must NOT have triggered MarkRendered.
	for _, c := range tracker.calls {
		if c.child.Kind == manifest.KindConfigMap {
			t.Errorf("MarkRendered unexpectedly called for ConfigMap child: %v", c.child)
		}
	}
}

func TestEmitRenderedChildren_NilTrackerAndNilFilterAreNoops(t *testing.T) {
	st := store.New()
	ts := task.New()
	hc, err := helm.NewClient(cacheroot.New(t.TempDir()))
	if err != nil {
		t.Fatalf("helm.NewClient: %v", err)
	}
	hc.SetSourceResolver(helm.NewStoreSourceResolver(st))

	c := New(st, ts, hc, helm.Options{}, false)
	// No RenderTracker, no Filter — configure with zero opts.
	c.Configure(ReconcileOptions{})
	c.Start(context.Background())
	t.Cleanup(func() { c.Close(); ts.BlockTillDone() })

	parent := manifest.NamedResource{
		Kind: manifest.KindHelmRelease, Namespace: "flux-system", Name: "chart",
	}
	ociDoc := map[string]any{
		"apiVersion": "source.toolkit.fluxcd.io/v1beta2",
		"kind":       "OCIRepository",
		"metadata":   map[string]any{"name": "chart-oci", "namespace": "flux-system"},
		"spec":       map[string]any{"url": "oci://ghcr.io/example"},
	}
	// Must not panic when tracker and filter are nil.
	c.emitRenderedChildren(parent, []map[string]any{ociDoc})

	// OCIRepository is still added to the store even without tracker/filter.
	ociID := manifest.NamedResource{
		Kind:      manifest.KindOCIRepository,
		Namespace: "flux-system",
		Name:      "chart-oci",
	}
	if obj := st.GetObject(ociID); obj == nil {
		t.Error("OCIRepository was not added to store when tracker/filter are nil")
	}
}

// TestRawProducerIndex_ExternalSecretWithExplicitTarget verifies that an
// ExternalSecret with spec.target.name set is indexed under that explicit
// target name, and that generatedValuesProducer returns the producer identity
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
	got, ok := c.generatedValuesProducer(target)
	if !ok {
		t.Fatal("generatedValuesProducer returned false; expected producer to be indexed")
	}
	if got.Kind != "ExternalSecret" || got.Namespace != "default" || got.Name != "app-creds" {
		t.Errorf("generatedValuesProducer returned %v; want ExternalSecret/default/app-creds", got)
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
	got, ok := c.generatedValuesProducer(target)
	if !ok {
		t.Fatal("generatedValuesProducer returned false; expected producer to be indexed via own name")
	}
	if got.Name != "my-secret" || got.Namespace != "staging" {
		t.Errorf("generatedValuesProducer returned %v; want ExternalSecret/staging/my-secret", got)
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
	got, ok := c.generatedValuesProducer(target)
	if !ok {
		t.Fatal("generatedValuesProducer returned false for SealedSecret with template.metadata.name")
	}
	if got.Name != "sealed-db" || got.Namespace != "prod" {
		t.Errorf("generatedValuesProducer returned %v; want SealedSecret/prod/sealed-db", got)
	}
}

// TestRawProducerIndex_Missing verifies that generatedValuesProducer returns
// false for a Secret that has no matching producer in the index.
func TestRawProducerIndex_Missing(t *testing.T) {
	c, _ := newTestControllerWithOptions(t, ReconcileOptions{})

	target := manifest.NamedResource{
		Kind:      manifest.KindSecret,
		Namespace: "default",
		Name:      "no-such-secret",
	}
	if _, ok := c.generatedValuesProducer(target); ok {
		t.Error("generatedValuesProducer returned true for a non-existent producer; want false")
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
	if _, ok := c.generatedValuesProducer(crossNS); ok {
		t.Error("generatedValuesProducer matched a producer from a different namespace; want false")
	}

	// Same namespace — must match.
	sameNS := manifest.NamedResource{
		Kind:      manifest.KindSecret,
		Namespace: "team-a",
		Name:      "svc-creds",
	}
	if _, ok := c.generatedValuesProducer(sameNS); !ok {
		t.Error("generatedValuesProducer did not match producer in same namespace; want true")
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

