package loader

import (
	"slices"
	"sync"
	"testing"

	helmv2 "github.com/fluxcd/helm-controller/api/v2"
	kustomizev1 "github.com/fluxcd/kustomize-controller/api/v1"
	sourcev1 "github.com/fluxcd/source-controller/api/v1"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/filtertest"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/change"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

func TestAddEmitted_AlreadySelectedSourceRetained(t *testing.T) {
	t.Parallel()
	parent := testutil.KustomizationID("apps", "parent")
	child := &manifest.HelmRelease{
		Name:      "app",
		Namespace: "apps",
		Chart: manifest.HelmChart{
			Source: manifest.NamedResource{
				Group:     manifest.SourceDomain,
				Kind:      manifest.KindOCIRepository,
				Namespace: "apps",
				Name:      "new-chart",
			},
		},
		ChartRef: &helmv2.CrossNamespaceSourceReference{
			Kind:      manifest.KindOCIRepository,
			Name:      "new-chart",
			Namespace: "apps",
		},
	}
	old := *child.Clone()
	old.ChartRef.Name = "old-chart"
	old.Chart = manifest.HelmChart{}
	oldSource := old.ChartSource()
	objects := filtertest.MapLister{old.Named(): &old}
	f := newAdmission(
		t,
		admissionConfig{
			changes: change.NewSet([]string{"parent.yaml", "old-chart.yaml"}),
			index: filterFixture{files: map[manifest.NamedResource]string{
				parent: "parent.yaml", oldSource: "old-chart.yaml",
			}, objects: objects}.index(),
			inventory: objects,
			root:      "",
		},
	)
	assert.Equal(t, f.ShouldReconcile(oldSource), true)
	assert.Equal(t, f.ShouldReconcile(child.Named()), true)
	assert.Equal(t, f.ShouldReconcile(child.ChartSource()), false)

	added := f.AddEmitted(parent, store.Child{Obj: child}, f.objs)

	assert.Equal(t, len(added), 1)
	for _, id := range []manifest.NamedResource{child.Named(), child.ChartSource()} {
		assert.Equal(t, f.ShouldReconcile(id), true)
		assert.Equal(t, slices.Contains(added, id), id == child.ChartSource())
	}
	assert.Equal(t, f.ShouldReconcile(oldSource), true)
	assert.Equal(t, slices.Contains(added, oldSource), false)
}

func TestAddEmitted_ConcurrentReplacementReturnsEachIDOnce(t *testing.T) {
	t.Parallel()
	parent := testutil.KustomizationID("apps", "parent")
	child := &manifest.HelmRelease{
		Name:      "app",
		Namespace: "apps",
		Chart: manifest.HelmChart{
			Source: manifest.NamedResource{
				Group:     manifest.SourceDomain,
				Kind:      manifest.KindOCIRepository,
				Namespace: "apps",
				Name:      "new-chart",
			},
		},
		ChartRef: &helmv2.CrossNamespaceSourceReference{
			Kind:      manifest.KindOCIRepository,
			Name:      "new-chart",
			Namespace: "apps",
		},
	}
	old := *child.Clone()
	old.ChartRef.Name = "old-chart"
	old.Chart = manifest.HelmChart{}
	objects := filtertest.MapLister{old.Named(): &old}
	f := newAdmission(
		t,
		admissionConfig{
			changes: change.NewSet([]string{"parent.yaml"}),
			index: filterFixture{
				files:   map[manifest.NamedResource]string{parent: "parent.yaml"},
				objects: objects,
			}.index(),
			inventory: objects,
			root:      "",
		},
	)
	for _, id := range []manifest.NamedResource{child.Named(), child.ChartSource(), old.ChartSource()} {
		assert.Equal(t, f.ShouldReconcile(id), false)
	}
	start := make(chan struct{})
	var ready, joined sync.WaitGroup
	var results [2][]manifest.NamedResource
	ready.Add(len(results))
	for i := range results {
		joined.Go(func() {
			ready.Done()
			<-start
			results[i] = f.AddEmitted(parent, store.Child{Obj: child}, f.objs)
		})
	}
	ready.Wait()
	close(start)
	joined.Wait()

	returned := make(map[manifest.NamedResource]int)
	for _, ids := range results {
		for _, id := range ids {
			returned[id]++
		}
	}
	assert.Diff(t, returned, map[manifest.NamedResource]int{child.Named(): 1, child.ChartSource(): 1})
	assert.Equal(t, f.ShouldReconcile(child.Named()), true)
	assert.Equal(t, f.ShouldReconcile(child.ChartSource()), true)
	assert.Equal(t, f.ShouldReconcile(old.ChartSource()), false)
}

// TestFilter_KeepByNameDoesNotLeakAcrossNamespaces pins the
// asymmetric-fallback contract: a fully-namespaced keep entry must
// NOT match a fully-namespaced lookup that happens to share
// (Kind, Name). Without this, a kept
// Kustomization.kustomize.toolkit.fluxcd.io/cluster-infra/external-secrets would silently scope-in
// an unrelated Kustomization.kustomize.toolkit.fluxcd.io/database/external-secrets (and likewise
// for the runtime AddEmitted / Add paths), broadening changed-only
// mode in ways the user can't see. The empty-namespace bridge in
// TestFilter_ShouldReconcileEmptyNamespaceFallback is preserved
// because it indexes only entries whose Namespace is empty.
func TestFilter_KeepByNameDoesNotLeakAcrossNamespaces(t *testing.T) {
	t.Parallel()
	before, after := t.TempDir(), t.TempDir()
	testutil.WriteFile(
		t,
		before,
		"changed.yaml",
		"apiVersion: kustomize.toolkit.fluxcd.io/v1\nkind: Kustomization\nmetadata: {name: same}\n",
	)
	testutil.WriteFile(
		t,
		after,
		"changed.yaml",
		"apiVersion: foreign.example/v1\nkind: Kustomization\nmetadata: {name: same}\n",
	)
	changes, err := change.Detect(t.Context(), testInput(before), testInput(after))
	assert.NoError(t, err)
	assert.Diff(t, changes.Paths(), []string{"changed.yaml"})
	for _, tc := range []struct {
		name, group, namespace, otherGroup, otherNamespace string
	}{
		{"namespace", manifest.FluxKustomizeDomain, "cluster-infra", manifest.FluxKustomizeDomain, "database"},
		{"real changed", manifest.FluxKustomizeDomain, "apps", "foreign.example", "apps"},
		{"foreign changed", "foreign.example", "apps", manifest.FluxKustomizeDomain, "apps"},
		{"real bare changed", manifest.FluxKustomizeDomain, "", "foreign.example", "apps"},
		{"foreign bare changed", "foreign.example", "", manifest.FluxKustomizeDomain, "apps"},
		{"core bare changed", "", "", manifest.FluxKustomizeDomain, "apps"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			kept := manifest.NamedResource{
				Group:     tc.group,
				Kind:      manifest.KindKustomization,
				Namespace: tc.namespace,
				Name:      "same",
			}
			collider := manifest.NamedResource{
				Group:     tc.otherGroup,
				Kind:      kept.Kind,
				Namespace: tc.otherNamespace,
				Name:      kept.Name,
			}
			files := map[manifest.NamedResource]string{kept: "changed.yaml", collider: "unchanged.yaml"}
			f := newAdmission(
				t,
				admissionConfig{
					changes:   changes,
					index:     filterFixture{files: files}.index(),
					inventory: filtertest.EmptyLister(),
					root:      "",
				},
			)
			assert.Equal(t, f.ShouldReconcile(kept), true)
			assert.Equal(t, f.ShouldReconcile(collider), false)
			projected := kept
			projected.Namespace = "effective"
			assert.Equal(t, f.ShouldReconcile(projected), false)
			child := kept
			child.Name, child.Namespace = "emitted", ""
			assert.Equal(t, len(f.AddEmitted(collider, store.Child{Obj: refObj(child)}, f.objs)), 0)
			assert.Equal(t, f.ShouldReconcile(child), false)
			f.AddEmitted(kept, store.Child{Obj: refObj(child)}, f.objs)
			assert.Equal(t, f.ShouldReconcile(child), true)
			child.Namespace = "effective"
			assert.Equal(t, f.ShouldReconcile(child), false)
			child.Group = tc.otherGroup
			child.Namespace = tc.otherNamespace
			if tc.group != tc.otherGroup {
				assert.Equal(t, f.ShouldReconcile(child), false)
			}
		})
	}
}

func TestFilter_TransitiveDepsHelmReleaseViaHelmChartCRD(t *testing.T) {
	t.Parallel()
	hr := &manifest.HelmRelease{
		Name:      "demo",
		Namespace: "apps",
		Chart: manifest.HelmChart{
			Source: manifest.NamedResource{
				Group:     manifest.SourceDomain,
				Kind:      manifest.KindHelmChart,
				Namespace: "apps",
				Name:      "demo-chart",
			},
		},
		ChartRef: &helmv2.CrossNamespaceSourceReference{
			Kind:      manifest.KindHelmChart,
			Name:      "demo-chart",
			Namespace: "apps",
		},
	}
	hc := &manifest.HelmChartSource{
		Name: "demo-chart", Namespace: "apps",
		SourceRef: sourcev1.LocalHelmChartSourceReference{
			Kind: manifest.KindOCIRepository, Name: "backing-oci",
		},
	}
	hrID := hr.Named()
	hcID := hc.Named()
	ociID := manifest.NamedResource{
		Group:     "source.toolkit.fluxcd.io",
		Kind:      manifest.KindOCIRepository,
		Namespace: "apps",
		Name:      "backing-oci",
	}

	t.Run("initialization", func(t *testing.T) {
		t.Parallel()
		testFilterTransitiveDepsHelmReleaseViaHelmChartCRDInitialization(
			t,
			&testFilterTransitiveDepsHelmReleaseViaHelmChartCRDInitializationInputs{
				hr:    &hr,
				hc:    &hc,
				hrID:  &hrID,
				hcID:  &hcID,
				ociID: &ociID,
			},
		)
	},
	)
	t.Run("runtime", func(t *testing.T) {
		t.Parallel()
		testFilterTransitiveDepsHelmReleaseViaHelmChartCRDRuntime(
			t,
			&testFilterTransitiveDepsHelmReleaseViaHelmChartCRDRuntimeInputs{
				hr:    &hr,
				hc:    &hc,
				hrID:  &hrID,
				hcID:  &hcID,
				ociID: &ociID,
			},
		)
	},
	)
}

// TestFilter_ReverseEdgeCentralizedOCIRepository pins the reverse-edge
// fix: when a centralized OCIRepository (its own Kustomization tree,
// separate from the consuming HelmReleases) has its spec.ref.tag
// bumped, the HelmRelease that chartRefs it must re-render even though
// the HR's own files didn't change. Pre-fix the filter walked only
// consumer→source edges, so the changed OCIRepository never reached its
// consumer and `diff hr` printed nothing.
//
// Mirrors github.com/Boemeltrein/TalosCluster: OCIRepository under
// repositories/oci owned by a `repositories` KS, HelmRelease under
// clusters/.../envoy-gateway/app owned by a different KS. The HR is NOT
// in the store at resolve() time (render-driven discovery); the reverse
// edge is driven by the discovery-supplied consumerRefs instead.
func TestFilter_ReverseEdgeCentralizedOCIRepository(t *testing.T) {
	t.Parallel()
	const (
		ociFile = "repositories/oci/envoy-gateway.yaml"
		hrFile  = "clusters/main/kubernetes/networking/envoy-gateway/app/helm-release.yaml"
		sibFile = "clusters/main/kubernetes/networking/envoy-gateway/app/other-hr.yaml"
	)
	oci := manifest.NamedResource{
		Group:     "source.toolkit.fluxcd.io",
		Kind:      manifest.KindOCIRepository,
		Namespace: "flux-system",
		Name:      "envoy-gateway",
	}
	hr := testutil.HelmReleaseID("envoy-gateway", "envoy-gateway")
	// Sibling HR under the SAME owner KS that consumes a DIFFERENT source
	// — it must stay out of keep (the owner KS is ancestor-only, so it
	// can't cascade unrelated siblings in).
	sibling := testutil.HelmReleaseID("envoy-gateway", "other")
	otherOCI := manifest.NamedResource{
		Group:     "source.toolkit.fluxcd.io",
		Kind:      manifest.KindOCIRepository,
		Namespace: "flux-system",
		Name:      "something-else",
	}
	reposKS := &manifest.Kustomization{
		Name: "repositories", Namespace: "flux-system",
		Path: "repositories",
	}
	appKS := &manifest.Kustomization{
		Name: "envoy-gateway", Namespace: "flux-system",
		Path: "clusters/main/kubernetes/networking/envoy-gateway/app",
	}

	f := newAdmission(
		t,
		admissionConfig{
			changes: change.NewSet([]string{ociFile}),
			index: filterFixture{files: map[manifest.NamedResource]string{
				oci:             ociFile,
				hr:              hrFile,
				sibling:         sibFile,
				reposKS.Named(): "repositories/flux-entry.yaml",
				appKS.Named():   "clusters/main/kubernetes/networking/envoy-gateway/ks.yaml",
			}, root: "", objects: filtertest.MapLister{reposKS.Named(): reposKS, appKS.Named(): appKS}, refs: map[manifest.NamedResource][]manifest.NamedResource{
				hr:      {oci},
				sibling: {otherOCI},
			}, excluded: nil, readers: nil, changed: change.NewSet([]string{ociFile})}.index(),
			inventory: filtertest.MapLister{reposKS.Named(): reposKS, appKS.Named(): appKS},
			root:      "",
		},
	)

	if !f.ShouldReconcile(oci) {
		t.Fatalf("changed OCIRepository must be in keep; keep=%v", f.KeepNames())
	}
	if !f.ShouldReconcile(hr) {
		t.Fatalf("reverse edge: HR consuming the changed OCIRepository must be in keep; keep=%v", f.KeepNames())
	}
	if !f.ShouldReconcile(appKS.Named()) {
		t.Errorf("HR owner KS must render (ancestor) to emit the HR; keep=%v", f.KeepNames())
	}
	if f.ShouldReconcile(sibling) {
		t.Errorf("sibling HR consuming an unchanged source must NOT be pulled in; keep=%v", f.KeepNames())
	}
	// Namespace scoping (scopedNamespaces → KeepNamespaces) gates diff
	// output: without the HR's namespace the command would scope it out
	// even if ShouldReconcile passed.
	if ns := f.KeepNamespaces(); ns != nil {
		{
			_, ok := ns["envoy-gateway"]
			assert.ErrorIf(t, !ok, "HR namespace must be in keep-namespaces for diff scoping; got %v", ns)
		}
	} else {
		t.Errorf("KeepNamespaces returned nil; expected envoy-gateway in scope")
	}
}

func TestFilter_ReverseEdgeHelmChartKeepsEmitter(t *testing.T) {
	t.Parallel()
	const ociFile = "repositories/oci/source.yaml"
	oci := manifest.NamedResource{
		Group:     manifest.SourceDomain,
		Kind:      manifest.KindOCIRepository,
		Namespace: "flux-system",
		Name:      "source",
	}
	chart := manifest.NamedResource{
		Group:     manifest.SourceDomain,
		Kind:      manifest.KindHelmChart,
		Namespace: "flux-system",
		Name:      "chart",
	}
	sibling := testutil.HelmReleaseID("apps", "other")
	app := &manifest.Kustomization{Name: "app", Namespace: "flux-system", Path: "apps/app"}
	meta := &manifest.Kustomization{Name: "apps", Namespace: "flux-system", Path: "apps"}
	objects := filtertest.MapLister{app.Named(): app, meta.Named(): meta}
	f := newAdmission(t, admissionConfig{changes: change.NewSet([]string{ociFile}), index: filterFixture{
		files: map[manifest.NamedResource]string{
			oci:          ociFile,
			chart:        "apps/app/chart.yaml",
			sibling:      "apps/app/other.yaml",
			app.Named():  "apps/app.yaml",
			meta.Named(): "flux-system/apps.yaml",
		},
		objects: objects,
		refs:    map[manifest.NamedResource][]manifest.NamedResource{chart: {oci}},
	}.index(), inventory: objects, root: ""})

	for _, id := range []manifest.NamedResource{oci, chart, app.Named(), meta.Named()} {
		if !f.ShouldReconcile(id) {
			t.Errorf("source, chart and emitting ancestors must be kept: missing %s; keep=%v", id, f.KeepNames())
		}
	}
	assert.Equal(t, f.ShouldReconcile(sibling), false)
	f.AddEmitted(app.Named(), store.Child{Obj: refObj(sibling)}, f.objs)
	assert.Equal(t, f.ShouldReconcile(sibling), false)
}

// TestFilter_ReverseEdgeNoCascadeFromChangedConsumer pins the guard on
// the reverse edge: it fires ONLY for a source whose OWN file changed,
// never for a source merely referenced by a changed consumer. Two
// HelmReleases share one OCIRepository; editing hrA's file must keep
// hrA but must NOT drag hrB in via the shared source — otherwise a
// single app edit would reverse-cascade every sibling sharing a common
// app-template source.
func TestFilter_ReverseEdgeNoCascadeFromChangedConsumer(t *testing.T) {
	t.Parallel()
	shared := manifest.NamedResource{
		Group:     "source.toolkit.fluxcd.io",
		Kind:      manifest.KindOCIRepository,
		Namespace: "flux-system",
		Name:      "app-template",
	}
	aID := testutil.HelmReleaseID("apps", "a")
	bID := testutil.HelmReleaseID("apps", "b")

	f := newAdmission(
		t,
		admissionConfig{
			changes: change.NewSet([]string{"apps/a/hr.yaml"}),
			index: filterFixture{files: map[manifest.NamedResource]string{
				aID:    "apps/a/hr.yaml",
				bID:    "apps/b/hr.yaml",
				shared: "repositories/oci/app-template.yaml",
			}, root: "", objects: filtertest.EmptyLister(), refs: map[manifest.NamedResource][]manifest.NamedResource{
				aID: {shared},
				bID: {shared},
			}, excluded: nil, readers: nil, changed: change.NewSet([]string{"apps/a/hr.yaml"})}.index(),
			inventory: filtertest.EmptyLister(),
			root:      "",
		},
	)

	if !f.ShouldReconcile(aID) {
		t.Fatalf("changed HR a must be in keep; keep=%v", f.KeepNames())
	}
	if f.ShouldReconcile(bID) {
		t.Errorf("unchanged sibling HR b reverse-cascaded via shared source; keep=%v", f.KeepNames())
	}
}

func TestFilter_ForwardEdgeChangedHRKeepsChartRefSource(t *testing.T) {
	t.Parallel()
	const (
		hrFile  = "kubernetes/apps/media/plex/app/helmrelease.yaml"
		sibFile = "kubernetes/apps/media/other/app/helmrelease.yaml"
		ociFile = "kubernetes/flux/repositories/app-template.yaml"
	)
	// Render-driven HRs are seeded with an empty namespace (discovery records
	// them pre-inheritance); the shared chartRef OCIRepository lives elsewhere.
	hr := testutil.HelmReleaseID("", "plex")
	sibling := testutil.HelmReleaseID("", "other")
	oci := manifest.NamedResource{
		Group:     "source.toolkit.fluxcd.io",
		Kind:      manifest.KindOCIRepository,
		Namespace: "flux-system",
		Name:      "app-template",
	}

	f := newAdmission(
		t,
		admissionConfig{
			changes: change.NewSet([]string{hrFile}),
			index: filterFixture{files: // only plex's file changed
			map[manifest.NamedResource]string{
				hr:      hrFile,
				sibling: sibFile,
				oci:     ociFile,
			}, root: "", objects: filtertest.EmptyLister(), refs: map[manifest.NamedResource][]manifest.NamedResource{
				hr:      {oci},
				sibling: {oci}, // shares the same chart source
			}, excluded: nil, readers: nil, changed: change.NewSet([]string{hrFile})}.index(),
			inventory: filtertest.EmptyLister(),
			root:      "",
		},
	)

	if !f.ShouldReconcile(hr) {
		t.Fatalf("changed HelmRelease must be in keep; keep=%v", f.KeepNames())
	}
	if !f.ShouldReconcile(oci) {
		t.Fatalf(
			"forward edge: the changed HR's chartRef OCIRepository must be in keep so its chart is fetched; keep=%v",
			f.KeepNames(),
		)
	}
	// Keeping the shared source via plex's forward edge must NOT reverse-
	// cascade to a sibling HR that merely shares it (its file is unchanged).
	if f.ShouldReconcile(sibling) {
		t.Errorf("sibling HR sharing the chart source must NOT be pulled in; keep=%v", f.KeepNames())
	}
}

// TestFilter_SubstituteFromConfigMapKeepsProducerKustomization pins
// issue #418: a kept Kustomization with a non-Optional
// postBuild.substituteFrom ConfigMap must pull the producer KS that
// renders that CM into keep, even when the producer's own files
// didn't change. Without this, changed-only mode fails reconcile
// with "ConfigMap/flux-system/cluster-settings: dependency not found"
// because the producer never runs and the CM never materializes.
//
// The producer joins keep as ancestor-only — NOT primary — so its
// render output for unrelated siblings can't cascade into changed-
// only scope (the #204 keep-cascade rationale).
func TestFilter_SubstituteFromConfigMapKeepsProducerKustomization(t *testing.T) {
	t.Parallel()
	clusterAppsObj := &manifest.Kustomization{
		Name: "cluster-apps", Namespace: "flux-system",
		Path: "kubernetes/apps",
		PostBuild: &kustomizev1.PostBuild{SubstituteFrom: []manifest.SubstituteReference{
			{Kind: manifest.KindConfigMap, Name: "cluster-settings"},
		}},
	}
	clusterVarsObj := &manifest.Kustomization{
		Name: "cluster-vars", Namespace: "flux-system",
		Path: "kubernetes/flux/vars",
		// The CM lives in a shared Component referenced by
		// cluster-vars — ownersOf returns cluster-vars for any
		// file under the resolved component path.
		Components: []string{"../../components/cluster-settings"},
	}
	ntfyObj := &manifest.Kustomization{
		Name: "ntfy", Namespace: "communication",
		Path: "kubernetes/apps/communication/ntfy/app",
	}
	hrObj := &manifest.HelmRelease{Name: "ntfy", Namespace: "communication"}
	// rawSettings is the discovery-indexed pre-render form:
	// Namespace="" because kustomize's namespace directive hasn't
	// run yet at file-walk time.
	rawSettings := &manifest.ConfigMap{Name: "cluster-settings", Namespace: ""}
	rawSettingsID := rawSettings.Named()

	clusterApps := clusterAppsObj.Named()
	clusterVars := clusterVarsObj.Named()
	ntfy := ntfyObj.Named()
	hr := hrObj.Named()
	// renderedSettings is the form the consumer KS waits on once
	// kustomize's namespace directive has stamped flux-system onto
	// the rendered CM.
	renderedSettings := manifest.NamedResource{
		Kind:      manifest.KindConfigMap,
		Namespace: "flux-system",
		Name:      "cluster-settings",
	}

	f := newAdmission(
		t,
		admissionConfig{
			changes: change.NewSet([]string{"kubernetes/apps/communication/ntfy/app/helmrelease.yaml"}),
			index: filterFixture{files: map[manifest.NamedResource]string{
				clusterApps:   "kubernetes/flux/cluster-apps.yaml",
				clusterVars:   "kubernetes/flux/cluster-vars.yaml",
				ntfy:          "kubernetes/apps/communication/ntfy/ks.yaml",
				hr:            "kubernetes/apps/communication/ntfy/app/helmrelease.yaml",
				rawSettingsID: "kubernetes/components/cluster-settings/cluster-settings.yaml",
			}, root: "", objects: filtertest.MapLister{
				clusterApps:   clusterAppsObj,
				clusterVars:   clusterVarsObj,
				ntfy:          ntfyObj,
				hr:            hrObj,
				rawSettingsID: rawSettings,
			}}.index(),
			inventory: filtertest.MapLister{
				clusterApps:   clusterAppsObj,
				clusterVars:   clusterVarsObj,
				ntfy:          ntfyObj,
				hr:            hrObj,
				rawSettingsID: rawSettings,
			},
			root: "",
		},
	)

	// The leaf HR's file changed, so ntfy + the cluster-apps
	// ancestor are pulled in; the rendered ConfigMap is a
	// substituteFrom dep of cluster-apps; and cluster-vars is the
	// producer of that ConfigMap.
	for _, id := range []manifest.NamedResource{clusterApps, ntfy, hr, renderedSettings, clusterVars} {
		if !f.ShouldReconcile(id) {
			t.Errorf("expected %s in keep; keep=%v", id, f.KeepNames())
		}
	}
	// The producer is ancestor-only: AddEmitted from cluster-vars
	// to a sentinel child must reject (producer is NOT primary).
	sentinel := testutil.KustomizationID("unrelated", "sentinel")
	f.AddEmitted(clusterVars, store.Child{Obj: refObj(sentinel)}, f.objs)
	if f.ShouldReconcile(sentinel) {
		t.Errorf(
			"producer cluster-vars must be ancestor-only (NOT primary) — AddEmitted leaked sentinel into keep; keep=%v",
			f.KeepNames(),
		)
	}
}

type testFilterTransitiveDepsHelmReleaseViaHelmChartCRDInitializationInputs struct {
	hr    **manifest.HelmRelease
	hc    **manifest.HelmChartSource
	hrID  *manifest.NamedResource
	hcID  *manifest.NamedResource
	ociID *manifest.NamedResource
}

func testFilterTransitiveDepsHelmReleaseViaHelmChartCRDInitialization(
	t *testing.T,
	testCaseInput *testFilterTransitiveDepsHelmReleaseViaHelmChartCRDInitializationInputs,
) {
	t.Helper()

	f := newAdmission(
		t,
		admissionConfig{
			changes: change.NewSet([]string{"hr.yaml"}),
			index: filterFixture{
				files: map[manifest.NamedResource]string{(*testCaseInput.hrID): "hr.yaml"},
				root:  "",
				objects: filtertest.MapLister{
					(*testCaseInput.hrID): (*testCaseInput.hr),
					(*testCaseInput.hcID): (*testCaseInput.hc),
				},
			}.index(),
			inventory: filtertest.MapLister{
				(*testCaseInput.hrID): (*testCaseInput.hr),
				(*testCaseInput.hcID): (*testCaseInput.hc),
			},
			root: "",
		},
	)
	for _, id := range []manifest.NamedResource{(*testCaseInput.hrID), (*testCaseInput.hcID), (*testCaseInput.ociID)} {
		if !f.ShouldReconcile(id) {
			t.Errorf("transitive dependency %s not selected: keep=%v", id, f.KeepNames())
		}
	}
}

type testFilterTransitiveDepsHelmReleaseViaHelmChartCRDRuntimeInputs struct {
	hr    **manifest.HelmRelease
	hc    **manifest.HelmChartSource
	hrID  *manifest.NamedResource
	hcID  *manifest.NamedResource
	ociID *manifest.NamedResource
}

func testFilterTransitiveDepsHelmReleaseViaHelmChartCRDRuntime(
	t *testing.T,
	testCaseInput *testFilterTransitiveDepsHelmReleaseViaHelmChartCRDRuntimeInputs,
) {
	t.Helper()

	parent := testutil.KustomizationID("apps", "parent")
	objects := filtertest.MapLister{(*testCaseInput.hcID): (*testCaseInput.hc)}
	f := newAdmission(
		t,
		admissionConfig{
			changes: change.NewSet([]string{"parent.yaml"}),
			index: filterFixture{
				files:   map[manifest.NamedResource]string{parent: "parent.yaml"},
				objects: objects,
			}.index(),
			inventory: objects,
			root:      "",
		},
	)
	for _, id := range []manifest.NamedResource{(*testCaseInput.hrID), (*testCaseInput.hcID), (*testCaseInput.ociID)} {
		assert.Equal(t, f.ShouldReconcile(id), false)
	}

	added := f.AddEmitted(parent, store.Child{Obj: (*testCaseInput.hr)}, f.objs)

	assert.Equal(t, len(added), 3)
	for _, id := range []manifest.NamedResource{(*testCaseInput.hrID), (*testCaseInput.hcID), (*testCaseInput.ociID)} {
		if !f.ShouldReconcile(id) || !slices.Contains(added, id) {
			t.Errorf(
				"transitive dependency %s not selected and returned: added=%v, keep=%v",
				id,
				added,
				f.KeepNames(),
			)
		}
	}
}
