package loader

import (
	"slices"
	"testing"

	fluxopv1 "github.com/controlplaneio-fluxcd/flux-operator/api/v1"
	helmv2 "github.com/fluxcd/helm-controller/api/v2"
	kustomizev1 "github.com/fluxcd/kustomize-controller/api/v1"
	sourcev1 "github.com/fluxcd/source-controller/api/v1"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/change"
	"github.com/home-operations/flate/internal/filtertest"
	"github.com/home-operations/flate/internal/store"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
)

func TestFilter_ContentDependencies(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		obj  manifest.BaseManifest
		deps []manifest.NamedResource
	}{
		{&manifest.HelmRelease{Name: "plex", Namespace: "media", Chart: manifest.HelmChart{Source: manifest.NamedResource{Group: manifest.SourceDomain, Kind: manifest.KindOCIRepository, Namespace: "flux-system", Name: "app-template"}}, ChartRef: &helmv2.CrossNamespaceSourceReference{Kind: manifest.KindOCIRepository, Name: "app-template", Namespace: "flux-system"}, ValuesFrom: []manifest.ValuesReference{{Kind: manifest.KindConfigMap, Name: "plex-values"}}}, []manifest.NamedResource{{Group: "source.toolkit.fluxcd.io", Kind: manifest.KindOCIRepository, Namespace: "flux-system", Name: "app-template"}, {Kind: manifest.KindConfigMap, Namespace: "media", Name: "plex-values"}}},
		{&manifest.GitRepository{Name: "source", Namespace: "apps", SecretRef: &manifest.LocalObjectReference{Name: "auth"}}, []manifest.NamedResource{{Kind: manifest.KindSecret, Namespace: "apps", Name: "auth"}}},
		{&manifest.OCIRepository{Name: "source", Namespace: "apps", SecretRef: &manifest.LocalObjectReference{Name: "auth"}, CertSecretRef: &manifest.LocalObjectReference{Name: "cert"}}, []manifest.NamedResource{{Kind: manifest.KindSecret, Namespace: "apps", Name: "auth"}, {Kind: manifest.KindSecret, Namespace: "apps", Name: "cert"}}},
		{&manifest.ResourceSet{Name: "set", Namespace: "apps", InputsFrom: []fluxopv1.InputProviderReference{{Name: "inputs"}}}, []manifest.NamedResource{{Group: "fluxcd.controlplane.io", Kind: manifest.KindResourceSetInputProvider, Namespace: "apps", Name: "inputs"}}},
		{&manifest.HelmChartSource{Name: "chart", Namespace: "apps", SourceRef: sourcev1.LocalHelmChartSourceReference{Name: "source"}}, []manifest.NamedResource{{Group: "source.toolkit.fluxcd.io", Kind: manifest.KindHelmRepository, Namespace: "apps", Name: "source"}}},
	} {
		t.Run(tc.obj.Named().Kind, func(t *testing.T) {
			t.Parallel()
			objects := filtertest.MapLister{tc.obj.Named(): tc.obj}
			f := newAdmission(
				t,
				admissionConfig{
					changes: change.NewSet([]string{"changed.yaml"}),
					index: new(filterFixture{
						files:   map[manifest.NamedResource]string{tc.obj.Named(): "changed.yaml"},
						objects: objects,
					}).index(),
					inventory: objects,
					root:      "",
				},
			)
			for _, id := range tc.deps {
				assert.Equal(t, f.Reconcile(id), true)
				if id.Group == "" {
					id.Group = "foreign.example"
				} else {
					id.Group = ""
				}
				assert.Equal(t, f.Reconcile(id), false)
			}
			assert.Equal(
				t,
				f.Reconcile(
					manifest.NamedResource{Kind: manifest.KindSecret, Namespace: "apps", Name: "unrelated"},
				),
				false,
			)
		})
	}
}

func TestFilter_DependsOnNotFollowed(t *testing.T) {
	t.Parallel()
	for _, mode := range []string{"present", "file_indexed", "bare_file_indexed", "generated", "absent", "selected"} {
		t.Run(mode, func(t *testing.T) {
			t.Parallel()
			testFilterDependsOnNotFollowedCase(t, &mode)
		},
		)
	}
}

func TestFilter_ShouldReconcileRequiresProvenNamespace(t *testing.T) {
	t.Parallel()
	bare := testutil.HelmReleaseID("", "x")
	effective := testutil.HelmReleaseID("media", "x")
	st := store.New()
	owner := testutil.KustomizationID("media", "parent")
	proof := manifest.Production{
		Owner:    owner,
		Authored: bare,
		File:     "child.yaml",
		Path:     "/repo/child.yaml",
		Source:   manifest.BootstrapSourceID,
	}
	index := &manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{
		bare:      {File: "child.yaml", Productions: []manifest.Production{proof}},
		effective: {Productions: []manifest.Production{proof}},
	}}
	f := newAdmission(
		t,
		admissionConfig{changes: change.NewSet([]string{"child.yaml"}), index: index, inventory: st, root: "/repo"},
	)
	assert.Equal(t, f.Reconcile(effective), true)
	foreign := effective
	foreign.Namespace = "unrelated"
	assert.Equal(t, f.Reconcile(foreign), false)
	foreign = effective
	foreign.Group = "foreign.example"
	assert.Equal(t, f.Reconcile(foreign), false)
}

func TestAddEmitted_StampedHelmReleasePullsChartSource(t *testing.T) {
	t.Parallel()
	for _, name := range []string{"new_child", "replaced_child"} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			testAddEmittedStampedHelmReleasePullsChartSourceCase(
				t,
				&name,
			)
		},
		)
	}
}

func testFilterDependsOnNotFollowedCase(t *testing.T, testCaseInput *string,
) {
	t.Helper()

	a := &manifest.Kustomization{
		Name:      "a",
		Namespace: "apps",
		DependsOn: []kustomizev1.DependencyReference{{Name: "b"}},
	}
	b := &manifest.Kustomization{Name: "b", Namespace: "apps"}
	producer := testutil.KustomizationID("apps", "producer")
	st := store.New()
	st.AddObject(a)
	index := &manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{
		a.Named(): {File: "a.yaml"},
		b.Named(): {File: "b.yaml", Productions: []manifest.Production{{Owner: producer}}},
	}}
	if (*testCaseInput) == "present" || (*testCaseInput) == "selected" {
		st.AddObject(b)
	}
	if (*testCaseInput) == "file_indexed" || (*testCaseInput) == "bare_file_indexed" {
		id := b.Named()
		if (*testCaseInput) == "bare_file_indexed" {
			id.Namespace = ""
		}
		index.Records[id] = manifest.DiscoveryRecord{Path: "/repo/b.yaml", File: "b.yaml"}
	}
	if (*testCaseInput) == "absent" {
		delete(index.Records, b.Named())
	}
	changes := []string{"a.yaml"}
	if (*testCaseInput) == "selected" {
		changes = append(changes, "b.yaml")
	}
	f := newAdmission(
		t,
		admissionConfig{changes: change.NewSet(changes), index: index, inventory: st, root: "/repo"},
	)
	assert.Equal(t, f.role(a.Named()), primary)
	want := absent
	switch *testCaseInput {
	case "generated", "absent":
		want = kept
	case "selected":
		want = primary
	}
	assert.Equal(t, f.role(b.Named()), want)
	assert.Equal(
		t,
		f.Reconcile(producer),
		(*testCaseInput) == "generated" || (*testCaseInput) == "selected",
	)
}

func testAddEmittedStampedHelmReleasePullsChartSourceCase(
	t *testing.T,
	testCaseInput *string,
) {
	t.Helper()

	parent := testutil.KustomizationID("flux-system", "cluster-apps")
	hr := &manifest.HelmRelease{
		Name:      "sonarr",
		Namespace: "media",
		Chart: manifest.HelmChart{
			Source: manifest.NamedResource{
				Group:     manifest.SourceDomain,
				Kind:      manifest.KindOCIRepository,
				Namespace: "flux-system",
				Name:      "app-template",
			},
		},
		ChartRef: &helmv2.CrossNamespaceSourceReference{
			Kind:      manifest.KindOCIRepository,
			Name:      "app-template",
			Namespace: "flux-system",
		},
	}
	chartSrc := manifest.NamedResource{
		Group:     manifest.SourceDomain,
		Kind:      manifest.KindOCIRepository,
		Namespace: "flux-system",
		Name:      "app-template",
	}
	oldSource := chartSrc
	oldSource.Name = "old-chart"
	objects := filtertest.MapLister{parent: &manifest.Kustomization{Name: parent.Name, Namespace: parent.Namespace}}
	if (*testCaseInput) == "replaced_child" {
		old := *hr
		old.Chart.Source.Name = oldSource.Name
		objects[old.Named()] = &old
	}
	f := newAdmission(
		t,
		admissionConfig{
			changes: change.NewSet([]string{"apps/media/kustomization.yaml"}),
			index: new(filterFixture{
				files:   map[manifest.NamedResource]string{parent: "apps/media/kustomization.yaml"},
				objects: objects,
			}).index(),
			inventory: objects,
			root:      "",
		},
	)
	assert.Equal(t, f.Reconcile(parent), true)
	for _, id := range []manifest.NamedResource{hr.Named(), chartSrc, oldSource} {
		assert.Equal(t, f.Reconcile(id), false)
	}

	added := f.AddEmitted(parent, new(store.Child{Obj: hr}), f.objs)

	assert.Equal(t, len(added), 2)
	for _, id := range []manifest.NamedResource{hr.Named(), chartSrc} {
		if !f.Reconcile(id) || !slices.Contains(added, id) {
			t.Errorf(
				"current identity %s not selected and returned: added=%v, keep=%v",
				id,
				added,
				f.KeepNames(),
			)
		}
	}
	if f.Reconcile(oldSource) || slices.Contains(added, oldSource) {
		t.Errorf("obsolete source selected or returned: added=%v, keep=%v", added, f.KeepNames())
	}
}
