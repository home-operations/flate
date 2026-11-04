package loader

import (
	"slices"
	"testing"

	fluxopv1 "github.com/controlplaneio-fluxcd/flux-operator/api/v1"
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
				assert.Equal(t, f.ShouldReconcile(id), true)
				if id.Group == "" {
					id.Group = "foreign.example"
				} else {
					id.Group = ""
				}
				assert.Equal(t, f.ShouldReconcile(id), false)
			}
			assert.Equal(
				t,
				f.ShouldReconcile(
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
	assert.Equal(t, f.ShouldReconcile(effective), true)
	foreign := effective
	foreign.Namespace = "unrelated"
	assert.Equal(t, f.ShouldReconcile(foreign), false)
	foreign = effective
	foreign.Group = "foreign.example"
	assert.Equal(t, f.ShouldReconcile(foreign), false)
}

func TestAddEmitted_CapturedTransitiveReferences(t *testing.T) {
	t.Parallel()
	parent := testutil.KustomizationID("apps", "parent")
	dependency := &manifest.GitRepository{Name: "dependency", Namespace: "apps"}
	dependency.SecretRef = &manifest.LocalObjectReference{Name: "captured"}
	st := store.New()
	st.AddObject(dependency)
	f := newAdmission(
		t,
		admissionConfig{
			changes:   change.NewSet([]string{"parent.yaml"}),
			index:     new(filterFixture{files: map[manifest.NamedResource]string{parent: "parent.yaml"}}).index(),
			inventory: st,
			root:      "",
		},
	)
	var reads store.ReadSet
	reads.Init(st, parent, nil)
	defer reads.Retire()
	reads.GetObject(dependency.Named())
	changed := *dependency
	changed.SecretRef = &manifest.LocalObjectReference{Name: "newer"}
	st.AddObject(&changed)
	child := &manifest.Kustomization{Name: "child", Namespace: "apps"}
	child.SourceRef.Kind, child.SourceRef.Name = manifest.KindGitRepository, dependency.Name
	f.AddEmitted(parent, store.Child{Obj: child}, &reads)
	assert.Equal(
		t,
		f.ShouldReconcile(manifest.NamedResource{Kind: manifest.KindSecret, Namespace: "apps", Name: "captured"}),
		true,
	)
	assert.Equal(
		t,
		f.ShouldReconcile(manifest.NamedResource{Kind: manifest.KindSecret, Namespace: "apps", Name: "newer"}),
		false,
	)
	assert.Equal(t, reads.Valid(), false)
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

func TestAddEmitted_KeptChildUsesIncomingReferences(t *testing.T) {
	t.Parallel()
	for _, name := range []string{"ancestor_only", "primary", "kept_emitter"} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			testAddEmittedKeptChildUsesIncomingReferencesCase(
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
	assert.Equal(t, f.keep[a.Named()], primary)
	want := absent
	switch *testCaseInput {
	case "generated", "absent":
		want = kept
	case "selected":
		want = primary
	}
	assert.Equal(t, f.keep[b.Named()], want)
	assert.Equal(
		t,
		f.ShouldReconcile(producer),
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
	objects := filtertest.EmptyLister()
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
	assert.Equal(t, f.ShouldReconcile(parent), true)
	for _, id := range []manifest.NamedResource{hr.Named(), chartSrc, oldSource} {
		assert.Equal(t, f.ShouldReconcile(id), false)
	}

	added := f.AddEmitted(parent, store.Child{Obj: hr}, f.objs)

	assert.Equal(t, len(added), 2)
	for _, id := range []manifest.NamedResource{hr.Named(), chartSrc} {
		if !f.ShouldReconcile(id) || !slices.Contains(added, id) {
			t.Errorf(
				"current identity %s not selected and returned: added=%v, keep=%v",
				id,
				added,
				f.KeepNames(),
			)
		}
	}
	if f.ShouldReconcile(oldSource) || slices.Contains(added, oldSource) {
		t.Errorf("obsolete source selected or returned: added=%v, keep=%v", added, f.KeepNames())
	}
}

func testAddEmittedKeptChildUsesIncomingReferencesCase(
	t *testing.T,
	testCaseInput *string,
) {
	t.Helper()

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
	stale := *child
	stale.Chart.Source.Name = "old-chart"
	stale.ChartRef = &helmv2.CrossNamespaceSourceReference{
		Kind:      manifest.KindOCIRepository,
		Name:      "old-chart",
		Namespace: "apps",
	}
	competitor := &manifest.Kustomization{Name: "competitor", Namespace: "apps"}
	index := new(filterFixture{files: map[manifest.NamedResource]string{parent: "parent.yaml"}}).index()
	index.Records[child.Named()] = manifest.DiscoveryRecord{
		Productions: []manifest.Production{{Owner: competitor.Named(), Source: manifest.BootstrapSourceID}},
	}
	objects := filtertest.MapLister{child.Named(): &stale, competitor.Named(): competitor}
	f := newAdmission(
		t,
		admissionConfig{
			changes:   change.NewSet([]string{"parent.yaml"}),
			index:     index,
			inventory: objects,
			root:      "",
		},
	)
	if (*testCaseInput) == "primary" {
		f.addUngated(child.Named())
	} else {
		f.mu.Lock()
		f.keep[child.Named()] = kept
		if (*testCaseInput) == "kept_emitter" {
			f.keep[parent] = kept
		}
		f.mu.Unlock()
	}
	assert.Equal(t, f.ShouldReconcile(child.Named()), true)
	assert.Equal(t, f.ShouldReconcile(child.ChartSource()), false)

	added := f.AddEmitted(parent, store.Child{Obj: child}, f.objs)

	want := []manifest.NamedResource{child.ChartSource()}
	if (*testCaseInput) != "primary" {
		want = append(want, competitor.Named())
		slices.SortFunc(want, manifest.NamedResource.Compare)
	}
	assert.Diff(t, added, want)
	assert.Equal(t, f.keep[competitor.Named()], kept)
	assert.Equal(t, f.ShouldReconcile(stale.ChartSource()), false)
	assert.Equal(t, f.ShouldReconcile(child.ChartSource()), true)
	sentinel := testutil.KustomizationID("apps", "sentinel")
	var sentinels []manifest.NamedResource
	if (*testCaseInput) != "kept_emitter" {
		sentinels = []manifest.NamedResource{sentinel}
	}
	assert.Diff(t, f.AddEmitted(child.Named(), store.Child{Obj: refObj(sentinel)}, f.objs), sentinels)
	assert.Equal(t, f.ShouldReconcile(sentinel), (*testCaseInput) != "kept_emitter")
}
