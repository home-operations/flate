package orchestrator

import (
	"path/filepath"
	"testing"

	"github.com/home-operations/flate/internal/selection"

	helmv2 "github.com/fluxcd/helm-controller/api/v2"

	"github.com/home-operations/flate/internal/admissiontest"
	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/baseline"
	"github.com/home-operations/flate/internal/change"
	"github.com/home-operations/flate/internal/controllers/base"
	"github.com/home-operations/flate/internal/controllers/kustomization"
	"github.com/home-operations/flate/internal/discovery"
	"github.com/home-operations/flate/internal/loader"
	"github.com/home-operations/flate/internal/store"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
)

func TestOrchestratorExistence_PromotionDoesNotAdmitControllerSiblings(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	testutil.WriteFile(t, dir, "bundle.yaml", `apiVersion: v1
kind: ConfigMap
metadata: {name: selected, namespace: apps}
data: {value: selected}
`+"---\n"+
		`apiVersion: helm.toolkit.fluxcd.io/v2
kind: HelmRelease
metadata: {name: raw-hr, namespace: apps}
spec:
  chart:
    spec:
      chart: charts/demo
      sourceRef: {kind: GitRepository, name: source, namespace: apps}
`+"---\n"+
		testutil.KustomizationYAML(
			"raw-ks",
			"apps",
			"  {path: ./apps, sourceRef: {kind: GitRepository, name: source}}\n",
		)+"---\n"+
		`apiVersion: fluxcd.controlplane.io/v1
kind: ResourceSet
metadata: {name: raw-rs, namespace: apps}
spec: {}
`+"---\n"+
		`apiVersion: seaweed.seaweedfs.com/v1
kind: Bucket
metadata: {name: foreign-source, namespace: apps}
spec: {}
`)
	id := manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: "apps", Name: "selected"}
	idx := loader.New(store.New())
	idx.Index.Records[id] = manifest.DiscoveryRecord{
		Authored:    id,
		Path:        filepath.Join(dir, "bundle.yaml"),
		Productions: []manifest.Production{{Authored: id, Path: filepath.Join(dir, "bundle.yaml")}},
	}
	st := store.New()
	idx.Store = st
	input, _, err := baseline.AdmitDirectory(t.Context(), dir, dir, manifest.WorkScope{})
	assert.NoError(t, err)
	st.SetArtifact(manifest.BootstrapSourceID, input)
	e := &orchestratorExistence{DiscoveryIndex: idx.Index, o: &Orchestrator{
		store: idx.Store, index: idx.Index, cfg: treeConfig{Config: Config{}},
		ksc: &kustomization.Controller{Controller: base.New(st, nil)},
	}}
	{
		promoted, err := e.Promote(t.Context(), selection.PrepareRequest{Reads: nil}, id)
		assert.FatalIf(t, err != nil || !promoted || st.GetObject(id) == nil, "%v", "selected data must be admitted")
	}
	for _, excluded := range []manifest.NamedResource{
		testutil.HelmReleaseID("apps", "raw-hr"),
		testutil.KustomizationID("apps", "raw-ks"),
		{Group: "fluxcd.controlplane.io", Kind: manifest.KindResourceSet, Namespace: "apps", Name: "raw-rs"},
		{Group: "source.toolkit.fluxcd.io", Kind: manifest.KindBucket, Namespace: "apps", Name: "foreign-source"},
	} {
		assert.ErrorIf(t, st.GetObject(excluded) != nil, "runtime promotion admitted %s", excluded)
	}
}

func TestOrchestratorExistence_PromotionPreservesSelectedContent(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	testutil.WriteFile(t, dir, "bundle.yaml", `apiVersion: v1
kind: ConfigMap
metadata: {name: selected, namespace: authored}
data: {values.yaml: "greeting: selected"}
---
apiVersion: v1
kind: ConfigMap
metadata: {name: excluded, namespace: apps}
data: {value: excluded}
---
apiVersion: source.toolkit.fluxcd.io/v1
kind: GitRepository
metadata: {name: chart, namespace: apps}
spec: {url: https://example.test/chart.git}
---
apiVersion: source.toolkit.fluxcd.io/v1
kind: GitRepository
metadata: {name: excluded-source, namespace: apps}
spec: {url: https://example.test/excluded.git}
`)
	st := store.New()
	hr := &manifest.HelmRelease{
		Name:      "consumer",
		Namespace: "apps",
		Chart: manifest.HelmChart{
			Source: manifest.NamedResource{
				Group:     manifest.SourceDomain,
				Kind:      manifest.KindGitRepository,
				Namespace: "apps",
				Name:      "chart",
			},
		},
		HelmReleaseSpec: helmv2.HelmReleaseSpec{
			Chart: &helmv2.HelmChartTemplate{
				Spec: helmv2.HelmChartTemplateSpec{
					Chart:   "chart",
					Version: "",
					SourceRef: &helmv2.CrossNamespaceObjectReference{
						Kind:       manifest.KindGitRepository,
						Name:       "chart",
						Namespace:  "apps",
						APIVersion: manifest.SourceDomain + "/v1",
					},
				},
			},
			ValuesFrom: []manifest.ValuesReference{{Kind: manifest.KindConfigMap, Name: "selected"}},
		},
	}
	st.AddObject(hr)
	selected := manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: "apps", Name: "selected"}
	excluded := manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: "apps", Name: "excluded"}
	chart := manifest.NamedResource{
		Group:     "source.toolkit.fluxcd.io",
		Kind:      manifest.KindGitRepository,
		Namespace: "apps",
		Name:      "chart",
	}
	excludedSource := manifest.NamedResource{
		Group:     "source.toolkit.fluxcd.io",
		Kind:      manifest.KindGitRepository,
		Namespace: "apps",
		Name:      "excluded-source",
	}
	files := map[manifest.NamedResource]string{
		hr.Named():     "consumer.yaml",
		selected:       "bundle.yaml",
		excluded:       "bundle.yaml",
		chart:          "bundle.yaml",
		excludedSource: "bundle.yaml",
	}
	idx, session := admissiontest.New(
		t,
		admissiontest.Config{
			Changes:   change.NewSet([]string{"consumer.yaml"}),
			Index:     indexForTest(files),
			Inventory: st,
		},
	)
	admissiontest.Commit(t, session, nil)
	input, _, err := baseline.AdmitDirectory(t.Context(), dir, dir, manifest.WorkScope{})
	assert.NoError(t, err)
	st.SetArtifact(manifest.BootstrapSourceID, input)
	testutil.WriteFile(t, dir, "kustomization.yaml", "namespace: apps\nresources: [bundle.yaml]\n")
	idx.SourceRoot = dir
	idx.Options.Session = session
	_, err = idx.Load(t.Context(), dir, manifest.NamedResource{}, nil)
	assert.NoError(t, err)
	assert.NoError(t, idx.FinalizeIndex(t.Context(), dir, true))
	controller := base.New(st, nil)
	controller.Configure(base.Options{Session: session, Index: idx.Index, Loader: idx})
	e := &orchestratorExistence{DiscoveryIndex: idx.Index, o: &Orchestrator{
		store:     idx.Store,
		index:     idx.Index,
		loader:    idx,
		cfg:       treeConfig{Config: Config{}},
		selection: session,
		ksc:       &kustomization.Controller{Controller: controller},
	}}
	size := len(selectionNamesForTest(session, st, idx.Index))
	reads := &store.ReadSet{}
	reads.Init(st, hr.Named(), nil)
	defer reads.Retire()
	reads.RequireData(chart)
	reads.RequireData(selected)
	{
		promoted, err := e.Promote(
			t.Context(),
			selection.PrepareRequest{Reads: reads},
			selected,
		)
		assert.FatalIf(t, err != nil || !promoted || st.GetObject(selected) == nil ||
			st.GetObject(chart) == nil, "%v", "selected values and required chart source must be admitted")
	}
	{
		got := st.GetObject(selected).(*manifest.ConfigMap).Data["values.yaml"]
		assert.ErrorIf(t, got != "greeting: selected", "selected values = %v", got)
	}
	for _, id := range []manifest.NamedResource{excluded, excludedSource} {
		{
			promoted, err := e.Promote(
				t.Context(),
				selection.PrepareRequest{Reads: nil},
				id,
			)
			assert.ErrorIf(t, err != nil || promoted || st.GetObject(id) != nil ||
				session.Snapshot().Reconcile(id), "excluded sibling %s must remain absent", id)
		}
	}
	assert.ErrorIf(
		t,
		len(selectionNamesForTest(session, st, idx.Index)) != size,
		"%v",
		"ordering promotion must not expand keep",
	)
	st.AddObject(&manifest.ConfigMap{Name: "excluded", Namespace: "apps"})
	{
		promoted, err := e.Promote(t.Context(), selection.PrepareRequest{Reads: nil}, excluded)
		assert.ErrorIf(t, err != nil || promoted, "%v", "availability must not bypass admission")
	}
}

func TestOrchestratorExistence_ExistingDataUsesNewSourceEvidence(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	file := "settings.yaml"
	testutil.WriteFile(
		t,
		dir,
		file,
		"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: settings, namespace: apps}\ndata: {value: selected}\n",
	)
	st := store.New()
	source := &manifest.GitRepository{Name: "source", Namespace: "apps"}
	data := &manifest.ConfigMap{Name: "settings", Namespace: "apps", Data: map[string]any{"value": "selected"}}
	st.AddObject(source)
	st.AddObject(data)
	id := data.Named()
	index := indexForTest(map[manifest.NamedResource]string{id: file})
	record := index.Records[id]
	record.Path = filepath.Join(dir, file)
	record.Productions = []manifest.Production{{Authored: id, Source: source.Named(), Path: record.Path, File: file}}
	index.Records[id] = record
	filterLoader, session := admissiontest.New(
		t,
		admissiontest.Config{Changes: change.NewSet([]string{file}), Index: index, Inventory: st, Root: dir},
	)
	admissiontest.Commit(t, session, nil)
	assert.FatalIf(
		t,
		session.Snapshot().Reconcile(id),
		"%v",
		"unavailable source evidence must not admit existing data",
	)
	input, _, err := baseline.AdmitDirectory(t.Context(), dir, dir, manifest.WorkScope{})
	assert.NoError(t, err)
	st.SetArtifact(source.Named(), input)
	controller := base.New(st, nil)
	controller.Configure(base.Options{Session: session, Loader: filterLoader, Index: index})
	e := &orchestratorExistence{DiscoveryIndex: index, o: &Orchestrator{
		store: st, index: index, selection: session, loader: filterLoader,
		ksc: &kustomization.Controller{Controller: controller},
	}}
	var reads store.ReadSet
	reads.Init(st, testutil.KustomizationID("apps", "reader"), nil)
	defer reads.Close()
	reads.RequireData(id)
	{
		promoted, err := e.Promote(t.Context(), selection.PrepareRequest{Reads: &reads}, id)
		assert.FatalIf(
			t,
			err != nil || !promoted || !session.Snapshot().Reconcile(id),
			"%v",
			"new source evidence must admit the existing changed data",
		)
	}
	assert.Equal(t, st.GetObject(id).(*manifest.ConfigMap).Data["value"], "selected")
}

func TestOrchestratorExistence_FileIndexNamespaceConvention(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name, recorded, requested string
		raw, want                 bool
	}{
		{"exact", "apps", "apps", true, true},
		{"inherited namespace", "", "apps", true, true},
		{"explicit other namespace", "other", "apps", true, false},
		{"bare query cannot match explicit", "apps", "", true, false},
		{"generated object", "apps", "apps", false, false},
		{"bare generated object", "", "apps", false, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			idx := loader.New(store.New())
			id := testutil.HelmReleaseID(tc.recorded, "target")
			if tc.raw {
				idx.Index.Records[id] = manifest.DiscoveryRecord{Authored: id, Path: "target.yaml"}
			} else {
				idx.Index.Records[id] = manifest.DiscoveryRecord{File: "target.yaml"}
			}
			e := &orchestratorExistence{DiscoveryIndex: idx.Index, o: &Orchestrator{store: idx.Store, index: idx.Index}}
			id.Namespace = tc.requested
			{
				got := e.IsFileIndexed(id)
				assert.ErrorIf(t, got != tc.want, "IsFileIndexed(%s) = %v, want %v", id, got, tc.want)
			}
		})
	}
}

func TestOrchestratorExistence_MissingBootstrapSources(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name, kind                     string
		absentDeclaration, stale, want bool
	}{
		{name: "oci", kind: manifest.KindOCIRepository},
		{name: "helm", kind: manifest.KindHelmRepository},
		{name: "bucket", kind: manifest.KindBucket},
		{name: "git", kind: manifest.KindGitRepository, want: true},
		{name: "missing requesting declaration", kind: manifest.KindGitRepository, absentDeclaration: true},
		{name: "invalid reads", kind: manifest.KindGitRepository, stale: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			root := t.TempDir()
			testutil.WriteFile(t, root, "apps/decoy", "working tree")
			st := store.New()
			result, err := discovery.Run(t.Context(), discovery.Config{Path: root, Store: st})
			assert.NoError(t, err)
			consumer := &manifest.Kustomization{Name: "consumer", Namespace: "apps"}
			if !tc.absentDeclaration {
				st.AddObject(consumer)
			}
			var reads store.ReadSet
			reads.Init(st, consumer.Named(), nil)
			defer reads.Retire()
			if tc.stale {
				marker := &manifest.ConfigMap{Name: "marker", Namespace: "apps"}
				reads.GetObject(marker.Named())
				st.AddObject(marker)
				assert.FatalIf(t, reads.Valid(), "%v", "fixture reads remain valid")
			}
			controller := base.New(st, nil)
			controller.Configure(base.Options{})
			o := &Orchestrator{
				store:     st,
				index:     result.Index,
				admission: result.Admission,
				ksc:       &kustomization.Controller{Controller: controller},
			}
			e := &orchestratorExistence{DiscoveryIndex: result.Index, o: o}
			id := manifest.NamedResource{
				Group:     manifest.SourceDomain,
				Kind:      tc.kind,
				Namespace: "sources",
				Name:      "missing",
			}
			promoted, err := e.Promote(t.Context(), selection.PrepareRequest{Reads: &reads}, id)
			assert.NoError(t, err)
			assert.Equal(t, promoted, tc.want)
			if !tc.want {
				assert.FatalIf(
					t,
					st.GetObject(id) != nil || st.GetArtifact(id, store.ArtifactContent) != nil,
					"%v",
					"unresolved source was published",
				)
				{
					status, ok := st.GetStatus(id)
					assert.FatalIf(t, ok && status.Status == store.StatusReady, "%v", "unresolved source became ready")
				}
			} else {
				assert.FatalIf(
					t,
					st.GetObject(id) == nil || st.GetArtifact(id, store.ArtifactContent) == nil,
					"%v",
					"Git control did not publish its alias",
				)
			}
		})
	}
}
