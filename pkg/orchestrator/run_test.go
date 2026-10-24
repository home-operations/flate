package orchestrator

import (
	"path/filepath"
	"testing"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/baseline"
	"github.com/home-operations/flate/pkg/change"
	"github.com/home-operations/flate/pkg/controllers/base"
	"github.com/home-operations/flate/pkg/controllers/kustomization"
	"github.com/home-operations/flate/pkg/loader"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
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
		testutil.KustomizationYAML("raw-ks", "apps", "  {path: ./apps, sourceRef: {kind: GitRepository, name: source}}\n")+"---\n"+
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
	idx.Index.Records[id] = manifest.DiscoveryRecord{Authored: id, Path: filepath.Join(dir, "bundle.yaml"), Productions: []manifest.Production{{Authored: id, Path: filepath.Join(dir, "bundle.yaml")}}}
	st := store.New()
	idx.Store = st
	input, _, err := baseline.AdmitDirectory(t.Context(), dir, dir)
	assert.NoError(t, err)
	st.SetArtifact(manifest.BootstrapSourceID, input)
	e := &orchestratorExistence{o: &Orchestrator{
		store: idx.Store, index: idx.Index, cfg: Config{WipeSecrets: true},
		ksc: &kustomization.Controller{Controller: base.New(st, nil)},
	}}
	if !e.Promote(t.Context(), nil, id) || st.GetObject(id) == nil {
		t.Fatal("selected data must be admitted")
	}
	for _, excluded := range []manifest.NamedResource{
		testutil.HelmReleaseID("apps", "raw-hr"),
		testutil.KustomizationID("apps", "raw-ks"),
		{Group: "fluxcd.controlplane.io", Kind: manifest.KindResourceSet, Namespace: "apps", Name: "raw-rs"},
		{Group: "source.toolkit.fluxcd.io", Kind: manifest.KindBucket, Namespace: "apps", Name: "foreign-source"},
	} {
		if st.GetObject(excluded) != nil {
			t.Errorf("runtime promotion admitted %s", excluded)
		}
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
		Name: "consumer", Namespace: "apps",
		Chart:      manifest.HelmChart{Source: manifest.NamedResource{Group: manifest.SourceDomain, Kind: manifest.KindGitRepository, Namespace: "apps", Name: "chart"}},
		ValuesFrom: []manifest.ValuesReference{{Kind: manifest.KindConfigMap, Name: "selected"}},
	}
	st.AddObject(hr)
	selected := manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: "apps", Name: "selected"}
	excluded := manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: "apps", Name: "excluded"}
	chart := manifest.NamedResource{Group: "source.toolkit.fluxcd.io", Kind: manifest.KindGitRepository, Namespace: "apps", Name: "chart"}
	excludedSource := manifest.NamedResource{Group: "source.toolkit.fluxcd.io", Kind: manifest.KindGitRepository, Namespace: "apps", Name: "excluded-source"}
	files := map[manifest.NamedResource]string{hr.Named(): "consumer.yaml", selected: "bundle.yaml", excluded: "bundle.yaml", chart: "bundle.yaml", excludedSource: "bundle.yaml"}
	filter := change.NewFilter(change.NewSet([]string{"consumer.yaml"}), indexForTest(files), st, "")
	idx := loader.New(store.New())
	for id := range files {
		record := manifest.DiscoveryRecord{Authored: id, Path: filepath.Join(dir, "bundle.yaml")}
		if id == selected {
			record.Authored.Namespace = "authored"
		}
		record.Productions = []manifest.Production{{Authored: record.Authored, Path: record.Path}}
		idx.Index.Records[id] = record
	}
	idx.Store = st
	input, _, err := baseline.AdmitDirectory(t.Context(), dir, dir)
	assert.NoError(t, err)
	st.SetArtifact(manifest.BootstrapSourceID, input)
	controller := base.New(st, nil)
	controller.Configure(base.Options{Filter: filter, Index: idx.Index})
	e := &orchestratorExistence{o: &Orchestrator{
		store: idx.Store, index: idx.Index, cfg: Config{WipeSecrets: true}, filter: filter,
		ksc: &kustomization.Controller{Controller: controller},
	}}
	size := len(filter.KeepNames())
	if !e.Promote(t.Context(), nil, selected) || st.GetObject(selected) == nil || st.GetObject(chart) == nil {
		t.Fatal("selected values and required chart source must be admitted")
	}
	if got := st.GetObject(selected).(*manifest.ConfigMap).Data["values.yaml"]; got != "greeting: selected" {
		t.Errorf("selected values = %v", got)
	}
	for _, id := range []manifest.NamedResource{excluded, excludedSource} {
		if e.Promote(t.Context(), nil, id) || st.GetObject(id) != nil || filter.ShouldReconcile(id) {
			t.Errorf("excluded sibling %s must remain absent", id)
		}
	}
	if len(filter.KeepNames()) != size {
		t.Error("ordering promotion must not expand keep")
	}
	st.AddObject(&manifest.ConfigMap{Name: "excluded", Namespace: "apps"})
	if e.Promote(t.Context(), nil, excluded) {
		t.Error("availability must not bypass admission")
	}
}

func TestOrchestratorExistence_ExistingDataUsesNewSourceEvidence(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	file := "settings.yaml"
	testutil.WriteFile(t, dir, file, "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: settings, namespace: apps}\ndata: {value: selected}\n")
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
	filter := change.NewFilter(change.NewSet([]string{file}), index, st, dir)
	if filter.ShouldReconcile(id) {
		t.Fatal("unavailable source evidence must not admit existing data")
	}
	input, _, err := baseline.AdmitDirectory(t.Context(), dir, dir)
	assert.NoError(t, err)
	st.SetArtifact(source.Named(), input)
	controller := base.New(st, nil)
	controller.Configure(base.Options{Filter: filter, Index: index})
	e := &orchestratorExistence{o: &Orchestrator{
		store: st, index: index, filter: filter,
		ksc: &kustomization.Controller{Controller: controller},
	}}
	var reads store.ReadSet
	reads.Init(st, testutil.KustomizationID("apps", "reader"), nil)
	defer reads.Close()
	if !e.Promote(t.Context(), &reads, id) || !filter.ShouldReconcile(id) {
		t.Fatal("new source evidence must admit the existing changed data")
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
			e := &orchestratorExistence{o: &Orchestrator{store: idx.Store, index: idx.Index}}
			id.Namespace = tc.requested
			if got := e.IsFileIndexed(id); got != tc.want {
				t.Errorf("IsFileIndexed(%s) = %v, want %v", id, got, tc.want)
			}
		})
	}
}
