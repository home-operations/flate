package orchestrator

import (
	"path/filepath"
	"testing"

	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/change"
	"github.com/home-operations/flate/pkg/loader"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

func TestOrchestratorExistence_PromotionDoesNotAdmitControllerSiblings(t *testing.T) {
	dir := t.TempDir()
	testutil.WriteFile(t, dir, "bundle.yaml", `apiVersion: v1
kind: ConfigMap
metadata: {name: selected, namespace: apps}
data: {value: selected}
---
apiVersion: helm.toolkit.fluxcd.io/v2
kind: HelmRelease
metadata: {name: raw-hr, namespace: apps}
spec:
  chart:
    spec:
      chart: charts/demo
      sourceRef: {kind: GitRepository, name: source, namespace: apps}
---
apiVersion: kustomize.toolkit.fluxcd.io/v1
kind: Kustomization
metadata: {name: raw-ks, namespace: apps}
spec: {path: ./apps, sourceRef: {kind: GitRepository, name: source}}
---
apiVersion: fluxcd.controlplane.io/v1
kind: ResourceSet
metadata: {name: raw-rs, namespace: apps}
spec: {}
---
apiVersion: seaweed.seaweedfs.com/v1
kind: Bucket
metadata: {name: foreign-source, namespace: apps}
spec: {}
`)
	id := manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: "apps", Name: "selected"}
	idx := loader.NewExistenceIndex()
	idx.Record(id, filepath.Join(dir, "bundle.yaml"))
	st := store.New()
	e := &orchestratorExistence{idx: idx, store: st, wipeSecrets: true}
	if !e.Promote(id) || st.GetObject(id) == nil {
		t.Fatal("selected data must be admitted")
	}
	for _, excluded := range []manifest.NamedResource{
		{Kind: manifest.KindHelmRelease, Namespace: "apps", Name: "raw-hr"},
		{Kind: manifest.KindKustomization, Namespace: "apps", Name: "raw-ks"},
		{Kind: manifest.KindResourceSet, Namespace: "apps", Name: "raw-rs"},
		{Kind: manifest.KindBucket, Namespace: "apps", Name: "foreign-source"},
	} {
		if st.GetObject(excluded) != nil {
			t.Errorf("runtime promotion admitted %s", excluded)
		}
	}
}

func TestOrchestratorExistence_PromotionPreservesSelectedContent(t *testing.T) {
	dir := t.TempDir()
	testutil.WriteFile(t, dir, "bundle.yaml", `apiVersion: v1
kind: ConfigMap
metadata: {name: selected, namespace: apps}
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
		Chart:      manifest.HelmChart{RepoKind: manifest.KindGitRepository, RepoNamespace: "apps", RepoName: "chart"},
		ValuesFrom: []manifest.ValuesReference{{Kind: manifest.KindConfigMap, Name: "selected"}},
	}
	st.AddObject(hr)
	selected := manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: "apps", Name: "selected"}
	excluded := manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: "apps", Name: "excluded"}
	chart := manifest.NamedResource{Kind: manifest.KindGitRepository, Namespace: "apps", Name: "chart"}
	excludedSource := manifest.NamedResource{Kind: manifest.KindGitRepository, Namespace: "apps", Name: "excluded-source"}
	files := map[manifest.NamedResource]string{hr.Named(): "consumer.yaml", selected: "bundle.yaml", excluded: "bundle.yaml", chart: "bundle.yaml", excludedSource: "bundle.yaml"}
	filter := change.NewFilter(change.NewSet([]string{"consumer.yaml"}), files, "", st)
	idx := loader.NewExistenceIndex()
	for id := range files {
		idx.Record(id, filepath.Join(dir, "bundle.yaml"))
	}
	e := &orchestratorExistence{idx: idx, store: st, wipeSecrets: true, filter: filter}
	size := filter.Size()
	if !e.Promote(selected) || st.GetObject(selected) == nil || st.GetObject(chart) == nil {
		t.Fatal("selected values and required chart source must be admitted")
	}
	for _, id := range []manifest.NamedResource{excluded, excludedSource} {
		if e.Promote(id) || st.GetObject(id) != nil || filter.ShouldReconcile(id) {
			t.Errorf("excluded sibling %s must remain absent", id)
		}
	}
	if filter.Size() != size {
		t.Error("ordering promotion must not expand keep")
	}
	st.AddObject(&manifest.ConfigMap{Name: "excluded", Namespace: "apps"})
	if e.Promote(excluded) {
		t.Error("availability must not bypass admission")
	}
}

func TestOrchestratorExistence_FileIndexNamespaceConvention(t *testing.T) {
	for _, tc := range []struct {
		name, recorded, requested string
		want                      bool
	}{
		{"exact", "apps", "apps", true},
		{"inherited namespace", "", "apps", true},
		{"explicit other namespace", "other", "apps", false},
		{"bare query cannot match explicit", "apps", "", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			idx := loader.NewExistenceIndex()
			id := manifest.NamedResource{Kind: manifest.KindHelmRelease, Namespace: tc.recorded, Name: "target"}
			idx.Record(id, "target.yaml")
			e := &orchestratorExistence{idx: idx}
			id.Namespace = tc.requested
			if got := e.IsFileIndexed(id); got != tc.want {
				t.Errorf("IsFileIndexed(%s) = %v, want %v", id, got, tc.want)
			}
		})
	}
}
