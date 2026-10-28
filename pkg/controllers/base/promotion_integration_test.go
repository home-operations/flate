package base_test

import (
	helmv2 "github.com/fluxcd/helm-controller/api/v2"
	"path/filepath"
	"testing"

	"github.com/home-operations/flate/internal/admissiontest"
	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/baseline"
	"github.com/home-operations/flate/pkg/change"
	"github.com/home-operations/flate/pkg/controllers/base"
	"github.com/home-operations/flate/pkg/loader"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

func TestLoaderPromotion_PreservesSelectedContent(t *testing.T) {
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
		Chart:           manifest.HelmChart{Source: manifest.NamedResource{Group: manifest.SourceDomain, Kind: manifest.KindGitRepository, Namespace: "apps", Name: "chart"}},
		HelmReleaseSpec: helmv2.HelmReleaseSpec{ValuesFrom: []manifest.ValuesReference{{Kind: manifest.KindConfigMap, Name: "selected"}}, Chart: &helmv2.HelmChartTemplate{Spec: helmv2.HelmChartTemplateSpec{Chart: "chart", SourceRef: &helmv2.CrossNamespaceObjectReference{Kind: manifest.KindGitRepository, Name: "chart", Namespace: "apps"}}}},
	}
	st.AddObject(hr)
	selected := manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: "apps", Name: "selected"}
	excluded := manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: "apps", Name: "excluded"}
	chart := manifest.NamedResource{Group: "source.toolkit.fluxcd.io", Kind: manifest.KindGitRepository, Namespace: "apps", Name: "chart"}
	excludedSource := manifest.NamedResource{Group: "source.toolkit.fluxcd.io", Kind: manifest.KindGitRepository, Namespace: "apps", Name: "excluded-source"}
	files := map[manifest.NamedResource]string{hr.Named(): "consumer.yaml", selected: "bundle.yaml", excluded: "bundle.yaml", chart: "bundle.yaml", excludedSource: "bundle.yaml"}
	policy := &manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{}}
	for id, file := range files {
		policy.Records[id] = manifest.DiscoveryRecord{File: file}
	}
	filterLoader := admissiontest.New(t, admissiontest.Config{Changes: change.NewSet([]string{"consumer.yaml"}), Index: policy, Inventory: st, Root: ""})
	admissiontest.Commit(t, filterLoader, nil)
	filter := filterLoader.Options.Filter
	index := &manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{}}
	for id := range files {
		record := manifest.DiscoveryRecord{Authored: id, Path: filepath.Join(dir, "bundle.yaml")}
		if id == selected {
			record.Authored.Namespace = "authored"
		}
		record.Productions = []manifest.Production{{Authored: record.Authored, Path: record.Path}}
		index.Records[id] = record
	}
	input, _, err := baseline.AdmitDirectory(t.Context(), dir, dir)
	assert.NoError(t, err)
	st.SetArtifact(manifest.BootstrapSourceID, input)
	controller := base.New(st, nil)
	controller.Configure(base.Options{Filter: filter, Loader: filterLoader, Index: index})
	promote := func(id manifest.NamedResource) (bool, error) {
		return loader.Promote(t.Context(), st, id, loader.PromoteOptions{
			Record: index.Records[id].Productions[0], WipeSecrets: true,
			Admit: func(object manifest.BaseManifest) (bool, error) {
				return controller.KeepEmitted(t.Context(), manifest.NamedResource{}, store.Child{Obj: object}, nil)
			},
		})
	}

	size := len(filter.KeepNames())
	if promoted, err := promote(selected); err != nil || !promoted || st.GetObject(selected) == nil || st.GetObject(chart) == nil {
		t.Fatal("selected values and required chart source must be admitted")
	}
	if got := st.GetObject(selected).(*manifest.ConfigMap).Data["values.yaml"]; got != "greeting: selected" {
		t.Errorf("selected values = %v", got)
	}
	for _, id := range []manifest.NamedResource{excluded, excludedSource} {
		if promoted, err := promote(id); err != nil || promoted || st.GetObject(id) != nil || filter.ShouldReconcile(id) {
			t.Errorf("excluded sibling %s must remain absent", id)
		}
	}
	if len(filter.KeepNames()) != size {
		t.Error("ordering promotion must not expand keep")
	}
	st.AddObject(&manifest.ConfigMap{Name: "excluded", Namespace: "apps"})
	if promoted, err := promote(excluded); err != nil || promoted {
		t.Error("availability must not bypass admission")
	}
}
