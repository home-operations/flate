package helm

import (
	"testing"

	helmv2 "github.com/fluxcd/helm-controller/api/v2"
	sourcev1 "github.com/fluxcd/source-controller/api/v1"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/pkg/manifest"
)

func TestPrepare_ChartReferenceNamespace(t *testing.T) {
	hr := &manifest.HelmRelease{Namespace: "effective", Chart: manifest.HelmChart{RepoKind: manifest.KindHelmChart, RepoNamespace: "authored", RepoName: "source"}}
	hr.ChartRef = &helmv2.CrossNamespaceSourceReference{Kind: manifest.KindHelmChart, Name: "source"}
	got, err := Prepare(hr, func(ns, name string) *manifest.HelmChartSource {
		assert.Equal(t, ns, "effective")
		assert.Equal(t, name, "source")
		return &manifest.HelmChartSource{Namespace: "sources", SourceRef: sourcev1.LocalHelmChartSourceReference{Kind: manifest.KindHelmRepository, Name: "resolved"}}
	}, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, got.Chart.RepoNamespace, "sources")
	assert.Equal(t, got.Chart.RepoName, "resolved")
	assert.Equal(t, hr.Chart.RepoNamespace, "authored")
	hr.ChartRef.Kind, hr.Chart.RepoKind = manifest.KindOCIRepository, manifest.KindOCIRepository
	got, err = Prepare(hr, nil, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, got.Chart.RepoNamespace, "effective")
}
