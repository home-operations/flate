package helm

import (
	"testing"

	helmv2 "github.com/fluxcd/helm-controller/api/v2"
	sourcev1 "github.com/fluxcd/source-controller/api/v1"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/pkg/manifest"
)

func TestPrepare_ChartReferenceNamespace(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct{ namespace, want string }{{"", "effective"}, {"foreign", "foreign"}} {
		namespace := tc.namespace
		t.Run("authored_"+namespace, func(t *testing.T) {
			t.Parallel()
			hr := &manifest.HelmRelease{Namespace: "authored", Chart: manifest.HelmChart{RepoGroup: manifest.SourceDomain, RepoKind: manifest.KindHelmChart, RepoNamespace: namespace, RepoName: "source"}}
			hr.ChartRef = &helmv2.CrossNamespaceSourceReference{Kind: manifest.KindHelmChart, Namespace: namespace, Name: "source"}
			projected := hr.WithNamespace("effective").(*manifest.HelmRelease)
			want := tc.want
			got, err := Prepare(projected, func(ns, name string) *manifest.HelmChartSource {
				assert.Equal(t, ns, want)
				assert.Equal(t, name, "source")
				return &manifest.HelmChartSource{Namespace: "sources", SourceRef: sourcev1.LocalHelmChartSourceReference{Kind: manifest.KindHelmRepository, Name: "resolved"}}
			}, nil, nil)
			if err != nil {
				t.Fatal(err)
			}
			assert.Equal(t, got.Chart.RepoNamespace, "sources")
			assert.Equal(t, got.Chart.RepoName, "resolved")
			assert.Equal(t, hr.Namespace, "authored")
			assert.Equal(t, hr.ChartRef.Namespace, namespace)
			assert.Equal(t, projected.Chart, hr.Chart)
			hr.ChartRef.Kind, hr.Chart.RepoKind = manifest.KindOCIRepository, manifest.KindOCIRepository
			got, err = Prepare(hr.WithNamespace("effective").(*manifest.HelmRelease), nil, nil, nil)
			if err != nil {
				t.Fatal(err)
			}
			assert.Equal(t, got.Chart.RepoNamespace, want)
		})
	}
}
