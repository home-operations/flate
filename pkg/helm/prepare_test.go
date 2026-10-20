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
			hr := &manifest.HelmRelease{Namespace: "authored", Chart: manifest.HelmChart{Source: manifest.NamedResource{Group: manifest.SourceDomain, Kind: manifest.KindHelmChart, Namespace: namespace, Name: "source"}}}
			hr.Values = map[string]any{"key": "original"}
			hr.ChartRef = &helmv2.CrossNamespaceSourceReference{Kind: manifest.KindHelmChart, Namespace: namespace, Name: "source"}
			projected := hr.WithNamespace("effective").(*manifest.HelmRelease)
			want := tc.want
			got, err := Prepare(projected, func(id manifest.NamedResource) *manifest.HelmChartSource {
				assert.Equal(t, id, manifest.NamedResource{Group: manifest.SourceDomain, Kind: manifest.KindHelmChart, Namespace: want, Name: "source"})
				return &manifest.HelmChartSource{Namespace: "sources", Chart: "resolved-name", ValuesFiles: []string{"custom.yaml"}, IgnoreMissingValuesFiles: true, SourceRef: sourcev1.LocalHelmChartSourceReference{Kind: manifest.KindHelmRepository, Name: "resolved"}}
			}, nil, nil)
			if err != nil {
				t.Fatal(err)
			}
			assert.Equal(t, got.Chart.Source.Namespace, "sources")
			assert.Equal(t, got.Chart.Source.Name, "resolved")
			assert.Equal(t, hr.Namespace, "authored")
			assert.Equal(t, hr.ChartRef.Namespace, namespace)
			assert.Equal(t, projected.Chart, hr.Chart)
			assert.Equal(t, got.Chart.Name, "resolved-name")
			assert.Equal(t, got.Chart.Version, "")
			assert.Diff(t, got.ChartValuesFiles, []string{"custom.yaml"})
			assert.Equal(t, got.IgnoreMissingValuesFiles, true)
			got.ChartRef.Namespace = "mutated"
			got.Values["key"] = "mutated"
			assert.Equal(t, hr.ChartRef.Namespace, namespace)
			assert.Equal(t, hr.Values["key"], any("original"))
			hr.ChartRef.Kind, hr.Chart.Source.Kind = manifest.KindOCIRepository, manifest.KindOCIRepository
			got, err = Prepare(hr.WithNamespace("effective").(*manifest.HelmRelease), nil, nil, nil)
			if err != nil {
				t.Fatal(err)
			}
			assert.Equal(t, got.Chart.Source.Namespace, want)
		})
	}
}
