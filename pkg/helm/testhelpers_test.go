package helm

import (
	helmv2 "github.com/fluxcd/helm-controller/api/v2"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
	"testing"
)

func ociRenderFixture(t *testing.T) (*store.Store, *manifest.HelmRelease, string) {
	t.Helper()
	dir := t.TempDir()
	writeChartFiles(t, dir, "podinfo", "6.15.0")
	testutil.WriteFile(t, dir, "values.yaml", "marker: default\n")
	testutil.WriteFile(t, dir, "prod.yaml", "marker: original\n")
	testutil.WriteFile(t, dir, "charts/child/Chart.yaml", "apiVersion: v2\nname: child\nversion: 1.2.3+child\n")
	testutil.WriteFile(t, dir, "templates/cm.yaml", `apiVersion: v1
kind: ConfigMap
metadata:
  name: {{ .Release.Name }}
  labels:
    helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | quote }}
data:
  version: {{ .Chart.Version | quote }}
  marker: {{ .Values.marker | quote }}
`)
	st := store.New()
	src := &manifest.OCIRepository{Name: "podinfo", Namespace: "apps", URL: "oci://example.test/podinfo"}
	st.AddObject(src)
	hr := &manifest.HelmRelease{
		Name: "podinfo", Namespace: "apps",
		ChartRef:         &helmv2.CrossNamespaceSourceReference{Kind: manifest.KindOCIRepository, Name: src.Name},
		Chart:            manifest.HelmChart{RepoKind: manifest.KindOCIRepository, RepoNamespace: src.Namespace, RepoName: src.Name},
		ChartValuesFiles: []string{"prod.yaml"},
	}
	st.AddObject(hr)
	return st, hr, dir
}
