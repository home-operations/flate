package manifest

import (
	"testing"

	helmv2 "github.com/fluxcd/helm-controller/api/v2"
	"github.com/home-operations/flate/internal/assert"
)

func TestHelmChart_DisplayNames(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name, namespace, want string
	}{
		{name: "without namespace", want: "charts"},
		{name: "with namespace", namespace: "flux-system", want: "flux-system-charts"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			source := NamedResource{Group: SourceDomain, Kind: KindHelmRepository, Namespace: tc.namespace, Name: "charts"}
			hr := &HelmRelease{Name: "release", Namespace: "apps", Chart: HelmChart{Name: "podinfo", Source: source}}
			identity := hr.Named()

			assert.Equal(t, hr.Chart.RepoFullName(), tc.want)
			assert.Equal(t, hr.Chart.ChartName(), tc.want+"/podinfo")
			assert.Equal(t, hr.Chart.Source, source)
			assert.Equal(t, hr.Named(), identity)
		})
	}
}

func TestHelmRelease_TracksDigest(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name, refKind, repoKind, apiVersion string
		want                                bool
	}{
		{name: "missing"},
		{name: "direct OCI", refKind: KindOCIRepository, want: true},
		{name: "explicit source OCI", refKind: KindOCIRepository, apiVersion: SourceDomain + "/v1", want: true},
		{name: "explicit core OCI", refKind: KindOCIRepository, apiVersion: "v1"},
		{name: "foreign OCI", refKind: KindOCIRepository, apiVersion: "foreign.example/v1"},
		{name: "other ref", refKind: KindHelmChart, repoKind: KindOCIRepository},
		{name: "synthesized OCI", repoKind: KindOCIRepository},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			hr := &HelmRelease{Chart: HelmChart{Source: NamedResource{Group: SourceDomain, Kind: tc.repoKind}}}
			if tc.refKind != "" {
				hr.ChartRef = &helmv2.CrossNamespaceSourceReference{APIVersion: tc.apiVersion, Kind: tc.refKind, Name: "chart"}
			}
			assert.Equal(t, hr.TracksDigest(), tc.want)
		})
	}
}
