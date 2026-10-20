package manifest

import (
	helmv2 "github.com/fluxcd/helm-controller/api/v2"
	"github.com/home-operations/flate/internal/assert"
	"testing"
)

func TestHelmRelease_UsesDirectOCIRepository(t *testing.T) {
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
			assert.Equal(t, hr.UsesDirectOCIRepository(), tc.want)
		})
	}
}
