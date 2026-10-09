package manifest

import (
	helmv2 "github.com/fluxcd/helm-controller/api/v2"
	"github.com/home-operations/flate/internal/assert"
	"testing"
)

func TestHelmRelease_UsesDirectOCIRepository(t *testing.T) {
	for _, tc := range []struct {
		name, refKind, repoKind string
		want                    bool
	}{
		{name: "missing"},
		{name: "direct OCI", refKind: KindOCIRepository, want: true},
		{name: "other ref", refKind: KindHelmChart, repoKind: KindOCIRepository},
		{name: "synthesized OCI", repoKind: KindOCIRepository},
	} {
		t.Run(tc.name, func(t *testing.T) {
			hr := &HelmRelease{Chart: HelmChart{RepoKind: tc.repoKind}}
			if tc.refKind != "" {
				hr.ChartRef = &helmv2.CrossNamespaceSourceReference{Kind: tc.refKind, Name: "chart"}
			}
			assert.Equal(t, hr.UsesDirectOCIRepository(), tc.want)
		})
	}
}
