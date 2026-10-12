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

func TestParseHelmRelease_DisableValidation(t *testing.T) {
	for _, tc := range []struct {
		name, specBody          string
		wantSchema, wantOpenAPI bool
	}{
		{name: "neither"},
		{name: "install only", specBody: "  install: {disableSchemaValidation: true}", wantSchema: true},
		{name: "upgrade only", specBody: "  upgrade: {disableOpenAPIValidation: true}", wantOpenAPI: true},
		{name: "both", specBody: "  install: {disableOpenAPIValidation: true}\n  upgrade: {disableSchemaValidation: true}", wantSchema: true, wantOpenAPI: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			hr, err := parseHelmRelease(helmReleaseDoc(t, tc.specBody))
			if err != nil {
				t.Fatal(err)
			}
			assert.Equal(t, hr.DisableSchemaValidation, tc.wantSchema)
			assert.Equal(t, hr.DisableOpenAPIValidation, tc.wantOpenAPI)
		})
	}
}
