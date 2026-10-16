package manifest

import (
	"slices"
	"testing"

	"github.com/home-operations/flate/internal/assert"
)

func TestKindInfo_Rosters(t *testing.T) {
	cases := []struct {
		obj     BaseManifest
		want    KindInfo
		version string
	}{
		{&Kustomization{}, KindInfo{Group: FluxKustomizeDomain, Kind: KindKustomization, Tracked: true, Runnable: true, Status: true, Discovery: true, Test: true, Parent: true, Refire: true, Structural: true, Render: true}, kustomizeAPIVersion},
		{&HelmRelease{}, KindInfo{Group: HelmReleaseDomain, Kind: KindHelmRelease, Tracked: true, Runnable: true, Status: true, Test: true, Parent: true, SourceOnly: true, TransientOutput: true, Render: true}, helmAPIVersion},
		{&GitRepository{}, KindInfo{Group: SourceDomain, Kind: KindGitRepository, Tracked: true, Runnable: true, Status: true, Discovery: true, Test: true, Refire: true, SourceArtifact: true}, sourceAPIVersion},
		{&OCIRepository{}, KindInfo{Group: SourceDomain, Kind: KindOCIRepository, Tracked: true, Runnable: true, Status: true, Discovery: true, Test: true, Refire: true, SourceArtifact: true}, sourceAPIVersion},
		{&HelmRepository{}, KindInfo{Group: SourceDomain, Kind: KindHelmRepository, Tracked: true, Runnable: true, Status: true, Discovery: true, Test: true, Refire: true}, sourceAPIVersion},
		{&HelmChartSource{}, KindInfo{Group: SourceDomain, Kind: KindHelmChart, Tracked: true, Runnable: true, Status: true, Discovery: true}, sourceAPIVersion},
		{&ExternalArtifact{}, KindInfo{Group: SourceDomain, Kind: KindExternalArtifact, Tracked: true, Runnable: true, Status: true, Discovery: true, Refire: true, SourceArtifact: true}, sourceAPIVersion},
		{&Bucket{}, KindInfo{Group: SourceDomain, Kind: KindBucket, Tracked: true, Runnable: true, Status: true, Discovery: true, Test: true, Refire: true, SourceArtifact: true}, sourceAPIVersion},
		{&ResourceSet{}, KindInfo{Group: FluxOperatorDomain, Kind: KindResourceSet, Tracked: true, Runnable: true, Discovery: true, Parent: true}, ""},
		{&ResourceSetInputProvider{}, KindInfo{Group: FluxOperatorDomain, Kind: KindResourceSetInputProvider, Tracked: true, Discovery: true}, ""},
		{&ConfigMap{}, KindInfo{Kind: KindConfigMap, Tracked: true, Data: true}, ""},
		{&Secret{}, KindInfo{Kind: KindSecret, Tracked: true, Data: true}, ""},
	}
	registry := slices.Collect(Kinds())
	assert.Equal(t, len(registry), len(cases))
	for i, tc := range cases {
		t.Run(tc.want.Kind, func(t *testing.T) {
			assert.Equal(t, registry[i], tc.want)
			assert.Equal(t, KindOf(tc.obj), tc.want)
			assert.Equal(t, tc.obj.Named().KindInfo(), tc.want)
			assert.Equal(t, tc.want.APIVersion(), tc.version)
			for _, group := range []string{tc.want.Group, "foreign.example"} {
				version := "v1"
				if group != "" {
					version = group + "/v1"
				}
				raw := &RawObject{APIVersion: version, Kind: tc.want.Kind}
				assert.Equal(t, KindOf(raw), KindInfo{Group: group, Kind: tc.want.Kind, apiVersion: version})
				if group != tc.want.Group {
					assert.Equal(t, raw.Named().KindInfo(), KindInfo{Group: group, Kind: tc.want.Kind})
				}
			}
		})
	}
	assert.Equal(t, KindOf(nil), KindInfo{})
	assert.Equal(t, (NamedResource{Kind: "Unknown"}).KindInfo(), KindInfo{Kind: "Unknown"})
}

func TestNamedResource_ReportHidden(t *testing.T) {
	for _, tc := range []struct {
		name string
		id   NamedResource
		want bool
	}{
		{"bootstrap", BootstrapSourceID, true},
		{"chart", NamedResource{Group: SourceDomain, Kind: KindHelmChart, Namespace: "apps", Name: "chart"}, true},
		{"foreign chart", NamedResource{Group: "foreign.example", Kind: KindHelmChart, Namespace: "apps", Name: "chart"}, false},
		{"core chart", NamedResource{Kind: KindHelmChart, Name: "chart"}, false},
		{"ordinary git", NamedResource{Group: SourceDomain, Kind: KindGitRepository, Namespace: BootstrapSourceID.Namespace, Name: "ordinary"}, false},
		{"other namespace", NamedResource{Group: BootstrapSourceID.Group, Kind: BootstrapSourceID.Kind, Namespace: "other", Name: BootstrapSourceID.Name}, false},
		{"other group", NamedResource{Group: "foreign.example", Kind: BootstrapSourceID.Kind, Namespace: BootstrapSourceID.Namespace, Name: BootstrapSourceID.Name}, false},
		{"other kind", NamedResource{Group: BootstrapSourceID.Group, Kind: KindOCIRepository, Namespace: BootstrapSourceID.Namespace, Name: BootstrapSourceID.Name}, false},
		{"empty", NamedResource{}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.id.ReportHidden(), tc.want)
		})
	}
}
