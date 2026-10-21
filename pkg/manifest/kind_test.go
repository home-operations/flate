package manifest

import (
	"slices"
	"testing"

	"github.com/home-operations/flate/internal/assert"
)

func TestKindInfo_Rosters(t *testing.T) {
	t.Parallel()
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
	assert.Equal(t, len(registry), len(cases)+1)
	for i, tc := range cases {
		t.Run(tc.want.Kind, func(t *testing.T) {
			t.Parallel()
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
	t.Parallel()
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
			t.Parallel()
			assert.Equal(t, tc.id.ReportHidden(), tc.want)
		})
	}
}

func TestKindInfo_ArtifactGenerator(t *testing.T) {
	t.Parallel()
	doc := map[string]any{"apiVersion": "source.extensions.fluxcd.io/v1beta1", "kind": "ArtifactGenerator", "metadata": map[string]any{"name": "generator", "namespace": "apps"}, "spec": map[string]any{}}
	obj, _, _, err := ParseDoc(doc, ParseDocOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := obj.(*RawObject); !ok {
		t.Fatalf("generator type = %T", obj)
	}
	want := KindInfo{Group: "source.extensions.fluxcd.io", Kind: "ArtifactGenerator", Tracked: true, Discovery: true}
	assert.Equal(t, obj.Named().KindInfo(), want)
	want.apiVersion = "source.extensions.fluxcd.io/v1beta1"
	assert.Equal(t, KindOf(obj), want)
}

func TestKindInfo_RawIdentity(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name, kind, apiVersion, group string
		tracked                       bool
	}{
		{"generator", "ArtifactGenerator", "source.extensions.fluxcd.io/v1beta1", "source.extensions.fluxcd.io", true},
		{"foreign_group", "ArtifactGenerator", "foreign.example/v1", "foreign.example", false},
		{"group_suffix", "ArtifactGenerator", "source.extensions.fluxcd.io.example/v1", "source.extensions.fluxcd.io.example", false},
		{"other_kind", "Deployment", "source.extensions.fluxcd.io/v1", "source.extensions.fluxcd.io", false},
		{"kind_case", "artifactGenerator", "source.extensions.fluxcd.io/v1", "source.extensions.fluxcd.io", false},
		{"no_slash", "ArtifactGenerator", "source.extensions.fluxcd.io", "", false},
		{"core", "ArtifactGenerator", "v1", "", false},
		{"empty_group", "ArtifactGenerator", "/v1", "", false},
		{"empty_version", "ArtifactGenerator", "source.extensions.fluxcd.io/", "source.extensions.fluxcd.io", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			raw := &RawObject{Kind: tc.kind, APIVersion: tc.apiVersion, Name: "generator", Namespace: "apps", Spec: map[string]any{
				"pathPattern": "@repo/{app}", "artifacts": []any{map[string]any{"name": "app-{app}"}},
			}}
			want := KindInfo{Group: tc.group, Kind: tc.kind, Tracked: tc.tracked, Discovery: tc.tracked, apiVersion: tc.apiVersion}
			assert.Equal(t, KindOf(raw), want)
			assert.Equal(t, raw.Named().Group, tc.group)
			assert.Equal(t, ProducerNeedsCandidates(raw), tc.tracked)
			targets := ProducerTargets(raw, NamedResource{Group: SourceDomain, Kind: KindExternalArtifact, Namespace: "apps", Name: "app-api"})
			assert.Equal(t, len(targets) == 1, tc.tracked)
		})
	}
}
