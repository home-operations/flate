package manifest

import (
	"testing"

	kustomizev1 "github.com/fluxcd/kustomize-controller/api/v1"

	"github.com/home-operations/flate/internal/assert"
)

func TestSourcePath_Facts(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name    string
		obj     BaseManifest
		source  NamedResource
		path    string
		present bool
	}{
		{
			name: "effective namespace",
			obj: (&Kustomization{Namespace: "authored", Path: "././apps/a",
				SourceRef: kustomizev1.CrossNamespaceSourceReference{Kind: KindGitRepository, Name: "source"}}).WithNamespace("effective"),
			source: NamedResource{Group: SourceDomain, Kind: KindGitRepository, Namespace: "effective", Name: "source"},
			path:   "././apps/a", present: true,
		},
		{
			name: "explicit namespace and api group",
			obj: &Kustomization{Namespace: "apps", Path: "../authored",
				SourceRef: kustomizev1.CrossNamespaceSourceReference{APIVersion: "custom.example/v1", Kind: KindOCIRepository, Namespace: "sources", Name: "source"}},
			source: NamedResource{Group: "custom.example", Kind: KindOCIRepository, Namespace: "sources", Name: "source"},
			path:   "../authored", present: true,
		},
		{
			name:    "empty path",
			obj:     &Kustomization{Namespace: "apps", SourceRef: kustomizev1.CrossNamespaceSourceReference{Kind: KindOCIRepository, Name: "source"}},
			source:  NamedResource{Group: SourceDomain, Kind: KindOCIRepository, Namespace: "apps", Name: "source"},
			present: true,
		},
		{
			name: "empty source with path", obj: &Kustomization{Namespace: "apps", Path: "./apps/a"},
			source: NamedResource{Namespace: "apps"}, path: "./apps/a", present: true,
		},
		{
			name:   "kind without source name",
			obj:    &Kustomization{Namespace: "apps", Path: "./apps/a", SourceRef: kustomizev1.CrossNamespaceSourceReference{Kind: KindOCIRepository}},
			source: NamedResource{Group: SourceDomain, Kind: KindOCIRepository, Namespace: "apps"}, path: "./apps/a", present: true,
		},
		{name: "empty declaration", obj: &Kustomization{}, present: true},
		{name: "helm release", obj: &HelmRelease{}},
		{name: "helm repository", obj: &HelmRepository{}},
		{name: "helm chart", obj: &HelmChartSource{}},
		{name: "git repository", obj: &GitRepository{}},
		{name: "oci repository", obj: &OCIRepository{}},
		{name: "external artifact", obj: &ExternalArtifact{}},
		{name: "bucket", obj: &Bucket{}},
		{name: "resource set", obj: &ResourceSet{}},
		{name: "resource set input provider", obj: &ResourceSetInputProvider{}},
		{name: "config map", obj: &ConfigMap{}},
		{name: "secret", obj: &Secret{}},
		{name: "raw object", obj: &RawObject{}},
		{
			name: "raw kustomization lookalike",
			obj: &RawObject{APIVersion: FluxKustomizeDomain + "/v1", Kind: KindKustomization,
				Spec: map[string]any{"path": "./apps/a", "sourceRef": map[string]any{"kind": KindGitRepository, "name": "source"}}},
		},
		{name: "foreign kustomization lookalike", obj: &RawObject{APIVersion: "custom.example/v1", Kind: KindKustomization}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			source, path, present := tc.obj.SourcePath()
			assert.Equal(t, source, tc.source)
			assert.Equal(t, path, tc.path)
			assert.Equal(t, present, tc.present)
			if ks, ok := tc.obj.(*Kustomization); ok {
				assert.Equal(t, source, ks.SourceID())
			}
		})
	}
}
