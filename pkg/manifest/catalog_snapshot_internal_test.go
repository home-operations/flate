package manifest

import (
	"slices"
	"testing"

	"github.com/google/go-cmp/cmp"

	"github.com/home-operations/flate/internal/assert"
)

func TestCatalogCodec_RoundTrip(t *testing.T) {
	t.Parallel()
	for _, object := range []BaseManifest{
		&Kustomization{Name: "root", Namespace: "apps", Labels: map[string]string{"app": "test"}, Annotations: map[string]string{"flag": "value"}},
		&HelmRelease{Name: "app", Namespace: "apps"},
		&GitRepository{Name: "repo", Namespace: "apps", Labels: map[string]string{"app": "test"}},
		&OCIRepository{Name: "repo", Namespace: "apps"},
		&HelmRepository{Name: "repo", Namespace: "apps"},
		&HelmChartSource{Name: "chart", Namespace: "apps"},
		&ExternalArtifact{Name: "artifact", Namespace: "apps"},
		&Bucket{Name: "bucket", Namespace: "apps"},
		&ResourceSet{Name: "set", Namespace: "apps", Suspend: true},
		&ResourceSetInputProvider{Name: "provider", Namespace: "apps"},
		&ConfigMap{Name: "cm", Namespace: "apps"},
		&Secret{Name: "secret", Namespace: "apps"},
		&RawObject{Kind: "Widget", APIVersion: "example.org/v1", Name: "widget", Namespace: "apps"},
	} {
		t.Run(object.Named().Kind, func(t *testing.T) {
			t.Parallel()
			catalog := CatalogObject(object, object.Named())
			snapshot, err := EncodeCatalog(catalog)
			assert.NoError(t, err)
			restored, err := DecodeCatalog(snapshot)
			assert.NoError(t, err)
			assert.Equal(t, Equal(catalog, restored), true)
			again, err := EncodeCatalog(restored)
			assert.NoError(t, err)
			assert.Diff(t, again, snapshot)
		})
	}
}

func TestCatalogCodec_NonemptyHelmChart(t *testing.T) {
	t.Parallel()
	parsed, err := ParseDoc(map[string]any{
		"apiVersion": "helm.toolkit.fluxcd.io/v2", "kind": "HelmRelease",
		"metadata": map[string]any{"name": "app", "namespace": "apps"},
		"spec": map[string]any{"chart": map[string]any{"spec": map[string]any{
			"chart": "app", "version": "1.2.3", "sourceRef": map[string]any{"kind": "HelmRepository", "name": "charts"},
		}}},
	}, ParseDocOptions{})
	assert.NoError(t, err)
	catalog := CatalogObject(parsed.Object, parsed.Object.Named())
	encoded, err := EncodeCatalog(catalog)
	assert.NoError(t, err)
	restored, err := DecodeCatalog(encoded)
	assert.NoError(t, err)
	if !Equal(catalog, restored) {
		t.Errorf("catalog changed: before=%#v after=%#v encoded=%s", catalog, restored, encoded)
	}
}

func TestCatalogCodec_ChartForms(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name string
		spec map[string]any
	}{
		{"inline-repository", map[string]any{"chart": map[string]any{"spec": map[string]any{
			"chart": "app", "version": "1.2.3", "sourceRef": map[string]any{"kind": "HelmRepository", "name": "charts"},
		}}}},
		{"inline-git", map[string]any{"chart": map[string]any{"spec": map[string]any{
			"chart": "./charts/app", "version": "1.2.3", "sourceRef": map[string]any{"kind": "GitRepository", "name": "charts"},
		}}}},
		{"referenced-chart", map[string]any{"chartRef": map[string]any{"kind": "HelmChart", "name": "chart", "namespace": "sources"}}},
		{"referenced-oci", map[string]any{"chartRef": map[string]any{"kind": "OCIRepository", "name": "chart"}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			parsed, err := ParseDoc(map[string]any{
				"apiVersion": "helm.toolkit.fluxcd.io/v2", "kind": "HelmRelease",
				"metadata": map[string]any{"name": "app", "namespace": "apps"}, "spec": tc.spec,
			}, ParseDocOptions{})
			assert.NoError(t, err)
			id := parsed.Object.Named()
			id.Namespace = "projected"
			catalog := CatalogObject(parsed.Object, id)
			encoded, err := EncodeCatalog(catalog)
			assert.NoError(t, err)
			restored, err := DecodeCatalog(encoded)
			assert.NoError(t, err)
			assert.Equal(t, Equal(catalog, restored), true)
			assert.Equal(t, restored.Named(), id)
			if diff := cmp.Diff(
				slices.Collect(References(catalog, ReferenceAll)),
				slices.Collect(References(restored, ReferenceAll)),
				cmp.AllowUnexported(Reference{}),
			); diff != "" {
				t.Errorf("catalog references (-want +got):\n%s", diff)
			}
			again, err := EncodeCatalog(restored)
			assert.NoError(t, err)
			assert.Diff(t, again, encoded)
		})
	}
}
