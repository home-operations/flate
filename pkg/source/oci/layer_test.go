package oci

import (
	"testing"

	"github.com/google/go-cmp/cmp"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	"helm.sh/helm/v4/pkg/registry"

	"github.com/home-operations/flate/pkg/manifest"
)

func TestPickLayer_Selection(t *testing.T) {
	t.Parallel()
	unknown := ocispec.Descriptor{MediaType: "application/vnd.unknown", Data: []byte("unknown")}
	chart := ocispec.Descriptor{MediaType: registry.ChartLayerMediaType, Data: []byte("chart")}
	prov := ocispec.Descriptor{MediaType: registry.ProvLayerMediaType, Data: []byte("provenance")}
	layers := []ocispec.Descriptor{unknown, chart, prov}
	cases := []struct {
		name     string
		layers   []ocispec.Descriptor
		selector *manifest.OCILayerSelector
		want     ocispec.Descriptor
		wantOK   bool
	}{
		{name: "nil selector picks first", layers: layers, want: unknown, wantOK: true},
		{name: "empty mediaType picks first", layers: layers, selector: &manifest.OCILayerSelector{}, want: unknown, wantOK: true},
		{
			name: "matches helm chart", layers: layers,
			selector: &manifest.OCILayerSelector{MediaType: registry.ChartLayerMediaType}, want: chart, wantOK: true,
		},
		{name: "unmatched", layers: layers, selector: &manifest.OCILayerSelector{MediaType: "application/missing"}},
		{name: "provenance first", layers: []ocispec.Descriptor{prov, chart}, want: chart, wantOK: true},
		{name: "all provenance", layers: []ocispec.Descriptor{prov}, want: prov, wantOK: true},
		{name: "empty"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, ok := pickLayer(tc.layers, (&manifest.OCIRepository{LayerSelector: tc.selector}).LayerMediaType())
			if ok != tc.wantOK {
				t.Fatalf("ok = %v, want %v", ok, tc.wantOK)
			}
			if diff := cmp.Diff(tc.want, got); diff != "" {
				t.Errorf("selected layer (-want +got):\n%s", diff)
			}
		})
	}
}
