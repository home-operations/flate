package oci

import (
	"testing"

	"github.com/google/go-cmp/cmp"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	"helm.sh/helm/v4/pkg/registry"
)

func TestPickLayer_Selection(t *testing.T) {
	t.Parallel()
	unknown := ocispec.Descriptor{MediaType: "application/vnd.unknown", Data: []byte("unknown")}
	chart := ocispec.Descriptor{MediaType: registry.ChartLayerMediaType, Data: []byte("chart")}
	otherChart := ocispec.Descriptor{MediaType: registry.ChartLayerMediaType, Data: []byte("other chart")}
	olderChart := ocispec.Descriptor{MediaType: registry.LegacyChartLayerMediaType, Data: []byte("older chart")}
	prov := ocispec.Descriptor{MediaType: registry.ProvLayerMediaType, Data: []byte("provenance")}
	layers := []ocispec.Descriptor{unknown, chart, prov}
	cases := []struct {
		name       string
		layers     []ocispec.Descriptor
		mediaTypes []string
		want       ocispec.Descriptor
		wantOK     bool
	}{
		{name: "nil selector picks first", layers: layers, want: unknown, wantOK: true},
		{name: "empty mediaType picks first", layers: layers, mediaTypes: []string{}, want: unknown, wantOK: true},
		{
			name: "matches helm chart", layers: layers,
			mediaTypes: []string{registry.ChartLayerMediaType}, want: chart, wantOK: true,
		},
		{name: "unmatched", layers: layers, mediaTypes: []string{"application/missing"}},
		{name: "provenance first", layers: []ocispec.Descriptor{prov, chart}, want: prov, wantOK: true},
		{name: "all provenance", layers: []ocispec.Descriptor{prov}, want: prov, wantOK: true},
		{name: "first matching chart", layers: []ocispec.Descriptor{prov, chart, otherChart}, mediaTypes: []string{registry.ChartLayerMediaType}, want: chart, wantOK: true},
		{name: "manifest order before allowed media order", layers: []ocispec.Descriptor{prov, olderChart, chart}, mediaTypes: []string{registry.ChartLayerMediaType, registry.LegacyChartLayerMediaType}, want: olderChart, wantOK: true},
		{name: "empty"},
		{name: "empty with media", mediaTypes: []string{registry.ChartLayerMediaType}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, ok := pickLayer(tc.layers, tc.mediaTypes)
			if ok != tc.wantOK {
				t.Fatalf("ok = %v, want %v", ok, tc.wantOK)
			}
			if diff := cmp.Diff(tc.want, got); diff != "" {
				t.Errorf("selected layer (-want +got):\n%s", diff)
			}
		})
	}
}
