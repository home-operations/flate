package oci

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"testing"

	ocispec "github.com/opencontainers/image-spec/specs-go/v1"

	"github.com/home-operations/flate/pkg/manifest"
)

func TestPickLayer(t *testing.T) {
	layers := []ocispec.Descriptor{
		{MediaType: "application/vnd.unknown"},
		{MediaType: "application/vnd.cncf.helm.chart.content.v1.tar+gzip"},
		{MediaType: "application/octet-stream"},
	}
	cases := []struct {
		name     string
		selector *manifest.OCILayerSelector
		wantMT   string
		wantOK   bool
	}{
		{"nil selector picks first", nil, "application/vnd.unknown", true},
		{"empty mediaType picks first", &manifest.OCILayerSelector{}, "application/vnd.unknown", true},
		{
			"matches helm chart",
			&manifest.OCILayerSelector{MediaType: "application/vnd.cncf.helm.chart.content.v1.tar+gzip"},
			"application/vnd.cncf.helm.chart.content.v1.tar+gzip",
			true,
		},
		{
			"unmatched",
			&manifest.OCILayerSelector{MediaType: "application/missing"},
			"", false,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, ok := pickLayer(layers, tc.selector)
			if ok != tc.wantOK {
				t.Fatalf("ok = %v, want %v", ok, tc.wantOK)
			}
			if ok && got.MediaType != tc.wantMT {
				t.Errorf("mediaType = %q, want %q", got.MediaType, tc.wantMT)
			}
		})
	}
}

func TestPickLayer_ProvenanceFirstSkipped(t *testing.T) {
	layers := []ocispec.Descriptor{
		{MediaType: "application/vnd.cncf.helm.chart.provenance.v1.prov"},
		{MediaType: "application/vnd.cncf.helm.chart.content.v1.tar+gzip"},
	}
	got, ok := pickLayer(layers, nil)
	if !ok {
		t.Fatal("pickLayer returned ok=false")
	}
	if got.MediaType != "application/vnd.cncf.helm.chart.content.v1.tar+gzip" {
		t.Errorf("mediaType = %q, want the content layer (provenance must be skipped)", got.MediaType)
	}
}

func mustTarGz(t *testing.T, files map[string]string) []byte {
	t.Helper()
	var buf bytes.Buffer
	gw := gzip.NewWriter(&buf)
	tw := tar.NewWriter(gw)
	for name, body := range files {
		if err := tw.WriteHeader(&tar.Header{
			Name:     name,
			Typeflag: tar.TypeReg,
			Size:     int64(len(body)),
			Mode:     0o644,
		}); err != nil {
			t.Fatal(err)
		}
		if _, err := tw.Write([]byte(body)); err != nil {
			t.Fatal(err)
		}
	}
	if err := tw.Close(); err != nil {
		t.Fatal(err)
	}
	if err := gw.Close(); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}
