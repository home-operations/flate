package oci

import (
	sourcev1 "github.com/fluxcd/source-controller/api/v1"
	"github.com/home-operations/flate/pkg/manifest"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
)

const (
	helmChartLayerMediaType      = "application/vnd.cncf.helm.chart.content.v1.tar+gzip"
	helmChartProvenanceMediaType = "application/vnd.cncf.helm.chart.provenance.v1.prov"
)

func layerMediaType(selector *manifest.OCILayerSelector) string {
	if selector == nil {
		return ""
	}
	return selector.MediaType
}
func effectiveLayerOperation(selector *manifest.OCILayerSelector) string {
	if selector == nil || selector.Operation == "" {
		return sourcev1.OCILayerExtract
	}
	return selector.Operation
}

// pickLayer returns the layer matching selector.MediaType, or — with no
// selector — the first non-provenance layer (falling back to layers[0]).
//
// The provenance skip is load-bearing. `helm push` lists a signed
// chart's detached-signature layer (helmChartProvenanceMediaType) AHEAD
// of the gzipped content layer, so the old "return layers[0]" handed the
// signature blob (PGP text, not a tarball) to gzip extraction — "gzip:
// invalid header" on every signed chart (cert-manager, truecharts,
// grafana/prometheus community, ...). Skipping only the provenance type
// keeps the documented default-to-first-layer behavior intact for every
// other shape, including generic single-layer Flux artifacts.
func pickLayer(layers []ocispec.Descriptor, selector *manifest.OCILayerSelector) (ocispec.Descriptor, bool) {
	if len(layers) == 0 {
		return ocispec.Descriptor{}, false
	}
	if selector != nil && selector.MediaType != "" {
		for _, l := range layers {
			if l.MediaType == selector.MediaType {
				return l, true
			}
		}
		return ocispec.Descriptor{}, false
	}
	for _, l := range layers {
		if l.MediaType != helmChartProvenanceMediaType {
			return l, true
		}
	}
	return layers[0], true
}
