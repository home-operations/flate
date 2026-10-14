package oci

import (
	sourcev1 "github.com/fluxcd/source-controller/api/v1"
	"github.com/home-operations/flate/pkg/manifest"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	"helm.sh/helm/v4/pkg/registry"
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

// pickLayer defaults to the first non-provenance layer, falling back to layers[0].
// Helm provenance may precede chart content and MUST NOT be extracted as a tarball.
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
		if l.MediaType != registry.ProvLayerMediaType {
			return l, true
		}
	}
	return layers[0], true
}
