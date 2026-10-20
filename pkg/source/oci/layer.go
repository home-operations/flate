package oci

import (
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	"helm.sh/helm/v4/pkg/registry"
)

// pickLayer defaults to the first non-provenance layer, falling back to layers[0].
// Helm provenance may precede chart content and MUST NOT be extracted as a tarball.
func pickLayer(layers []ocispec.Descriptor, mediaType string) (ocispec.Descriptor, bool) {
	if len(layers) == 0 {
		return ocispec.Descriptor{}, false
	}
	if mediaType != "" {
		for _, l := range layers {
			if l.MediaType == mediaType {
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
