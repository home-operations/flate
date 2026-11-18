package oci

import (
	"slices"

	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
)

func pickLayer(layers []ocispec.Descriptor, mediaTypes []string) (ocispec.Descriptor, bool) {
	if len(layers) == 0 {
		return ocispec.Descriptor{}, false
	}
	if len(mediaTypes) != 0 {
		for _, l := range layers {
			if slices.Contains(mediaTypes, l.MediaType) {
				return l, true
			}
		}
		return ocispec.Descriptor{}, false
	}
	return layers[0], true
}
