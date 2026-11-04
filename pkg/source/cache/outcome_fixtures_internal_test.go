package cache

import (
	"bytes"
	"errors"
	"testing"

	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	"oras.land/oras-go/v2/content"
	"oras.land/oras-go/v2/errdef"
)

func pushOutcomeFixture(t *testing.T, c *Cache, config, data []byte) ocispec.Descriptor {
	t.Helper()
	for _, blob := range []struct {
		media string
		data  []byte
	}{{outcomeConfig, config}, {ocispec.MediaTypeImageManifest, data}} {
		desc := content.NewDescriptorFromBytes(blob.media, blob.data)
		if err := c.store.Push(
			t.Context(),
			desc,
			bytes.NewReader(blob.data),
		); err != nil &&
			!errors.Is(err, errdef.ErrAlreadyExists) {
			t.Fatal(err)
		}
	}
	return content.NewDescriptorFromBytes(ocispec.MediaTypeImageManifest, data)
}
