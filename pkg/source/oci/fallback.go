package oci

import (
	"context"
	"io"
	"os"

	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
)

// flatFallbackStorage is the content.Storage we wire into
// orasfile.NewWithFallbackStorage so blobs lacking an
// `org.opencontainers.image.title` annotation still land on disk.
//
// orasfile's default fallback is in-memory, so any unnamed blob — every
// Flux `vnd.cncf.flux.content.v1.tar+gzip` content layer, its config,
// and the image manifest itself — never appears in the slot directory.
// applyLayerSelector then short-circuits on ErrNotExist when it tries
// to read the manifest from disk (see layer.go), and the slot is left
// holding only the `.flate-digest` sentinel. Downstream `kustomize
// build` renders zero manifests rather than failing — silently.
//
// Every fallback blob lands at `slot/<hex>` because that's the layout
// digestPath / readSlotManifest / cleanupBlobs already address. Fetch +
// Exists read from the same path so oras can resume / dedup blobs it
// already pulled within a single Copy.
type flatFallbackStorage struct{ root string }

func (s *flatFallbackStorage) Push(_ context.Context, desc ocispec.Descriptor, content io.Reader) error {
	path := digestPath(s.root, desc.Digest)
	out, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o600) //nolint:gosec // path lives under fetcher-owned slot
	if err != nil {
		return err
	}
	// oras-go bounds the reader to desc.Size during Copy, so plain io.Copy
	// is not unbounded here.
	if _, err := io.Copy(out, content); err != nil { //nolint:gosec // size-bounded by oras's descriptor verification
		_ = out.Close()
		return err
	}
	return out.Close()
}

func (s *flatFallbackStorage) Fetch(_ context.Context, desc ocispec.Descriptor) (io.ReadCloser, error) {
	return os.Open(digestPath(s.root, desc.Digest)) //nolint:gosec // path lives under fetcher-owned slot
}

func (s *flatFallbackStorage) Exists(_ context.Context, desc ocispec.Descriptor) (bool, error) {
	_, err := os.Stat(digestPath(s.root, desc.Digest))
	if os.IsNotExist(err) {
		return false, nil
	}
	return err == nil, err
}
