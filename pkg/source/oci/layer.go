package oci

import (
	"archive/tar"
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/opencontainers/go-digest"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	"oras.land/oras-go/v2/registry/remote"

	"github.com/home-operations/flate/pkg/manifest"
)

// copiedLayerFilename is the deterministic name layers are stored
// under when LayerSelector.Operation is "copy". Downstream consumers
// (e.g. Kustomization spec.path on an OCIRepository whose artifact
// is shipped as a tarball) look for this name when they need the
// raw blob.
const copiedLayerFilename = "layer.tar.gz"

// applyLayerSelector post-processes an OCI artifact written into
// slot by oras.Copy. After oras.Copy, the slot contains every blob
// in the artifact named by its sha256 digest. This function:
//
//   - Reads the manifest from the slot to find the layer matching
//     selector.MediaType (or the first layer when MediaType is empty).
//   - For Operation = "extract" (Flux's default), untars the layer's
//     tar.gz blob into the slot root.
//   - For Operation = "copy", renames the layer blob to layer.tar.gz.
//   - Cleans up the now-redundant digest-named files.
//
// When selector is nil the default extract behavior still applies —
// matches source-controller's behavior when spec.layerSelector is
// omitted but the artifact has exactly one tarball layer.
func applyLayerSelector(
	_ context.Context,
	_ *remote.Repository,
	slot string,
	manifestDigest string,
	selector *manifest.OCILayerSelector,
) error {
	man, err := readSlotManifest(slot, manifestDigest)
	if err != nil {
		// No manifest in slot (e.g. helm registry already pulled
		// the chart elsewhere) — nothing to do.
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return err
	}
	layer, ok := pickLayer(man.Layers, selector)
	if !ok {
		if selector != nil && selector.MediaType != "" {
			return fmt.Errorf("no layer matched mediaType %q (manifest has %d layer(s))",
				selector.MediaType, len(man.Layers))
		}
		return nil
	}

	op := manifest.OCILayerOperationExtract
	if selector != nil && selector.Operation != "" {
		op = selector.Operation
	}

	layerPath := digestPath(slot, layer.Digest)
	switch op {
	case manifest.OCILayerOperationExtract:
		if err := extractTarGz(layerPath, slot); err != nil {
			return fmt.Errorf("extract layer: %w", err)
		}
	case manifest.OCILayerOperationCopy:
		dest := filepath.Join(slot, copiedLayerFilename)
		if err := os.Rename(layerPath, dest); err != nil {
			return fmt.Errorf("copy layer: %w", err)
		}
	default:
		return fmt.Errorf("unsupported layer operation %q", op)
	}

	return cleanupBlobs(slot)
}

// readSlotManifest finds and decodes the OCI image manifest written
// by orasfile under the given digest.
func readSlotManifest(slot, digestStr string) (*ocispec.Manifest, error) {
	path := digestPath(slot, digest.Digest(digestStr))
	b, err := os.ReadFile(path) //nolint:gosec // slot is fetcher-owned cache path
	if err != nil {
		return nil, err
	}
	var m ocispec.Manifest
	if err := json.Unmarshal(b, &m); err != nil {
		return nil, fmt.Errorf("decode manifest: %w", err)
	}
	return &m, nil
}

// pickLayer returns the first layer matching selector.MediaType,
// or the first layer overall when selector is nil or MediaType is empty.
func pickLayer(layers []ocispec.Descriptor, selector *manifest.OCILayerSelector) (ocispec.Descriptor, bool) {
	if len(layers) == 0 {
		return ocispec.Descriptor{}, false
	}
	if selector == nil || selector.MediaType == "" {
		return layers[0], true
	}
	for _, l := range layers {
		if l.MediaType == selector.MediaType {
			return l, true
		}
	}
	return ocispec.Descriptor{}, false
}

// digestPath resolves a digest to its on-disk path inside slot,
// matching orasfile's layout: "<algorithm>:<hex>" → "<hex>" (the
// algorithm prefix is stripped, file lives at the slot root).
func digestPath(slot string, d digest.Digest) string {
	_, hex, found := strings.Cut(d.String(), ":")
	if !found {
		hex = d.String()
	}
	return filepath.Join(slot, hex)
}

// cleanupBlobs removes the digest-named files orasfile wrote — by
// this point the manifest's selected layer has either been extracted
// or renamed, so the raw blobs are noise.
func cleanupBlobs(slot string) error {
	entries, err := os.ReadDir(slot)
	if err != nil {
		return err
	}
	for _, e := range entries {
		if e.IsDir() || e.Name() == copiedLayerFilename || e.Name() == cachedDigestFile {
			continue
		}
		// orasfile writes files named by their sha256 hex digest.
		// Anything that isn't the (already-renamed) layer or sentinel
		// matches that shape and is safe to drop.
		if looksLikeDigest(e.Name()) {
			_ = os.Remove(filepath.Join(slot, e.Name()))
		}
	}
	return nil
}

func looksLikeDigest(name string) bool {
	if len(name) != 64 {
		return false
	}
	for _, c := range name {
		switch {
		case c >= '0' && c <= '9':
		case c >= 'a' && c <= 'f':
		default:
			return false
		}
	}
	return true
}

// extractTarGz unpacks a gzipped tarball into dst. Refuses path
// traversal entries (../) so a malicious artifact can't write
// outside the cache slot.
func extractTarGz(src, dst string) error {
	f, err := os.Open(src) //nolint:gosec // src lives under the fetcher's cache slot
	if err != nil {
		return err
	}
	defer func() { _ = f.Close() }()
	gz, err := gzip.NewReader(f)
	if err != nil {
		return fmt.Errorf("gzip: %w", err)
	}
	defer func() { _ = gz.Close() }()
	tr := tar.NewReader(gz)
	for {
		hdr, err := tr.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return fmt.Errorf("tar: %w", err)
		}
		clean := filepath.Clean(hdr.Name)
		if strings.HasPrefix(clean, "..") || strings.Contains(clean, ".."+string(filepath.Separator)) {
			return fmt.Errorf("tar entry escapes target directory: %q", hdr.Name)
		}
		target := filepath.Join(dst, clean)
		switch hdr.Typeflag {
		case tar.TypeDir:
			if err := os.MkdirAll(target, 0o750); err != nil {
				return err
			}
		case tar.TypeReg:
			if err := os.MkdirAll(filepath.Dir(target), 0o750); err != nil {
				return err
			}
			out, err := os.OpenFile(target, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o600) //nolint:gosec // target stays under dst
			if err != nil {
				return err
			}
			if _, err := io.Copy(out, tr); err != nil { //nolint:gosec // tar.Reader is size-bounded by header
				_ = out.Close()
				return err
			}
			if err := out.Close(); err != nil {
				return err
			}
		}
	}
	if err := os.Remove(src); err != nil && !os.IsNotExist(err) {
		return err
	}
	return nil
}
