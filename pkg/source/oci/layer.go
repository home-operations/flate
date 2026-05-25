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

	"github.com/home-operations/flate/pkg/manifest"
)

// copiedLayerFilename is the deterministic name layers are stored
// under when LayerSelector.Operation is "copy". Downstream consumers
// (e.g. Kustomization spec.path on an OCIRepository whose artifact
// is shipped as a tarball) look for this name when they need the
// raw blob.
const copiedLayerFilename = "layer.tar.gz"

// stagedLayerFilename is where applyLayerSelector parks the selected
// layer before wiping the OCI Image Layout dirs (blobs/, ingest/,
// oci-layout, index.json). Staging at the slot root lets the layout
// wipe run with no surviving file handles into deleted subtrees, and
// the dot-prefix keeps it from colliding with anything kustomize
// would treat as a resource. extractTarGz removes it on success.
const stagedLayerFilename = ".flate-layer.tar.gz"

// ociLayoutArtifacts are the files / directories oras-go's content/oci
// store writes alongside the blobs we actually want — the OCI Image
// Layout metadata (index.json, oci-layout, blobs/, ingest/). Downstream
// consumers (kustomize) only want the extracted tarball contents at
// the slot root, so cleanupOCILayout sweeps these away once
// applyLayerSelector has moved / extracted the selected layer.
var ociLayoutArtifacts = []string{
	ocispec.ImageBlobsDir,   // "blobs"
	"ingest",                // content/oci.Storage's temp-rename area
	ocispec.ImageLayoutFile, // "oci-layout"
	ocispec.ImageIndexFile,  // "index.json"
}

// effectiveLayerOperation returns the operation applyLayerSelector
// will run for a given selector — Extract by default, honoring an
// explicit override otherwise. Exposed so callers (the OCI fetcher)
// can branch behavior such as source-ignore application that only
// makes sense for the extracted-contents shape.
func effectiveLayerOperation(selector *manifest.OCILayerSelector) string {
	if selector == nil || selector.Operation == "" {
		return manifest.OCILayerOperationExtract
	}
	return selector.Operation
}

// applyLayerSelector post-processes an OCI artifact written into slot
// by oras.Copy. After Copy, the slot is laid out per the OCI Image
// Layout spec (see ociLayoutArtifacts). This function:
//
//   - Reads the manifest blob to find the layer matching
//     selector.MediaType (or the single layer when no selector is set).
//   - Stages the layer at slot/<stagedLayerFilename> so the layout
//     wipe in the next step can't take open handles or user-tarball
//     entries that collide with OCI well-known names down with it.
//   - For Operation = "extract" (Flux's default), untars the staged
//     layer into the slot root. extractTarGz removes the staged file
//     on success.
//   - For Operation = "copy", renames the staged layer to
//     <copiedLayerFilename>.
//   - Wipes the OCI Image Layout artifacts.
//
// When the selector is nil and the artifact has multiple layers, the
// function errors rather than silently picking layers[0] — matches
// Flux source-controller's "ambiguous selection" behavior and avoids
// rendering the wrong layer for multi-layer artifacts.
func applyLayerSelector(
	_ context.Context,
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
	if len(man.Layers) > 1 && (selector == nil || selector.MediaType == "") {
		mts := make([]string, len(man.Layers))
		for i, l := range man.Layers {
			mts[i] = l.MediaType
		}
		return fmt.Errorf("OCI artifact has %d layers but no spec.layerSelector.mediaType to disambiguate (layer mediaTypes: %v)",
			len(man.Layers), mts)
	}
	layer, ok := pickLayer(man.Layers, selector)
	if !ok {
		if selector != nil && selector.MediaType != "" {
			return fmt.Errorf("no layer matched mediaType %q (manifest has %d layer(s))",
				selector.MediaType, len(man.Layers))
		}
		return nil
	}

	op := effectiveLayerOperation(selector)

	staged := filepath.Join(slot, stagedLayerFilename)
	if err := os.Rename(digestPath(slot, layer.Digest), staged); err != nil {
		return fmt.Errorf("stage layer: %w", err)
	}
	// Wipe the layout BEFORE the operation runs so extract / copy
	// can never collide with surviving OCI artifact directories.
	if err := cleanupOCILayout(slot); err != nil {
		return fmt.Errorf("cleanup oci layout: %w", err)
	}

	switch op {
	case manifest.OCILayerOperationExtract:
		if err := extractTarGz(staged, slot); err != nil {
			return fmt.Errorf("extract layer: %w", err)
		}
	case manifest.OCILayerOperationCopy:
		if err := os.Rename(staged, filepath.Join(slot, copiedLayerFilename)); err != nil {
			return fmt.Errorf("copy layer: %w", err)
		}
	default:
		return fmt.Errorf("unsupported layer operation %q", op)
	}
	return nil
}

// readSlotManifest decodes the OCI image manifest oras.Copy wrote into
// the layout under the given digest.
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
// matching the OCI Image Layout spec: `<slot>/blobs/<algo>/<hex>`.
// Aligns with oras-go's content/oci.Storage Push.
func digestPath(slot string, d digest.Digest) string {
	algo := d.Algorithm().String()
	hex := d.Encoded()
	if hex == "" {
		// Malformed digest — Encoded() returns "" rather than error.
		// Fall back to splitting on ":" so the path stays inside slot
		// rather than escaping or hitting a bare blobs/<algo>/ dir.
		_, hex, _ = strings.Cut(d.String(), ":")
		if hex == "" {
			hex = d.String()
		}
	}
	return filepath.Join(slot, ocispec.ImageBlobsDir, algo, hex)
}

// hasUnfinishedOCILayout reports whether slot still carries any of
// the OCI Image Layout artifacts cleanupOCILayout is supposed to
// wipe at the end of a successful fetch. Their presence means the
// slot is in an inconsistent state: either applyLayerSelector
// didn't run to completion, or a concurrent process modified the
// slot after a successful fetch landed `.flate-digest`. The OCI
// fetcher's cache-hit path uses this as a sentinel to reset stale
// slots before serving them.
func hasUnfinishedOCILayout(slot string) bool {
	for _, name := range ociLayoutArtifacts {
		if _, err := os.Stat(filepath.Join(slot, name)); err == nil {
			return true
		}
	}
	return false
}

// cleanupOCILayout removes the OCI Image Layout artifacts oras-go wrote
// alongside the artifact blobs. By the time this runs the selected
// layer has already been staged outside the layout subtree, so removal
// is safe — but the order matters: a layout wipe BEFORE staging would
// leave nothing to extract from.
func cleanupOCILayout(slot string) error {
	for _, name := range ociLayoutArtifacts {
		if err := os.RemoveAll(filepath.Join(slot, name)); err != nil {
			return err
		}
	}
	return nil
}

// maxExtractedBytes caps the total bytes extractTarGz writes to disk
// per call. Defends against zip-bomb-style artifacts (small gzip,
// huge tar) that would otherwise fill the cache slot until ENOSPC.
// 1 GiB is generous enough for every helm chart and flux-manifests
// shape seen in the wild — the largest known charts are ~50 MiB
// extracted — while still bounding a malicious or buggy publisher.
//
// var (not const) so tests can lower it without writing real-size
// tarballs. Production callers must NOT override it.
var maxExtractedBytes int64 = 1 << 30

// extractTarGz unpacks a gzipped tarball into dst. Rejects any entry
// whose resolved path escapes dst — covers `../` traversal, absolute
// paths in the tar header (e.g. `/etc/passwd`), and symlink-pivoting
// hardlinks. Symlinks/hardlinks/devices are silently skipped rather
// than honored; Flux's source-controller does the same and helm
// charts never depend on them. Output is capped at maxExtractedBytes
// to bound zip-bomb-style artifacts.
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
	remaining := maxExtractedBytes
	for {
		hdr, err := tr.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return fmt.Errorf("tar: %w", err)
		}
		target, err := safeJoinTarPath(dst, hdr.Name)
		if err != nil {
			return err
		}
		switch hdr.Typeflag {
		case tar.TypeDir:
			if err := os.MkdirAll(target, 0o750); err != nil {
				return err
			}
		case tar.TypeReg:
			if err := os.MkdirAll(filepath.Dir(target), 0o750); err != nil {
				return err
			}
			out, err := os.OpenFile(target, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o600) //nolint:gosec // target stays under dst (safeJoinTarPath enforced)
			if err != nil {
				return err
			}
			// LimitReader caps per-call output AND turns a header that
			// over-claims size into a short read at the limit (rather
			// than an unbounded write). Track remaining manually so a
			// single oversized entry surfaces a clear "extract limit"
			// error instead of a torso-shaped truncated file.
			n, err := io.Copy(out, io.LimitReader(tr, remaining+1))
			if err != nil {
				_ = out.Close()
				return err
			}
			if n > remaining {
				_ = out.Close()
				_ = os.Remove(target)
				return fmt.Errorf("tar entry %q exceeds %d-byte extract limit", hdr.Name, maxExtractedBytes)
			}
			remaining -= n
			if err := out.Close(); err != nil {
				return err
			}
		default:
			// Skip symlinks, hardlinks, devices, FIFOs, etc.
			// Honoring symlinks would re-open the traversal surface
			// (point at /etc/passwd, then a subsequent TypeReg
			// "writes through" the link); flate has no use case
			// for these and Flux's source-controller does the same.
		}
	}
	if err := os.Remove(src); err != nil && !os.IsNotExist(err) {
		return err
	}
	return nil
}

// safeJoinTarPath joins a tar entry's declared name against dst and
// verifies the resolved path stays strictly inside dst. Defends
// against three escape shapes:
//
//   - Relative traversal: `../../escape.txt` (filepath.Clean
//     collapses; Rel reports `..` prefix).
//   - Absolute path: `/etc/passwd` (filepath.Join silently strips the
//     leading `/` and roots inside dst, which Rel can't detect after
//     the fact — so we reject any entryName that filepath.IsAbs flags
//     OR that has a Windows-style volume name, BEFORE the Join).
//   - Symlink-pivot: a prior symlink entry creating a back-pointer.
//     Mitigated by extractTarGz's default-case silent skip of
//     symlinks; this guard catches the residual case if symlinks
//     are ever re-enabled.
//
// Mirrors the bucket source's safeJoinUnderSlot — both packages need
// the same guarantee.
func safeJoinTarPath(dst, entryName string) (string, error) {
	clean := filepath.Clean(entryName)
	if filepath.IsAbs(clean) || filepath.VolumeName(clean) != "" {
		return "", fmt.Errorf("tar entry escapes target directory: %q", entryName)
	}
	target := filepath.Join(dst, clean)
	rel, err := filepath.Rel(dst, target)
	if err != nil || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return "", fmt.Errorf("tar entry escapes target directory: %q", entryName)
	}
	return target, nil
}
