package oci

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/opencontainers/go-digest"
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

func TestApplyLayerSelector_Extract(t *testing.T) {
	slot := t.TempDir()

	chartFiles := map[string]string{
		"Chart.yaml":        "name: example\nversion: 1.0.0\n",
		"templates/cm.yaml": "kind: ConfigMap\n",
		"values.yaml":       "replicas: 1\n",
	}
	layerBytes := mustTarGz(t, chartFiles)
	layerDigest := writeBlob(t, slot, layerBytes)

	manifestBytes, manifestDigest := writeManifest(t, slot, []ocispec.Descriptor{
		{
			MediaType: "application/vnd.cncf.helm.chart.content.v1.tar+gzip",
			Digest:    layerDigest,
			Size:      int64(len(layerBytes)),
		},
	})
	_ = manifestBytes

	if err := applyLayerSelector(t.Context(), slot, manifestDigest.String(),
		&manifest.OCILayerSelector{
			MediaType: "application/vnd.cncf.helm.chart.content.v1.tar+gzip",
			Operation: manifest.OCILayerOperationExtract,
		}); err != nil {
		t.Fatalf("applyLayerSelector: %v", err)
	}

	for name, want := range chartFiles {
		path := filepath.Join(slot, name)
		got, err := os.ReadFile(path) //nolint:gosec // path is inside t.TempDir()
		if err != nil {
			t.Fatalf("read %s: %v", path, err)
		}
		if string(got) != want {
			t.Errorf("%s = %q, want %q", name, got, want)
		}
	}
	// OCI Image Layout artifacts should be wiped after extract.
	for _, name := range ociLayoutArtifacts {
		if _, err := os.Stat(filepath.Join(slot, name)); !os.IsNotExist(err) {
			t.Errorf("leftover OCI layout artifact: %s (err: %v)", name, err)
		}
	}
	if _, err := os.Stat(filepath.Join(slot, stagedLayerFilename)); !os.IsNotExist(err) {
		t.Errorf("staged layer file not removed after extract")
	}
}

func TestApplyLayerSelector_Copy(t *testing.T) {
	slot := t.TempDir()

	layerBytes := mustTarGz(t, map[string]string{"hello.txt": "world"})
	layerDigest := writeBlob(t, slot, layerBytes)
	_, manifestDigest := writeManifest(t, slot, []ocispec.Descriptor{
		{
			MediaType: "application/vnd.cncf.helm.chart.content.v1.tar+gzip",
			Digest:    layerDigest,
			Size:      int64(len(layerBytes)),
		},
	})

	if err := applyLayerSelector(t.Context(), slot, manifestDigest.String(),
		&manifest.OCILayerSelector{
			MediaType: "application/vnd.cncf.helm.chart.content.v1.tar+gzip",
			Operation: manifest.OCILayerOperationCopy,
		}); err != nil {
		t.Fatalf("applyLayerSelector: %v", err)
	}

	got, err := os.ReadFile(filepath.Join(slot, copiedLayerFilename)) //nolint:gosec // inside t.TempDir()
	if err != nil {
		t.Fatalf("read layer.tar.gz: %v", err)
	}
	if !bytes.Equal(got, layerBytes) {
		t.Errorf("copied layer differs from source layer (len got=%d want=%d)", len(got), len(layerBytes))
	}
}

func TestApplyLayerSelector_MediaTypeUnmatched(t *testing.T) {
	slot := t.TempDir()
	layerBytes := mustTarGz(t, map[string]string{"x": "y"})
	layerDigest := writeBlob(t, slot, layerBytes)
	_, manifestDigest := writeManifest(t, slot, []ocispec.Descriptor{
		{
			MediaType: "application/vnd.unknown",
			Digest:    layerDigest,
			Size:      int64(len(layerBytes)),
		},
	})

	err := applyLayerSelector(t.Context(), slot, manifestDigest.String(),
		&manifest.OCILayerSelector{MediaType: "application/vnd.cncf.helm.chart.content.v1.tar+gzip"})
	if err == nil {
		t.Fatalf("expected error for unmatched mediaType")
	}
}

// TestSafeJoinTarPath covers the three escape shapes the helper is
// supposed to reject: relative `../` traversal, absolute paths in the
// tar header, and the happy path that must NOT be rejected.
func TestSafeJoinTarPath(t *testing.T) {
	t.Parallel()
	dst := t.TempDir()
	cases := []struct {
		name      string
		entry     string
		wantError bool
	}{
		{"relative traversal", "../escape.txt", true},
		{"deep relative traversal", "../../../etc/passwd", true},
		{"absolute path", "/etc/passwd", true},
		{"absolute deep path", "/var/run/secrets/token", true},
		{"sneaky cleaned traversal", "foo/../../escape", true},
		{"clean traversal back to dst is fine", "foo/../bar.txt", false},
		{"plain relative", "Chart.yaml", false},
		{"nested relative", "templates/cm.yaml", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, err := safeJoinTarPath(dst, tc.entry)
			if tc.wantError {
				if err == nil {
					t.Errorf("safeJoinTarPath(%q) = %q, nil; want escape error", tc.entry, got)
				}
				return
			}
			if err != nil {
				t.Errorf("safeJoinTarPath(%q): %v", tc.entry, err)
			}
		})
	}
}

func TestApplyLayerSelector_TraversalRejected(t *testing.T) {
	slot := t.TempDir()
	// Craft a tarball with a ../ entry.
	var buf bytes.Buffer
	gw := gzip.NewWriter(&buf)
	tw := tar.NewWriter(gw)
	body := []byte("malicious")
	if err := tw.WriteHeader(&tar.Header{
		Name:     "../escape.txt",
		Typeflag: tar.TypeReg,
		Size:     int64(len(body)),
		Mode:     0o644,
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := tw.Write(body); err != nil {
		t.Fatal(err)
	}
	_ = tw.Close()
	_ = gw.Close()

	layerDigest := writeBlob(t, slot, buf.Bytes())
	_, manifestDigest := writeManifest(t, slot, []ocispec.Descriptor{
		{
			MediaType: "application/vnd.cncf.helm.chart.content.v1.tar+gzip",
			Digest:    layerDigest,
			Size:      int64(buf.Len()),
		},
	})

	err := applyLayerSelector(t.Context(), slot, manifestDigest.String(),
		&manifest.OCILayerSelector{
			MediaType: "application/vnd.cncf.helm.chart.content.v1.tar+gzip",
			Operation: manifest.OCILayerOperationExtract,
		})
	if err == nil {
		t.Fatalf("expected traversal rejection")
	}
}

// mustTarGz produces a gzipped tarball with the given files.
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

// writeBlob writes payload at the OCI Image Layout blob path
// (slot/blobs/sha256/<hex>) and returns its OCI digest.
func writeBlob(t *testing.T, slot string, payload []byte) digest.Digest {
	t.Helper()
	sum := sha256.Sum256(payload)
	hexs := hex.EncodeToString(sum[:])
	dir := filepath.Join(slot, ocispec.ImageBlobsDir, "sha256")
	if err := os.MkdirAll(dir, 0o750); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, hexs), payload, 0o600); err != nil { //nolint:gosec // inside t.TempDir()
		t.Fatal(err)
	}
	return digest.Digest("sha256:" + hexs)
}

// writeManifest writes an OCI manifest blob (under blobs/sha256) for
// layers and returns (raw bytes, digest).
func writeManifest(t *testing.T, slot string, layers []ocispec.Descriptor) ([]byte, digest.Digest) {
	t.Helper()
	m := ocispec.Manifest{
		MediaType: ocispec.MediaTypeImageManifest,
		Layers:    layers,
	}
	b, err := json.Marshal(m)
	if err != nil {
		t.Fatal(err)
	}
	return b, writeBlob(t, slot, b)
}
