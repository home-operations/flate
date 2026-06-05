package helm

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	sourcev1 "github.com/fluxcd/source-controller/api/v1"

	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source/cacheroot"
	"github.com/home-operations/flate/pkg/store"
)

// TestLocateOCIChart_PrefersSourceArtifactExtract is the headline of
// the unification: when the source.oci.Fetcher has materialized an
// OCIRepository to an EXTRACTED slot (Flux's default
// layerSelector.operation), locateOCIChart returns that slot directly.
//
// This is also what makes spec.verify (cosign), spec.layerSelector,
// spec.certSecretRef, etc. apply to Helm chart pulls — they all fire
// during the source.Fetcher.Fetch call, and Helm consumes the
// already-verified, already-selected artifact.
func TestLocateOCIChart_PrefersSourceArtifactExtract(t *testing.T) {
	t.Parallel()

	slot := t.TempDir()
	writeChartFiles(t, slot, "mychart", "0.1.0")

	cli, hr := setupOCIChartTest(t, slot, "extracted")

	path, err := cli.locateOCIChart(hr)
	if err != nil {
		t.Fatalf("locateOCIChart: %v", err)
	}
	if path != slot {
		t.Errorf("path = %q, want extracted slot %q", path, slot)
	}
	// loader.Load(dir) should succeed on the extracted layout.
	if _, err := cli.LoadChart(t.Context(), hr); err != nil {
		t.Errorf("LoadChart on extracted slot: %v", err)
	}
}

// TestLocateOCIChart_PrefersSourceArtifactCopy covers
// layerSelector.operation=copy: the slot holds layer.tar.gz rather
// than an extracted chart tree. locateOCIChart should return the tgz
// path so helm's FileLoader handles it.
func TestLocateOCIChart_PrefersSourceArtifactCopy(t *testing.T) {
	t.Parallel()

	slot := t.TempDir()
	chartTGZ := buildChartTarGz(t, "mychart", "0.1.0")
	tgzPath := filepath.Join(slot, copiedOCILayerFilename)
	testutil.WriteFileAt(t, tgzPath, string(chartTGZ))

	cli, hr := setupOCIChartTest(t, slot, "copied")

	path, err := cli.locateOCIChart(hr)
	if err != nil {
		t.Fatalf("locateOCIChart: %v", err)
	}
	if path != tgzPath {
		t.Errorf("path = %q, want copied tgz %q", path, tgzPath)
	}
	if _, err := cli.LoadChart(t.Context(), hr); err != nil {
		t.Errorf("LoadChart on copied layer.tar.gz: %v", err)
	}
}

// TestLocateOCIChart_NoArtifactErrors covers the no-fetch shape: when an
// OCIRepository is Ready but carries no SourceArtifact (e.g. an embedder
// wired source.ExistenceFetcher for the kind), locateOCIChart fails loud with
// an artifact-not-available error rather than silently falling back to an
// anonymous, unverified registry pull. Every OCIRepository now real-fetches
// through the source controller, so a missing artifact is a genuine error.
func TestLocateOCIChart_NoArtifactErrors(t *testing.T) {
	t.Parallel()

	cli, err := NewClient(cacheroot.New(t.TempDir()))
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	st := store.New()
	repo := &manifest.OCIRepository{
		Name: "chart", Namespace: "flux-system",
		OCIRepositorySpec: sourcev1.OCIRepositorySpec{
			URL:       "oci://ghcr.io/test/chart",
			Reference: &sourcev1.OCIRepositoryRef{Tag: "0.1.0"},
		},
	}
	st.AddObject(repo)
	// Intentionally NO SetArtifact.
	cli.SetSourceResolver(NewStoreSourceResolver(st))

	hr := &manifest.HelmRelease{
		Name: "demo", Namespace: "default",
		Chart: manifest.HelmChart{
			Name:          "chart",
			RepoName:      "chart",
			RepoNamespace: "flux-system",
			RepoKind:      manifest.KindOCIRepository,
			Version:       "0.1.0",
		},
	}

	_, err = cli.locateOCIChart(hr)
	if err == nil {
		t.Fatal("expected error when OCIRepository has no SourceArtifact")
	}
	if !errors.Is(err, manifest.ErrObjectNotFound) {
		t.Errorf("error should wrap ErrObjectNotFound; got: %v", err)
	}
	if !strings.Contains(err.Error(), "not available") {
		t.Errorf("error should mention the artifact is not available; got: %v", err)
	}
}

// TestLocateOCIChart_PrefersSourceArtifactChartnameSubdir covers the
// most common chart-as-OCI shape: `helm package` emits a tarball with
// a single top-level `<chartname>/` directory, and operation=extract
// (Flux's default) preserves that — so the chart files end up under
// `<slot>/<chartname>/` rather than at the slot root. The hr.Chart.Name
// can differ from the on-disk dir name (publishers may rename), so the
// resolver scans for the single Chart.yaml-bearing subdir.
func TestLocateOCIChart_PrefersSourceArtifactChartnameSubdir(t *testing.T) {
	t.Parallel()

	slot := t.TempDir()
	chartDir := filepath.Join(slot, "vector") // dir name matches the publisher's chart, not hr.Chart.Name
	writeChartFiles(t, chartDir, "vector", "0.52.0")
	// Real source/oci slots also contain `.flate-digest` (and possibly
	// `.flate-layer.tar.gz` from a `copy` op). Drop one to verify the
	// hidden-prefix filter in findChartSubdir doesn't mistake it for
	// a Chart.yaml-less subdir.
	testutil.WriteFileAt(t, filepath.Join(slot, ".flate-digest"), "sha256:abcd")

	cli, hr := setupOCIChartTest(t, slot, "subdir")
	// hr.Chart.Name purposely differs from the on-disk subdir name to
	// pin that the resolver doesn't rely on a name match.
	hr.Chart.Name = "vector-aggregator"

	path, err := cli.locateOCIChart(hr)
	if err != nil {
		t.Fatalf("locateOCIChart: %v", err)
	}
	if path != chartDir {
		t.Errorf("path = %q, want chart subdir %q", path, chartDir)
	}
	if _, err := cli.LoadChart(t.Context(), hr); err != nil {
		t.Errorf("LoadChart on chartname subdir: %v", err)
	}
}

// TestLocateOCIChart_AmbiguousSubdirs covers the multi-subdir case:
// when an OCI artifact unexpectedly contains MORE than one
// Chart.yaml-bearing subdir, refuse to guess. Better a loud error
// than silently rendering the wrong chart. The error must call out
// the bundle-of-charts shape so the operator gets an actionable
// hint, not just "Chart.yaml missing".
func TestLocateOCIChart_AmbiguousSubdirs(t *testing.T) {
	t.Parallel()

	slot := t.TempDir()
	for _, name := range []string{"chart-a", "chart-b"} {
		writeChartFiles(t, filepath.Join(slot, name), name, "0.1.0")
	}

	cli, hr := setupOCIChartTest(t, slot, "ambiguous")

	_, err := cli.locateOCIChart(hr)
	if err == nil {
		t.Fatal("expected error for ambiguous subdirs")
	}
	if !strings.Contains(err.Error(), "multiple") || !strings.Contains(err.Error(), "bundle-of-charts") {
		t.Errorf("error message should distinguish ambiguous case (mention 'multiple' + 'bundle-of-charts'); got: %v", err)
	}
}

// TestOCIChartPathFromArtifact_MissingLayer verifies the explicit
// error when the source fetcher landed a slot that's missing all
// expected shapes — points the operator at the obvious fix
// (layerSelector misconfiguration) instead of failing later inside
// helm's loader with a less-clear message.
func TestOCIChartPathFromArtifact_MissingLayer(t *testing.T) {
	t.Parallel()
	slot := t.TempDir()
	_, err := chartPathFromArtifact(slot)
	if err == nil {
		t.Fatal("expected error for empty slot")
	}
	if !strings.Contains(err.Error(), "Chart.yaml") || !strings.Contains(err.Error(), "layerSelector") {
		t.Errorf("error message should name the missing shapes and hint at layerSelector; got: %v", err)
	}
}

// TestChartPathFromArtifact_HTTPTgz covers the HTTP HelmChart fetcher's
// layout: a chart.tgz at the slot root resolves to that tarball path.
func TestChartPathFromArtifact_HTTPTgz(t *testing.T) {
	t.Parallel()
	slot := t.TempDir()
	tgz := filepath.Join(slot, "chart.tgz")
	if err := os.WriteFile(tgz, []byte("chart"), 0o600); err != nil {
		t.Fatal(err)
	}
	got, err := chartPathFromArtifact(slot)
	if err != nil {
		t.Fatalf("chartPathFromArtifact: %v", err)
	}
	if got != tgz {
		t.Errorf("path = %q, want %q", got, tgz)
	}
}

// setupOCIChartTest builds the common helm.Client + store + HR for
// the source-artifact-preferred path. The slot is registered as the
// OCIRepository's SourceArtifact, matching what source.oci.Fetcher
// would have produced.
func setupOCIChartTest(t *testing.T, slot, label string) (*Client, *manifest.HelmRelease) {
	t.Helper()
	cli, err := NewClient(cacheroot.New(t.TempDir()))
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	st := store.New()
	repo := &manifest.OCIRepository{
		Name: "chart-" + label, Namespace: "flux-system",
		OCIRepositorySpec: sourcev1.OCIRepositorySpec{
			URL: "oci://ghcr.io/test/chart-" + label,
		},
	}
	st.AddObject(repo)
	st.SetArtifact(repo.Named(), &store.SourceArtifact{
		Kind:      manifest.KindOCIRepository,
		URL:       repo.URL,
		LocalPath: slot,
	})
	cli.SetSourceResolver(NewStoreSourceResolver(st))

	hr := &manifest.HelmRelease{
		Name: "demo", Namespace: "default",
		Chart: manifest.HelmChart{
			Name:          "mychart",
			RepoName:      repo.Name,
			RepoNamespace: repo.Namespace,
			RepoKind:      manifest.KindOCIRepository,
		},
	}
	return cli, hr
}

// writeChartFiles drops a minimal helm chart at root/<name-from-Chart.yaml-dir>
// — used for the "extract" layout test where source.oci leaves chart
// files at slot root.
func writeChartFiles(t *testing.T, root, name, version string) {
	t.Helper()
	testutil.WriteFile(t, root, "Chart.yaml",
		"apiVersion: v2\nname: "+name+"\nversion: "+version+"\n")
	testutil.WriteFile(t, root, "templates/_helpers.tpl", "")
}

// buildChartTarGz returns a gzipped tarball of a minimal helm chart
// — used for the "copy" layout test where source.oci leaves the
// chart at slot/layer.tar.gz.
func buildChartTarGz(t *testing.T, name, version string) []byte {
	t.Helper()
	var buf bytes.Buffer
	gw := gzip.NewWriter(&buf)
	tw := tar.NewWriter(gw)
	files := map[string]string{
		name + "/Chart.yaml":             "apiVersion: v2\nname: " + name + "\nversion: " + version + "\n",
		name + "/templates/_helpers.tpl": "",
	}
	for path, body := range files {
		if err := tw.WriteHeader(&tar.Header{
			Name:     path,
			Typeflag: tar.TypeReg,
			Size:     int64(len(body)),
			Mode:     0o644,
		}); err != nil {
			t.Fatal(err)
		}
		if _, err := io.WriteString(tw, body); err != nil {
			t.Fatal(err)
		}
	}
	_ = tw.Close()
	_ = gw.Close()
	return buf.Bytes()
}
