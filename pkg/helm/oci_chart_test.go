package helm

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"io"
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
// layerSelector.operation), locateOCIChart returns that slot directly
// instead of issuing a duplicate pull via helm's registry client.
//
// This is also what makes spec.verify (cosign), spec.layerSelector,
// spec.certSecretRef, etc. apply to Helm chart pulls — they all fire
// during the source.Fetcher.Fetch call, and now Helm consumes the
// already-verified, already-selected artifact instead of re-pulling
// over a separate code path.
func TestLocateOCIChart_PrefersSourceArtifactExtract(t *testing.T) {
	t.Parallel()

	slot := t.TempDir()
	writeChartFiles(t, slot, "mychart", "0.1.0")

	cli, hr := setupOCIChartTest(t, slot, "extracted")

	path, err := cli.locateOCIChart(t.Context(), hr)
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

	path, err := cli.locateOCIChart(t.Context(), hr)
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

// TestLocateOCIChart_FallsBackWhenNoArtifact covers the
// --enable-oci=false shape: source.ExistenceFetcher leaves the
// OCIRepository Ready with no SourceArtifact, so locateOCIChart must
// fall through to helm's registry client. We don't have a real
// registry here, but the fallback's first action is to try the
// helm-side cache file; we pre-populate it so the fallback returns
// success without a network roundtrip. The point of the test is that
// when no artifact is present, the fallback path runs (and does NOT
// error out with the artifact-missing message).
func TestLocateOCIChart_FallsBackWhenNoArtifact(t *testing.T) {
	t.Parallel()

	cli, err := NewClient(cacheroot.New(t.TempDir()))
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	st := store.New()
	const repoURL = "oci://ghcr.io/test/chart"
	repo := &manifest.OCIRepository{
		Name: "chart", Namespace: "flux-system",
		OCIRepositorySpec: sourcev1.OCIRepositorySpec{
			URL:       repoURL,
			Reference: &sourcev1.OCIRepositoryRef{Tag: "0.1.0"},
		},
	}
	st.AddObject(repo)
	// Intentionally NO SetArtifact — this is the --enable-oci=false shape.
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

	// Pre-populate the helm cache target so fetchOCIChart's cache-hit
	// branch returns without network. Target naming is
	// safeName(trimmedRef)+"-"+version+".tgz" — full ref (registry+
	// path), not just the basename, to avoid cross-registry collisions.
	cacheTarget := filepath.Join(cli.cacheDir, "ghcr.io-test-chart-0.1.0.tgz")
	testutil.WriteFileAt(t, cacheTarget, string(buildChartTarGz(t, "chart", "0.1.0")))

	path, err := cli.locateOCIChart(t.Context(), hr)
	if err != nil {
		t.Fatalf("locateOCIChart fallback: %v", err)
	}
	if path != cacheTarget {
		t.Errorf("path = %q, want fallback cache %q", path, cacheTarget)
	}
}

// TestLocateOCIChart_RoutesThroughPullerWhenWired pins the
// unification (4.4): when an OCIPuller is wired, locateOCIChart
// invokes it with the typed OCIRepository — applying spec.verify /
// certSecretRef / etc. — rather than falling back to the
// registry-client pull. We satisfy the puller with a stub that
// builds a slot containing an extracted chart, mirroring the shape
// source/oci.Fetcher's applyLayerSelector produces.
func TestLocateOCIChart_RoutesThroughPullerWhenWired(t *testing.T) {
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
	cli.SetSourceResolver(NewStoreSourceResolver(st))

	slot := t.TempDir()
	chartDir := filepath.Join(slot, "chart")
	testutil.WriteFileAt(t, filepath.Join(chartDir, "Chart.yaml"),
		"apiVersion: v2\nname: chart\nversion: 0.1.0\n")
	var pulledFor *manifest.OCIRepository
	cli.SetOCIPuller(stubPuller{
		fetch: func(_ context.Context, r *manifest.OCIRepository) (*store.SourceArtifact, error) {
			pulledFor = r
			return &store.SourceArtifact{
				Kind:      manifest.KindOCIRepository,
				URL:       r.URL,
				LocalPath: slot,
			}, nil
		},
	})

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

	path, err := cli.locateOCIChart(t.Context(), hr)
	if err != nil {
		t.Fatalf("locateOCIChart with puller: %v", err)
	}
	if path != chartDir {
		t.Errorf("path = %q, want chart subdir %q", path, chartDir)
	}
	if pulledFor == nil {
		t.Fatal("puller was not invoked")
	}
	if pulledFor.URL != repo.URL {
		t.Errorf("puller called with URL %q, want %q", pulledFor.URL, repo.URL)
	}
}

// stubPuller satisfies OCIPuller for tests.
type stubPuller struct {
	fetch func(context.Context, *manifest.OCIRepository) (*store.SourceArtifact, error)
}

func (s stubPuller) Fetch(ctx context.Context, r *manifest.OCIRepository) (*store.SourceArtifact, error) {
	return s.fetch(ctx, r)
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

	path, err := cli.locateOCIChart(t.Context(), hr)
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

	_, err := cli.locateOCIChart(t.Context(), hr)
	if err == nil {
		t.Fatal("expected error for ambiguous subdirs")
	}
	if !strings.Contains(err.Error(), "multiple") || !strings.Contains(err.Error(), "bundle-of-charts") {
		t.Errorf("error message should distinguish ambiguous case (mention 'multiple' + 'bundle-of-charts'); got: %v", err)
	}
}

// TestLocateOCIChart_FallbackSemverRefused covers the fallback's
// semver guard: spec.ref.semver requires the source.oci.Fetcher
// (which lists tags from the registry); the helm-side fallback has
// no way to resolve it, so we error early with an explicit message
// rather than letting helm's registry client return a cryptic
// "invalid tag" deeper down.
func TestLocateOCIChart_FallbackSemverRefused(t *testing.T) {
	t.Parallel()

	cli, err := NewClient(cacheroot.New(t.TempDir()))
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	st := store.New()
	repo := &manifest.OCIRepository{
		Name: "semver", Namespace: "flux-system",
		OCIRepositorySpec: sourcev1.OCIRepositorySpec{
			URL:       "oci://ghcr.io/test/chart",
			Reference: &sourcev1.OCIRepositoryRef{SemVer: ">=1.0.0"},
		},
	}
	st.AddObject(repo)
	// No SetArtifact — forces the fallback.
	cli.SetSourceResolver(NewStoreSourceResolver(st))

	hr := &manifest.HelmRelease{
		Name: "demo", Namespace: "default",
		Chart: manifest.HelmChart{
			Name: "chart", RepoName: "semver", RepoNamespace: "flux-system",
			RepoKind: manifest.KindOCIRepository,
		},
	}

	_, err = cli.locateOCIChart(t.Context(), hr)
	if err == nil {
		t.Fatal("expected error for semver in fallback mode")
	}
	if !strings.Contains(err.Error(), "semver") || !strings.Contains(err.Error(), "enable-oci") {
		t.Errorf("error should name the cause (semver) and the resolution (enable-oci); got: %v", err)
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
	_, err := ociChartPathFromArtifact(slot)
	if err == nil {
		t.Fatal("expected error for empty slot")
	}
	if !strings.Contains(err.Error(), "Chart.yaml") || !strings.Contains(err.Error(), "layerSelector") {
		t.Errorf("error message should name the missing shapes and hint at layerSelector; got: %v", err)
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
