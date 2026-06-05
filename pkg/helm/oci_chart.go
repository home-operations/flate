package helm

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/home-operations/flate/pkg/manifest"
)

// locateOCIChart resolves a chart whose source is an OCIRepository. The
// source controller's oci.Fetcher has already pulled the artifact (applying
// spec.verify cosign verification, layerSelector, certSecretRef,
// proxySecretRef, insecure, ignore, and semver tag resolution) into a slot
// under the shared source cache; the HR depwait blocks render until that
// source is Ready, so the artifact is on disk by the time this runs. Reading
// it from the Store keeps every Flux OCIRepository feature working uniformly
// for both Kustomization and HelmRelease consumers — the same artifact-read
// shape as locateHelmChart / locateLocalChart. This also covers
// HelmRepository(type=oci) charts, which the HR controller repoints to a
// synthesized OCIRepository fetched the same way.
func (c *Client) locateOCIChart(hr *manifest.HelmRelease) (string, error) {
	r := c.resolveOCIRepo(hr)
	if r == nil {
		return "", fmt.Errorf("%w: OCIRepository %s not registered", manifest.ErrObjectNotFound, hr.Chart.RepoFullName())
	}
	art := c.resolveLocalSource(hr)
	if art == nil || art.LocalPath == "" {
		return "", fmt.Errorf("%w: OCIRepository %s/%s artifact not available for HelmRelease %s",
			manifest.ErrObjectNotFound, r.Namespace, r.Name, hr.Named().NamespacedName())
	}
	path, err := chartPathFromArtifact(art.LocalPath)
	if err != nil {
		return "", fmt.Errorf("OCIRepository %s/%s: %w", r.Namespace, r.Name, err)
	}
	return path, nil
}

// locateHelmChart resolves a chart whose source is a (synthesized) HelmChart.
// The source controller has already fetched the chart artifact into the Store
// — an HTTP tarball (chart.tgz) or an OCI slot — so this just reads it and
// resolves the loadable path. This is the authoritative path for every
// HelmRepository-backed chart (the HR controller repoints them here via
// materializeHelmChartSource).
func (c *Client) locateHelmChart(hr *manifest.HelmRelease) (string, error) {
	art := c.resolveLocalSource(hr)
	if art == nil || art.LocalPath == "" {
		return "", fmt.Errorf("%w: HelmChart %s not available for HelmRelease %s",
			manifest.ErrObjectNotFound, hr.Chart.RepoFullName(), hr.Named().NamespacedName())
	}
	path, err := chartPathFromArtifact(art.LocalPath)
	if err != nil {
		return "", fmt.Errorf("HelmChart %s: %w", hr.Chart.RepoFullName(), err)
	}
	return path, nil
}

// chartPathFromArtifact picks the right chart path under a chart
// SourceArtifact's slot, handling every layout the fetchers produce:
//
//  1. Chart.yaml at slot root — the rare shape where a chart-as-OCI
//     artifact is published WITHOUT helm's standard `<chartname>/`
//     wrapper directory. Slot itself is the chart root.
//  2. layer.tar.gz at slot root — operation=copy on an OCIRepository's
//     layerSelector. A packaged chart tgz; helm's loader.Load handles it.
//  3. chart.tgz at slot root — the HTTP HelmChart fetcher's downloaded
//     tarball.
//  4. <slot>/<chartname>/Chart.yaml — the common shape: `helm package`
//     emits tarballs with a single top-level directory named after
//     the chart, and operation=extract (Flux's default) preserves
//     that layout when unpacking. The chart name in the dir comes
//     from the artifact, NOT hr.Chart.Name (those can differ), so we
//     scan for the single subdir that contains a Chart.yaml.
//
// Probing the filesystem keeps this hr.Chart.Name-independent and
// works uniformly across vendor packaging styles and source kinds.
func chartPathFromArtifact(slot string) (string, error) {
	if _, err := os.Stat(filepath.Join(slot, chartYamlFilename)); err == nil {
		return slot, nil
	}
	for _, name := range []string{copiedOCILayerFilename, httpChartFilename} {
		p := filepath.Join(slot, name)
		if _, err := os.Stat(p); err == nil {
			return p, nil
		}
	}
	switch sub, status := findChartSubdir(slot); status {
	case chartSubdirFound:
		return sub, nil
	case chartSubdirAmbiguous:
		// More than one Chart.yaml-bearing subdir — distinct failure
		// from "no chart found", and the right hint is "this is a
		// bundle-of-charts artifact, not a single chart".
		return "", fmt.Errorf("chart artifact at %s contains multiple Chart.yaml-bearing subdirs; "+
			"flate cannot disambiguate a bundle-of-charts artifact", slot)
	}
	return "", fmt.Errorf("chart artifact at %s has none of %s, %s, %s, nor a <name>/Chart.yaml subdir — "+
		"chart layer missing or layerSelector misconfigured",
		slot, chartYamlFilename, copiedOCILayerFilename, httpChartFilename)
}

// chartSubdirStatus is the typed result of findChartSubdir. The
// caller branches between "not found" and "ambiguous" to surface
// distinct error messages — the operator hint is different.
type chartSubdirStatus int

const (
	chartSubdirNotFound chartSubdirStatus = iota
	chartSubdirFound
	chartSubdirAmbiguous
)

// findChartSubdir scans the immediate children of slot for one that
// contains a Chart.yaml — the shape produced by `helm package` when
// extracted via operation=extract. Hidden entries (anything starting
// with `.`) are skipped: this safely covers the .flate-* sentinels and
// any incidental dotfiles. Valid charts never use a dot-prefixed
// top-level directory.
//
// Returns ("", chartSubdirNotFound) when no subdir matches and
// ("", chartSubdirAmbiguous) when multiple match, so the caller can
// emit a specific error for each.
func findChartSubdir(slot string) (string, chartSubdirStatus) {
	entries, err := os.ReadDir(slot)
	if err != nil {
		return "", chartSubdirNotFound
	}
	var match string
	for _, e := range entries {
		if !e.IsDir() || strings.HasPrefix(e.Name(), ".") {
			continue
		}
		if _, err := os.Stat(filepath.Join(slot, e.Name(), chartYamlFilename)); err != nil {
			continue
		}
		if match != "" {
			return "", chartSubdirAmbiguous
		}
		match = filepath.Join(slot, e.Name())
	}
	if match == "" {
		return "", chartSubdirNotFound
	}
	return match, chartSubdirFound
}

// chartYamlFilename / copiedOCILayerFilename / httpChartFilename mirror, by
// string value, the on-disk names the fetchers write: source/oci.applyLayerSelector
// (Chart.yaml, layer.tar.gz) and the helmchart HTTP fetcher (chart.tgz, via
// Cache.PutBytes). Kept as constants here (and not imported across packages) to
// avoid a pkg/helm → pkg/source dependency for three static strings.
const (
	chartYamlFilename      = "Chart.yaml"
	copiedOCILayerFilename = "layer.tar.gz"
	httpChartFilename      = "chart.tgz"
)
