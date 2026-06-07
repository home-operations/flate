package helm

import (
	"fmt"
	"os"
	"path/filepath"

	chart "helm.sh/helm/v4/pkg/chart/v2"

	"github.com/home-operations/flate/pkg/manifest"
)

// ChartLoadResult is the loaded chart plus the on-disk path it came from.
//
// Fingerprint, when non-empty, is a content-addressed sha256 hex of
// the chart's loader.Load inputs (Metadata + Templates + Files +
// Schema + chart defaults + subchart contents). Computed lazily by
// LoadChart so the template-output cache can build a stable key
// without re-walking the chart on every render. Memoized per
// (Client, path) keyed by the same (mtime, size) fingerprint
// chartCacheEntry uses, so a mutable OCI re-push invalidates this
// digest just as it invalidates the cached *chart.Chart pointer.
// Empty when the template cache is disabled.
type ChartLoadResult struct {
	Path        string
	Chart       *chart.Chart
	Fingerprint string
}

// locateLocalChart resolves a chart whose source is a fetched on-disk
// artifact — GitRepository, Bucket, or ExternalArtifact. The chart
// lives at <artifact.LocalPath>/<chart.Name> in every case.
func (c *Client) locateLocalChart(hr *manifest.HelmRelease) (string, error) {
	art := c.resolveLocalSource(hr)
	if art == nil {
		return "", fmt.Errorf("%w: %s %s not available for HelmRelease %s",
			manifest.ErrObjectNotFound, hr.Chart.RepoKind, hr.Chart.RepoFullName(), hr.Named().NamespacedName())
	}
	path := filepath.Join(art.LocalPath, hr.Chart.Name)
	if _, err := os.Stat(filepath.Join(path, chartYamlFilename)); err != nil {
		return "", fmt.Errorf("chart not found at %s: %w", path, err)
	}
	return path, nil
}
