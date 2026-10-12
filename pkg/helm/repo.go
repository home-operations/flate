package helm

import (
	"fmt"
	"os"
	"path/filepath"

	chart "helm.sh/helm/v4/pkg/chart/v2"
	"helm.sh/helm/v4/pkg/chart/v2/util"

	"github.com/home-operations/flate/pkg/manifest"
)

// ChartLoadResult holds a chart, its location within the source, and its content fingerprint.
// Fingerprint is populated when template caching or valuesFiles requires it.
type ChartLoadResult struct {
	Path        string
	Chart       *chart.Chart
	Fingerprint string
}

// locateLocalChart resolves a chart whose source is a fetched on-disk
// artifact: GitRepository or ExternalArtifact. The chart
// lives at <artifact.LocalPath>/<chart.Name> in every case.
func (c *Client) locateLocalChart(hr *manifest.HelmRelease) (string, error) {
	art := c.resolveLocalSource(hr)
	if art == nil {
		return "", fmt.Errorf("%w: %s %s not available for HelmRelease %s",
			manifest.ErrObjectNotFound, hr.Chart.RepoKind, hr.Chart.RepoFullName(), hr.Named().NamespacedName())
	}
	path := filepath.Join(art.LocalPath, hr.Chart.Name)
	if _, err := os.Stat(filepath.Join(path, util.ChartfileName)); err != nil {
		return "", fmt.Errorf("chart not found at %s: %w", path, err)
	}
	return path, nil
}
