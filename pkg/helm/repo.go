package helm

import (
	chart "helm.sh/helm/v4/pkg/chart/v2"
)

// ChartLoadResult holds a chart, its location within the source, and its content fingerprint.
// Fingerprint is populated when template caching or valuesFiles requires it.
type ChartLoadResult struct {
	// Path labels the chart location; content is read only through SourceArtifact.FS.
	Path        string
	Chart       *chart.Chart
	Fingerprint string
}
