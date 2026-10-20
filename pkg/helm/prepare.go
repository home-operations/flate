package helm

import (
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/values"
)

// Prepare clones a HelmRelease, resolves its chartRef, and expands its values
// before Client.Template. Pass SourceResolver.HelmChart as lookup when an
// orchestrator is available. The supplied cache shares parsed valuesFrom data
// across renders; the returned release belongs to the caller.
func Prepare(hr *manifest.HelmRelease, lookup manifest.HelmChartLookup, provider values.Provider, cache *values.Cache) (*manifest.HelmRelease, error) {
	hr = hr.Clone()
	if source := hr.ChartSource(); source.Name != "" {
		hr.Chart.Source = source
	}
	if err := hr.ResolveChartRef(lookup); err != nil {
		return nil, err
	}
	if err := values.ExpandValueReferences(hr, provider, cache); err != nil {
		return nil, err
	}
	return hr, nil
}
