package helm

import (
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/values"
)

// Prepare clones a HelmRelease, resolves its chartRef and expands its values.
func Prepare(
	hr *manifest.HelmRelease,
	lookup func(manifest.NamedResource) manifest.BaseManifest,
	provider values.Provider,
	cache *values.Cache,
) (*manifest.HelmRelease, error) {
	hr = hr.Clone()
	if err := hr.ResolveChartSource(lookup); err != nil {
		return nil, err
	}
	err := values.ExpandValueReferences(hr, provider, cache)
	if err != nil {
		return nil, err
	}
	return hr, nil
}
