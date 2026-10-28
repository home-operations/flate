package helm

import (
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/values"
)

// Prepare clones a HelmRelease, resolves its chartRef, expands its values and
// returns required wiped values files omitted from expansion.
func Prepare(hr *manifest.HelmRelease, lookup func(manifest.NamedResource) manifest.BaseManifest, provider values.Provider, cache *values.Cache) (*manifest.HelmRelease, []manifest.NamedResource, error) {
	hr = hr.Clone()
	if err := hr.ResolveChartSource(lookup); err != nil {
		return nil, nil, err
	}
	omitted, err := values.ExpandValueReferences(hr, provider, cache)
	if err != nil {
		return nil, omitted, err
	}
	return hr, omitted, nil
}
