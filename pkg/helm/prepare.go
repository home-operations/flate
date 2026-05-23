package helm

import (
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/values"
)

// Prepare runs the standard pre-render dance for a HelmRelease so it
// is ready to feed into Client.Template / TemplateDocs:
//
//  1. Clone hr so subsequent mutations don't touch the store-canonical
//     copy (the immutability contract every flate controller honors).
//  2. Resolve hr.spec.chartRef via the supplied lookup. A chartRef
//     points at a Flux HelmChart CR — typically emitted by a parent
//     Kustomization render — and is rewritten into the concrete
//     hr.Chart projection. Pass SourceResolver.HelmChart when an
//     orchestrator is available, or a custom lookup otherwise.
//  3. Expand values / valuesFrom against the supplied provider so
//     hr.Values reflects the merged result a render would consume.
//
// Embedders rendering a single HelmRelease without standing up the
// orchestrator's HR controller call Prepare then TemplateDocs. The
// returned *HelmRelease is the cloned, resolved one — pass it to
// Template / TemplateDocs and discard once rendered.
func Prepare(hr *manifest.HelmRelease, lookup manifest.HelmChartLookup, provider values.Provider) (*manifest.HelmRelease, error) {
	hr = hr.Clone()
	if err := hr.ResolveChartRef(lookup); err != nil {
		return nil, err
	}
	if err := values.ExpandValueReferences(hr, provider); err != nil {
		return nil, err
	}
	return hr, nil
}
