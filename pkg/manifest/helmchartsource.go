package manifest

import (
	"cmp"

	sourcev1 "github.com/fluxcd/source-controller/api/v1"
)

// HelmChartSource is the Flux HelmChart CR — the standalone source-
// kind that emits a chart artifact a HelmRelease can reference via
// spec.chartRef. Distinct from the inline HelmChart projection that
// lives next to HelmRelease.
type HelmChartSource struct {
	Name      string `json:"name"      yaml:"name"`
	Namespace string `json:"namespace" yaml:"namespace"`

	sourcev1.HelmChartSpec `json:",inline" yaml:",inline"`
}

// Named identifies the chart resource.
func (h *HelmChartSource) Named() NamedResource {
	return NamedResource{Kind: KindHelmChart, Namespace: h.Namespace, Name: h.Name}
}

// parseHelmChartSource decodes a standalone HelmChart CRD via the
// source-controller typed schema.
func parseHelmChartSource(doc map[string]any) (*HelmChartSource, error) {
	var cr sourcev1.HelmChart
	if err := decodeCR(doc, &cr, "HelmChart", SourceDomain); err != nil {
		return nil, err
	}
	if cr.Spec.Chart == "" {
		return nil, inputf("HelmChart missing spec.chart")
	}
	if cr.Spec.SourceRef.Name == "" {
		return nil, inputf("HelmChart missing spec.sourceRef.name")
	}
	cr.Spec.SourceRef.Kind = cmp.Or(cr.Spec.SourceRef.Kind, KindHelmRepository)
	return &HelmChartSource{
		Name:          cr.Name,
		Namespace:     cr.Namespace,
		HelmChartSpec: cr.Spec,
	}, nil
}
