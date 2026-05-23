package manifest

import (
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

// ResourceFullName is "<namespace>-<name>".
func (h *HelmChartSource) ResourceFullName() string {
	return h.Namespace + "-" + h.Name
}

// ParseHelmChartSource decodes a standalone HelmChart CRD via the
// source-controller typed schema.
func ParseHelmChartSource(doc map[string]any) (*HelmChartSource, error) {
	if err := checkAPIVersion(doc, SourceDomain); err != nil {
		return nil, err
	}
	var cr sourcev1.HelmChart
	if err := decodeTyped(doc, &cr); err != nil {
		return nil, inputf("HelmChart decode: %w", err)
	}
	if cr.Name == "" {
		return nil, inputf("HelmChart missing metadata.name")
	}
	ns := cr.Namespace
	if ns == "" {
		ns = DefaultNamespace
	}
	if cr.Spec.Chart == "" {
		return nil, inputf("HelmChart missing spec.chart")
	}
	if cr.Spec.SourceRef.Name == "" {
		return nil, inputf("HelmChart missing spec.sourceRef.name")
	}
	if cr.Spec.SourceRef.Kind == "" {
		cr.Spec.SourceRef.Kind = KindHelmRepository
	}
	return &HelmChartSource{
		Name:          cr.Name,
		Namespace:     ns,
		HelmChartSpec: cr.Spec,
	}, nil
}
