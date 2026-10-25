package manifest

import (
	"cmp"
	"time"

	"github.com/Masterminds/semver/v3"
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
	return NamedResource{Group: kindTable[kindHelmChartSource].Group, Kind: KindHelmChart, Namespace: h.Namespace, Name: h.Name}
}

// Suspended reports whether reconciliation is paused on this resource.
func (h *HelmChartSource) Suspended() bool { return h.Suspend }

// ExistenceOnly reports whether readiness requires no artifact acquisition.
func (h *HelmChartSource) ExistenceOnly() bool {
	return h.SourceRef.Kind != "" && h.SourceRef.Kind != KindHelmRepository
}

// SourceInterval returns the authored source refresh interval.
func (h *HelmChartSource) SourceInterval() time.Duration { return h.Interval.Duration }

// ChartVersion returns the source's declared chart version.
func (h *HelmChartSource) ChartVersion() string { return h.Version }

// Content returns the chart selection for HTTP acquisition.
func (h *HelmChartSource) Content() any {
	return struct {
		Chart   string
		Version string
	}{h.Chart, h.Version}
}

// WithNamespace returns a shallow namespace copy; borrowed payloads MUST remain immutable.
func (h *HelmChartSource) WithNamespace(namespace string) BaseManifest {
	out := *h
	out.Namespace = namespace
	return &out
}

// ProjectionCompatible reports whether an eligible previous projection can be reused.
func (h *HelmChartSource) ProjectionCompatible(BaseManifest) bool { return true }

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

func (h *HelmChartSource) references(yield func(Reference) bool) {
	kind := cmp.Or(h.SourceRef.Kind, KindHelmRepository)
	chart := helmChartFromSource(h)
	chartPath, files := chart.sourceInputs(h.ValuesFiles)
	yield(Reference{
		NamedResource: Resolve(NamedResource{Group: referenceGroup(kind, h.SourceRef.APIVersion), Kind: kind, Name: h.SourceRef.Name}, "", h.Namespace),
		Use:           "source", Content: h.SourceRef.Name != "", Files: files, ChartPath: chartPath,
	})
}

func (h *HelmChartSource) kind() KindInfo { return kindTable[kindHelmChartSource].KindInfo }

// ExactVersion reports whether the chart version is a concrete semver, including partial versions.
func (h *HelmChartSource) ExactVersion() bool {
	_, err := semver.NewVersion(h.Version)
	return err == nil
}
