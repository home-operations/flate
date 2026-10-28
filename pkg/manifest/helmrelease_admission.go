package manifest

import (
	"cmp"
	"regexp"
	"slices"
	"strings"
	"unicode/utf8"

	helmv2 "github.com/fluxcd/helm-controller/api/v2"
	sourcev1 "github.com/fluxcd/source-controller/api/v1"
)

var (
	templateInterval = regexp.MustCompile(`^([0-9]+(\.[0-9]+)?(ms|s|m|h))+$`)
	templateTimeout  = regexp.MustCompile(`^([0-9]+(\.[0-9]+)?(ms|s|m))+$`)
)

func normalizeChartTemplate(spec *helmv2.HelmReleaseSpec) error {
	if (spec.Chart == nil) == (spec.ChartRef == nil) {
		return inputf("exactly one of spec.chart or spec.chartRef must be set")
	}
	if ref := spec.ChartRef; ref != nil {
		if !slices.Contains([]string{KindHelmChart, KindOCIRepository, KindExternalArtifact}, ref.Kind) || ref.Name == "" || utf8.RuneCountInString(ref.Name) > 253 || utf8.RuneCountInString(ref.Namespace) > 63 {
			return inputf("invalid spec.chartRef kind, name or namespace")
		}
		return nil
	}
	template := spec.Chart
	kind := cmp.Or(template.Kind, KindHelmChart)
	if kind != KindHelmChart && kind != KindOCIRepository {
		return inputf("spec.chart.kind must be HelmChart or OCIRepository")
	}
	s := &template.Spec
	if kind == KindOCIRepository {
		if !strings.HasPrefix(s.URL, sourcev1.OCIRepositoryPrefix) {
			return inputf("spec.chart.spec.url must start with oci://")
		}
		if s.Chart != "" || s.SourceRef != nil || len(s.ValuesFiles) != 0 || s.IgnoreMissingValuesFiles || s.Version != "" && s.Version != "*" || s.ReconcileStrategy != "" && s.ReconcileStrategy != sourcev1.ReconcileStrategyChartVersion {
			return inputf("spec.chart.spec contains HelmChart-only fields for an OCIRepository template")
		}
	} else {
		if s.URL != "" {
			return inputf("spec.chart.spec.url requires spec.chart.kind OCIRepository")
		}
		if s.Chart == "" || utf8.RuneCountInString(s.Chart) > 2048 {
			return inputf("spec.chart.spec.chart must contain 1 to 2048 characters")
		}
		ref := s.SourceRef
		if ref == nil || !slices.Contains([]string{KindHelmRepository, KindGitRepository, KindBucket}, ref.Kind) || ref.Name == "" || utf8.RuneCountInString(ref.Name) > 253 || utf8.RuneCountInString(ref.Namespace) > 63 {
			return inputf("invalid spec.chart.spec.sourceRef kind, name or namespace")
		}
	}
	if s.ReconcileStrategy != "" && s.ReconcileStrategy != sourcev1.ReconcileStrategyChartVersion && s.ReconcileStrategy != sourcev1.ReconcileStrategyRevision {
		return inputf("invalid spec.chart.spec.reconcileStrategy")
	}
	if s.Provider != "" && !slices.Contains([]string{sourcev1.GenericOCIProvider, sourcev1.AmazonOCIProvider, sourcev1.AzureOCIProvider, sourcev1.GoogleOCIProvider}, s.Provider) {
		return inputf("invalid spec.chart.spec.provider")
	}
	if selector := s.LayerSelector; selector != nil && selector.Operation != "" && selector.Operation != sourcev1.OCILayerCopy && selector.Operation != sourcev1.OCILayerExtract {
		return inputf("invalid spec.chart.spec.layerSelector.operation")
	}
	if s.Verify != nil && s.Verify.Provider != "" && s.Verify.Provider != "cosign" && s.Verify.Provider != "notation" {
		return inputf("invalid spec.chart.spec.verify.provider")
	}
	if s.Interval != nil && s.Interval.Duration < 0 || s.Timeout != nil && s.Timeout.Duration < 0 {
		return inputf("spec.chart.spec interval and timeout cannot be negative")
	}
	return nil
}

func validateChartTemplateDocument(doc map[string]any, spec *helmv2.HelmReleaseSpec) error {
	if err := normalizeChartTemplate(spec); err != nil {
		return err
	}
	rawSpec, _ := doc["spec"].(map[string]any)
	if ref := spec.ChartRef; ref != nil {
		raw, _ := rawSpec["chartRef"].(map[string]any)
		if raw["namespace"] != nil && ref.Namespace == "" {
			return inputf("spec.chartRef.namespace cannot be empty")
		}
		return nil
	}
	rawTemplate, _ := rawSpec["chart"].(map[string]any)
	raw, _ := rawTemplate["spec"].(map[string]any)
	if raw == nil {
		return inputf("spec.chart.spec is required")
	}
	if rawTemplate["kind"] != nil && spec.Chart.Kind == "" {
		return inputf("spec.chart.kind cannot be empty")
	}
	if spec.Chart.Kind == KindOCIRepository {
		for _, key := range []string{"chart", "sourceRef", "valuesFiles", "ignoreMissingValuesFiles"} {
			if raw[key] != nil {
				return inputf("spec.chart.spec.%s cannot be set for OCIRepository", key)
			}
		}
		if raw["version"] != nil && raw["version"] != "*" {
			return inputf("spec.chart.spec.version must be omitted or * for OCIRepository")
		}
	} else if raw["url"] != nil {
		return inputf("spec.chart.spec.url requires spec.chart.kind OCIRepository")
	}
	for _, key := range []string{"provider", "reconcileStrategy"} {
		if raw[key] == "" {
			return inputf("spec.chart.spec.%s cannot be empty", key)
		}
	}
	for _, constraint := range []struct {
		field   string
		pattern *regexp.Regexp
	}{{"interval", templateInterval}, {"timeout", templateTimeout}} {
		if value, ok := raw[constraint.field].(string); ok && !constraint.pattern.MatchString(value) {
			return inputf("invalid spec.chart.spec.%s duration", constraint.field)
		}
	}
	if ref, _ := raw["sourceRef"].(map[string]any); ref != nil && ref["namespace"] != nil && ref["namespace"] == "" {
		return inputf("spec.chart.spec.sourceRef.namespace cannot be empty")
	}
	if selector, _ := raw["layerSelector"].(map[string]any); selector != nil && selector["operation"] == "" {
		return inputf("spec.chart.spec.layerSelector.operation cannot be empty")
	}
	for _, key := range []string{"secretRef", "certSecretRef", "proxySecretRef"} {
		if ref, _ := raw[key].(map[string]any); ref != nil && ref["name"] == nil {
			return inputf("spec.chart.spec.%s.name is required", key)
		}
	}
	if verify, _ := raw["verify"].(map[string]any); verify != nil {
		if verify["provider"] == "" {
			return inputf("spec.chart.spec.verify.provider cannot be empty")
		}
		for _, key := range []string{"secretRef", "trustedRootSecretRef"} {
			if ref, _ := verify[key].(map[string]any); ref != nil && ref["name"] == nil {
				return inputf("spec.chart.spec.verify.%s.name is required", key)
			}
		}
		identities, _ := verify["matchOIDCIdentity"].([]any)
		for _, identity := range identities {
			fields, _ := identity.(map[string]any)
			if fields["issuer"] == nil || fields["subject"] == nil {
				return inputf("spec.chart.spec.verify.matchOIDCIdentity requires issuer and subject")
			}
		}
	}
	return nil
}
