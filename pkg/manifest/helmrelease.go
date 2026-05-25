package manifest

import (
	"cmp"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"maps"
	"slices"

	helmv2 "github.com/fluxcd/helm-controller/api/v2"
)

// HelmChart is the embedded chart template inside a HelmRelease.spec.chart
// (or the resolved form of HelmRelease.spec.chartRef). It is NOT the
// stand-alone HelmChart CRD — see HelmChartSource for that.
type HelmChart struct {
	// Name is the chart name within the source repository.
	Name string `json:"name" yaml:"name"`
	// Version is omitted from output for tidy diffs but kept in memory.
	Version string `json:"-" yaml:"-"`
	// RepoName, RepoNamespace, RepoKind identify the sourceRef.
	RepoName      string `json:"repoName" yaml:"repoName"`
	RepoNamespace string `json:"repoNamespace" yaml:"repoNamespace"`
	RepoKind      string `json:"repoKind" yaml:"repoKind"`
}

// RepoFullName is "<namespace>-<name>" — the canonical id of the source.
func (h HelmChart) RepoFullName() string {
	return h.RepoNamespace + "-" + h.RepoName
}

// ChartName is "<repoFullName>/<chartName>" — used as the helm chart ref.
func (h HelmChart) ChartName() string {
	return h.RepoFullName() + "/" + h.Name
}

// chartFromHelmRelease projects the chart reference out of a typed
// HelmRelease spec, unifying spec.chartRef and spec.chart into one
// resolved shape. defaultNamespace fills in an omitted ref namespace.
func chartFromHelmRelease(spec *helmv2.HelmReleaseSpec, defaultNamespace string) (HelmChart, error) {
	if ref := spec.ChartRef; ref != nil {
		if ref.Kind == "" {
			return HelmChart{}, inputf("HelmRelease missing spec.chartRef.kind")
		}
		if ref.Name == "" {
			return HelmChart{}, inputf("HelmRelease missing spec.chartRef.name")
		}
		return HelmChart{
			Name:          ref.Name,
			RepoName:      ref.Name,
			RepoNamespace: cmp.Or(ref.Namespace, defaultNamespace),
			RepoKind:      ref.Kind,
		}, nil
	}
	tmpl := spec.Chart
	if tmpl == nil {
		return HelmChart{}, inputf("HelmRelease missing spec.chart or spec.chartRef")
	}
	chartName := tmpl.Spec.Chart
	if chartName == "" {
		return HelmChart{}, inputf("HelmRelease missing spec.chart.spec.chart")
	}
	srcName := tmpl.Spec.SourceRef.Name
	if srcName == "" {
		return HelmChart{}, inputf("HelmRelease missing spec.chart.spec.sourceRef.name")
	}
	return HelmChart{
		Name:          chartName,
		Version:       tmpl.Spec.Version,
		RepoName:      srcName,
		RepoNamespace: cmp.Or(tmpl.Spec.SourceRef.Namespace, defaultNamespace),
		RepoKind:      cmp.Or(tmpl.Spec.SourceRef.Kind, KindHelmRepository),
	}, nil
}

// helmChartFromSource constructs a HelmChart from a resolved
// HelmChartSource. The Repo* fields are read from the embedded
// SourceRef (which always shares the HelmChart's namespace per Flux
// schema).
func helmChartFromSource(src *HelmChartSource) HelmChart {
	return HelmChart{
		Name:          src.Chart,
		Version:       src.Version,
		RepoName:      src.SourceRef.Name,
		RepoNamespace: src.Namespace,
		RepoKind:      cmp.Or(src.SourceRef.Kind, KindHelmRepository),
	}
}

// HelmRelease is the Flux HelmRelease CRD. The embedded
// helmv2.HelmReleaseSpec promotes Suspend, TargetNamespace,
// ServiceAccountName, StorageNamespace, ReleaseName (as a field),
// Install, Upgrade, ValuesFrom, PostRenderers, KubeConfig,
// MaxHistory, etc. to the top level.
//
// Several flate-specific projection fields live alongside the
// embedded Spec:
//   - Chart is the union of spec.chart and spec.chartRef. Shadows the
//     embedded Spec.Chart (*HelmChartTemplate) — Spec.ChartRef is
//     untouched and still accessible if a consumer needs the raw ref.
//   - Values is the decoded form of spec.values (map[string]any) so
//     consumers don't have to JSON-decode on every access. Shadows
//     the embedded Spec.Values (*apiextensionsv1.JSON).
//   - DependsOn is normalized to []DependencyRef (with ReadyExpr).
//     Shadows the embedded Spec.DependsOn ([]DependencyReference).
//   - CRDsPolicy collapses spec.install.crds vs spec.upgrade.crds.
//   - DisableSchema/OpenAPIValidation collapse Install vs Upgrade.
//   - ChartValuesFiles + IgnoreMissingValuesFiles are sourced from
//     spec.chart.spec.valuesFiles OR the referenced HelmChart CRD.
type HelmRelease struct {
	Name      string `json:"name"      yaml:"name"`
	Namespace string `json:"namespace" yaml:"namespace"`

	helmv2.HelmReleaseSpec `json:",inline" yaml:",inline"`

	// Chart is the resolved chart reference, unifying spec.chart and
	// spec.chartRef into one shape. Shadows the embedded Spec.Chart.
	Chart HelmChart `json:"chart" yaml:"chart"`

	// Values is the decoded form of spec.values. Shadows the embedded
	// Spec.Values (*apiextensionsv1.JSON).
	Values map[string]any `json:"-" yaml:"-"`

	// DependsOn is the normalized form of spec.dependsOn (carrying any
	// ReadyExpr). Shadows the embedded Spec.DependsOn ([]DependencyReference).
	DependsOn []DependencyRef `json:"-" yaml:"-"`

	Labels      map[string]string `json:"-" yaml:"-"`
	Annotations map[string]string `json:"-" yaml:"-"`

	// CRDsPolicy collapses spec.install.crds vs spec.upgrade.crds. One
	// of "" (chart's helm default), "Skip", "Create", "CreateReplace".
	// Upgrade wins over Install when both are set, matching
	// helm-controller's "upgrade-after-install" model.
	CRDsPolicy string `json:"-" yaml:"-"`

	// DisableSchemaValidation / DisableOpenAPIValidation collapse the
	// install vs upgrade equivalents into single flags.
	DisableSchemaValidation  bool `json:"-" yaml:"-"`
	DisableOpenAPIValidation bool `json:"-" yaml:"-"`

	// ChartValuesFiles are values files baked into the chart that
	// should be merged BEFORE the HR's own Values overrides.
	ChartValuesFiles         []string `json:"-" yaml:"-"`
	IgnoreMissingValuesFiles bool     `json:"-" yaml:"-"`
}

// Named identifies the release.
func (h *HelmRelease) Named() NamedResource {
	return NamedResource{Kind: KindHelmRelease, Namespace: h.Namespace, Name: h.Name}
}

// Clone returns a copy of h safe for in-place mutation during a single
// reconcile pass. Deep-copies every mutable reference field —
// reconcile bodies, prepare passes, and orchestrator stamping all
// observe the same canonical store-owned object across goroutines,
// so a partial clone is a footgun the moment any of those grow a
// new mutation. Cheap: typical HR has <10 labels/annotations and
// short DependsOn / ChartValuesFiles.
func (h *HelmRelease) Clone() *HelmRelease {
	out := *h
	out.Values = DeepCopyMap(h.Values)
	out.ChartValuesFiles = slices.Clone(h.ChartValuesFiles)
	out.DependsOn = slices.Clone(h.DependsOn)
	out.Labels = maps.Clone(h.Labels)
	out.Annotations = maps.Clone(h.Annotations)
	return &out
}

// GetLabels returns the HelmRelease's metadata.labels. Implements the
// shared accessor pkg/depwait projectObject uses for CEL exposure.
func (h *HelmRelease) GetLabels() map[string]string { return h.Labels }

// GetAnnotations returns the HelmRelease's metadata.annotations.
func (h *HelmRelease) GetAnnotations() map[string]string { return h.Annotations }

// ReleaseName returns the resolved Helm release name — spec.releaseName
// when set, otherwise metadata.name — passed through release.ShortenName
// so HRs whose effective name is >53 characters get the same hash-
// suffixed shortened form a real cluster sees. Mirrors helm-controller's
// internal/release.ShortenName (a 40-char prefix + "-" + 12 hex chars of
// sha256(name)). Without shortening, charts referencing `.Release.Name`
// rendered different resource names/labels in flate vs cluster.
// Always non-empty.
func (h *HelmRelease) ReleaseName() string {
	return shortenReleaseName(cmp.Or(h.HelmReleaseSpec.ReleaseName, h.Name))
}

// shortenReleaseName mirrors helm-controller/internal/release.ShortenName.
// Inlined to keep pkg/manifest free of a helm-controller import (the
// canonical impl lives in an internal/ tree we cannot reference).
func shortenReleaseName(name string) string {
	if len(name) <= 53 {
		return name
	}
	const maxLength = 53
	const shortHashLength = 12
	sum := fmt.Sprintf("%x", sha256.Sum256([]byte(name)))
	return name[:maxLength-(shortHashLength+1)] + "-" + sum[:shortHashLength]
}

// ReleaseNamespace returns TargetNamespace when set, otherwise Namespace.
func (h *HelmRelease) ReleaseNamespace() string {
	return cmp.Or(h.TargetNamespace, h.Namespace)
}

// NamespacedName is "<namespace>/<name>".
func (h *HelmRelease) NamespacedName() string { return h.Namespace + "/" + h.Name }

// HelmChartLookup returns the HelmChartSource at (namespace, name) or
// nil when not present. The shape ResolveChartRef accepts so callers
// can pass either a SourceResolver method (orchestrator path) or a
// custom lookup (embedders with their own source registry).
type HelmChartLookup func(namespace, name string) *HelmChartSource

// ResolveChartRef replaces a chartRef placeholder with the resolved
// source. When the chartRef resolves to a HelmChart CRD, its
// spec.valuesFiles + spec.ignoreMissingValuesFiles propagate onto the
// HelmRelease so the rendering pipeline can merge them ahead of
// HR.Values.
//
// The chart rewrite is unconditional once we find the source — even
// when the HelmChart CRD has no spec.version, we need RepoKind to flip
// from KindHelmChart to the underlying real source (HelmRepository /
// OCIRepository / GitRepository) so downstream chart loading can
// dispatch. Without it, LocateChart would see RepoKind=HelmChart and
// fall through with an "unsupported chart repo kind" error.
func (h *HelmRelease) ResolveChartRef(lookup HelmChartLookup) error {
	if h.Chart.RepoKind != KindHelmChart || h.Chart.Version != "" {
		return nil
	}
	src := lookup(h.Chart.RepoNamespace, h.Chart.RepoName)
	if src == nil {
		return fmt.Errorf("%w: HelmChartSource %s not found for HelmRelease %s",
			ErrObjectNotFound, h.Chart.RepoFullName(), h.NamespacedName())
	}
	h.Chart = helmChartFromSource(src)
	if len(src.ValuesFiles) > 0 {
		h.ChartValuesFiles = src.ValuesFiles
		h.IgnoreMissingValuesFiles = src.IgnoreMissingValuesFiles
	}
	return nil
}

// parseHelmRelease decodes a HelmRelease CR via the helm-controller
// typed schema (helm-controller/api/v2). The chart vs chartRef
// normalization is preserved by chartFromHelmRelease.
func parseHelmRelease(doc map[string]any) (*HelmRelease, error) {
	if err := checkAPIVersion(doc, HelmReleaseDomain); err != nil {
		return nil, err
	}
	var cr helmv2.HelmRelease
	if err := decodeTyped(doc, &cr); err != nil {
		return nil, inputf("HelmRelease decode: %w", err)
	}
	if cr.Name == "" {
		return nil, inputf("HelmRelease missing metadata.name")
	}
	// metadata.namespace is optional — Flux Kustomizations commonly
	// inject it via spec.targetNamespace. Treat the empty string as
	// "inherit later".
	chart, err := chartFromHelmRelease(&cr.Spec, cr.Namespace)
	if err != nil {
		return nil, err
	}
	var values map[string]any
	if cr.Spec.Values != nil && len(cr.Spec.Values.Raw) > 0 {
		if err := json.Unmarshal(cr.Spec.Values.Raw, &values); err != nil {
			return nil, inputf("HelmRelease spec.values: %w", err)
		}
	}
	disableSchema := (cr.Spec.Install != nil && cr.Spec.Install.DisableSchemaValidation) ||
		(cr.Spec.Upgrade != nil && cr.Spec.Upgrade.DisableSchemaValidation)
	disableOpenAPI := (cr.Spec.Install != nil && cr.Spec.Install.DisableOpenAPIValidation) ||
		(cr.Spec.Upgrade != nil && cr.Spec.Upgrade.DisableOpenAPIValidation)
	// Upgrade.CRDs wins over Install.CRDs — helm-controller's
	// upgrade-after-install model means the upgrade policy is what
	// the running cluster sees once any release is past its first
	// install.
	crdsPolicy := ""
	if cr.Spec.Install != nil {
		crdsPolicy = string(cr.Spec.Install.CRDs)
	}
	if cr.Spec.Upgrade != nil && cr.Spec.Upgrade.CRDs != "" {
		crdsPolicy = string(cr.Spec.Upgrade.CRDs)
	}
	var dependsOn []DependencyRef
	for _, dep := range cr.Spec.DependsOn {
		if dep.Name == "" {
			return nil, inputf("HelmRelease missing dependsOn.name")
		}
		dependsOn = append(dependsOn, DependencyRef{
			NamedResource: NamedResource{Kind: KindHelmRelease, Namespace: cmp.Or(dep.Namespace, cr.Namespace), Name: dep.Name},
			ReadyExpr:     dep.ReadyExpr,
		})
	}
	// spec.chart.spec.valuesFiles (inline template). spec.chartRef
	// case is handled later by ResolveChartRef once HelmChartSource is
	// available in the store.
	var chartValuesFiles []string
	var ignoreMissingValuesFiles bool
	if cr.Spec.Chart != nil {
		chartValuesFiles = cr.Spec.Chart.Spec.ValuesFiles
		ignoreMissingValuesFiles = cr.Spec.Chart.Spec.IgnoreMissingValuesFiles
	}

	return &HelmRelease{
		Name:                     cr.Name,
		Namespace:                cr.Namespace,
		HelmReleaseSpec:          cr.Spec,
		Chart:                    chart,
		Values:                   values,
		DependsOn:                dependsOn,
		Labels:                   cr.Labels,
		Annotations:              cr.Annotations,
		DisableSchemaValidation:  disableSchema,
		DisableOpenAPIValidation: disableOpenAPI,
		ChartValuesFiles:         chartValuesFiles,
		IgnoreMissingValuesFiles: ignoreMissingValuesFiles,
		CRDsPolicy:               crdsPolicy,
	}, nil
}
