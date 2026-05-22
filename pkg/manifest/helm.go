package manifest

import (
	"encoding/json"
	"fmt"
	"slices"

	helmv2 "github.com/fluxcd/helm-controller/api/v2"
	sourcev1 "github.com/fluxcd/source-controller/api/v1"
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
		ns := ref.Namespace
		if ns == "" {
			ns = defaultNamespace
		}
		return HelmChart{
			Name:          ref.Name,
			RepoName:      ref.Name,
			RepoNamespace: ns,
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
	srcNamespace := tmpl.Spec.SourceRef.Namespace
	if srcNamespace == "" {
		srcNamespace = defaultNamespace
	}
	repoKind := tmpl.Spec.SourceRef.Kind
	if repoKind == "" {
		repoKind = KindHelmRepository
	}
	return HelmChart{
		Name:          chartName,
		Version:       tmpl.Spec.Version,
		RepoName:      srcName,
		RepoNamespace: srcNamespace,
		RepoKind:      repoKind,
	}, nil
}

// HelmChartFromSource constructs a HelmChart from a resolved
// HelmChartSource. The Repo* fields are read from the embedded
// SourceRef (which always shares the HelmChart's namespace per Flux
// schema).
func HelmChartFromSource(src *HelmChartSource) HelmChart {
	kind := src.SourceRef.Kind
	if kind == "" {
		kind = KindHelmRepository
	}
	return HelmChart{
		Name:          src.Chart,
		Version:       src.Version,
		RepoName:      src.SourceRef.Name,
		RepoNamespace: src.Namespace,
		RepoKind:      kind,
	}
}

// HelmChartSource is the standalone HelmChart CRD
// (source.toolkit.fluxcd.io/v1 HelmChart). The embedded HelmChartSpec
// promotes Chart, Version, SourceRef, ValuesFiles,
// IgnoreMissingValuesFiles, ReconcileStrategy, Suspend, Verify to the
// top level for ergonomic access.
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
		return nil, inputf("HelmChart decode: %v", err)
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
		Name:           cr.Name,
		Namespace:      ns,
		HelmChartSpec:  cr.Spec,
	}, nil
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

	Images []string          `json:"images,omitempty" yaml:"images,omitempty"`
	Labels map[string]string `json:"-"                yaml:"-"`

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

// ReleaseName returns the resolved Helm release name — spec.releaseName
// when set, otherwise metadata.name. Mirrors helm-controller's behavior
// at template time. Always non-empty.
func (h *HelmRelease) ReleaseName() string {
	if h.HelmReleaseSpec.ReleaseName != "" {
		return h.HelmReleaseSpec.ReleaseName
	}
	return h.Name
}

// ReleaseNamespace returns TargetNamespace when set, otherwise Namespace.
func (h *HelmRelease) ReleaseNamespace() string {
	if h.TargetNamespace != "" {
		return h.TargetNamespace
	}
	return h.Namespace
}

// RepoName is the HelmRepository identifier (namespace-name).
func (h *HelmRelease) RepoName() string {
	return h.Chart.RepoNamespace + "-" + h.Chart.RepoName
}

// NamespacedName is "<namespace>/<name>".
func (h *HelmRelease) NamespacedName() string { return h.Namespace + "/" + h.Name }

// ResolveChartRef replaces a chartRef placeholder with the resolved
// source. helmCharts is keyed by ResourceFullName. When the chartRef
// resolves to a HelmChart CRD, its spec.valuesFiles +
// spec.ignoreMissingValuesFiles propagate onto the HelmRelease so the
// rendering pipeline can merge them ahead of HR.Values.
//
// The chart rewrite is unconditional once we find the source — even
// when the HelmChart CRD has no spec.version, we need RepoKind to flip
// from KindHelmChart to the underlying real source (HelmRepository /
// OCIRepository / GitRepository) so downstream chart loading can
// dispatch. Without it, LocateChart would see RepoKind=HelmChart and
// fall through with an "unsupported chart repo kind" error.
func (h *HelmRelease) ResolveChartRef(helmCharts map[string]*HelmChartSource) error {
	if h.Chart.RepoKind != KindHelmChart || h.Chart.Version != "" {
		return nil
	}
	src, ok := helmCharts[h.Chart.RepoFullName()]
	if !ok {
		return fmt.Errorf("%w: HelmChartSource %s not found for HelmRelease %s",
			ErrObjectNotFound, h.Chart.RepoFullName(), h.NamespacedName())
	}
	h.Chart = HelmChartFromSource(src)
	if len(src.ValuesFiles) > 0 {
		h.ChartValuesFiles = src.ValuesFiles
		h.IgnoreMissingValuesFiles = src.IgnoreMissingValuesFiles
	}
	return nil
}

// ParseHelmRelease decodes a HelmRelease CR via the helm-controller
// typed schema (helm-controller/api/v2). The chart vs chartRef
// normalization is preserved by chartFromHelmRelease.
func ParseHelmRelease(doc map[string]any) (*HelmRelease, error) {
	if err := checkAPIVersion(doc, HelmReleaseDomain); err != nil {
		return nil, err
	}
	var cr helmv2.HelmRelease
	if err := decodeTyped(doc, &cr); err != nil {
		return nil, inputf("HelmRelease decode: %v", err)
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
	vfs := slices.Clone(cr.Spec.ValuesFrom)
	var values map[string]any
	if cr.Spec.Values != nil && len(cr.Spec.Values.Raw) > 0 {
		if err := json.Unmarshal(cr.Spec.Values.Raw, &values); err != nil {
			return nil, inputf("HelmRelease spec.values: %v", err)
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
		depNS := dep.Namespace
		if depNS == "" {
			depNS = cr.Namespace
		}
		dependsOn = append(dependsOn, DependencyRef{
			NamedResource: NamedResource{Kind: KindHelmRelease, Namespace: depNS, Name: dep.Name},
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

	// Ensure ValuesFrom is a defensive clone — Spec is embedded by
	// value so this is the only slice that could escape via aliasing.
	cr.Spec.ValuesFrom = vfs
	cr.Spec.PostRenderers = slices.Clone(cr.Spec.PostRenderers)

	return &HelmRelease{
		Name:                     cr.Name,
		Namespace:                cr.Namespace,
		HelmReleaseSpec:          cr.Spec,
		Chart:                    chart,
		Values:                   values,
		DependsOn:                dependsOn,
		Labels:                   cr.Labels,
		DisableSchemaValidation:  disableSchema,
		DisableOpenAPIValidation: disableOpenAPI,
		ChartValuesFiles:         chartValuesFiles,
		IgnoreMissingValuesFiles: ignoreMissingValuesFiles,
		CRDsPolicy:               crdsPolicy,
	}, nil
}

// HelmRepository is the Flux HelmRepository CRD. The embedded
// sourcev1.HelmRepositorySpec promotes URL, Type, Provider, SecretRef,
// CertSecretRef, PassCredentials, Insecure, Suspend, etc. to the top
// level so consumers write h.URL / h.Type rather than h.Spec.URL.
type HelmRepository struct {
	Name      string `json:"name"      yaml:"name"`
	Namespace string `json:"namespace" yaml:"namespace"`

	sourcev1.HelmRepositorySpec `json:",inline" yaml:",inline"`
}

// Named identifies the repo.
func (h *HelmRepository) Named() NamedResource {
	return NamedResource{Kind: KindHelmRepository, Namespace: h.Namespace, Name: h.Name}
}

// RepoName is "<namespace>-<name>".
func (h *HelmRepository) RepoName() string { return h.Namespace + "-" + h.Name }

// ParseHelmRepository decodes a HelmRepository CR via the
// source-controller typed schema.
func ParseHelmRepository(doc map[string]any) (*HelmRepository, error) {
	if err := checkAPIVersion(doc, SourceDomain); err != nil {
		return nil, err
	}
	var cr sourcev1.HelmRepository
	if err := decodeTyped(doc, &cr); err != nil {
		return nil, inputf("HelmRepository decode: %v", err)
	}
	if cr.Name == "" {
		return nil, inputf("HelmRepository missing metadata.name")
	}
	if cr.Spec.URL == "" {
		return nil, inputf("HelmRepository missing spec.url")
	}
	if cr.Spec.Type == "" {
		cr.Spec.Type = RepoTypeDefault
	}
	return &HelmRepository{
		Name:               cr.Name,
		Namespace:          cr.Namespace,
		HelmRepositorySpec: cr.Spec,
	}, nil
}
