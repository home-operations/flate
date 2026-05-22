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

// HelmChartFromSource constructs a HelmChart from a resolved HelmChartSource.
func HelmChartFromSource(src *HelmChartSource) HelmChart {
	return HelmChart{
		Name:          src.Chart,
		Version:       src.Version,
		RepoName:      src.RepoName,
		RepoNamespace: src.RepoNamespace,
		RepoKind:      src.RepoKind,
	}
}

// HelmChartSource is the standalone HelmChart CRD
// (source.toolkit.fluxcd.io/v1 HelmChart).
type HelmChartSource struct {
	Name                     string   `json:"name" yaml:"name"`
	Namespace                string   `json:"namespace" yaml:"namespace"`
	Chart                    string   `json:"chart" yaml:"chart"`
	Version                  string   `json:"version,omitempty" yaml:"version,omitempty"`
	RepoName                 string   `json:"repoName" yaml:"repoName"`
	RepoNamespace            string   `json:"repoNamespace" yaml:"repoNamespace"`
	RepoKind                 string   `json:"repoKind" yaml:"repoKind"`
	Suspend                  bool     `json:"-" yaml:"-"`
	ValuesFiles              []string `json:"-" yaml:"-"`
	IgnoreMissingValuesFiles bool     `json:"-" yaml:"-"`
	// ReconcileStrategy is "ChartVersion" (default) or "Revision". Flate
	// does not re-trigger reconciles; this is parsed for round-tripping
	// fidelity so consumers can observe the upstream intent.
	ReconcileStrategy string `json:"-" yaml:"-"`
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
	repoKind := cr.Spec.SourceRef.Kind
	if repoKind == "" {
		repoKind = KindHelmRepository
	}
	return &HelmChartSource{
		Name:                     cr.Name,
		Namespace:                ns,
		Chart:                    cr.Spec.Chart,
		Version:                  cr.Spec.Version,
		RepoName:                 cr.Spec.SourceRef.Name,
		RepoNamespace:            ns,
		RepoKind:                 repoKind,
		Suspend:                  cr.Spec.Suspend,
		ValuesFiles:              cr.Spec.ValuesFiles,
		IgnoreMissingValuesFiles: cr.Spec.IgnoreMissingValuesFiles,
		ReconcileStrategy:        cr.Spec.ReconcileStrategy,
	}, nil
}

// HelmRelease is the Flux HelmRelease CRD.
type HelmRelease struct {
	Name                     string            `json:"name" yaml:"name"`
	Namespace                string            `json:"namespace" yaml:"namespace"`
	Chart                    HelmChart         `json:"chart" yaml:"chart"`
	// ReleaseNameOverride holds spec.releaseName when set. Empty means
	// helm-controller defaults to metadata.name. Use ReleaseName() to
	// resolve.
	ReleaseNameOverride      string            `json:"-" yaml:"-"`
	TargetNamespace          string            `json:"-" yaml:"-"`
	Values                   map[string]any    `json:"-" yaml:"-"`
	ValuesFrom               []ValuesReference `json:"-" yaml:"-"`
	Images                   []string          `json:"images,omitempty" yaml:"images,omitempty"`
	Labels                   map[string]string `json:"-" yaml:"-"`
	DependsOn                []DependencyRef   `json:"-" yaml:"-"`
	Suspend                  bool              `json:"-" yaml:"-"`
	DisableSchemaValidation  bool              `json:"-" yaml:"-"`
	DisableOpenAPIValidation bool              `json:"-" yaml:"-"`
	// CRDsPolicy mirrors spec.install.crds / spec.upgrade.crds. One of
	// "" (chart's helm default), "Skip", "Create", "CreateReplace".
	// "Skip" suppresses CRDs from the rendered output. "Create" and
	// "CreateReplace" both include CRDs — they differ only in
	// cluster-apply semantics which flate doesn't perform.
	// Upgrade wins over Install when both are set, matching
	// helm-controller's "upgrade-after-install" model.
	CRDsPolicy string `json:"-" yaml:"-"`
	// ServiceAccountName is the SA Flux's helm-controller impersonates
	// when applying the release. Flate renders offline so it has no
	// effect here, but the field is preserved for fidelity with the
	// upstream CR and so future render-time policy checks can observe it.
	ServiceAccountName string `json:"-" yaml:"-"`
	// ChartValuesFiles are values files baked into the chart that
	// should be merged BEFORE the HR's own Values overrides. Sourced
	// from either spec.chart.spec.valuesFiles (inline template) or the
	// referenced HelmChart CRD's spec.valuesFiles (when chartRef is
	// used; populated by ResolveChartRef).
	ChartValuesFiles []string `json:"-" yaml:"-"`
	// IgnoreMissingValuesFiles: when true, missing ChartValuesFiles
	// entries are skipped instead of erroring.
	IgnoreMissingValuesFiles bool `json:"-" yaml:"-"`
}

// Named identifies the release.
func (h *HelmRelease) Named() NamedResource {
	return NamedResource{Kind: KindHelmRelease, Namespace: h.Namespace, Name: h.Name}
}

// ReleaseName returns the resolved Helm release name — spec.releaseName
// when set, otherwise metadata.name. Mirrors helm-controller's behavior
// at template time. Always non-empty.
func (h *HelmRelease) ReleaseName() string {
	if h.ReleaseNameOverride != "" {
		return h.ReleaseNameOverride
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

// ResourceDependencies returns the resources whose readiness gates this
// HelmRelease's reconciliation: the release itself, its chart repo, and
// any valuesFrom references.
func (h *HelmRelease) ResourceDependencies() []NamedResource {
	deps := []NamedResource{h.Named()}
	deps = append(deps, NamedResource{Kind: h.Chart.RepoKind, Namespace: h.Chart.RepoNamespace, Name: h.Chart.RepoName})
	seen := make(map[string]struct{})
	for _, ref := range h.ValuesFrom {
		if _, ok := seen[ref.Name]; ok {
			continue
		}
		seen[ref.Name] = struct{}{}
		deps = append(deps, NamedResource{Kind: ref.Kind, Namespace: h.Namespace, Name: ref.Name})
	}
	return deps
}

// ResolveChartRef replaces a chartRef placeholder with the resolved source
// when version was not pinned. helmCharts is keyed by ResourceFullName.
// When the chartRef resolves to a HelmChart CRD, its spec.valuesFiles +
// spec.ignoreMissingValuesFiles propagate onto the HelmRelease so the
// rendering pipeline can merge them ahead of HR.Values.
func (h *HelmRelease) ResolveChartRef(helmCharts map[string]*HelmChartSource) error {
	if h.Chart.RepoKind != KindHelmChart || h.Chart.Version != "" {
		return nil
	}
	src, ok := helmCharts[h.Chart.RepoFullName()]
	if !ok {
		return fmt.Errorf("%w: HelmChartSource %s not found for HelmRelease %s",
			ErrObjectNotFound, h.Chart.RepoFullName(), h.NamespacedName())
	}
	if src.Version != "" {
		h.Chart = HelmChartFromSource(src)
	}
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

	return &HelmRelease{
		Name:                     cr.Name,
		Namespace:                cr.Namespace,
		Chart:                    chart,
		ReleaseNameOverride:      cr.Spec.ReleaseName,
		TargetNamespace:          cr.Spec.TargetNamespace,
		Values:                   values,
		ValuesFrom:               vfs,
		Labels:                   cr.Labels,
		DependsOn:                dependsOn,
		Suspend:                  cr.Spec.Suspend,
		DisableSchemaValidation:  disableSchema,
		DisableOpenAPIValidation: disableOpenAPI,
		ChartValuesFiles:         chartValuesFiles,
		IgnoreMissingValuesFiles: ignoreMissingValuesFiles,
		ServiceAccountName:       cr.Spec.ServiceAccountName,
		CRDsPolicy:               crdsPolicy,
	}, nil
}

// HelmRepository is the Flux HelmRepository CRD.
type HelmRepository struct {
	Name      string `json:"name" yaml:"name"`
	Namespace string `json:"namespace" yaml:"namespace"`
	URL       string `json:"url" yaml:"url"`
	// RepoType is "default" or "oci".
	RepoType        string                `json:"repoType,omitempty" yaml:"repoType,omitempty"`
	Provider        string                `json:"provider,omitempty" yaml:"provider,omitempty"`
	SecretRef       *LocalObjectReference `json:"secretRef,omitempty" yaml:"secretRef,omitempty"`
	CertSecretRef   *LocalObjectReference `json:"certSecretRef,omitempty" yaml:"certSecretRef,omitempty"`
	PassCredentials bool                  `json:"-" yaml:"-"`
	Insecure        bool                  `json:"-" yaml:"-"`
	Suspend         bool                  `json:"-" yaml:"-"`
}

// Named identifies the repo.
func (h *HelmRepository) Named() NamedResource {
	return NamedResource{Kind: KindHelmRepository, Namespace: h.Namespace, Name: h.Name}
}

// RepoName is "<namespace>-<name>".
func (h *HelmRepository) RepoName() string { return h.Namespace + "-" + h.Name }

// HelmChartName returns the chart ref used with the helm SDK. For OCI
// repos the chart name is appended to the URL.
func (h *HelmRepository) HelmChartName(chart HelmChart) string {
	if h.RepoType == RepoTypeOCI {
		return h.URL + "/" + chart.Name
	}
	return chart.ChartName()
}

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
	repoType := cr.Spec.Type
	if repoType == "" {
		repoType = RepoTypeDefault
	}
	out := &HelmRepository{
		Name:            cr.Name,
		Namespace:       cr.Namespace,
		URL:             cr.Spec.URL,
		RepoType:        repoType,
		Provider:        cr.Spec.Provider,
		PassCredentials: cr.Spec.PassCredentials,
		Insecure:        cr.Spec.Insecure,
		Suspend:         cr.Spec.Suspend,
	}
	if cr.Spec.SecretRef != nil && cr.Spec.SecretRef.Name != "" {
		out.SecretRef = cr.Spec.SecretRef
	}
	if cr.Spec.CertSecretRef != nil && cr.Spec.CertSecretRef.Name != "" {
		out.CertSecretRef = cr.Spec.CertSecretRef
	}
	return out, nil
}
