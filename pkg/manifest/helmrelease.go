package manifest

import (
	"cmp"
	"encoding/json"
	"maps"
	"path"
	"reflect"
	"slices"
	"strings"

	helmv2 "github.com/fluxcd/helm-controller/api/v2"
	"github.com/fluxcd/pkg/apis/kustomize"
	"k8s.io/apimachinery/pkg/runtime"
)

// HelmChart is the embedded chart template inside a HelmRelease.spec.chart
// (or the resolved form of HelmRelease.spec.chartRef). It is NOT the
// stand-alone HelmChart CRD — see HelmChartSource for that.
type HelmChart struct {
	// Name is the chart name within the source repository.
	Name string `json:"name" yaml:"name"`
	// Version is omitted from output for tidy diffs but kept in memory.
	Version string        `json:"-" yaml:"-"`
	Source  NamedResource `json:"source" yaml:"source"`
}

// ChartLocation validates the source kind and selects a source path or automatic archive input.
func (h HelmChart) ChartLocation() (string, bool, error) {
	if h.hasSourceChartPath() {
		return path.Clean(h.Name), false, nil
	}
	switch h.Source.Kind {
	case KindOCIRepository, KindHelmChart:
		return "", true, nil
	case KindHelmRepository, "":
		return "", false, inputf("HelmRepository chart %s reached chart loading unmaterialized: render via the orchestrator", h.RepoFullName())
	default:
		return "", false, inputf("unsupported chart repo kind %s", h.Source.Kind)
	}
}

func (h HelmChart) hasSourceChartPath() bool {
	return h.Source.Kind == KindGitRepository || h.Source.Kind == KindBucket || h.Source.Kind == KindExternalArtifact
}

// UsesSourceValuesFiles reports whether values files are relative to the source artifact root.
func (h HelmChart) UsesSourceValuesFiles() bool {
	return h.Source.Kind == KindGitRepository || h.Source.Kind == KindBucket
}

func (h *HelmChart) sourceInputs(files []string) (string, []string) {
	if h.Source.Group != SourceDomain {
		return "", nil
	}
	if !h.UsesSourceValuesFiles() {
		files = nil
	}
	if !h.hasSourceChartPath() || h.Name == "" || strings.HasPrefix(h.Name, "/") || strings.IndexByte(h.Name, 0) >= 0 {
		return "", files
	}
	// Reference iteration MUST validate without allocating a normalized path.
	depth := 0
	for part := range strings.SplitSeq(h.Name, "/") {
		switch part {
		case "", ".":
		case "..":
			depth--
		default:
			depth++
		}
		if depth < 0 {
			return "", files
		}
	}
	return h.Name, files
}

// RepoFullName returns the source name prefixed by its namespace when present.
func (h HelmChart) RepoFullName() string {
	if h.Source.Namespace == "" {
		return h.Source.Name
	}
	return h.Source.Namespace + "-" + h.Source.Name
}

// ChartName is "<repoFullName>/<chartName>" — used as the helm chart ref.
func (h HelmChart) ChartName() string {
	return h.RepoFullName() + "/" + h.Name
}

func helmChartFromSource(src *HelmChartSource) HelmChart {
	return HelmChart{
		Name: src.Chart, Version: src.Version,
		Source: NamedResource{
			Group: referenceGroup(cmp.Or(src.SourceRef.Kind, KindHelmRepository), src.SourceRef.APIVersion),
			Kind:  cmp.Or(src.SourceRef.Kind, KindHelmRepository), Namespace: src.Namespace, Name: src.SourceRef.Name,
		},
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
//   - DisableSchemaValidation projects install policy.
//   - ChartValuesFiles + IgnoreMissingValuesFiles are sourced from
//     spec.chart.spec.valuesFiles OR the referenced HelmChart CRD.
type HelmRelease struct {
	Name      string `json:"name"      yaml:"name"`
	Namespace string `json:"namespace" yaml:"namespace"`

	helmv2.HelmReleaseSpec `json:",inline" yaml:",inline"`

	// Chart is the resolved chart reference, unifying spec.chart and
	// spec.chartRef into one shape. Shadows the embedded Spec.Chart.
	Chart            HelmChart `json:"chart" yaml:"chart"`
	source           NamedResource
	chartDeclaration *HelmChartSource

	// Values is the decoded form of spec.values. Shadows the embedded
	// Spec.Values (*apiextensionsv1.JSON).
	Values map[string]any `json:"-" yaml:"-"`

	Labels      map[string]string `json:"-" yaml:"-"`
	Annotations map[string]string `json:"-" yaml:"-"`

	// DisableSchemaValidation projects install policy.
	DisableSchemaValidation bool `json:"-" yaml:"-"`

	// ChartValuesFiles are selected source or chart files merged before release values.
	ChartValuesFiles         []string `json:"-" yaml:"-"`
	IgnoreMissingValuesFiles bool     `json:"-" yaml:"-"`
}

func (h *HelmRelease) equal(other *HelmRelease) bool {
	if h == nil || other == nil {
		return h == other
	}
	return h.Name == other.Name && h.Namespace == other.Namespace && h.Chart == other.Chart &&
		h.DisableSchemaValidation == other.DisableSchemaValidation && h.IgnoreMissingValuesFiles == other.IgnoreMissingValuesFiles &&
		maps.Equal(h.Labels, other.Labels) && maps.Equal(h.Annotations, other.Annotations) && slices.Equal(h.ChartValuesFiles, other.ChartValuesFiles) &&
		reflect.DeepEqual(&h.HelmReleaseSpec, &other.HelmReleaseSpec) && reflect.DeepEqual(h.Values, other.Values)
}

// Suspended reports whether reconciliation is paused.
func (h *HelmRelease) Suspended() bool { return h.Suspend }

// Named identifies the release.
func (h *HelmRelease) Named() NamedResource {
	return NamedResource{Group: kindTable[kindHelmRelease].Group, Kind: KindHelmRelease, Namespace: h.Namespace, Name: h.Name}
}

// WithNamespace returns a shallow namespace copy; borrowed payloads MUST remain immutable.
func (h *HelmRelease) WithNamespace(namespace string) BaseManifest {
	out := *h
	out.Namespace = namespace
	_ = out.normalizeChartSource()
	return &out
}

// ProjectionCompatible reports whether an eligible previous projection can be reused.
func (h *HelmRelease) ProjectionCompatible(BaseManifest) bool { return true }

// TracksDigest reports whether the declared chart source uses OCI revision identity.
func (h *HelmRelease) TracksDigest() bool {
	source := h.chartProjection().declared
	return source.Group == SourceDomain && source.Kind == KindOCIRepository
}

// Clone deep-copies mutable fields; store-owned specs MUST remain immutable.
func (h *HelmRelease) Clone() *HelmRelease {
	out := *h
	out.source = NamedResource{}
	out.chartDeclaration = nil
	h.DeepCopyInto(&out.HelmReleaseSpec)
	out.Values = DeepCopyMap(h.Values)
	out.ChartValuesFiles = slices.Clone(h.ChartValuesFiles)
	out.Labels = maps.Clone(h.Labels)
	out.Annotations = maps.Clone(h.Annotations)
	return &out
}

// GetLabels returns the HelmRelease's metadata.labels.
func (h *HelmRelease) GetLabels() map[string]string { return h.Labels }

// GetAnnotations returns the HelmRelease's metadata.annotations.
func (h *HelmRelease) GetAnnotations() map[string]string { return h.Annotations }

// RenderDocument derives a Flux desired document; callers MUST keep it immutable.
func (h *HelmRelease) RenderDocument() (map[string]any, error) {
	doc, err := runtime.DefaultUnstructuredConverter.ToUnstructured(&helmv2.HelmRelease{
		APIVersion: HelmReleaseDomain + "/v2", Kind: KindHelmRelease,
		Name: h.Name, Namespace: h.Namespace, Labels: h.Labels, Annotations: h.Annotations,
		Spec: h.HelmReleaseSpec,
	})
	if err != nil {
		return nil, inputf("project HelmRelease spec: %w", err)
	}
	return doc, nil
}

// ReleaseName returns the upstream defaulted and shortened Helm release name.
func (h *HelmRelease) ReleaseName() string {
	release := helmv2.HelmRelease{Name: h.Name, Spec: h.HelmReleaseSpec}
	return shortenReleaseName(release.GetReleaseName())
}

// shortenReleaseName mirrors helm-controller/internal/release.ShortenName.
// Inlined to keep pkg/manifest free of a helm-controller import (the
// canonical impl lives in an internal/ tree we cannot reference).
func shortenReleaseName(name string) string {
	const (
		maxLength       = 53
		shortHashLength = 12
	)
	if len(name) <= maxLength {
		return name
	}
	return name[:maxLength-(shortHashLength+1)] + "-" + SHA256Hex([]byte(name))[:shortHashLength]
}

// ReleaseNamespace returns TargetNamespace when set, otherwise Namespace.
func (h *HelmRelease) ReleaseNamespace() string {
	return cmp.Or(h.TargetNamespace, h.Namespace)
}

// ChartSource returns the effective source reference used by the chart.
func (h *HelmRelease) ChartSource() NamedResource {
	if h.Chart.Source.Name != "" {
		return Resolve(h.Chart.Source, "", h.Namespace)
	}
	return h.chartProjection().declared
}

// HelmReleaseHistoryPolicy binds declaration inputs outside the raw render key to accepted history.
type HelmReleaseHistoryPolicy struct {
	DeclaredChart      NamedResource
	Template           *helmv2.HelmChartTemplate
	Suspend            bool
	PostRenderStrategy helmv2.PostRenderStrategy
	DependsOn          []helmv2.DependencyReference
	ValuesFrom         []helmv2.ValuesReference
	HealthCheckExprs   []kustomize.CustomHealthCheck
	Annotations        map[string]string
}

// HistoryPolicy returns a frozen projection of emitted-child admission inputs.
func (h *HelmRelease) HistoryPolicy() HelmReleaseHistoryPolicy {
	policy := HelmReleaseHistoryPolicy{Suspend: h.Suspend, PostRenderStrategy: h.PostRenderStrategy}
	policy.DeclaredChart = h.chartProjection().declared
	if template := h.HelmReleaseSpec.Chart; template != nil {
		policy.Template = template.DeepCopy()
	}
	if len(h.DependsOn) > 0 {
		policy.DependsOn = slices.Clone(h.DependsOn)
	}
	if len(h.ValuesFrom) > 0 {
		policy.ValuesFrom = slices.Clone(h.ValuesFrom)
	}
	if len(h.HealthCheckExprs) > 0 {
		policy.HealthCheckExprs = slices.Clone(h.HealthCheckExprs)
	}
	if h.CommonMetadata != nil && len(h.CommonMetadata.Annotations) > 0 {
		policy.Annotations = maps.Clone(h.CommonMetadata.Annotations)
	}
	return policy
}

// Equal reports whether two frozen policies admit the same accepted history.
func (p HelmReleaseHistoryPolicy) Equal(other HelmReleaseHistoryPolicy) bool {
	return p.DeclaredChart == other.DeclaredChart && equalChartTemplate(p.Template, other.Template) && p.Suspend == other.Suspend && p.PostRenderStrategy == other.PostRenderStrategy && slices.Equal(p.DependsOn, other.DependsOn) && slices.Equal(p.ValuesFrom, other.ValuesFrom) && slices.Equal(p.HealthCheckExprs, other.HealthCheckExprs) && maps.Equal(p.Annotations, other.Annotations)
}

func parseHelmRelease(doc map[string]any) (*HelmRelease, error) {
	var cr helmv2.HelmRelease
	if err := decodeCR(doc, &cr, "HelmRelease", HelmReleaseDomain); err != nil {
		return nil, err
	}
	if err := validateChartTemplateDocument(doc, &cr.Spec); err != nil {
		return nil, inputf("HelmRelease %s/%s: %w", cr.Namespace, cr.Name, err)
	}
	var values map[string]any
	if cr.Spec.Values != nil && len(cr.Spec.Values.Raw) > 0 {
		if err := json.Unmarshal(cr.Spec.Values.Raw, &values); err != nil {
			return nil, inputf("HelmRelease spec.values: %w", err)
		}
	}
	for _, dep := range cr.Spec.DependsOn {
		if dep.Name == "" {
			return nil, inputf("HelmRelease missing dependsOn.name")
		}
	}
	// spec.chart.spec.valuesFiles (inline template). spec.chartRef
	// case is handled later by ResolveChartSource once HelmChartSource is
	// available in the store.
	var chartValuesFiles []string
	var ignoreMissingValuesFiles bool
	if cr.Spec.Chart != nil {
		chartValuesFiles = cr.Spec.Chart.Spec.ValuesFiles
		ignoreMissingValuesFiles = cr.Spec.Chart.Spec.IgnoreMissingValuesFiles
	}

	out := &HelmRelease{
		Name:                     cr.Name,
		Namespace:                cr.Namespace,
		HelmReleaseSpec:          cr.Spec,
		Values:                   values,
		Labels:                   cr.Labels,
		Annotations:              cr.Annotations,
		DisableSchemaValidation:  cr.GetInstall().DisableSchemaValidation,
		ChartValuesFiles:         chartValuesFiles,
		IgnoreMissingValuesFiles: ignoreMissingValuesFiles,
	}
	if err := out.normalizeChartSource(); err != nil {
		return nil, err
	}
	return out, nil
}

func (h *HelmRelease) references(yield func(Reference) bool) {
	if h.HelmReleaseSpec.Chart != nil || h.ChartRef != nil {
		projection := h.chartProjection()
		source := h.Chart.Source
		if source.Name == "" {
			source = projection.declared
		} else {
			source = Resolve(source, "", h.Namespace)
		}
		if declared := projection.declared; declared.Name != "" {
			stage := GateStage(0)
			artifact := source == declared
			if artifact {
				stage = GateArtifacts
			}
			if h.ChartRef != nil && declared.Group == SourceDomain && declared.Kind == KindHelmChart {
				stage |= GateInputs
			}
			namespaceDefault := h.HelmReleaseSpec.Chart != nil
			if h.ChartRef != nil {
				namespaceDefault = h.ChartRef.Namespace == ""
			} else if h.HelmReleaseSpec.Chart != nil && h.HelmReleaseSpec.Chart.Spec.SourceRef != nil {
				namespaceDefault = h.HelmReleaseSpec.Chart.Spec.SourceRef.Namespace == ""
			}
			if !yield(Reference{NamespaceDefault: namespaceDefault, NamedResource: declared, Use: "chart", Content: true, Gate: stage, Artifact: artifact, ChartSource: artifact, ArtifactStages: GateArtifacts}) {
				return
			}
		}
		if backing := projection.backing; backing.Source.Name != "" {
			path, files := backing.sourceInputs(h.ChartValuesFiles)
			artifact := source == backing.Source
			stage := GateStage(0)
			if artifact {
				stage = GateArtifacts
			}
			if !yield(Reference{NamespaceDefault: h.Chart.Source.Namespace == "", NamedResource: backing.Source, Use: "source", Content: true, Files: files, ChartPath: path, Gate: stage, Artifact: artifact, ChartSource: artifact, ArtifactStages: GateArtifacts}) {
				return
			}
		}
		if !credentialReferences(yield, h.Namespace, projection.credentials[:]...) {
			return
		}
	}

	for i := range h.DependsOn {
		dep := &h.DependsOn[i]
		if !yield(Reference{
			NamedResource: Resolve(NamedResource{Group: kindTable[kindHelmRelease].Group, Kind: KindHelmRelease, Namespace: dep.Namespace, Name: dep.Name}, "", h.Namespace),
			Use:           "dependsOn", Position: i, ReadyExpr: referenceExpression(&dep.ReadyExpr), Gate: GateInputs, Ordering: true, NamespaceDefault: dep.Namespace == "",
		}) {
			return
		}
	}
	for i := range h.ValuesFrom {
		ref := &h.ValuesFrom[i]
		if !yield(Reference{
			Kind: ref.Kind, Name: ref.Name, Namespace: h.Namespace,
			Use: "valuesFrom", Position: i, Optional: ref.Optional, Content: true, NamespaceDefault: true, Key: ref.GetValuesKey(),
		}) {
			return
		}
	}
}

func (h *HelmRelease) kind() KindInfo { return kindTable[kindHelmRelease].KindInfo }

// Content returns effective render inputs; emission metadata does not affect replay.
func (h *HelmRelease) Content(source any, omitted []NamedResource) any {
	return struct {
		Omitted                  []NamedResource
		Source                   any
		ReleaseName              string
		ReleaseNamespace         string
		Chart                    HelmChart
		Values                   map[string]any
		Spec                     helmv2.HelmReleaseSpec
		ChartValuesFiles         []string
		IgnoreMissingValuesFiles bool
		DisableSchemaValidation  bool
	}{
		Omitted:                  omitted,
		Source:                   source,
		ReleaseName:              h.ReleaseName(),
		ReleaseNamespace:         h.ReleaseNamespace(),
		Chart:                    h.Chart,
		Values:                   h.Values,
		Spec:                     h.HelmReleaseSpec,
		ChartValuesFiles:         h.ChartValuesFiles,
		IgnoreMissingValuesFiles: h.IgnoreMissingValuesFiles,
		DisableSchemaValidation:  h.DisableSchemaValidation,
	}
}

func (*HelmRelease) outputFields() string {
	return `interval timeout maxHistory persistentClient
 dependsOn.name dependsOn.namespace dependsOn.readyExpr
 driftDetection.mode driftDetection.ignore.paths.* driftDetection.ignore.target.group driftDetection.ignore.target.version driftDetection.ignore.target.kind driftDetection.ignore.target.namespace driftDetection.ignore.target.name driftDetection.ignore.target.annotationSelector driftDetection.ignore.target.labelSelector
install.timeout install.strategy.name install.strategy.retryInterval install.remediation.retries install.remediation.ignoreTestFailures install.remediation.remediateLastFailure install.disableWait install.disableWaitForJobs
upgrade.timeout upgrade.strategy.name upgrade.strategy.retryInterval upgrade.remediation.retries upgrade.remediation.ignoreTestFailures upgrade.remediation.remediateLastFailure upgrade.remediation.strategy upgrade.disableWait upgrade.disableWaitForJobs upgrade.cleanupOnFail
rollback.timeout rollback.disableWait rollback.disableWaitForJobs rollback.disableHooks rollback.recreate rollback.force rollback.cleanupOnFail rollback.serverSideApply
uninstall.timeout uninstall.disableHooks uninstall.keepHistory uninstall.disableWait uninstall.deletionPropagation`
}
