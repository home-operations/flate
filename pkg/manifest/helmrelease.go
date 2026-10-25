package manifest

import (
	"cmp"
	"encoding/json"
	"fmt"
	"maps"
	"path"
	"slices"
	"strings"

	helmv2 "github.com/fluxcd/helm-controller/api/v2"
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

// Materializes reports whether the chart requires HelmRepository acquisition.
func (h HelmChart) Materializes() bool {
	return h.Source.Group == SourceDomain && (h.Source.Kind == "" || h.Source.Kind == KindHelmRepository)
}

// ChartLocation validates the source kind and selects a source path or artifact discovery.
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

// RepoFullName returns the source namespace-name display label.
func (h HelmChart) RepoFullName() string {
	return h.Source.Namespace + "-" + h.Source.Name
}

// ChartName is "<repoFullName>/<chartName>" — used as the helm chart ref.
func (h HelmChart) ChartName() string {
	return h.RepoFullName() + "/" + h.Name
}

func chartFromHelmRelease(spec *helmv2.HelmReleaseSpec) (HelmChart, error) {
	if ref := spec.ChartRef; ref != nil {
		if ref.Kind == "" {
			return HelmChart{}, inputf("HelmRelease missing spec.chartRef.kind")
		}
		if ref.Name == "" {
			return HelmChart{}, inputf("HelmRelease missing spec.chartRef.name")
		}
		// A chartRef name identifies the source, not the chart inside it.
		// ResolveChartRef reads the chart name from a HelmChartSource;
		// other source kinds determine it from their artifact's layout.
		return HelmChart{Source: NamedResource{
			Group: referenceGroup(ref.Kind, ref.APIVersion), Kind: ref.Kind,
			Namespace: ref.Namespace, Name: ref.Name,
		}}, nil
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
		Name: chartName, Version: tmpl.Spec.Version,
		Source: NamedResource{
			Group: referenceGroup(cmp.Or(tmpl.Spec.SourceRef.Kind, KindHelmRepository), tmpl.Spec.SourceRef.APIVersion),
			Kind:  cmp.Or(tmpl.Spec.SourceRef.Kind, KindHelmRepository), Namespace: tmpl.Spec.SourceRef.Namespace, Name: srcName,
		},
	}, nil
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
//   - CRDsPolicy collapses spec.install.crds vs spec.upgrade.crds.
//   - DisableSchemaValidation projects install policy; OpenAPI combines both actions.
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

	Labels      map[string]string `json:"-" yaml:"-"`
	Annotations map[string]string `json:"-" yaml:"-"`

	// CRDsPolicy collapses spec.install.crds vs spec.upgrade.crds. One
	// of "" (chart's helm default), "Skip", "Create", "CreateReplace".
	// Upgrade wins over Install when both are set, matching
	// helm-controller's "upgrade-after-install" model.
	CRDsPolicy string `json:"-" yaml:"-"`

	// DisableSchemaValidation projects install policy. DisableOpenAPIValidation
	// combines install and upgrade policy.
	DisableSchemaValidation  bool `json:"-" yaml:"-"`
	DisableOpenAPIValidation bool `json:"-" yaml:"-"`

	// ChartValuesFiles are selected source or chart files merged before release values.
	ChartValuesFiles         []string `json:"-" yaml:"-"`
	IgnoreMissingValuesFiles bool     `json:"-" yaml:"-"`
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
	return &out
}

// ProjectionCompatible reports whether an eligible previous projection can be reused.
func (h *HelmRelease) ProjectionCompatible(BaseManifest) bool { return true }

// TracksDigest reports whether chartRef directly selects an OCIRepository chart identity.
func (h *HelmRelease) TracksDigest() bool {
	return h.ChartRef != nil && h.ChartRef.Kind == KindOCIRepository &&
		referenceGroup(h.ChartRef.Kind, h.ChartRef.APIVersion) == kindTable[kindOCIRepository].Group
}

// Clone deep-copies mutable fields; store-owned specs MUST remain immutable.
func (h *HelmRelease) Clone() *HelmRelease {
	out := *h
	h.DeepCopyInto(&out.HelmReleaseSpec)
	out.Values = DeepCopyMap(h.Values)
	out.ChartValuesFiles = slices.Clone(h.ChartValuesFiles)
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
	var source NamedResource
	for ref := range References(h, ReferenceRelations) {
		if ref.Use == "chart" || ref.Use == "source" {
			source = ref.NamedResource
		}
	}
	return source
}

// HelmChartLookup returns the HelmChartSource at the full identity, or nil when absent.
type HelmChartLookup func(NamedResource) *HelmChartSource

// ResolveChartRef replaces an unresolved HelmChart reference and propagates its values files.
// Versionless HelmCharts MUST resolve to their underlying source for acquisition.
func (h *HelmRelease) ResolveChartRef(lookup HelmChartLookup) error {
	if h.Chart.Source.Kind != KindHelmChart || h.Chart.Source.Group != kindTable[kindHelmChartSource].Group || h.Chart.Version != "" {
		return nil
	}
	id := h.ChartSource()
	src := lookup(id)
	if src == nil {
		return fmt.Errorf("%w: HelmChartSource %s not found for HelmRelease %s",
			ErrObjectNotFound, id.NamespacedName(), h.Named().NamespacedName())
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
	var cr helmv2.HelmRelease
	if err := decodeCR(doc, &cr, "HelmRelease", HelmReleaseDomain); err != nil {
		return nil, err
	}
	// Resolve ${VAR:=default} / ${VAR:-default} on valuesFrom refs,
	// mirroring the Kustomization spec.path / dependsOn precedent
	// (kustomization.go). A configMapGenerator emits a literal key
	// (e.g. biohazard.yaml); a valuesKey "${CLUSTER_NAME:=biohazard}.yaml"
	// must collapse to it so the lookup matches. Resolved here once so
	// both the value lookup and its cache key see the same string. Bare
	// ${VAR} (no default) is left for postBuild substitution.
	for i := range cr.Spec.ValuesFrom {
		cr.Spec.ValuesFrom[i].ValuesKey = resolveEnvsubstDefaults(cr.Spec.ValuesFrom[i].ValuesKey)
		cr.Spec.ValuesFrom[i].Name = resolveEnvsubstDefaults(cr.Spec.ValuesFrom[i].Name)
	}
	// metadata.namespace is optional — Flux Kustomizations commonly
	// inject it via spec.targetNamespace. Treat the empty string as
	// "inherit later".
	chart, err := chartFromHelmRelease(&cr.Spec)
	if err != nil {
		return nil, err
	}
	var values map[string]any
	if cr.Spec.Values != nil && len(cr.Spec.Values.Raw) > 0 {
		if err := json.Unmarshal(cr.Spec.Values.Raw, &values); err != nil {
			return nil, inputf("HelmRelease spec.values: %w", err)
		}
	}
	install, upgrade := cr.GetInstall(), cr.GetUpgrade()
	disableSchema := install.DisableSchemaValidation
	disableOpenAPI := install.DisableOpenAPIValidation || upgrade.DisableOpenAPIValidation
	// The upgrade policy takes precedence after the first install.
	crdsPolicy := string(cmp.Or(upgrade.CRDs, install.CRDs))
	for _, dep := range cr.Spec.DependsOn {
		if dep.Name == "" {
			return nil, inputf("HelmRelease missing dependsOn.name")
		}
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
		Labels:                   cr.Labels,
		Annotations:              cr.Annotations,
		DisableSchemaValidation:  disableSchema,
		DisableOpenAPIValidation: disableOpenAPI,
		ChartValuesFiles:         chartValuesFiles,
		IgnoreMissingValuesFiles: ignoreMissingValuesFiles,
		CRDsPolicy:               crdsPolicy,
	}, nil
}

func (h *HelmRelease) references(yield func(Reference) bool) {
	if h.hasChartReferences() {
		source := Resolve(h.Chart.Source, "", h.Namespace)
		chartPath, files := h.Chart.sourceInputs(h.ChartValuesFiles)
		var authored NamedResource
		gate := func(id NamedResource) (GateStage, bool) {
			if id != source || id.Name == "" {
				return 0, false
			}
			stage := GateArtifacts
			if id.Group == SourceDomain && id.Kind == KindHelmChart && h.Chart.Version == "" {
				stage |= GateInputs
			}
			return stage, true
		}
		if ref := h.ChartRef; ref != nil {
			authored = Resolve(NamedResource{Group: referenceGroup(ref.Kind, ref.APIVersion), Kind: ref.Kind, Namespace: ref.Namespace, Name: ref.Name}, "", h.Namespace)
			if source.Kind == "" && source.Name == "" {
				source = authored
			}
			stage, artifact := gate(authored)
			if !yield(Reference{NamedResource: authored, Use: "chart", Content: true, Gate: stage, Artifact: artifact, ChartSource: artifact, ArtifactStages: GateArtifacts}) {
				return
			}
		} else if tmpl := h.HelmReleaseSpec.Chart; tmpl != nil {
			ref := tmpl.Spec.SourceRef
			authored = Resolve(NamedResource{Group: referenceGroup(cmp.Or(ref.Kind, KindHelmRepository), ref.APIVersion), Kind: cmp.Or(ref.Kind, KindHelmRepository), Namespace: ref.Namespace, Name: ref.Name}, "", h.Namespace)
			refFiles := files
			refChartPath := chartPath
			if authored != source {
				refFiles = nil
				refChartPath = ""
			}
			if source.Kind == "" && source.Name == "" {
				source = authored
			}
			stage, artifact := gate(authored)
			if !yield(Reference{NamedResource: authored, Use: "source", Content: true, Files: refFiles, ChartPath: refChartPath, Gate: stage, Artifact: artifact, ChartSource: artifact, ArtifactStages: GateArtifacts}) {
				return
			}
		}
		if source.Kind != "" && authored != source {
			stage, artifact := gate(source)
			if !yield(Reference{
				NamedResource: source,
				Use:           "source", Content: true, Files: files, ChartPath: chartPath, Gate: stage, Artifact: artifact, ChartSource: artifact,
				ArtifactStages: GateArtifacts,
			}) {
				return
			}
		}
	}

	for i := range h.DependsOn {
		dep := &h.DependsOn[i]
		if !yield(Reference{
			NamedResource: Resolve(NamedResource{Group: kindTable[kindHelmRelease].Group, Kind: KindHelmRelease, Namespace: dep.Namespace, Name: dep.Name}, "", h.Namespace),
			Use:           "dependsOn", Position: i, ReadyExpr: referenceExpression(&dep.ReadyExpr), Gate: GateInputs, Ordering: true,
		}) {
			return
		}
	}
	for i := range h.ValuesFrom {
		ref := &h.ValuesFrom[i]
		if !yield(Reference{
			Kind: ref.Kind, Name: ref.Name, Namespace: h.Namespace,
			Use: "valuesFrom", Position: i, Optional: ref.Optional, Content: true,
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
		CRDsPolicy               string
		DisableSchemaValidation  bool
		DisableOpenAPIValidation bool
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
		CRDsPolicy:               h.CRDsPolicy,
		DisableSchemaValidation:  h.DisableSchemaValidation,
		DisableOpenAPIValidation: h.DisableOpenAPIValidation,
	}
}
