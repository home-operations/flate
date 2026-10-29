package manifest

import (
	"cmp"
	"fmt"
	"maps"
	"slices"
	"strings"

	"github.com/Masterminds/semver/v3"
	helmv2 "github.com/fluxcd/helm-controller/api/v2"
	sourcev1 "github.com/fluxcd/source-controller/api/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

type chartSource struct {
	declared    NamedResource
	version     string
	backing     HelmChart
	credentials [3]*LocalObjectReference
}

func equalChartTemplate(a, b *helmv2.HelmChartTemplate) bool {
	if a == nil || b == nil {
		return a == b
	}
	if a.Kind != b.Kind || a.Spec.Chart != b.Spec.Chart || a.Spec.Version != b.Spec.Version || a.Spec.ReconcileStrategy != b.Spec.ReconcileStrategy || a.Spec.IgnoreMissingValuesFiles != b.Spec.IgnoreMissingValuesFiles || !equalValue(a.Spec.SourceRef, b.Spec.SourceRef) || !slices.Equal(a.Spec.ValuesFiles, b.Spec.ValuesFiles) {
		return false
	}
	if a.ObjectMeta == nil || b.ObjectMeta == nil {
		if a.ObjectMeta != b.ObjectMeta {
			return false
		}
	} else if !maps.Equal(a.ObjectMeta.Labels, b.ObjectMeta.Labels) || !maps.Equal(a.ObjectMeta.Annotations, b.ObjectMeta.Annotations) {
		return false
	}
	x, y := &a.Spec.OCIRepositorySpec, &b.Spec.OCIRepositorySpec
	if x.URL != y.URL || x.Provider != y.Provider || x.ServiceAccountName != y.ServiceAccountName || x.Insecure != y.Insecure || x.Suspend != y.Suspend || !equalValue(x.Reference, y.Reference) || !equalValue(x.LayerSelector, y.LayerSelector) || !equalValue(x.SecretRef, y.SecretRef) || !equalValue(x.CertSecretRef, y.CertSecretRef) || !equalValue(x.ProxySecretRef, y.ProxySecretRef) || !equalValue(x.Interval, y.Interval) || !equalValue(x.Timeout, y.Timeout) || !equalValue(x.Ignore, y.Ignore) {
		return false
	}
	if x.Verify == nil || y.Verify == nil {
		return x.Verify == y.Verify
	}
	return x.Verify.Provider == y.Verify.Provider && equalValue(x.Verify.SecretRef, y.Verify.SecretRef) && equalValue(x.Verify.TrustedRootSecretRef, y.Verify.TrustedRootSecretRef) && slices.Equal(x.Verify.MatchOIDCIdentity, y.Verify.MatchOIDCIdentity)
}

func equalValue[T comparable](a, b *T) bool {
	return a == b || a != nil && b != nil && *a == *b
}

func (h *HelmRelease) normalizeChartSource() error {
	if err := normalizeChartTemplate(&h.HelmReleaseSpec); err != nil {
		return err
	}
	h.source = NamedResource{}
	h.chartDeclaration = nil
	source := h.deriveChartSource()
	h.source = source.declared
	h.Chart = HelmChart{Name: source.backing.Name, Version: source.backing.Version, Source: source.declared}
	if source.backing.hasSourceChartPath() {
		h.Chart.Source = source.backing.Source
	}
	if template := h.HelmReleaseSpec.Chart; template != nil {
		h.ChartValuesFiles = template.Spec.ValuesFiles
		h.IgnoreMissingValuesFiles = template.Spec.IgnoreMissingValuesFiles
	}
	return nil
}

func (h *HelmRelease) deriveChartSource() chartSource {
	source := chartSource{}
	if ref := h.ChartRef; ref != nil {
		source.declared = Resolve(NamedResource{Group: referenceGroup(ref.Kind, ref.APIVersion), Kind: ref.Kind, Namespace: ref.Namespace, Name: ref.Name}, "", h.Namespace)
	} else if template := h.HelmReleaseSpec.Chart; template != nil {
		source.declared = h.source
		if source.declared.Name == "" {
			release := helmv2.HelmRelease{Name: h.Name, Namespace: h.Namespace, Spec: h.HelmReleaseSpec}
			ref := release.GetHelmChartTemplateReference()
			source.declared = NamedResource{Group: SourceDomain, Kind: ref.Kind, Namespace: ref.Namespace, Name: ref.Name}
		}
		if source.declared.Kind == KindHelmChart && template.Spec.SourceRef != nil {
			backing := template.Spec.SourceRef
			source.backing = HelmChart{Name: template.Spec.Chart, Version: template.Spec.GetVersion(),
				Source: Resolve(NamedResource{Group: referenceGroup(backing.Kind, backing.APIVersion), Kind: backing.Kind, Name: backing.Name, Namespace: backing.Namespace}, "", h.Namespace)}
			source.version = source.backing.Version
		} else if source.declared.Kind == KindOCIRepository {
			source.version = ociReferenceVersion(template.Spec.Reference)
			source.credentials = [3]*LocalObjectReference{template.Spec.SecretRef, template.Spec.CertSecretRef, template.Spec.ProxySecretRef}
		}
	}
	return source
}

func (h *HelmRelease) chartProjection() chartSource {
	source := h.deriveChartSource()
	if h.chartDeclaration != nil {
		source.backing = helmChartFromSource(h.chartDeclaration)
	}
	return source
}

// ResolveChartSource normalizes a declaration and resolves its effective immutable chart input.
func (h *HelmRelease) ResolveChartSource(lookup func(NamedResource) BaseManifest) error {
	if err := h.normalizeChartSource(); err != nil {
		return err
	}
	id := h.source
	if h.Chart.hasSourceChartPath() || id.Kind != KindHelmChart || id.Group != SourceDomain {
		return nil
	}
	var src *HelmChartSource
	if lookup != nil {
		src, _ = lookup(id).(*HelmChartSource)
	}
	if src == nil {
		return fmt.Errorf("%w: HelmChartSource %s not found for HelmRelease %s", ErrObjectNotFound, id.NamespacedName(), h.Named().NamespacedName())
	}
	h.chartDeclaration = src
	h.Chart.Name, h.Chart.Version = src.Chart, src.Version
	if src.ExistenceOnly() {
		h.Chart.Source = helmChartFromSource(src).Source
	}
	h.ChartValuesFiles, h.IgnoreMissingValuesFiles = src.ValuesFiles, src.IgnoreMissingValuesFiles
	return nil
}

// GeneratedObjects implements Flux's chart-template rule without mutating the release.
func (h *HelmRelease) GeneratedObjects() ([]BaseManifest, error) {
	view := *h
	if err := view.normalizeChartSource(); err != nil {
		return nil, err
	}
	if view.HelmReleaseSpec.Chart == nil || view.Suspended() {
		return nil, nil
	}
	template := view.HelmReleaseSpec.Chart.DeepCopy()
	id := view.source
	metadata := metav1.ObjectMeta{Name: id.Name, Namespace: id.Namespace}
	if template.ObjectMeta != nil {
		metadata.Labels, metadata.Annotations = maps.Clone(template.ObjectMeta.Labels), maps.Clone(template.ObjectMeta.Annotations)
	}
	if metadata.Labels == nil {
		metadata.Labels = make(map[string]string, 2)
	}
	metadata.Labels[HelmReleaseDomain+"/name"], metadata.Labels[HelmReleaseDomain+"/namespace"] = h.Name, h.Namespace
	release := helmv2.HelmRelease{Spec: view.HelmReleaseSpec}
	interval := release.GetTemplateInterval()
	var object BaseManifest
	if id.Kind == KindOCIRepository {
		spec := template.Spec.OCIRepositorySpec
		spec.Interval = &interval
		if spec.LayerSelector == nil {
			spec.LayerSelector = &sourcev1.OCILayerSelector{MediaType: "application/vnd.cncf.helm.chart.content.v1.tar+gzip", Operation: sourcev1.OCILayerCopy}
		}
		generated := NewOCIRepository(sourcev1.OCIRepository{ObjectMeta: metadata, Spec: spec})
		generated.diagnosticOwner, generated.hasDiagnostic = h.Named(), true
		object = generated
	} else {
		spec := sourcev1.HelmChartSpec{Chart: template.Spec.Chart, Version: template.Spec.GetVersion(), Interval: interval,
			ReconcileStrategy: template.Spec.GetReconcileStrategy(), ValuesFiles: template.Spec.ValuesFiles, IgnoreMissingValuesFiles: template.Spec.IgnoreMissingValuesFiles,
			SourceRef: sourcev1.LocalHelmChartSourceReference{Kind: template.Spec.SourceRef.Kind, Name: template.Spec.SourceRef.Name}}
		if verify := template.Spec.Verify; verify != nil {
			spec.Verify = &sourcev1.HelmChartVerification{Provider: cmp.Or(verify.Provider, "cosign"), SecretRef: verify.SecretRef}
		}
		generated := NewHelmChartSource(sourcev1.HelmChart{ObjectMeta: metadata, Spec: spec})
		generated.diagnosticOwner, generated.hasDiagnostic = h.Named(), true
		object = generated
	}
	return []BaseManifest{object}, nil
}

// OCIRepository adapts a HelmRepository transport request without publishing a source.
func (h *HelmChartSource) OCIRepository(repository *HelmRepository) *OCIRepository {
	var ref *sourcev1.OCIRepositoryRef
	if h.Version != "" {
		ref = &sourcev1.OCIRepositoryRef{}
		_, constraintErr := semver.NewConstraint(h.Version)
		switch {
		case strings.Contains(h.Version, ":"):
			ref.Digest = h.Version
		case h.ExactVersion():
			ref.Tag = h.Version
		case constraintErr == nil:
			ref.SemVer = h.Version
		default:
			ref.Tag = h.Version
		}
	}
	repo := NewOCIRepository(sourcev1.OCIRepository{
		Name: h.Name, Namespace: h.Namespace,
		Spec: sourcev1.OCIRepositorySpec{
			URL:       strings.TrimSuffix(repository.URL, "/") + "/" + h.Chart,
			Reference: ref, Provider: repository.Provider,
			SecretRef: repository.SecretRef, CertSecretRef: repository.CertSecretRef,
			Insecure: repository.Insecure,
			Timeout:  &metav1.Duration{Duration: repository.OperationTimeout()},
			Interval: &metav1.Duration{Duration: repository.SourceInterval()},
		},
	})
	subject := DiagnosticFor(h.Named(), h)
	repo.diagnosticOwner, repo.diagnosticPath, repo.hasDiagnostic = subject.Resource, subject.Path, true
	return repo
}
