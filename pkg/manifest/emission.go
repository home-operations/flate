package manifest

import (
	"slices"

	fluxopv1 "github.com/controlplaneio-fluxcd/flux-operator/api/v1"
)

var ownershipKeys = [...][2]string{
	{FluxKustomizeDomain + "/name", FluxKustomizeDomain + "/namespace"},
	{HelmReleaseDomain + "/name", HelmReleaseDomain + "/namespace"},
	{fluxopv1.OwnerLabelResourceSetName, fluxopv1.OwnerLabelResourceSetNamespace},
}

// EmissionPolicy contains borrowed metadata and decoded-document admission facts.
type EmissionPolicy struct {
	Labels, Annotations         map[string]string
	OwnerKeys, OwnerValues      [2]string
	Owner                       bool
	CommonFirst                 bool
	HookAnnotation              string
	CommonExcludedKind          string
	PreserveEmptyOwnerNamespace bool
	Namespace                   string
	InternalNamespace           bool
	ExcludeChildren             bool
	StringLabels                bool
}

// SelectDocument reports output and child admission for a document.
func (p EmissionPolicy) SelectDocument(doc map[string]any, skipKinds []string) (bool, bool) {
	visible := len(skipKinds) == 0 || !slices.Contains(skipKinds, DocKind(doc))
	return visible, visible || !p.ExcludeChildren
}

// IsOwnershipLabel reports whether semantic comparison ignores an owner stamp.
func IsOwnershipLabel(key string) bool {
	for _, keys := range ownershipKeys {
		if key == keys[0] || key == keys[1] {
			return true
		}
	}
	return false
}

// EmissionPolicy returns Kustomization overlays and target-namespace admission.
func (k *Kustomization) EmissionPolicy() EmissionPolicy {
	p := EmissionPolicy{OwnerKeys: ownershipKeys[0], OwnerValues: [2]string{k.Name, k.Namespace}, Owner: k.Name != "", Namespace: k.TargetNamespace}
	if k.CommonMetadata != nil {
		p.Labels, p.Annotations = k.CommonMetadata.Labels, k.CommonMetadata.Annotations
	}
	p.StringLabels = p.Owner || len(p.Labels) > 0
	return p
}

// EmissionPolicy returns HelmRelease overlays, hook exclusions and child admission.
func (h *HelmRelease) EmissionPolicy() EmissionPolicy {
	p := EmissionPolicy{OwnerKeys: ownershipKeys[1], OwnerValues: [2]string{h.Name, h.Namespace}, Owner: true,
		CommonFirst: true, HookAnnotation: "helm.sh/hook", CommonExcludedKind: KindCustomResourceDefinition,
		PreserveEmptyOwnerNamespace: true, Namespace: h.ReleaseNamespace(), ExcludeChildren: true}
	if h.CommonMetadata != nil {
		p.Labels, p.Annotations = h.CommonMetadata.Labels, h.CommonMetadata.Annotations
	}
	return p
}

// EmissionPolicy returns ResourceSet overlays and internal-only namespace admission.
func (r *ResourceSet) EmissionPolicy() EmissionPolicy {
	p := EmissionPolicy{OwnerKeys: ownershipKeys[2], OwnerValues: [2]string{r.Name, r.Namespace}, Owner: true,
		PreserveEmptyOwnerNamespace: true, Namespace: r.Namespace, InternalNamespace: true}
	if r.CommonMetadata != nil {
		p.Labels, p.Annotations = r.CommonMetadata.Labels, r.CommonMetadata.Annotations
	}
	return p
}
