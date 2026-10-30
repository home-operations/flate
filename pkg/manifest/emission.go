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
	DocumentScope               bool
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
	p := EmissionPolicy{OwnerKeys: ownershipKeys[0], OwnerValues: [2]string{k.Name, k.Namespace}, Owner: k.Name != "" && !k.hasDiagnostic, Namespace: k.TargetNamespace}
	p.DocumentScope = k.hasDiagnostic
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

// MatchesNamespace reports whether the target can belong to the producer's namespace projection.
func (p EmissionPolicy) MatchesNamespace(target NamedResource) bool {
	return p.InternalNamespace || p.ExcludeChildren || p.Namespace == "" || HasEnvsubstReference(p.Namespace) ||
		target.Namespace == "" || !target.KindInfo().Namespaced() || target.Namespace == p.Namespace
}

// Stamp applies producer overlays to caller-owned decoded documents before substitution.
func (p EmissionPolicy) Stamp(docs []DocOccurrence) {
	if !p.Owner && len(p.Labels) == 0 && len(p.Annotations) == 0 {
		return
	}
	for _, occurrence := range docs {
		if occurrence.Err != nil || occurrence.Document == nil {
			continue
		}
		doc := occurrence.Document
		md := EnsureMetadata(doc)
		hook := func() bool {
			if p.HookAnnotation == "" {
				return false
			}
			annotations, _ := md["annotations"].(map[string]any)
			_, found := annotations[p.HookAnnotation]
			return found
		}
		owner := func() {
			if !p.Owner || hook() {
				return
			}
			labels, _ := md["labels"].(map[string]any)
			if labels == nil {
				labels = make(map[string]any, 2)
				md["labels"] = labels
			}
			labels[p.OwnerKeys[0]] = p.OwnerValues[0]
			if p.OwnerValues[1] != "" || p.PreserveEmptyOwnerNamespace {
				labels[p.OwnerKeys[1]] = p.OwnerValues[1]
			}
		}
		if !p.CommonFirst {
			owner()
		}
		if !hook() && (p.CommonExcludedKind == "" || DocKind(doc) != p.CommonExcludedKind) {
			MergeStringMap(md, "labels", p.Labels)
			MergeStringMap(md, "annotations", p.Annotations)
		}
		if p.CommonFirst {
			owner()
		}
	}
}

// SelectOutput reports whether this producer's batch or document matches requested output scope.
func (p EmissionPolicy) SelectOutput(doc map[string]any, roots []NamedResource, namespaces map[string]struct{}, labels map[string]string) bool {
	if !p.DocumentScope {
		return true
	}
	if roots[0].Name != "" || !slices.ContainsFunc(roots, func(root NamedResource) bool {
		return root.Group == FluxKustomizeDomain && root.Kind == KindKustomization
	}) {
		return false
	}
	id := DocIdentity(doc)
	if id.Namespace != "" && namespaces != nil {
		if _, ok := namespaces[id.Namespace]; !ok {
			return false
		}
	}
	md, _ := doc["metadata"].(map[string]any)
	actual, _ := md["labels"].(map[string]any)
	for key, value := range labels {
		if got, ok := actual[key]; !ok || got != value {
			return false
		}
	}
	return true
}
