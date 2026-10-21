package resourceset

import (
	fluxopv1 "github.com/controlplaneio-fluxcd/flux-operator/api/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"

	"github.com/home-operations/flate/pkg/manifest"
)

// DedupKey identifies a document by its authored API group, kind, namespace and name.
func DedupKey(doc map[string]any) manifest.NamedResource {
	kind := manifest.DocKind(doc)
	name, ns := manifest.DocMetadata(doc)
	gv, _ := schema.ParseGroupVersion(manifest.DocAPIVersion(doc))
	return manifest.NamedResource{Group: gv.Group, Kind: kind, Namespace: ns, Name: name}
}

func applyCommonMetadata(doc map[string]any, cm *fluxopv1.CommonMetadata) {
	if cm == nil || (len(cm.Labels) == 0 && len(cm.Annotations) == 0) {
		return
	}
	md := manifest.EnsureMetadata(doc)
	manifest.MergeStringMap(md, "labels", cm.Labels)
	manifest.MergeStringMap(md, "annotations", cm.Annotations)
}

func applyOwnerLabels(doc map[string]any, rs *manifest.ResourceSet) {
	md := manifest.EnsureMetadata(doc)
	manifest.MergeStringMap(md, "labels", map[string]string{
		fluxopv1.OwnerLabelResourceSetName:      rs.Name,
		fluxopv1.OwnerLabelResourceSetNamespace: rs.Namespace,
	})
}

func disabledByReconcileAnnotation(doc map[string]any) bool {
	md, _ := doc["metadata"].(map[string]any)
	ann, _ := md["annotations"].(map[string]any)
	v, _ := ann[fluxopv1.ReconcileAnnotation].(string)
	return v == fluxopv1.DisabledValue
}
