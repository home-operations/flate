package resourceset

import (
	fluxopv1 "github.com/controlplaneio-fluxcd/flux-operator/api/v1"

	"github.com/home-operations/flate/pkg/manifest"
)

// DedupKey identifies a rendered doc by (apiVersion, kind, namespace,
// name) for cross-render deduplication. Returns "" when kind or name
// are missing — signal to the caller to drop the doc rather than
// merge with an "empty key" collision pile.
//
// Exported so the orchestrator's post-Run RS-extension pass can share
// the same identity rule as the in-package RS render — without that,
// a name-grouped RS that emits the same child from two namespace
// variants could land both copies in the parent KS's extension list.
func DedupKey(doc map[string]any) string {
	kind := manifest.DocKind(doc)
	name, ns := manifest.DocMetadata(doc)
	if kind == "" || name == "" {
		return ""
	}
	return manifest.DocAPIVersion(doc) + "|" + kind + "|" + ns + "|" + name
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
	labels, _ := md["labels"].(map[string]any)
	if labels == nil {
		labels = make(map[string]any, 2)
	}
	labels[fluxopv1.OwnerLabelResourceSetName] = rs.Name
	labels[fluxopv1.OwnerLabelResourceSetNamespace] = rs.Namespace
	md["labels"] = labels
}

func disabledByReconcileAnnotation(doc map[string]any) bool {
	md, _ := doc["metadata"].(map[string]any)
	ann, _ := md["annotations"].(map[string]any)
	v, _ := ann[fluxopv1.ReconcileAnnotation].(string)
	return v == fluxopv1.DisabledValue
}
