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

func defaultNamespace(doc map[string]any, ns string) {
	if ns == "" {
		return
	}
	// Read existing metadata.namespace without creating one — if the
	// doc already names a namespace, we're done. A nil-map lookup
	// returns the zero value, so this is safe pre-ensure.
	md, _ := doc["metadata"].(map[string]any)
	if cur, _ := md["namespace"].(string); cur != "" {
		return
	}
	// Don't inject a namespace on cluster-scoped kinds — match the
	// upstream operator behavior of leaving Namespace, ClusterRole etc.
	// without a metadata.namespace.
	if isClusterScoped(doc) {
		return
	}
	ensureMetadata(doc)["namespace"] = ns
}

func isClusterScoped(doc map[string]any) bool {
	switch manifest.DocKind(doc) {
	case "Namespace",
		"ClusterRole", "ClusterRoleBinding",
		"CustomResourceDefinition",
		"PersistentVolume",
		"StorageClass",
		"PriorityClass",
		"MutatingWebhookConfiguration", "ValidatingWebhookConfiguration",
		"APIService",
		"Node":
		return true
	}
	return false
}

func applyCommonMetadata(doc map[string]any, cm *fluxopv1.CommonMetadata) {
	if cm == nil || (len(cm.Labels) == 0 && len(cm.Annotations) == 0) {
		return
	}
	md := ensureMetadata(doc)
	mergeStringMap(md, "labels", cm.Labels)
	mergeStringMap(md, "annotations", cm.Annotations)
}

// ensureMetadata returns doc["metadata"] as a map[string]any, lazily
// creating one when absent (or when present as a non-map). Used by
// the writers below; reads should prefer a direct type-assert + nil-
// tolerant access so we don't allocate when nothing will be written.
func ensureMetadata(doc map[string]any) map[string]any {
	md, _ := doc["metadata"].(map[string]any)
	if md == nil {
		md = map[string]any{}
		doc["metadata"] = md
	}
	return md
}

func mergeStringMap(md map[string]any, key string, in map[string]string) {
	if len(in) == 0 {
		return
	}
	out, _ := md[key].(map[string]any)
	if out == nil {
		out = make(map[string]any, len(in))
	}
	for k, v := range in {
		out[k] = v
	}
	md[key] = out
}

func disabledByReconcileAnnotation(doc map[string]any) bool {
	md, _ := doc["metadata"].(map[string]any)
	ann, _ := md["annotations"].(map[string]any)
	if v, _ := ann[fluxopv1.ReconcileAnnotation].(string); v == fluxopv1.DisabledValue {
		return true
	}
	return false
}
