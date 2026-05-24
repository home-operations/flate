package resourceset

import fluxopv1 "github.com/controlplaneio-fluxcd/flux-operator/api/v1"

func dedupKey(doc map[string]any) string {
	apiVersion, _ := doc["apiVersion"].(string)
	kind, _ := doc["kind"].(string)
	md, _ := doc["metadata"].(map[string]any)
	name, _ := md["name"].(string)
	ns, _ := md["namespace"].(string)
	if kind == "" || name == "" {
		return ""
	}
	return apiVersion + "|" + kind + "|" + ns + "|" + name
}

func defaultNamespace(doc map[string]any, ns string) {
	if ns == "" {
		return
	}
	md, _ := doc["metadata"].(map[string]any)
	if md == nil {
		md = map[string]any{}
		doc["metadata"] = md
	}
	if cur, _ := md["namespace"].(string); cur != "" {
		return
	}
	// Don't inject a namespace on cluster-scoped kinds — match the
	// upstream operator behavior of leaving Namespace, ClusterRole etc.
	// without a metadata.namespace.
	if isClusterScoped(doc) {
		return
	}
	md["namespace"] = ns
}

func isClusterScoped(doc map[string]any) bool {
	kind, _ := doc["kind"].(string)
	switch kind {
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
	md, _ := doc["metadata"].(map[string]any)
	if md == nil {
		md = map[string]any{}
		doc["metadata"] = md
	}
	mergeStringMap(md, "labels", cm.Labels)
	mergeStringMap(md, "annotations", cm.Annotations)
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
