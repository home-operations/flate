package manifest

// StripResourceAttributes removes the listed annotation/label keys
// from a raw Kubernetes resource's metadata and the pod-template
// metadata for every workload shape Helm charts decorate. Used to cut
// chart-bump noise (helm.sh/chart, checksum/config, …) out of diffs
// before they reach the diff backend — dyff matches K8s lists by
// identifier but still flags string-value changes verbatim, so
// annotations whose values rotate on every chart update would
// otherwise produce one entry per resource.
//
// Coverage:
//
//   - .metadata (every resource)
//   - .spec.template.metadata (Deployment, StatefulSet, DaemonSet,
//     ReplicaSet, Job — anything with a pod template)
//   - .spec.jobTemplate.metadata AND
//     .spec.jobTemplate.spec.template.metadata (CronJob — both the
//     JobTemplateSpec and its nested PodTemplateSpec)
//   - .spec.volumeClaimTemplates[*].metadata (StatefulSet — Helm
//     charts decorate PVC templates with chart labels too)
//
// Without these extra walks, real chart bumps on bitnami/postgresql,
// kube-prometheus-stack, app-template CronJobs, etc. produce diff
// noise on every chart-version rotation despite the strip pass.
func StripResourceAttributes(resource map[string]any, attrs []string) {
	stripObjectMetadata(resource, attrs)
	if spec, ok := resource["spec"].(map[string]any); ok {
		// Deployment / StatefulSet / DaemonSet / ReplicaSet / Job pod
		// template.
		if tmpl, ok := spec["template"].(map[string]any); ok {
			stripObjectMetadata(tmpl, attrs)
		}
		// CronJob jobTemplate + its nested pod template.
		if jobTmpl, ok := spec["jobTemplate"].(map[string]any); ok {
			stripObjectMetadata(jobTmpl, attrs)
			if jobSpec, ok := jobTmpl["spec"].(map[string]any); ok {
				if podTmpl, ok := jobSpec["template"].(map[string]any); ok {
					stripObjectMetadata(podTmpl, attrs)
				}
			}
		}
		// StatefulSet PVC templates — Helm puts chart labels here too.
		stripMetadataInList(spec, "volumeClaimTemplates", attrs)
	}
}

// stripObjectMetadata strips the configured attrs from parent's
// "metadata" field when present. No-op when metadata is absent or
// isn't a map. Centralizes the type assertion + nil guard so the
// outer walker stays readable as a navigation of the K8s object
// graph rather than a pile of typed-map dances.
func stripObjectMetadata(parent map[string]any, attrs []string) {
	meta, ok := parent["metadata"].(map[string]any)
	if !ok {
		return
	}
	stripAttrs(meta, attrs)
}

// stripMetadataInList walks parent[listKey] as a []any of
// map[string]any objects and strips attrs from each item's metadata.
// Covers StatefulSet volumeClaimTemplates and List items uniformly.
func stripMetadataInList(parent map[string]any, listKey string, attrs []string) {
	items, ok := parent[listKey].([]any)
	if !ok {
		return
	}
	for _, it := range items {
		if obj, ok := it.(map[string]any); ok {
			stripObjectMetadata(obj, attrs)
		}
	}
}

func stripAttrs(metadata map[string]any, attrs []string) {
	for _, key := range []string{"annotations", "labels"} {
		val, ok := metadata[key].(map[string]any)
		if !ok || val == nil {
			continue
		}
		for _, a := range attrs {
			delete(val, a)
		}
		if len(val) == 0 {
			delete(metadata, key)
		}
	}
}

// EnsureMetadata returns doc["metadata"] as a map[string]any, lazily
// creating one when absent (or when present as a non-map). Used by
// writers that mutate metadata.labels / metadata.annotations; readers
// should prefer a direct type-assert + nil-tolerant access so we
// don't allocate when nothing will be written.
func EnsureMetadata(doc map[string]any) map[string]any {
	md, _ := doc["metadata"].(map[string]any)
	if md == nil {
		md = map[string]any{}
		doc["metadata"] = md
	}
	return md
}

// MergeStringMap merges in into md[key] as a map[string]any. Creates
// the inner map when absent; existing entries with the same key are
// overwritten. No-op when in is empty.
//
// Used by the ResourceSet output + Helm post-render passes to merge
// CommonMetadata (labels / annotations) onto rendered docs without
// touching unrelated entries.
func MergeStringMap(md map[string]any, key string, in map[string]string) {
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
