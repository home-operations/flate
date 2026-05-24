package manifest

// StripResourceAttributes removes the listed annotation/label keys
// from a raw Kubernetes resource's metadata, the pod-template
// metadata for every workload shape Helm charts decorate, and the
// items of a List. Used to cut chart-bump noise (helm.sh/chart,
// checksum/config, …) out of diffs before they reach the diff
// backend — dyff matches K8s lists by identifier but still flags
// string-value changes verbatim, so annotations whose values rotate
// on every chart update would otherwise produce one entry per
// resource.
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
//   - List.items[*].metadata (recursing one level into each item)
//
// Without these extra walks, real chart bumps on bitnami/postgresql,
// kube-prometheus-stack, app-template CronJobs, etc. produce diff
// noise on every chart-version rotation despite the strip pass.
func StripResourceAttributes(resource map[string]any, attrs []string) {
	if metadata, ok := resource["metadata"].(map[string]any); ok {
		stripAttrs(metadata, attrs)
	}
	if spec, ok := resource["spec"].(map[string]any); ok {
		// Deployment / StatefulSet / DaemonSet / ReplicaSet / Job pod
		// template.
		if tmpl, ok := spec["template"].(map[string]any); ok {
			if meta, ok := tmpl["metadata"].(map[string]any); ok {
				stripAttrs(meta, attrs)
			}
		}
		// CronJob jobTemplate + its nested pod template.
		if jobTmpl, ok := spec["jobTemplate"].(map[string]any); ok {
			if meta, ok := jobTmpl["metadata"].(map[string]any); ok {
				stripAttrs(meta, attrs)
			}
			if jobSpec, ok := jobTmpl["spec"].(map[string]any); ok {
				if podTmpl, ok := jobSpec["template"].(map[string]any); ok {
					if meta, ok := podTmpl["metadata"].(map[string]any); ok {
						stripAttrs(meta, attrs)
					}
				}
			}
		}
		// StatefulSet PVC templates — Helm puts chart labels here too.
		if pvcs, ok := spec["volumeClaimTemplates"].([]any); ok {
			for _, pvc := range pvcs {
				m, ok := pvc.(map[string]any)
				if !ok {
					continue
				}
				if meta, ok := m["metadata"].(map[string]any); ok {
					stripAttrs(meta, attrs)
				}
			}
		}
	}
	if kind, _ := resource["kind"].(string); kind == "List" {
		if items, ok := resource["items"].([]any); ok {
			for _, it := range items {
				m, ok := it.(map[string]any)
				if !ok {
					continue
				}
				if meta, ok := m["metadata"].(map[string]any); ok {
					stripAttrs(meta, attrs)
				}
			}
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
