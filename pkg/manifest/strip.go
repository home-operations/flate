package manifest

import (
	"maps"
	"strings"
)

// StripResourceAttributes removes the listed annotation/label keys
// from a raw Kubernetes resource's metadata and the pod-template
// metadata for every workload shape Helm charts decorate. Used to cut
// chart-bump noise (helm.sh/chart, checksum/config, …) out of diffs
// before they reach the diff backend — dyff matches K8s lists by
// identifier but still flags string-value changes verbatim, so
// annotations whose values rotate on every chart update would
// otherwise produce one entry per resource.
//
// An attr ending in "/" is a prefix match (e.g. "checksum/" drops
// checksum/config, checksum/secret, checksum/secrets, and any other
// chart-specific suffix in one entry); every other attr is an exact
// key match.
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
		if tmpl, ok := spec["template"].(map[string]any); ok {
			stripObjectMetadata(tmpl, attrs)
		}
		if jobTmpl, ok := spec["jobTemplate"].(map[string]any); ok {
			stripObjectMetadata(jobTmpl, attrs)
			if jobSpec, ok := jobTmpl["spec"].(map[string]any); ok {
				if podTmpl, ok := jobSpec["template"].(map[string]any); ok {
					stripObjectMetadata(podTmpl, attrs)
				}
			}
		}
		items, _ := spec["volumeClaimTemplates"].([]any)
		for _, item := range items {
			if obj, ok := item.(map[string]any); ok {
				stripObjectMetadata(obj, attrs)
			}
		}
	}
}

func stripObjectMetadata(parent map[string]any, attrs []string) {
	meta, ok := parent["metadata"].(map[string]any)
	if !ok {
		return
	}
	for _, key := range []string{"annotations", "labels"} {
		val, ok := meta[key].(map[string]any)
		if !ok || val == nil {
			continue
		}
		for _, a := range attrs {
			if strings.HasSuffix(a, "/") {
				maps.DeleteFunc(val, func(k string, _ any) bool {
					return strings.HasPrefix(k, a)
				})
				continue
			}
			delete(val, a)
		}
		if len(val) == 0 {
			delete(meta, key)
		}
	}
}

// StripResourceFields deletes each dotted field path from resource.
// A path like "spec.restic.unlock" navigates nested maps and deletes
// the leaf key from its parent map; a segment that is absent or whose
// value is not a map stops that path (no-op). Only the leaf is removed
// — parent maps are left in place, so both sides of a diff collapse to
// the same shape rather than one side losing an emptied parent.
//
// Navigation is map-only (no list indexing or wildcards), which covers
// the case it exists for: chart-emitted volatile scalars whose value
// rotates every render, e.g. volsync's
// spec.restic.unlock: {{ now | date "20060102150405" }}. This is the
// spec-field analogue of StripResourceAttributes (metadata keys); the
// diff layer applies both before handing docs to the diff backend.
func StripResourceFields(resource map[string]any, paths []string) {
	for _, p := range paths {
		if p == "" {
			continue
		}
		deleteFieldPath(resource, strings.Split(p, "."))
	}
}

// deleteFieldPath removes the leaf named by segs from the nested map m,
// walking map-typed segments only. No-op when a segment is missing or
// not a map.
func deleteFieldPath(m map[string]any, segs []string) {
	if len(segs) == 1 {
		delete(m, segs[0])
		return
	}
	child, ok := m[segs[0]].(map[string]any)
	if !ok {
		return
	}
	deleteFieldPath(child, segs[1:])
}
