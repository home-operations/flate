package base

import "github.com/home-operations/flate/pkg/manifest"

// Stamp applies producer overlays to caller-owned decoded documents before substitution.
func Stamp(policy manifest.EmissionPolicy, docs []map[string]any) {
	if !policy.Owner && len(policy.Labels) == 0 && len(policy.Annotations) == 0 {
		return
	}
	for _, doc := range docs {
		md := manifest.EnsureMetadata(doc)
		hook := func() bool {
			if policy.HookAnnotation == "" {
				return false
			}
			annotations, _ := md["annotations"].(map[string]any)
			_, found := annotations[policy.HookAnnotation]
			return found
		}
		owner := func() {
			if !policy.Owner || hook() {
				return
			}
			labels, _ := md["labels"].(map[string]any)
			if labels == nil {
				labels = make(map[string]any, 2)
				md["labels"] = labels
			}
			labels[policy.OwnerKeys[0]] = policy.OwnerValues[0]
			if policy.OwnerValues[1] != "" || policy.PreserveEmptyOwnerNamespace {
				labels[policy.OwnerKeys[1]] = policy.OwnerValues[1]
			}
		}
		if !policy.CommonFirst {
			owner()
		}
		if !hook() && (policy.CommonExcludedKind == "" || manifest.DocKind(doc) != policy.CommonExcludedKind) {
			manifest.MergeStringMap(md, "labels", policy.Labels)
			manifest.MergeStringMap(md, "annotations", policy.Annotations)
		}
		if policy.CommonFirst {
			owner()
		}
	}
}
