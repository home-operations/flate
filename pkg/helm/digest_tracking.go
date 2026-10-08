package helm

import (
	"strings"

	"github.com/home-operations/flate/pkg/manifest"
)

// DisablesChartDigestTracking reports whether inline instance kustomize patches
// contain DisableChartDigestTracking=true for helm-controller or no target.
// Missing or malformed values do not match. The release is never mutated.
func DisablesChartDigestTracking(hr *manifest.HelmRelease) bool {
	if hr == nil {
		return false
	}
	instance, ok := hr.Values["instance"].(map[string]any)
	if !ok {
		return false
	}
	kustomize, ok := instance["kustomize"].(map[string]any)
	if !ok {
		return false
	}
	patches, ok := kustomize["patches"].([]any)
	if !ok {
		return false
	}
	for _, raw := range patches {
		patch, ok := raw.(map[string]any)
		if !ok {
			continue
		}
		text, ok := patch["patch"].(string)
		if !ok || !strings.Contains(text, "DisableChartDigestTracking=true") {
			continue
		}
		if rawTarget, present := patch["target"]; present {
			target, ok := rawTarget.(map[string]any)
			if !ok || target["name"] != "helm-controller" {
				continue
			}
		}
		return true
	}
	return false
}
