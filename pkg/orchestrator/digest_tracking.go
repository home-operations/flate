package orchestrator

import (
	"strings"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

	"github.com/home-operations/flate/pkg/manifest"
)

// disablesChartDigestTracking reports whether inline instance kustomize patches
// contain DisableChartDigestTracking=true for helm-controller or no target.
// Missing or malformed values do not match. The release is never mutated.
func disablesChartDigestTracking(hr *manifest.HelmRelease) bool {
	if hr == nil {
		return false
	}
	// Guard intermediate types so the accessor cannot allocate a malformed-path error.
	instance, ok := hr.Values["instance"].(map[string]any)
	if !ok {
		return false
	}
	kustomize, ok := instance["kustomize"].(map[string]any)
	if !ok {
		return false
	}
	field, _, _ := unstructured.NestedFieldNoCopy(kustomize, "patches")
	patches, ok := field.([]any)
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
			if !ok {
				continue
			}
			name, ok := target["name"].(string)
			if !ok || name != "helm-controller" {
				continue
			}
		}
		return true
	}
	return false
}
