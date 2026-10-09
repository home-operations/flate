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
	if hr == nil || len(hr.Values) == 0 {
		return false
	}
	field, found, err := unstructured.NestedFieldNoCopy(hr.Values, "instance", "kustomize", "patches")
	if err != nil || !found {
		return false
	}
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
			if !ok || target["name"] != "helm-controller" {
				continue
			}
		}
		return true
	}
	return false
}
