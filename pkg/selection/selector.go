package selection

import (
	"slices"

	"github.com/home-operations/flate/pkg/manifest"
)

func selectorComplete(ref *manifest.Reference) bool {
	selector := ref.Selector
	for key, value := range selector.MatchLabels {
		if manifest.HasEnvsubstReference(key) || manifest.HasEnvsubstReference(value) {
			return false
		}
	}
	for _, expression := range selector.MatchExpressions {
		if manifest.HasEnvsubstReference(expression.Key) ||
			manifest.HasEnvsubstReference(string(expression.Operator)) ||
			slices.ContainsFunc(expression.Values, manifest.HasEnvsubstReference) {
			return false
		}
	}
	return true
}
