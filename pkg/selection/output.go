package selection

import (
	"slices"

	"github.com/home-operations/flate/pkg/manifest"
)

func selectOutput(
	doc map[string]any,
	roots []manifest.NamedResource,
	namespaces map[string]struct{},
	labels map[string]string,
) bool {
	if roots[0].Name != "" || !slices.ContainsFunc(roots, func(root manifest.NamedResource) bool {
		return root.KindInfo().Structural
	}) {
		return false
	}
	id := manifest.DocIdentity(doc)
	if id.Namespace != "" && namespaces != nil {
		if _, ok := namespaces[id.Namespace]; !ok {
			return false
		}
	}
	md, _ := doc["metadata"].(map[string]any)
	actual, _ := md["labels"].(map[string]any)
	for key, value := range labels {
		if got, ok := actual[key]; !ok || got != value {
			return false
		}
	}
	return true
}
