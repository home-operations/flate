// Package image discovers tagged or digested named references in rendered
// Kubernetes manifests using distribution/reference. Discovery is advisory:
// strings in arbitrary fields and unknown kinds are included when they parse,
// so unrelated values such as "count:up0" may appear in image reports.
package image

import (
	"maps"
	"slices"

	"github.com/distribution/reference"
)

// Extract returns nil when no images are found.
func Extract(doc map[string]any) []string {
	set := map[string]struct{}{}
	walk(doc, set)
	if len(set) == 0 {
		return nil
	}
	return slices.Sorted(maps.Keys(set))
}

func walk(v any, set map[string]struct{}) {
	switch tv := v.(type) {
	case map[string]any:
		for _, child := range tv {
			walk(child, set)
		}
	case []any:
		for _, item := range tv {
			walk(item, set)
		}
	case string:
		if isImageRef(tv) {
			set[tv] = struct{}{}
		}
	}
}

func isImageRef(s string) bool {
	ref, err := reference.ParseNormalizedNamed(s)
	if err != nil {
		return false
	}
	_, tagged := ref.(reference.Tagged)
	_, digested := ref.(reference.Digested)
	return tagged || digested
}
