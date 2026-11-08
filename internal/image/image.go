// Package image discovers declared image fields and full tagged or digested
// references in rendered manifests using distribution/reference.
package image

import (
	"maps"
	"slices"
	"strings"

	"github.com/distribution/reference"
)

// Extract returns sorted, unique references from image, imageName and Image-suffixed
// fields, their direct reference strings, and full tagged or digested references
// elsewhere, or nil when no images are found.
func Extract(doc map[string]any) []string {
	set := map[string]struct{}{}
	walk(doc, false, set)
	if len(set) == 0 {
		return nil
	}
	return slices.Sorted(maps.Keys(set))
}

func walk(v any, declared bool, set map[string]struct{}) {
	switch tv := v.(type) {
	case map[string]any:
		for key, child := range tv {
			childDeclared := key == "image" || key == "imageName" || strings.HasSuffix(key, "Image")
			if declared && key == "reference" {
				_, childDeclared = child.(string)
			}
			walk(child, childDeclared, set)
		}
	case []any:
		for _, item := range tv {
			walk(item, false, set)
		}
	case string:
		if isImageRef(tv, declared) {
			set[tv] = struct{}{}
		}
	}
}

func isImageRef(s string, declared bool) bool {
	if !declared {
		if len(s) < 5 || strings.Contains(s, "://") || strings.HasPrefix(s, "/") {
			return false
		}
		if !strings.ContainsAny(s, ":@") || strings.Contains(s, "${") || !strings.Contains(s, "/") {
			return false
		}
	}
	ref, err := reference.ParseNormalizedNamed(s)
	if err != nil {
		return false
	}
	if declared {
		return true
	}
	_, tagged := ref.(reference.Tagged)
	_, digested := ref.(reference.Digested)
	return tagged || digested
}
