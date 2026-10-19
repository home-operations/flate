package values

import (
	"strings"

	"github.com/fluxcd/pkg/envsubst"

	"github.com/home-operations/flate/pkg/manifest"
)

// DependencySubstitutions is an immutable snapshot of unambiguous discovered inline values.
type DependencySubstitutions struct {
	values map[string]string
}

// NewDependencySubstitutions captures agreeing inline declarations and excludes conflicting keys.
func NewDependencySubstitutions(kustomizations []*manifest.Kustomization) DependencySubstitutions {
	union := map[string]string{}
	conflicting := map[string]struct{}{}
	for _, ks := range kustomizations {
		if ks.PostBuild == nil {
			continue
		}
		for k, v := range manifest.VarsMap(ks.PostBuild.Substitute) {
			if _, bad := conflicting[k]; bad {
				continue
			}
			if prev, seen := union[k]; seen && prev != v {
				delete(union, k)
				conflicting[k] = struct{}{}
				continue
			}
			union[k] = v
		}
	}
	return DependencySubstitutions{values: union}
}

// Resolve evaluates defaults and supplied values, retaining the input on evaluation errors.
func (d DependencySubstitutions) Resolve(text string) string {
	if !strings.Contains(text, "${") {
		return text
	}
	out, err := envsubst.Eval(text, func(name string) (string, bool) {
		value, ok := d.values[name]
		return value, ok
	})
	if err != nil {
		return text
	}
	return out
}
