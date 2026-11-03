package manifest

import "slices"

// Selection describes immutable output intent; its zero value admits every identity.
type Selection struct {
	Roots   []NamedResource
	Changed bool
}

// Matches reports whether an identity is eligible under the requested roots.
// Namespace restrictions retain cluster-scoped output; complete render-kind requests include standalone output.
func (s Selection) Matches(id NamedResource) bool {
	return s.matches(id, true)
}

// MatchesScope reports whether a kind and namespace belong to the requested output scope.
func (s Selection) MatchesScope(id NamedResource) bool {
	return s.matches(id, false)
}

func (s Selection) matches(id NamedResource, named bool) bool {
	if len(s.Roots) == 0 {
		return true
	}
	for _, root := range s.Roots {
		if root.Group == id.Group && root.Kind == id.Kind &&
			(!named || root.Name == "" || root.Name == id.Name) &&
			(root.Namespace == "" || id.Namespace == "" || root.Namespace == id.Namespace) {
			return true
		}
	}
	all := !named || s.Roots[0].Name == ""
	for _, root := range s.Roots {
		all = all && (!named || root.Name == "") && root.Namespace == s.Roots[0].Namespace
	}
	if all {
		for kind := range Kinds() {
			if kind.Render && !slices.ContainsFunc(s.Roots, func(root NamedResource) bool { return root.Group == kind.Group && root.Kind == kind.Kind }) {
				all = false
				break
			}
		}
		if all {
			return s.Roots[0].Namespace == "" || id.Namespace == "" || id.Namespace == s.Roots[0].Namespace
		}
	}

	return false
}
