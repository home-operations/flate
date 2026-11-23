package selection

import (
	"slices"

	"github.com/home-operations/flate/pkg/manifest"
)

// Request containers MUST remain immutable for the invocation.
type Request struct {
	Roots   []manifest.NamedResource
	Changed bool
	Labels  map[string]string
}

// AllRoots reports whether all root identities are eligible before change filtering.
func (s Request) AllRoots() bool {
	return len(s.Roots) == 0 || s.Roots[0].Namespace == "" && s.includesRenderKinds(true)
}

// RestrictedPackageLookup reports whether a render request limits names or namespaces.
func (s Request) RestrictedPackageLookup() bool {
	return slices.ContainsFunc(s.Roots, func(root manifest.NamedResource) bool {
		return (root.Name != "" || !s.Changed && root.Namespace != "") && root.KindInfo().Render
	})
}

// Matches reports whether an identity is eligible under the requested roots.
// Namespace restrictions retain cluster-scoped output; complete render-kind requests include standalone output.
func (s Request) Matches(id manifest.NamedResource) bool {
	return s.matches(id, true)
}

// MatchesScope reports whether a kind and namespace belong to the requested output scope.
func (s Request) MatchesScope(id manifest.NamedResource) bool {
	return s.matches(id, false)
}

func (s Request) matches(id manifest.NamedResource, named bool) bool {
	if len(s.Roots) == 0 {
		return true
	}
	for _, root := range s.Roots {
		if rootMatchesIdentity(root, id, named) {
			return true
		}
	}
	if s.includesRenderKinds(named) {
		return s.Roots[0].Namespace == "" || id.Namespace == "" || id.Namespace == s.Roots[0].Namespace
	}

	return false
}

func rootMatchesIdentity(root, id manifest.NamedResource, named bool) bool {
	groupAndKindMatch := root.Group == id.Group && root.Kind == id.Kind
	nameMatches := !named || root.Name == "" || root.Name == id.Name
	exactNamespace := named && root.Name != "" && id.KindInfo().Namespaced()
	namespaceMatches := root.Namespace == "" || root.Namespace == id.Namespace || id.Namespace == "" && !exactNamespace
	return groupAndKindMatch && nameMatches && namespaceMatches
}

func (s Request) includesRenderKinds(named bool) bool {
	all := !named || s.Roots[0].Name == ""
	for _, root := range s.Roots {
		all = all && (!named || root.Name == "") && root.Namespace == s.Roots[0].Namespace
	}
	if !all {
		return false
	}
	for kind := range manifest.Kinds() {
		if kind.Render &&
			!slices.ContainsFunc(
				s.Roots,
				func(root manifest.NamedResource) bool { return root.Group == kind.Group && root.Kind == kind.Kind },
			) {
			return false
		}
	}
	return true
}
