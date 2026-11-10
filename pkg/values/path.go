package values

import (
	"github.com/fluxcd/pkg/envsubst"

	"github.com/home-operations/flate/pkg/manifest"
)

// ResolveStaticPath resolves a produced path from declared parent substitutions.
func ResolveStaticPath(
	path string,
	parent *manifest.Kustomization,
	provider Provider,
	missing missingPolicy,
) (string, bool) {
	parent = parent.Clone()
	unknownInputs := false
	var inline map[string]string
	if parent.PostBuild != nil {
		inline = parent.PostBuild.Substitute
		for _, ref := range parent.PostBuild.SubstituteFrom {
			id := manifest.NamedResource{Kind: ref.Kind, Namespace: parent.Namespace, Name: ref.Name}
			unknownInputs = unknownInputs || provider.Data(id) == nil
		}
	}
	if err := ExpandPostBuildSubstituteReference(parent, provider, missing); err != nil {
		return path, false
	}
	var variables map[string]string
	if parent.PostBuild != nil {
		variables = manifest.VarsMap(parent.PostBuild.Substitute)
	}
	if !SubstitutionActive(parent) && !unknownInputs {
		return path, true
	}
	incomplete := false
	resolved, err := envsubst.Eval(path, func(name string) (string, bool) {
		value, known := variables[name]
		_, fixed := inline[name]
		incomplete = incomplete || unknownInputs && !fixed
		return value, known
	})
	return resolved, err == nil && !incomplete
}
