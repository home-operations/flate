package values

import (
	"github.com/fluxcd/pkg/envsubst"

	"github.com/home-operations/flate/pkg/manifest"
)

// ResolveStaticValue never certifies a substitution that depends on unread input.
func ResolveStaticValue(
	value string,
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
			result, err := Classify(
				new(manifest.Reference{NamedResource: id, Use: substitutionUse}),
				new(provider.Lookup(id)),
			)
			unknownInputs = unknownInputs || err != nil || result.State() != Readable
		}
	}
	if err := ExpandPostBuildSubstituteReference(parent, provider, missing); err != nil {
		return value, false
	}
	var variables map[string]string
	if parent.PostBuild != nil {
		variables = manifest.VarsMap(parent.PostBuild.Substitute)
	}
	if !SubstitutionActive(parent) && !unknownInputs {
		return value, true
	}
	incomplete := false
	resolved, err := envsubst.Eval(value, func(name string) (string, bool) {
		value, known := variables[name]
		_, fixed := inline[name]
		incomplete = incomplete || unknownInputs && !fixed
		return value, known
	})
	return resolved, err == nil && !incomplete
}
