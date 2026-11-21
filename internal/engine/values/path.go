package values

import (
	"github.com/fluxcd/pkg/envsubst"

	"github.com/home-operations/flate/pkg/manifest"
)

// ResolveStaticValue never certifies a substitution that depends on unread input.
func ResolveStaticValue(
	value string,
	parent *manifest.Kustomization,
	provider provider,
	missing missingPolicy,
) (string, bool) {
	parent = new(*parent)
	if parent.PostBuild != nil {
		parent.PostBuild = new(*parent.PostBuild)
	}
	unknownInputs := false
	var inline map[string]string
	var inputs []Result
	if parent.PostBuild != nil {
		inline = parent.PostBuild.Substitute
		for ref := range manifest.References(parent, manifest.ReferenceRelations) {
			if keyed, data := ref.DataInput(); !data || keyed {
				continue
			}
			result := Resolve(
				&ResolveRequest{Reference: ref, Facts: provider.Lookup(ref.NamedResource), Permission: missing},
			)
			unknownInputs = unknownInputs || !result.StaticCertain()
			inputs = append(inputs, result)
		}
	}
	if err := ExpandPostBuildSubstituteReference(parent, inputs); err != nil {
		if parent.PostBuild != nil {
			parent.PostBuild.Substitute = inline
		}
		unknownInputs = true
	}
	var variables map[string]string
	if parent.PostBuild != nil {
		variables = manifest.VarsMap(parent.PostBuild.Substitute)
	}
	if !staticSubstitutionActive(parent, unknownInputs) {
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
