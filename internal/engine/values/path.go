package values

import (
	"github.com/fluxcd/pkg/envsubst"

	"github.com/home-operations/flate/pkg/manifest"
)

// ResolveStaticValue reports absent input only when the scalar remains unknown.
//
//nolint:revive // Input policy and inherited-name uncertainty stay explicit without a one-caller options type.
func ResolveStaticValue(
	value string,
	parent *manifest.Kustomization,
	provider provider,
	missing missingPolicy,
	unknownInputs bool,
) (resolved string, known, needsInput bool) { //nolint:nonamedreturns // The names distinguish the two boolean results.
	parent = new(*parent)
	if parent.PostBuild != nil {
		parent.PostBuild = new(*parent.PostBuild)
	}
	absentInput := false
	var inline map[string]string
	var inputs []Result
	if parent.PostBuild != nil {
		inline = parent.PostBuild.Substitute
		for result := range SubstitutionInputs(parent, provider, missing) {
			unknownInputs = unknownInputs || !result.StaticCertain()
			absentInput = absentInput || result.NeedsInput()
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
	if !SubstitutionActive(parent, unknownInputs) {
		return value, true, false
	}
	incomplete := false
	resolved, err := envsubst.Eval(value, func(name string) (string, bool) {
		value, found := variables[name]
		_, fixed := inline[name]
		incomplete = incomplete || unknownInputs && !fixed
		return value, found
	})
	known = err == nil && !incomplete
	return resolved, known, !known && absentInput
}
