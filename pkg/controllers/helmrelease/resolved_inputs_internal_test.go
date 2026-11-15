package helmrelease

import (
	"github.com/home-operations/flate/pkg/depwait"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/values"
)

func resolveTestInputs(
	obj manifest.BaseManifest,
	provider values.Provider,
	missing *depwait.MissingPolicy,
) []values.Result {
	var inputs []values.Result
	for ref := range manifest.References(obj, manifest.ReferenceRelations) {
		if ref.Use != "valuesFrom" && ref.Use != "substituteFrom" {
			continue
		}
		inputs = append(
			inputs,
			values.Resolve(
				&values.ResolveRequest{Reference: ref, Facts: provider.Lookup(ref.NamedResource), Permission: missing},
			),
		)
	}
	return inputs
}
