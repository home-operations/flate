package orchestrator

import (
	"github.com/home-operations/flate/internal/engine/depwait"
	"github.com/home-operations/flate/internal/engine/store"
	"github.com/home-operations/flate/internal/engine/values"
	"github.com/home-operations/flate/pkg/manifest"
)

func resolveHistoryInputs(
	obj manifest.BaseManifest,
	provider interface {
		Lookup(manifest.NamedResource) store.ReferenceFacts
	},
	missing *depwait.MissingPolicy,
) []values.Result {
	var inputs []values.Result
	for ref := range manifest.References(obj, manifest.ReferenceRelations) {
		if ref.Use == "valuesFrom" || ref.Use == "substituteFrom" {
			inputs = append(
				inputs,
				values.Resolve(
					&values.ResolveRequest{
						Reference:  ref,
						Facts:      provider.Lookup(ref.NamedResource),
						Permission: missing,
					},
				),
			)
		}
	}
	return inputs
}
