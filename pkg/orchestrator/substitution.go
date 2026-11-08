package orchestrator

import (
	"cmp"
	"fmt"
	"maps"
	"slices"

	"github.com/home-operations/flate/internal/loader"
	"github.com/home-operations/flate/internal/store"
	"github.com/home-operations/flate/pkg/manifest"
)

func snapshotSubstitutions(cfg *Config) error {
	cfg.AllowMissingKinds = slices.Clone(cfg.AllowMissingKinds)
	if len(cfg.Supply) == 0 {
		return nil
	}
	inputs := make(map[manifest.NamedResource]store.Declaration, len(cfg.Supply))
	for _, input := range cfg.Supply {
		path := cmp.Or(input.InputPath, "SDK input")
		warnings := slices.Clone(input.Warnings)
		for i := range warnings {
			warnings[i].Detail = slices.Clone(warnings[i].Detail)
			warnings[i].Related = slices.Clone(warnings[i].Related)
		}
		child, outcome, _ := loader.Prepare(manifest.DocOccurrence{
			Document: manifest.DeepCopyMap(input.Doc),
			Ordinal:  input.Ordinal,
			Warnings: warnings,
		},
			manifest.ParseDocOptions{RequiredIdentity: true})
		if outcome.Err != nil || child.Obj == nil {
			return fmt.Errorf(
				"%w: supply %q document %d: invalid Kubernetes object",
				manifest.ErrInput,
				path,
				input.Ordinal+1,
			)
		}
		inputs[child.Obj.Named()] = store.Declaration{
			Obj:       child.Obj,
			Doc:       child.Doc,
			Origin:    manifest.SuppliedDeclaration,
			InputPath: path,
			Ordinal:   input.Ordinal,
			Warnings:  child.Warnings,
		}
	}
	cfg.Supply = make([]store.Declaration, 0, len(inputs))
	for _, id := range slices.SortedFunc(maps.Keys(inputs), manifest.NamedResource.Compare) {
		cfg.Supply = append(cfg.Supply, inputs[id])
	}
	return loader.ValidateDeclarations(cfg.Supply)
}
