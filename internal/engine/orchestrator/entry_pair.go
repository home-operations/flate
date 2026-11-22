package orchestrator

import (
	"cmp"
	"context"

	"github.com/home-operations/flate/internal/engine/change"
	"github.com/home-operations/flate/internal/engine/change/inputprojection"
	"github.com/home-operations/flate/internal/engine/selection"
	"github.com/home-operations/flate/internal/engine/store"
	"github.com/home-operations/flate/pkg/manifest"
)

func compareEntryDeclarations(ctx context.Context, trees [2]*Orchestrator) (*change.Set, error) {
	stores := [2]*store.Store{trees[0].store, trees[1].store}
	work := trees[0].cfg.work.Scope()
	indexes := [2]*manifest.DiscoveryIndex{trees[0].index, trees[1].index}
	pair := change.SourcePair{Stores: stores, Indexes: indexes}
	changes, err := change.Qualify(ctx, trees[0].cfg.ExternalChanges, pair, work)
	if err != nil {
		return nil, err
	}
	changes, err = changes.WithReaders(
		ctx,
		indexes,
		[2]map[manifest.NamedResource]inputprojection.Projection{
			trees[0].loader.InputContent, trees[1].loader.InputContent,
		},
		work,
	)
	if err != nil {
		return nil, err
	}
	return changes.WithDeclarations(ctx, change.DeclarationInputs{Stores: stores, Work: work})
}

func (o *Orchestrator) admitEntryComparison(
	ctx context.Context, output *store.Emission, changes *change.Set,
) error {
	o.selection.ResolveEntryComparison(changes != nil)
	if output == nil {
		return nil
	}
	o.cfg.ExternalChanges = cmp.Or(changes, o.cfg.ExternalChanges)
	result, accepted, err := o.prepareAdmission(ctx, selection.AdmissionOptions{
		Request: selection.PrepareRequest{
			Invocation: true,
			Owner:      o.entryBinding.Producer,
			Generation: output.Children,
		},
		Authority: output.OutputReads,
		Stage:     selection.Execution,
	})
	if err != nil || !accepted {
		return err
	}
	o.recordAdmission(&result)
	return o.seedAndDrain(ctx)
}
