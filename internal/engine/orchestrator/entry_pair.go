package orchestrator

import (
	"cmp"
	"context"
	"errors"
	"sync"

	"github.com/home-operations/flate/internal/engine/change"
	"github.com/home-operations/flate/internal/engine/change/inputprojection"
	"github.com/home-operations/flate/internal/engine/selection"
	"github.com/home-operations/flate/internal/engine/store"
	"github.com/home-operations/flate/pkg/manifest"
)

func (r *Invocation) renderEntryPairs(ctx context.Context) map[int]bool {
	var pairs [][2]int
	var paired map[int]bool
	for i := range r.results {
		tree := r.results[i].Orchestrator
		if tree == nil || !tree.selection.EntryComparisonPending() {
			continue
		}
		peer, j, _ := r.opposite(i)
		if peer == nil {
			tree.selection.ResolveEntryComparison(false)
			continue
		}
		if tree.cfg.ID.Side != manifest.TreeBase {
			continue
		}
		if paired == nil {
			paired = map[int]bool{}
		}
		pairs = append(pairs, [2]int{i, j})
		paired[i], paired[j] = true, true
	}
	for _, pair := range pairs {
		r.wg.Go(func() { r.renderEntryPair(ctx, pair) })
	}
	return paired
}

func (r *Invocation) renderEntryPair(ctx context.Context, pair [2]int) {
	trees := [2]*Orchestrator{r.results[pair[0]].Orchestrator, r.results[pair[1]].Orchestrator}
	var tasks sync.WaitGroup
	var failures [2]error
	var outputs [2]*store.Emission
	for side, tree := range trees {
		tasks.Go(func() {
			failures[side] = tree.run(ctx)
			output, _ := tree.store.GetArtifact(tree.entryBinding.Producer, store.ArtifactContent).(*store.Emission)
			if output != nil && output.Complete && output.OutputReads.Valid() {
				outputs[side] = output
			}
		})
	}
	tasks.Wait()

	var changes *change.Set
	var compareErr error
	if outputs[0] != nil && outputs[1] != nil {
		changes, compareErr = compareEntryDeclarations(ctx, trees)
	}
	for side, tree := range trees {
		tasks.Go(func() {
			defer tree.stop()
			err := errors.Join(failures[side], compareErr)
			r.collectTree(ctx, pair[side], tree.finishEntryRender(ctx, outputs[side], changes, err))
		})
	}
	tasks.Wait()
}

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

func (o *Orchestrator) finishEntryRender(
	ctx context.Context, output *store.Emission, changes *change.Set, err error,
) error {
	o.selection.ResolveEntryComparison(changes != nil)
	prior := o.selection.Snapshot()
	if err == nil && output != nil {
		o.cfg.ExternalChanges = cmp.Or(changes, o.cfg.ExternalChanges)
		result, accepted, admissionErr := o.prepareAdmission(ctx, selection.AdmissionOptions{
			Request: selection.PrepareRequest{
				Invocation: true,
				Owner:      o.entryBinding.Producer,
				Generation: output.Children,
			},
			Authority: output.OutputReads,
			Stage:     selection.Execution,
		})
		err = admissionErr
		if accepted {
			o.recordAdmission(&result)
		}
	}
	if err == nil && o.selection.Snapshot() != prior {
		return o.runAfter(ctx, prior)
	}
	o.inputs.Close()
	o.tasks.BlockTillDone()
	return o.finishDAG(err)
}
