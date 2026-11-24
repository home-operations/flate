package orchestrator

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"
	"sync"

	"github.com/home-operations/flate/internal/engine/change"
	"github.com/home-operations/flate/internal/engine/selection"
	"github.com/home-operations/flate/internal/engine/store"
	"github.com/home-operations/flate/pkg/manifest"
)

type pairedDAG struct {
	done func()
	err  error
}

func (r *Invocation) renderPairs(ctx context.Context) map[int]bool {
	var paired map[int]bool
	for i := range r.results {
		if paired[i] {
			continue
		}
		result := &r.results[i]
		o := result.Orchestrator
		if o == nil {
			continue
		}
		other, j, _ := r.opposite(i)
		if other == nil {
			if o.selection.EntryComparisonPending() {
				o.selection.ResolveEntryComparison(false)
			}
			continue
		}
		if result.ID.Side != manifest.TreeBase {
			continue
		}
		entry := o.selection.EntryComparisonPending() || other.selection.EntryComparisonPending()
		owners := o.orderingPairs
		if !entry && len(owners) == 0 {
			continue
		}
		if paired == nil {
			paired = make(map[int]bool)
		}
		paired[i], paired[j] = true, true
		r.wg.Go(func() { r.renderPair(ctx, [2]int{i, j}, owners, entry) })
	}
	return paired
}

func (r *Invocation) prepareOrderingComparison(ctx context.Context, base, head *Orchestrator) error {
	comparisonOnly := r.Config.HeadOnly && base.cfg.Selection.Changed
	var owners [][2]manifest.NamedResource
	if head != nil && base.cfg.ExternalChanges != nil && head.cfg.ExternalChanges != nil {
		var err error
		owners, err = orderingOwners(ctx, [2]*Orchestrator{base, head}, comparisonOnly)
		if err != nil {
			return err
		}
	}
	base.orderingPairs = owners
	if comparisonOnly {
		base.selection.SetOrderingComparison(orderingIDs(owners, 0))
	}
	return nil
}

func orderingOwners(
	ctx context.Context, trees [2]*Orchestrator, comparisonOnly bool,
) ([][2]manifest.NamedResource, error) {
	var transforming [2][]manifest.NamedResource
	for i, tree := range trees {
		tree.loader.Options.Changes = tree.cfg.ExternalChanges
		transforming[i] = tree.loader.SourceFacts(manifest.BootstrapSourceID, nil).Context.TransformingProducers
	}
	owners, err := pairedOrderingOwners(ctx, trees, transforming, false)
	if err != nil || !comparisonOnly || len(owners) == 0 {
		return owners, err
	}
	var changed [2]map[manifest.NamedResource]bool
	for i, tree := range trees {
		request := selection.PrepareRequest{
			OrderingCandidates: orderingIDs(owners, i),
		}
		tree.admissionRequest(&request)
		result, _, err := tree.selection.Admit(ctx, selection.AdmissionOptions{
			Request: request,
			Stage:   selection.Execution,
		})
		if err != nil {
			return nil, err
		}
		changed[i] = result.OrderingChanged
	}
	return slices.DeleteFunc(owners, func(pair [2]manifest.NamedResource) bool {
		return !changed[0][pair[0]] && !changed[1][pair[1]]
	}), nil
}

func orderingIDs(owners [][2]manifest.NamedResource, side int) []manifest.NamedResource {
	if len(owners) == 0 {
		return nil
	}
	ids := make([]manifest.NamedResource, 0, len(owners))
	for i := range owners {
		ids = append(ids, owners[i][side])
	}
	return ids
}

func (r *Invocation) renderPair(
	ctx context.Context, indexes [2]int, candidates [][2]manifest.NamedResource, entry bool,
) {
	trees := [2]*Orchestrator{r.results[indexes[0]].Orchestrator, r.results[indexes[1]].Orchestrator}
	workOnly := r.workOnlyBase(indexes[0])
	var tasks sync.WaitGroup
	var branches [2]pairedDAG
	for side, tree := range trees {
		tasks.Go(func() { tree.drainPairedDAG(ctx, &branches[side]) })
	}
	tasks.Wait()

	if entry {
		comparePairEntries(ctx, trees, &branches)
	}
	var enriched *change.Set
	if branches[0].err == nil && branches[1].err == nil && len(candidates) != 0 {
		enriched, branches[0].err = compareOrderingRemovals(ctx, trees, candidates, workOnly)
		branches[1].err = branches[0].err
	}
	var comparisonErr error
	for side, tree := range trees {
		branch := &branches[side]
		if workOnly && side == 0 {
			tasks.Go(func() {
				comparisonErr = tree.finishOrderingComparison(ctx, branch)
				r.collectTree(ctx, indexes[side], branch.err)
			})
			continue
		}
		tasks.Go(func() {
			if branch.done != nil {
				if enriched != nil {
					branch.err = errors.Join(branch.err, tree.admitEffectiveOrdering(ctx, enriched))
				}
				branch.err = tree.finishRun(ctx, branch.err)
				branch.done()
			}
			if !workOnly {
				r.collectTree(ctx, indexes[side], branch.err)
			}
		})
	}
	tasks.Wait()
	if workOnly {
		r.collectTree(ctx, indexes[1], errors.Join(branches[1].err, comparisonErr))
	}
}

func (o *Orchestrator) finishOrderingComparison(ctx context.Context, branch *pairedDAG) error {
	if branch.done == nil {
		return nil
	}
	defer branch.done()

	branch.err = o.finishDAG(ctx, branch.err)
	failed := o.store.FailedResources()
	decision := o.selection.Snapshot()
	maps.DeleteFunc(failed, func(id manifest.NamedResource, _ store.StatusInfo) bool {
		return !decision.Assess(id)
	})
	if branch.err != nil {
		branch.err = errors.Join(branch.err, o.reconcileFailures(o.aggregateFailures(failed)))
		return nil
	}
	var cause error
	if len(failed) != 0 {
		// The work-only base has no resource rows for selection to reconstruct.
		cause = manifest.NewError(manifest.ErrFlux, nil, o.failureMessage(failed))
	}
	if failure := o.reconcileFailures(cause); failure != nil {
		return fmt.Errorf("compare base ordering outputs: %w", failure)
	}
	return nil
}

func compareOrderingRemovals(
	ctx context.Context, trees [2]*Orchestrator, candidates [][2]manifest.NamedResource, comparisonOnly bool,
) (*change.Set, error) {
	base, head := trees[0], trees[1]
	settled, err := pairedOrderingOwners(ctx, trees, [2][]manifest.NamedResource{orderingIDs(candidates, 0)}, true)
	if err != nil {
		return nil, err
	}
	before, after := base.selection.Snapshot(), head.selection.Snapshot()
	owners := slices.DeleteFunc(settled, func(pair [2]manifest.NamedResource) bool {
		if !slices.Contains(candidates, pair) || !before.Reconcile(pair[0]) || !after.Reconcile(pair[1]) {
			return true
		}
		return !comparisonOnly && !before.Changed(pair[0]) && !after.Changed(pair[1])
	})
	changes := base.cfg.ExternalChanges
	enriched := changes.WithEffectiveRemovals(
		[2]*store.Store{base.store, head.store}, owners,
		func(side int, id manifest.NamedResource) string { return trees[side].loader.IndexedDeclarationFile(id) },
	)
	if enriched == changes {
		return nil, nil //nolint:nilnil // No effective removal leaves the admission input unchanged.
	}
	return enriched, nil
}

func comparePairEntries(ctx context.Context, trees [2]*Orchestrator, branches *[2]pairedDAG) {
	var outputs [2]*store.Emission
	for side, tree := range trees {
		output, _ := tree.store.GetArtifact(tree.entryBinding.Producer, store.ArtifactContent).(*store.Emission)
		if output != nil && output.Complete && output.OutputReads.Valid() {
			outputs[side] = output
		}
	}
	var changes *change.Set
	var compareErr error
	if outputs[0] != nil && outputs[1] != nil {
		changes, compareErr = compareEntryDeclarations(ctx, trees)
	}
	var tasks sync.WaitGroup
	for side, tree := range trees {
		tasks.Go(func() {
			branch := &branches[side]
			branch.err = errors.Join(branch.err, compareErr)
			if branch.err != nil {
				tree.selection.ResolveEntryComparison(false)
				return
			}
			branch.err = tree.admitEntryComparison(ctx, outputs[side], changes)
		})
	}
	tasks.Wait()
}

func pairedOrderingOwners(
	ctx context.Context, trees [2]*Orchestrator, candidates [2][]manifest.NamedResource, settled bool,
) ([][2]manifest.NamedResource, error) {
	var packages orderingOwnerIndexes
	var owners [][2]manifest.NamedResource
	for side, tree := range trees {
		peerSide := 1 - side
		peerTree := trees[peerSide]
		for _, id := range candidates[side] {
			obj := tree.orderingDeclaration(id, settled)
			if obj == nil {
				continue
			}
			peer := id
			if peerTree.orderingDeclaration(id, false) == nil {
				var err error
				peer, err = packages.renamedOwner(ctx, trees, side, obj)
				if err != nil {
					return nil, err
				}
			}
			if peer == (manifest.NamedResource{}) || settled && peerTree.store.GetDeclaration(peer) == nil {
				continue
			}
			var pair [2]manifest.NamedResource
			pair[side], pair[peerSide] = id, peer
			owners = append(owners, pair)
		}
	}
	slices.SortFunc(owners, func(a, b [2]manifest.NamedResource) int {
		if order := a[0].Compare(b[0]); order != 0 {
			return order
		}
		return a[1].Compare(b[1])
	})
	return slices.Compact(owners), nil
}

func (o *Orchestrator) orderingDeclaration(id manifest.NamedResource, settled bool) manifest.BaseManifest {
	declaration := o.store.GetDeclaration(id)
	if declaration == nil && !settled {
		return o.loader.CatalogObject(id)
	}
	return declaration
}

type orderingOwnerIndexes [2]map[orderingPackage]manifest.NamedResource

func (p *orderingOwnerIndexes) renamedOwner(
	ctx context.Context, trees [2]*Orchestrator, side int, object manifest.BaseManifest,
) (manifest.NamedResource, error) {
	source, packagePath, applies := object.SourcePath()
	if !applies {
		return manifest.NamedResource{}, nil
	}
	tree := trees[side]
	var reads store.ReadSet
	reads.Init(tree.store, manifest.NamedResource{}, nil)
	defer reads.Retire()
	key, available, err := tree.orderingPackageKey(ctx, &reads, source, packagePath)
	if err != nil || !available {
		return manifest.NamedResource{}, err
	}
	if p[side] == nil {
		for i, tree := range trees {
			p[i], err = tree.orderingPackageOwners(ctx, tree.loader.PathObjects())
			if err != nil {
				return manifest.NamedResource{}, err
			}
		}
	}
	peer := p[1-side][key]
	if p[side][key] != object.Named() || peer == (manifest.NamedResource{}) ||
		tree.orderingDeclaration(peer, false) != nil {
		return manifest.NamedResource{}, nil
	}
	return peer, nil
}

type orderingPackage struct {
	source manifest.NamedResource
	path   string
}

func (o *Orchestrator) orderingPackageKey(
	ctx context.Context, reads *store.ReadSet, source manifest.NamedResource, packagePath string,
) (orderingPackage, bool, error) {
	resolved, available, err := o.loader.ResolveRoot(ctx, reads, source, cmp.Or(packagePath, "."))
	return orderingPackage{source, resolved}, available, err
}

func (o *Orchestrator) orderingPackageOwners(
	ctx context.Context, objects []selection.OccurrenceFacts,
) (map[orderingPackage]manifest.NamedResource, error) {
	owners := make(map[orderingPackage]manifest.NamedResource, len(objects))
	var reads store.ReadSet
	reads.Init(o.store, manifest.NamedResource{}, nil)
	defer reads.Retire()
	for i := range objects {
		obj := &objects[i]
		source, packagePath, applies := obj.SourcePath()
		if !applies {
			continue
		}
		key, available, err := o.orderingPackageKey(ctx, &reads, source, packagePath)
		if err != nil {
			return nil, err
		}
		if !available {
			continue
		}
		if prior, exists := owners[key]; exists && prior != obj.Identity {
			owners[key] = manifest.NamedResource{}
			continue
		}
		owners[key] = obj.Identity
	}
	return owners, nil
}

func (o *Orchestrator) drainPairedDAG(ctx context.Context, branch *pairedDAG) {
	unwatch := o.watchRun(ctx)
	done, err := o.beginDAG(ctx)
	if err != nil {
		unwatch()
		branch.err = err
		return
	}
	branch.done = func() {
		done()
		unwatch()
	}
	branch.err = o.scheduler.Drain(ctx)
}

func (o *Orchestrator) admitEffectiveOrdering(ctx context.Context, enriched *change.Set) error {
	o.cfg.ExternalChanges = enriched
	_, accepted, err := o.prepareAdmission(ctx, selection.AdmissionOptions{
		Request: selection.PrepareRequest{Invocation: true},
		Stage:   selection.Execution,
	})
	if err != nil || !accepted {
		return err
	}
	return o.seedAndDrain(ctx)
}

func (o *Orchestrator) seedAndDrain(ctx context.Context) error {
	seeds, err := o.seedNodes()
	if err != nil {
		return err
	}
	o.scheduler.Seed(seeds)
	return o.scheduler.Drain(ctx)
}
