package discovery

import (
	"cmp"
	"context"
	"slices"

	"github.com/home-operations/flate/internal/engine/change/inputprojection"
	"github.com/home-operations/flate/internal/engine/selection"
	"github.com/home-operations/flate/internal/engine/source/safepath"
	"github.com/home-operations/flate/internal/engine/store"
	"github.com/home-operations/flate/pkg/manifest"
)

func (w *qualificationWalk) advanceOperands(ctx context.Context) error {
	q := w.q
	if err := w.observe(ctx); err != nil {
		return err
	}
	if q.pending {
		return nil
	}
	q.operandsSettled = false
	if q.session != nil && !q.session.finalized {
		q.pending = true
		return nil
	}
	before := len(q.active)
	for _, operands := range q.packages {
		if q.changedOnly && len(operands) > 1 {
			ranked, err := w.rankEntryOperands(ctx, operands)
			if err != nil {
				return err
			}
			operands = ranked
		}
		w.admitOperandPackage(operands)
	}
	q.packages = nil
	if len(q.active) > before {
		if q.changedOnly {
			q.candidates = []manifest.NamedResource{q.active[before]}
		} else {
			q.candidates = slices.Clone(q.active[before:])
		}
		q.pending = true
		return nil
	}
	failed, err := w.visitActiveOperands(ctx)
	if err != nil || q.pending {
		return err
	}
	q.operandsSettled = true
	if failed != (manifest.NamedResource{}) && len(q.matches) == 0 {
		q.pending = true
		q.candidates = []manifest.NamedResource{failed}
		w.reads.RequireQualification(failed)
		return nil
	}
	return nil
}

func (w *qualificationWalk) rankEntryOperands(
	ctx context.Context, operands []selection.OccurrenceFacts,
) ([]selection.OccurrenceFacts, error) {
	hints := make(map[manifest.NamedResource]int)
	for i := range operands {
		operand := &operands[i]
		_, target, applies := operand.SourcePath()
		if !applies || !operand.Identity.KindInfo().Structural {
			continue
		}
		hint, err := w.q.loader.EntryProducerHint(ctx, w.admission.input, target)
		if err != nil {
			return nil, err
		}
		if hint {
			hints[operand.Identity] = 1
		}
	}
	if len(hints) == 0 {
		return operands, nil
	}
	ranked := slices.Clone(operands)
	slices.SortStableFunc(ranked, func(a, b selection.OccurrenceFacts) int {
		return cmp.Compare(hints[b.Identity], hints[a.Identity])
	})
	return ranked, nil
}

func (w *qualificationWalk) visitActiveOperands(ctx context.Context) (manifest.NamedResource, error) {
	q := w.q
	var failed manifest.NamedResource
	for _, id := range q.active {
		if q.changedOnly && w.coveredOperand(id) {
			continue
		}
		obj := w.reads.GetDeclaration(id)
		if obj == nil || obj.Suspended() {
			continue
		}
		status, _ := w.reads.GetStatus(id)
		switch status.Outcome() {
		case store.OutcomeFailed, store.OutcomeBlocked:
			if failed == (manifest.NamedResource{}) {
				failed = id
			}
			continue
		case store.OutcomeSkipped, store.OutcomeSuspended:
			continue
		case store.OutcomePending, store.OutcomePassed, store.OutcomeUnchanged:
		}
		if q.session != nil {
			q.pending = true
			q.candidates = []manifest.NamedResource{id}
			break
		}
		q.root = id
		evidence := applicationEvidence{object: obj, root: id}
		w.captureOwner(&evidence)
		_, err := w.visit(ctx, &evidence)
		if err != nil || q.pending {
			return failed, err
		}
	}
	return failed, nil
}

func (w *qualificationWalk) coveredOperand(id manifest.NamedResource) bool {
	if !id.KindInfo().Structural || len(w.q.matches) == 0 {
		return false
	}
	record, _ := w.q.loader.Index.Lookup(id)
	if record.Path == "" || !safepath.Contains(w.admission.input.LocalPath, record.Path) {
		return false
	}
	file := w.admission.input.OriginPath(record.Path)
	for i := range w.q.matches {
		match := &w.q.matches[i]
		owner, ok := match.object.(*manifest.Kustomization)
		if !ok || !match.visibleGeneration || match.source.artifact == nil {
			continue
		}
		output, accepted := acceptedApplicationOutput(w.reads, owner.Named())
		if accepted && slices.ContainsFunc(output.Children, func(child store.Child) bool {
			return child.Obj != nil && child.Obj.Named() == id
		}) && w.q.loader.EntryPackageReached(match.source.artifact, match.application.Path, owner, file) {
			return true
		}
	}
	return false
}

func (w *qualificationWalk) visitPublishedRoots(ctx context.Context) error {
	q := w.q
	roots := q.loader.EntryRoots(q.published(w.store, w.admission), w.reads)
	for i := range roots {
		evidence, err := w.root(ctx, &roots[i])
		if err != nil || q.pending {
			return err
		}
		if evidence == nil {
			continue
		}
		q.root = evidence.object.Named()
		matched, err := w.visit(ctx, evidence)
		if err != nil || matched || q.pending {
			return err
		}
	}
	if q.closed || !w.admission.entryDemand {
		return nil
	}
	inventories, err := w.admission.generatorInventories(ctx)
	if err != nil {
		return err
	}
	for _, frame := range inventories {
		batch := qualificationBatch{children: frame.children, private: true}
		matched, err := w.follow(ctx, &batch, manifest.NamedResource{})
		if err != nil || matched || q.pending {
			return err
		}
	}
	return nil
}

func (a *Admission) generatorInventories(ctx context.Context) ([]*declarationRender, error) {
	// An enclosing aggregate applies its transformations after the nested package builds.
	for _, dir := range slices.Backward(a.generatorPackages) {
		owner := manifest.NewEntryKustomization(manifest.BootstrapSourceID, dir, manifest.NamedResource{})
		frame, err := a.packageInventory(ctx, owner, inputprojection.Authored(a.bootstrapArtifact()))
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		if err != nil {
			continue
		}
		var generators []store.Child
		for i := range frame.children {
			child := &frame.children[i]
			if _, applies := manifest.ApplicationFor(child.Obj); applies && !manifest.KindOf(child.Obj).Runnable {
				generators = append(generators, *child)
			}
		}
		a.privateInventories = append(a.privateInventories, &declarationRender{owner: owner, children: generators})
	}
	a.generatorPackages = nil
	return a.privateInventories, ctx.Err()
}

// Physical package members start work without publishing an aggregate.
func (w *qualificationWalk) admitOperandPackage(operands []selection.OccurrenceFacts) {
	for i := range operands {
		operand := &operands[i]
		obj := occurrenceObject(operand)
		if obj == nil || obj.Suspended() || operand.RequiresBuild {
			continue
		}
		if w.store.GetDeclaration(obj.Named()) == nil {
			w.reads.AdmitOperand(obj, manifest.BootstrapSourceID)
		}
		if manifest.KindOf(obj).Parent {
			w.q.active = append(w.q.active, obj.Named())
		}
	}
}
