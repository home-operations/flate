package discovery

import (
	"context"
	"slices"

	"github.com/home-operations/flate/internal/engine/change/inputprojection"
	"github.com/home-operations/flate/internal/engine/selection"
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
		w.admitOperandPackage(operands)
	}
	q.packages = nil
	if len(q.active) > before {
		q.candidates = slices.Clone(q.active[before:])
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

func (w *qualificationWalk) visitActiveOperands(ctx context.Context) (manifest.NamedResource, error) {
	q := w.q
	var failed manifest.NamedResource
	for _, id := range q.active {
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
	inventories, err := w.admission.enclosingInventories(ctx)
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

func (a *Admission) enclosingInventories(ctx context.Context) ([]*declarationRender, error) {
	// An enclosing aggregate applies its transformations after the nested package builds.
	for _, dir := range slices.Backward(a.enclosingPackages) {
		owner := manifest.NewEntryKustomization(manifest.BootstrapSourceID, dir, manifest.NamedResource{})
		frame, err := a.packageInventory(ctx, owner, inputprojection.Authored(a.input))
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
	a.enclosingPackages = nil
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
