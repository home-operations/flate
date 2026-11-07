package discovery

import (
	"context"
	"slices"

	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

func (w *qualificationWalk) follow(
	ctx context.Context,
	batch *qualificationBatch,
	owner manifest.NamedResource,
) (bool, error) {
	children := slices.Clone(batch.children)
	slices.SortFunc(children, func(a, b store.Child) int { return a.Obj.Named().Compare(b.Obj.Named()) })
	continuations := make([]applicationEvidence, 0, len(children))
	for i := range children {
		child := &children[i]
		if child.Obj == nil {
			continue
		}
		evidence := w.followedChild(batch, child, owner)
		if w.q.pending {
			return false, nil
		}
		if evidence == nil {
			continue
		}
		matched, err := w.matchChild(ctx, batch, evidence)
		if err != nil || w.q.pending {
			return false, err
		}
		if !matched {
			continuations = append(continuations, *evidence)
		}
	}
	if len(w.q.matches) != 0 {
		return true, nil
	}
	if batch.root != "" &&
		owner == manifest.NewEntryKustomization(manifest.BootstrapSourceID, w.admission.entryPath(), manifest.NamedResource{}).
			Named() {
		return false, nil
	}
	for i := range continuations {
		found, err := w.visit(ctx, &continuations[i])
		if err != nil || found || w.q.pending {
			return found, err
		}
	}
	return false, nil
}

func (w *qualificationWalk) followedChild(
	batch *qualificationBatch,
	child *store.Child,
	owner manifest.NamedResource,
) *applicationEvidence {
	evidence := &applicationEvidence{
		object:    child.Obj,
		document:  child.Doc,
		owner:     owner,
		builtRoot: batch.root,
		child:     child,
	}
	if batch.root != "" {
		if current := w.reads.GetDeclaration(child.Obj.Named()); current != nil {
			accepted := &applicationEvidence{object: current}
			w.captureOwner(accepted)
			if accepted.owner != (manifest.NamedResource{}) {
				return accepted
			}
		}
		return evidence
	}
	accepted, pending := evidence.ownerAccepts(w.reads)
	if !accepted {
		w.q.pending = w.q.pending || pending
		return nil
	}
	return evidence
}

func (w *qualificationWalk) matchChild(
	ctx context.Context,
	batch *qualificationBatch,
	evidence *applicationEvidence,
) (bool, error) {
	eligible, err := w.admission.readApplication(ctx, w.store, w.reads, evidence)
	if err != nil || !eligible {
		return false, err
	}
	if batch.root != "" {
		if candidate, ok := evidence.object.(*manifest.Kustomization); ok &&
			w.admission.reuseEntry(ctx, w.reads, evidence, candidate) {
			current, err := w.admitConstructed(ctx, evidence)
			if err != nil || !current {
				return false, err
			}
		}
	}
	evidence.root = w.q.root
	w.q.matches = append(w.q.matches, *evidence)
	return true, nil
}
