package discovery

import (
	"context"
	"slices"

	"github.com/home-operations/flate/internal/engine/store"
	"github.com/home-operations/flate/pkg/manifest"
)

func (w *qualificationWalk) follow(
	ctx context.Context,
	batch *qualificationBatch,
	owner manifest.NamedResource,
) (bool, error) {
	previousMatches := len(w.q.matches)
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
		if !matched && !batch.private {
			continuations = append(continuations, *evidence)
		}
	}
	if len(w.q.matches) > previousMatches {
		return true, nil
	}
	if batch.root != "" &&
		owner == manifest.NewEntryKustomization(manifest.BootstrapSourceID, w.admission.entryPath(), manifest.NamedResource{}).
			Named() {
		continuations = w.nativeContinuations(continuations)
	}
	for i := range continuations {
		found, err := w.visit(ctx, &continuations[i])
		if err != nil || found || w.q.pending {
			return found, err
		}
	}
	return false, nil
}

func (w *qualificationWalk) nativeContinuations(candidates []applicationEvidence) []applicationEvidence {
	// A history request can need the native inventory without demanding its applications.
	if w.q.session != nil && w.q.session.entryReceiptRequest != nil && !w.q.session.entryDemand {
		return nil
	}
	projections := w.q.loader.SourceFacts(manifest.BootstrapSourceID, w.reads).Context.TemplateProjections
	return slices.DeleteFunc(candidates, func(evidence applicationEvidence) bool {
		kind := manifest.KindOf(evidence.object)
		if !kind.Parent || kind.Structural {
			return true
		}
		objects := projections[evidence.object.Named()].Objects
		return !slices.ContainsFunc(objects, func(object manifest.BaseManifest) bool {
			_, applies := manifest.ApplicationFor(object)
			return applies
		})
	})
}

func (w *qualificationWalk) followedChild(
	batch *qualificationBatch,
	child *store.Child,
	owner manifest.NamedResource,
) *applicationEvidence {
	evidence := &applicationEvidence{
		object:            child.Obj,
		document:          child.Doc,
		owner:             owner,
		builtRoot:         batch.root,
		child:             child,
		visibleGeneration: child.VisibleGeneration,
		private:           batch.private,
	}
	if batch.root != "" || batch.private {
		if current := w.reads.GetDeclaration(child.Obj.Named()); current != nil {
			accepted := &applicationEvidence{object: current}
			w.captureOwner(accepted)
			if accepted.owner != (manifest.NamedResource{}) {
				return accepted
			}
		}
		return evidence
	}
	for i := range child.Productions {
		production := &child.Productions[i]
		if production.Owner == owner && production.Declaration != (manifest.NamedResource{}) {
			evidence.declaration = production.Declaration
			w.reads.GetDeclaration(evidence.declaration)
			break
		}
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
	if batch.private {
		evidence.root = evidence.object.Named()
		if evidence.private {
			if slices.ContainsFunc(w.probed, func(known manifest.BaseManifest) bool {
				return manifest.Equal(known, evidence.object)
			}) {
				return false, nil
			}
			w.probed = append(w.probed, evidence.object)
		}
		return w.visit(ctx, evidence)
	}
	if !manifest.KindOf(evidence.object).Runnable {
		if batch.root != "" {
			return w.visitGeneration(ctx, evidence)
		}
		return false, nil
	}
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
