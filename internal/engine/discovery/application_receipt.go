package discovery

import (
	"cmp"
	"context"
	"path/filepath"
	"slices"

	"github.com/home-operations/flate/internal/engine/kustomize"
	"github.com/home-operations/flate/internal/engine/store"
	"github.com/home-operations/flate/pkg/manifest"
)

func (d *Session) entryReceipt(ctx context.Context, a *Admission, input *store.SourceArtifact) {
	q := a.qualification
	if q.receiptRead {
		return
	}
	q.receiptRead = true
	relative, _ := filepath.Rel(input.LocalPath, d.cfg.Path)
	root := manifest.NewEntryKustomization(
		manifest.BootstrapSourceID,
		filepath.ToSlash(relative),
		manifest.NamedResource{},
	)
	a.entryRequest = kustomize.RenderRequest{
		Work:                d.cfg.Work.RenderWork(),
		EntryOccurrences:    a.entryOccurrences,
		Source:              input,
		SubPath:             root.Path,
		StrictSubstitutions: d.cfg.StrictSubstitutions,
		SkipKinds:           d.cfg.SkipKinds,
		Preparation:         &a.entryPreparation,
	}
	q.previous = d.cfg.Trees.PreviousEntry(ctx, a.entryRequest)
	a.publishEntry(nil, manifest.NamedResource{})
	for i := range q.previous.Inputs {
		input := &q.previous.Inputs[i]
		if input.Binding.Present {
			q.refresh = append(q.refresh, input.Resource)
		}
	}
}

//nolint:gocritic // Results distinguish accepted reuse from pending refresh or replay.
func (w *qualificationWalk) matchReceipt(
	ctx context.Context, evidence *applicationEvidence, execution manifest.BaseManifest,
) (bool, bool) {
	q := w.q
	if q.previous.Receipt == nil || len(q.matches) != 0 {
		return false, false
	}
	frontier := q.previous.Owner
	if execution == nil {
		if q.previous.Owner != cmp.Or(evidence.owner, evidence.declaration) ||
			!manifest.ValueEqual(q.previous.Application, evidence.document) {
			return false, false
		}
	} else if q.previous.Owner != execution.Named() {
		if q.previous.Root != evidence.object.Named() {
			return false, false
		}
		frontier = execution.Named()
	}
	return w.reusePrevious(ctx, frontier)
}

//nolint:gocritic // Results distinguish accepted reuse from pending refresh.
func (w *qualificationWalk) reusePrevious(ctx context.Context, frontier manifest.NamedResource) (bool, bool) {
	q, a, reads := w.q, w.admission, w.reads
	// An unbound receipt identifies possible history. Only demand can make its inputs required.
	if !a.entryDemand && a.entryEvidence == nil {
		return false, false
	}
	if ready, pending := q.refreshReceiptSources(reads); !ready {
		return false, pending
	}
	q.captureReceiptInputs(reads)
	if !q.trees.EntryValid(ctx, q.previous, w.store) {
		if reads.Valid() {
			q.previous = kustomize.EntryEvidence{}
		}
		return false, false
	}
	parsed, err := manifest.ParseDoc(q.previous.Application, manifest.ParseDocOptions{})
	if err != nil || parsed.Object == nil {
		q.previous = kustomize.EntryEvidence{}
		return false, false
	}
	evidence := applicationEvidence{
		object:   parsed.Object,
		document: q.previous.Application,
		owner:    q.previous.Owner,
		root:     q.previous.Root,
	}
	switch {
	case receiptRenderOwner(evidence.owner):
		if accepted, pending := w.receiptOwnerReady(&evidence, frontier); !accepted {
			return false, pending
		}
	case a.entryBound.Load():
		if accepted, _ := evidence.ownerAccepts(reads); !accepted {
			q.previous = kustomize.EntryEvidence{}
			return false, false
		}
	}
	matched, err := w.readApplication(ctx, reads, &evidence)
	if err != nil || !matched || !reads.Valid() || len(evidence.reaches) != len(a.entryTargets) {
		return false, false
	}
	accepted := q.previous
	evidence.receipt = &accepted
	q.recordMatch(&evidence)
	return true, false
}

func receiptRenderOwner(owner manifest.NamedResource) bool {
	return owner.KindInfo().Render && !owner.ReportHidden()
}

//nolint:gocritic // A pending descendant can require replay of its current ancestor.
func (w *qualificationWalk) receiptOwnerReady(
	evidence *applicationEvidence, frontier manifest.NamedResource,
) (bool, bool) {
	accepted, pending := evidence.captureReceiptOwner(w.reads)
	if !pending {
		return accepted, false
	}
	w.q.replay = frontier
	if frontier != evidence.owner {
		return false, false
	}
	w.reads.RequireQualification(evidence.owner)
	w.q.pending = true
	w.q.candidates = []manifest.NamedResource{evidence.owner}
	return false, true
}

func (q *entryQualification) retainReceiptWait(reads *store.ReadSet) bool {
	if q == nil || q.waiting != nil || !reads.Valid() {
		return false
	}
	if !q.receiptPending && (q.replay == (manifest.NamedResource{}) || q.replay != q.previous.Owner) {
		return false
	}
	q.waiting = reads
	return true
}

//nolint:gocritic // Results distinguish available sources from unfinished input refresh.
func (q *entryQualification) refreshReceiptSources(reads *store.ReadSet) (bool, bool) {
	for _, id := range q.refresh {
		reads.GetArtifact(id, store.ArtifactContent)
		status, _ := reads.GetStatus(id)
		if status.IsUnavailable() {
			q.previous = kustomize.EntryEvidence{}
			return false, false
		}
		if status.Outcome() != store.OutcomePassed {
			q.pending = true
			q.receiptPending = true
			return false, true
		}
	}
	return true, false
}

//nolint:gocritic // Results distinguish current ownership from an unfinished publication.
func (e *applicationEvidence) captureReceiptOwner(reads *store.ReadSet) (bool, bool) {
	owner := reads.GetDeclaration(e.owner)
	status, _ := reads.GetStatus(e.owner)
	if status.IsUnavailable() || owner != nil && owner.Suspended() {
		return false, false
	}
	if owner == nil {
		return false, status.Outcome() == store.OutcomePending
	}
	output, complete := acceptedApplicationOutput(reads, e.owner)
	if !complete {
		return false, status.Outcome() == store.OutcomePending ||
			output != nil && !output.OutputReads.Valid()
	}
	current := reads.GetDeclaration(e.object.Named())
	if !manifest.Equal(current, e.object) {
		return false, false
	}
	index := slices.IndexFunc(output.Children, func(child store.Child) bool {
		return child.Obj != nil && manifest.Equal(child.Obj, current) &&
			manifest.ValueEqual(child.Doc, e.document)
	})
	if index < 0 {
		return false, false
	}
	child := &output.Children[index]
	e.object, e.child, e.visibleGeneration = current, child, child.VisibleGeneration
	for i := range child.Productions {
		production := &child.Productions[i]
		if production.Owner == e.owner && production.Declaration != (manifest.NamedResource{}) {
			e.declaration = production.Declaration
			reads.GetDeclaration(e.declaration)
			break
		}
	}
	return true, false
}

func (q *entryQualification) captureReceiptInputs(reads *store.ReadSet) {
	for i := range q.previous.Inputs {
		input := &q.previous.Inputs[i]
		if input.Binding.Present {
			reads.GetArtifact(input.Resource, store.ArtifactContent|store.ArtifactRevision)
		} else {
			reads.ReferenceFacts(input.Resource, q.loader.Index, manifest.NamedResource{})
		}
	}
}
