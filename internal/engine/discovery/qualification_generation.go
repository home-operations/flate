package discovery

import (
	"cmp"
	"context"
	"slices"

	"github.com/home-operations/flate/internal/engine/selection"
	"github.com/home-operations/flate/internal/engine/store"
	"github.com/home-operations/flate/pkg/manifest"
)

func runnableApplication(object manifest.BaseManifest) (manifest.EntryApplication, bool) {
	if !manifest.KindOf(object).Runnable {
		return manifest.EntryApplication{}, false
	}
	return manifest.ApplicationFor(object)
}

func (w *qualificationWalk) captureConstructed(
	ctx context.Context, evidence *applicationEvidence, id manifest.NamedResource,
) (bool, error) {
	if evidence.builtRoot == "" || w.q.session == nil || manifest.KindOf(evidence.object).Structural {
		return true, nil
	}
	w.reads.RequireQualification(id)
	return w.admitConstructed(ctx, evidence)
}

func (w *qualificationWalk) visitGeneration(ctx context.Context, evidence *applicationEvidence) (bool, error) {
	if _, applies := manifest.ApplicationFor(evidence.object); !applies {
		return false, nil
	}
	if evidence.owner != (manifest.NamedResource{}) {
		if evidence.builtRoot != "" && !evidence.owner.ReportHidden() {
			w.await(evidence, evidence.owner)
		}
		if evidence.builtRoot == "" || !evidence.owner.ReportHidden() {
			return false, nil
		}
	}
	if w.reads.Policy(evidence.object.Named()).Supplied() {
		matched, err := w.readApplication(ctx, w.reads, evidence)
		if matched {
			w.q.recordMatch(evidence)
		}
		return matched, err
	}
	output, err := w.admission.prepareGeneration(evidence.object, w.reads.WorkScope())
	if evidence.private {
		if canceled := ctx.Err(); canceled != nil || err != nil {
			return false, canceled
		}
	}
	if err != nil {
		return false, err
	}
	for i := range output.Children {
		child := &output.Children[i]
		if _, applies := runnableApplication(child.Obj); applies {
			return w.visitSyncRoot(ctx, evidence, &output, i)
		}
	}
	return false, nil
}

func (w *qualificationWalk) visitSyncRoot(
	ctx context.Context, evidence *applicationEvidence, output *store.ChildDocuments, root int,
) (bool, error) {
	child := &output.Children[root]
	application, _ := runnableApplication(child.Obj)
	generated := &applicationEvidence{
		object: child.Obj, document: child.Doc, owner: evidence.owner, declaration: evidence.object.Named(),
		builtRoot: evidence.builtRoot, child: child, root: cmp.Or(evidence.root, w.q.root),
		visibleGeneration: child.VisibleGeneration, private: evidence.private,
	}
	current := w.reads.GetDeclaration(child.Obj.Named())
	if manifest.Equal(current, child.Obj) {
		generated.object = current
	}
	if i := slices.IndexFunc(output.Children, func(candidate store.Child) bool {
		return candidate.Obj.Named() == application.Source
	}); i >= 0 {
		generated.generatedSource = output.Children[i].Obj
	}
	if reused, pending := w.matchReceipt(ctx, generated, nil); reused || pending {
		return reused, nil
	}
	eligible, err := w.readApplication(ctx, w.reads, generated)
	if err != nil {
		return false, err
	}
	if !eligible {
		if generated.private || w.store.GetDeclaration(generated.object.Named()) == nil {
			return false, nil
		}
		return w.continueRoute(ctx, generated)
	}
	if current, err := w.captureSyncGeneration(ctx, evidence, output, root); err != nil || !current {
		return false, err
	}
	w.q.recordMatch(generated)
	return true, nil
}

func (w *qualificationWalk) captureSyncGeneration(
	ctx context.Context, evidence *applicationEvidence, output *store.ChildDocuments, root int,
) (bool, error) {
	if w.q.session == nil {
		return true, nil
	}
	source := manifest.BootstrapSourceID
	if owner := w.reads.GetDeclaration(evidence.owner); owner != nil {
		if id, _, applies := owner.SourcePath(); applies {
			source = id
		}
	}
	generation := slices.Clone(output.Children)
	for i := range generation {
		generation[i].Productions = []manifest.Production{{
			Owner: evidence.owner, Declaration: evidence.object.Named(), Source: source,
		}}
	}
	application, _ := runnableApplication(generation[root].Obj)
	generation[root].Entry = &application
	committed, err := w.commit(ctx, &selection.PrepareRequest{
		Generation: generation, Reads: w.reads, Invocation: true,
	})
	if err != nil {
		return false, err
	}
	if !committed || !w.reads.Valid() {
		w.q.changed = w.q.changed || !w.reads.Valid()
		w.q.pending = true
		return false, nil
	}
	return true, nil
}

func (p *entryPlanner) scopeGeneration(
	occurrence *selection.OccurrenceFacts, object manifest.BaseManifest,
) ([]selection.OccurrenceFacts, error) {
	if !generatesEntry(occurrence) || occurrence.RequiresBuild {
		return nil, nil
	}
	output, err := p.session.admission().prepareGeneration(object, p.session.cfg.Work.Source(occurrence.Identity))
	if err != nil {
		return nil, err
	}
	return p.preparedOccurrences(p.inputs[occurrence.Declaration.Source], occurrence.Declaration, output.Children), nil
}

func (p *entryPlanner) preparedOccurrences(
	input *store.SourceArtifact,
	declaration selection.OccurrenceID,
	children []store.Child,
) []selection.OccurrenceFacts {
	facts := make([]selection.OccurrenceFacts, 0, len(children))
	for i := range children {
		child := &children[i]
		occurrence := p.session.loader.ProducerFacts(child.Obj).Occurrence
		occurrence.Declaration, occurrence.Mounted = declaration, input
		key := occurrenceIdentity(&occurrence)
		key.input = input
		if p.objects == nil {
			p.objects = map[occurrenceKey]store.Child{}
		}
		p.objects[key] = *child
		facts = append(facts, occurrence)
	}
	return facts
}
