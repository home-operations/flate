package discovery

import (
	"cmp"
	"context"
	"slices"

	"github.com/home-operations/flate/internal/engine/selection"
	"github.com/home-operations/flate/internal/engine/store"
	"github.com/home-operations/flate/pkg/manifest"
)

type syncSource struct {
	source, application manifest.BaseManifest
	owner               manifest.NamedResource
	occurrence          selection.OccurrenceID
	predicted           bool
}

func (s *syncSource) equal(other *syncSource) bool {
	return s == other || s != nil && other != nil && *s == *other
}

func (s *syncSource) emitted() bool {
	return s != nil && s.owner != (manifest.NamedResource{})
}

func runnableApplication(object manifest.BaseManifest) (manifest.EntryApplication, bool) {
	if !manifest.KindOf(object).Runnable {
		return manifest.EntryApplication{}, false
	}
	return manifest.ApplicationFor(object)
}

func generationSource(application *manifest.EntryApplication, children []store.Child) *store.Child {
	for i := range children {
		child := &children[i]
		if manifest.KindOf(child.Obj).IsSource() && child.Obj.Named() == application.Source {
			return child
		}
	}
	return nil
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
	source := generationSource(&application, output.Children)
	if source != nil {
		generated.generatedSource = source.Obj
		occurrence := w.admission.syncOccurrence(evidence.object.Named(), w.reads)
		declared := w.reads.GetDeclaration(evidence.object.Named()) == evidence.object
		if w.admission.syncInScope(&application, &occurrence, declared) {
			generated.source.sync = &syncSource{
				source: source.Obj, application: child.Obj, occurrence: occurrence.Declaration,
			}
		}
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

func (a *Admission) classifyEmittedSource(
	id manifest.NamedResource, candidate, previous *entryResolution, reads *store.ReadSet,
) bool {
	owner := a.store.EmissionOwner(id)
	if owner == (manifest.NamedResource{}) && previous.sync.emitted() && previous.sameDeclaration(candidate) {
		owner = previous.sync.owner
	}
	if owner == (manifest.NamedResource{}) {
		return false
	}
	getDeclaration := a.store.GetDeclaration
	if reads != nil {
		getDeclaration = reads.GetDeclaration
	}
	// Capture the pair's declarations. The source can supply the producer's input, so
	// it MUST NOT subscribe to the producer's entire output.
	output, _ := a.store.GetArtifact(owner, store.ArtifactContent).(*store.Emission)
	priorOwned := previous.sync.emitted() && previous.sync.owner == owner
	if output == nil || !output.Complete {
		if !priorOwned {
			return false
		}
		candidate.provider, candidate.entry = store.ProviderGenerator, false
		return true
	}
	// The checkout can invalidate the producer's reads without changing either declaration.
	// Both declarations MUST still belong to the complete inventory.
	for i := range output.Children {
		child := &output.Children[i]
		application, applies := runnableApplication(child.Obj)
		if !applies || !child.VisibleGeneration || application.Source != id {
			continue
		}
		source := generationSource(&application, output.Children)
		if source == nil || !source.VisibleGeneration || source.Obj != candidate.object ||
			getDeclaration(child.Obj.Named()) != child.Obj {
			continue
		}
		occurrence := a.syncOccurrence(owner, reads)
		if !a.syncInScope(&application, &occurrence, true) {
			continue
		}
		candidate.sync = &syncSource{
			source: source.Obj, application: child.Obj, owner: owner, occurrence: occurrence.Declaration,
		}
		candidate.entry = true
		return false
	}
	if priorOwned &&
		!slices.ContainsFunc(output.Children, func(child store.Child) bool { return child.Obj == candidate.object }) {
		candidate.object, candidate.provider, candidate.entry = nil, store.ProviderAbsent, false
		return true
	}
	return false
}

func (a *Admission) syncOccurrence(id manifest.NamedResource, reads *store.ReadSet) selection.OccurrenceFacts {
	if a.loader != nil {
		return a.loader.RootEvidence(id, reads).Occurrence
	}
	return selection.OccurrenceFacts{}
}

// An exact application origin requires observed authority. A projected pair needs its declaration in scope.
func (a *Admission) syncInScope(
	application *manifest.EntryApplication, occurrence *selection.OccurrenceFacts, observed bool,
) bool {
	origin := a.input.ApplicationOrigin(application.Path)
	if origin == "" {
		return false
	}
	if observed && a.input.MatchesOrigin(a.path, origin) {
		return true
	}
	return occurrence != nil && occurrence.Declaration.File != "" &&
		containsDeclaration(a.input.OriginPath(a.path), &occurrence.Declaration, occurrence.Mounted, a.input)
}

func (d *Session) addGenerationSources(
	groups map[manifest.NamedResource]*entrySourceGroup, object manifest.BaseManifest,
	occurrence *selection.OccurrenceFacts,
) error {
	admission := d.admission()
	output, err := admission.prepareGeneration(object, d.cfg.Work.Source(object.Named()))
	if err != nil {
		return err
	}
	evidence := selection.CaptureRoot(d.loader.RootEvidence(object.Named(), nil), d.loader.Store, d.loader.Index)
	owner := selection.Root(evidence).Owner
	owned := owner != (manifest.NamedResource{}) && !owner.ReportHidden()
	var declaration *selection.OccurrenceFacts
	if !owned {
		declaration = &evidence.Occurrence
	}
	for i := range output.Children {
		child := &output.Children[i]
		if !manifest.KindOf(child.Obj).IsSource() {
			continue
		}
		id := child.Obj.Named()
		group := entrySourceGroupFor(groups, id)
		if group.object == nil {
			group.object, group.document, group.projected = child.Obj, child.Doc, true
			group.projectedFromParent = owned
		}
		group.required = true
	}
	for i := range output.Children {
		root := &output.Children[i]
		application, valid := runnableApplication(root.Obj)
		if !valid {
			continue
		}
		source := generationSource(&application, output.Children)
		if source == nil {
			continue
		}
		group := groups[application.Source]
		reaches := d.occurrenceReachesEntry(&application, occurrence)
		group.entry = group.entry || reaches
		if group.object == source.Obj && admission.syncInScope(&application, declaration, reaches) {
			group.sync = &syncSource{
				source: source.Obj, application: root.Obj, occurrence: evidence.Occurrence.Declaration,
			}
		}
	}
	return nil
}

func (p *entryPlanner) scopedSyncSource(
	selected *selection.OccurrenceFacts, source *store.Child, scope *rootScope, reached bool,
) *syncSource {
	admission := p.session.admission()
	for occurrence := range scope.objects() {
		if occurrence.Declaration != selected.Declaration || occurrence.Mounted != selected.Mounted {
			continue
		}
		key := occurrenceIdentity(occurrence)
		key.input = p.inputs[occurrence.Declaration.Source]
		root := p.objects[key]
		application, applies := runnableApplication(root.Obj)
		if applies && root.VisibleGeneration && application.Source == source.Obj.Named() &&
			admission.syncInScope(&application, selected, false) {
			return &syncSource{source: source.Obj, application: root.Obj, occurrence: selected.Declaration}
		}
	}
	before := p.session.resolvedSources.snapshot()[source.Obj.Named()]
	mounted := selected.Mounted != nil && selected.Mounted == before.artifact
	if reached && mounted && before.object == source.Obj {
		return before.sync
	}
	return nil
}
