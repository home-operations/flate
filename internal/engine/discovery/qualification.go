package discovery

import (
	"cmp"
	"context"
	"encoding/json"
	"fmt"
	"iter"
	"path/filepath"
	"slices"

	"github.com/home-operations/flate/internal/engine/kustomize"
	"github.com/home-operations/flate/internal/engine/loader"
	"github.com/home-operations/flate/internal/engine/selection"
	"github.com/home-operations/flate/internal/engine/store"
	"github.com/home-operations/flate/pkg/manifest"
)

type entryQualification struct {
	operandsSettled bool
	changedOnly     bool
	packages        [][]selection.OccurrenceFacts
	active          []manifest.NamedResource
	preparation     *selection.Preparation
	arrivals        *applicationEvidence
	loader          *loader.Loader
	trees           *kustomize.TreeCache
	session         *Session
	prepared        map[manifest.NamedResource]*declarationPreparation
	previous        kustomize.EntryEvidence
	root            manifest.NamedResource
	refresh         []manifest.NamedResource
	candidates      []manifest.NamedResource
	reach           *selection.EntryReach
	matches         []applicationEvidence
	winners         map[selection.OccurrenceID]manifest.NamedResource
	pending         bool
	closed          bool
	waiting         *store.ReadSet
	proposal        *store.ReadSet
	receiptRead     bool
	receiptPending  bool
	replay          manifest.NamedResource
	changed         bool
}

type applicationEvidence struct {
	root              manifest.NamedResource
	object            manifest.BaseManifest
	document          map[string]any
	application       manifest.EntryApplication
	source            entryResolution
	generatedSource   manifest.BaseManifest
	owner             manifest.NamedResource
	declaration       manifest.NamedResource
	private           bool
	history           bool
	receipt           *kustomize.EntryEvidence
	descendant        bool
	reaches           []selection.OccurrenceID
	visibleGeneration bool
	lookup            bool
	lookupReach       bool
	builtRoot         string
	child             *store.Child
	next              *applicationEvidence
}

// AddApplication MUST only record arrivals because store listeners cannot perform qualification.
func (a *Admission) AddApplication(_ *store.Store, obj manifest.BaseManifest) {
	if a == nil || obj == nil {
		return
	}
	if a.input != nil {
		native := manifest.NewEntryKustomization(manifest.BootstrapSourceID, a.entryPath(), manifest.NamedResource{}).
			Named()
		if obj.Named() == native {
			return
		}
	}
	_, application := manifest.ApplicationFor(obj)
	if !application && !manifest.KindOf(obj).Parent {
		return
	}
	for {
		previous := a.applications.Load()
		if previous != nil && previous.object == obj {
			return
		}
		evidence := &applicationEvidence{object: obj, next: previous}
		if a.applications.CompareAndSwap(previous, evidence) {
			return
		}
	}
}

// AddHistory MUST qualify historical declarations against current parent authority.
//
//nolint:gocritic // The SDK preserves the discovery record value snapshot.
func (a *Admission) AddHistory(_ *store.Store, id manifest.NamedResource, record manifest.DiscoveryRecord) {
	if a == nil || record.Application == "" {
		return
	}
	var document map[string]any
	if json.Unmarshal([]byte(record.Application), &document) != nil {
		return
	}
	parsed, err := manifest.ParseDoc(document, manifest.ParseDocOptions{})
	if err != nil || parsed.Object == nil || parsed.Object.Named() != id {
		return
	}
	for {
		previous := a.applications.Load()
		evidence := &applicationEvidence{object: parsed.Object, document: document, history: true, next: previous}
		if a.applications.CompareAndSwap(previous, evidence) {
			return
		}
	}
}

func (a *Admission) applicationSource(
	st *store.Store,
	reads *store.ReadSet,
	application *manifest.EntryApplication,
) entryResolution {
	if reads == nil {
		reads = &store.ReadSet{}
		reads.Init(st, manifest.BootstrapSourceID, nil)
		defer st.Retire(reads)
		defer reads.ReleaseCapture()
	}
	resolved := entryResolution{object: reads.GetDeclaration(application.Source)}
	resolved.artifact, _ = reads.GetArtifact(application.Source, store.ArtifactContent).(*store.SourceArtifact)
	if resolved.artifact != nil && !resolved.artifact.Reads.Valid() {
		resolved.artifact = nil
	}
	if resolved.object == nil {
		previous := a.resolvedSources.snapshot()[application.Source]
		if a.session != nil || !previous.projected {
			resolved.object = previous.object
		}
	}
	return resolved
}

func acceptedApplicationOutput(reads *store.ReadSet, owner manifest.NamedResource) (*store.Emission, bool) {
	if reads.GetDeclaration(owner) == nil {
		return nil, false
	}
	output, _ := reads.GetArtifact(owner, store.ArtifactContent).(*store.Emission)
	if output == nil || !output.Complete {
		return output, false
	}
	return output, output.OutputReads.Valid() && len(output.Failures) == 0
}

// Unavailable output MUST remain pending, while complete omission withdraws support.
//
//nolint:gocritic // Results distinguish accepted membership from unresolved support.
func (e *applicationEvidence) ownerAccepts(reads *store.ReadSet) (bool, bool) {
	if e.owner == (manifest.NamedResource{}) {
		return manifest.Equal(reads.GetDeclaration(e.object.Named()), e.object), false
	}
	output, accepted := acceptedApplicationOutput(reads, e.owner)
	if !accepted {
		reads.RequireMaterializer(e.owner)
		return false, true
	}
	member := slices.ContainsFunc(
		output.Children,
		func(child store.Child) bool { return child.Obj != nil && manifest.Equal(child.Obj, e.object) },
	)
	return member && manifest.Equal(reads.GetDeclaration(e.object.Named()), e.object), false
}

func (a *Admission) readApplication(
	ctx context.Context,
	st *store.Store,
	reads *store.ReadSet,
	evidence *applicationEvidence,
) (bool, error) {
	application, valid := manifest.ApplicationFor(evidence.object)
	if !valid {
		return false, nil
	}
	evidence.application = application
	resolved := a.applicationSource(st, reads, &application)
	if evidence.generatedSource != nil {
		previous := a.resolvedSources.snapshot()[application.Source]
		if resolved.object == nil {
			resolved.object = evidence.generatedSource
		} else if evidence.lookup && previous.synthetic && previous.ownsDeclaration(resolved.object) {
			resolved = entryResolution{object: evidence.generatedSource}
		}
	}
	if resolved.artifact == nil && resolved.object != nil && resolved.object.Composition().Composed() {
		evidence.source = resolved
		if !evidence.private && a.reachesEntry(&application, nil) {
			reads.RequireData(application.Source)
			if q := a.qualification; q != nil {
				q.pending, q.candidates = true, []manifest.NamedResource{application.Source}
			}
		}
		return false, nil
	}
	if resolved.artifact == nil {
		var err error
		resolved, err = a.resolve(
			ctx,
			application.Source,
			&sourceRequest{
				object:    resolved.object,
				reference: sourceReference(evidence.object, application.Source),
				reads:     reads,
				work:      reads.WorkScope(),
				entry:     a.reachesEntry(&application, nil),
				projected: resolved.object != nil && resolved.object == evidence.generatedSource,
			},
		)
		if err != nil {
			return false, err
		}
	}
	evidence.source = resolved
	return a.applicationReach(evidence), ctx.Err()
}

func (q *entryQualification) published(st *store.Store, a *Admission) iter.Seq[manifest.BaseManifest] {
	native := manifest.NewEntryKustomization(manifest.BootstrapSourceID, a.entryPath(), manifest.NamedResource{}).
		Named()
	return func(yield func(manifest.BaseManifest) bool) {
		objects := st.ListObjects("", "")
		for _, obj := range objects {
			if q.session == nil && obj.Named() == native {
				continue
			}
			if !yield(obj) {
				return
			}
		}
		for evidence := q.arrivals; evidence != nil; evidence = evidence.next {
			if evidence.history || q.session == nil && evidence.object.Named() == native {
				continue
			}
			if !yield(evidence.object) {
				return
			}
		}
	}
}

type applicationRoute struct {
	object manifest.BaseManifest
	source *store.SourceArtifact
	owner  manifest.NamedResource
	root   string
}

type qualificationVisit uint8

const (
	qualificationVisiting qualificationVisit = iota + 1
	qualificationSettled
)

func (q *entryQualification) advance(ctx context.Context, a *Admission, st *store.Store, reads *store.ReadSet) error {
	q.proposal = nil
	if q.waiting != nil {
		if q.waiting.Valid() && q.arrivals == a.applications.Load() {
			return nil
		}
		q.retireWait()
	}
	previous := q.candidates
	q.pending, q.candidates, q.matches, q.receiptPending, q.changed = false, nil, nil, false, false
	q.replay = manifest.NamedResource{}
	q.reach = nil
	q.winners = nil
	q.arrivals = a.applications.Load()
	walk := qualificationWalk{
		q:         q,
		admission: a,
		store:     st,
		reads:     reads,
		visits:    map[applicationRoute]qualificationVisit{},
	}
	if err := walk.advance(ctx, len(previous) != 0); err != nil {
		return err
	}
	q.orderMatches()
	return nil
}

//nolint:nilnil // Unavailable declarations leave the supported prefix pending.
func (w *qualificationWalk) root(ctx context.Context, root *selection.OccurrenceFacts) (*applicationEvidence, error) {
	object := w.store.GetDeclaration(root.Identity)
	if root.RequiresBuild && object == nil {
		return nil, nil
	}
	if object != nil {
		object = w.reads.GetDeclaration(root.Identity)
	}
	if object != nil {
		evidence := &applicationEvidence{object: object, root: object.Named()}
		w.captureOwner(evidence)
		if cmp.Or(evidence.owner, evidence.declaration) != (manifest.NamedResource{}) {
			return evidence, nil
		}
	}
	if object == nil || root.Declaration != (selection.OccurrenceID{}) &&
		!w.reads.Policy(root.Identity).Supplied() {
		object = w.q.loader.CatalogObject(root.Identity)
	}
	if object == nil && root.Declaration != (selection.OccurrenceID{}) && w.q.session != nil && !w.q.closed {
		w.reads.RequireQualification(root.Identity)
		before := w.store.GetDeclaration(root.Identity)
		committed, err := w.commit(ctx, &selection.PrepareRequest{Reads: w.reads, Invocation: true})
		w.q.changed = w.q.changed || !manifest.Equal(before, w.store.GetDeclaration(root.Identity))
		if err != nil {
			return nil, err
		}
		if !committed || !w.reads.Valid() {
			w.q.pending = true
			return nil, nil
		}
		object = w.reads.GetDeclaration(root.Identity)
	}
	if object == nil {
		return nil, nil
	}
	evidence := &applicationEvidence{object: object}
	if record, ok := w.q.loader.Index.Lookup(root.Identity); ok && record.Declaration != nil {
		evidence.document = w.q.loader.IndexedObject(root.Identity, record).Doc
	}
	return evidence, nil
}

func (w *qualificationWalk) enoughMatches() bool {
	if len(w.admission.entryTargets) == 0 {
		return len(w.q.matches) != 0
	}
	return len(w.q.winners) == len(w.admission.entryTargets)
}

func (q *entryQualification) recordMatch(evidence *applicationEvidence) {
	if len(evidence.reaches) != 0 {
		if q.winners == nil {
			q.winners = make(map[selection.OccurrenceID]manifest.NamedResource, len(evidence.reaches))
		}
		for _, occurrence := range evidence.reaches {
			q.winners[occurrence] = evidence.object.Named()
		}
	}
	q.matches = append(q.matches, *evidence)
}

func (w *qualificationWalk) observe(ctx context.Context) error {
	seen := make(map[manifest.NamedResource]bool, len(w.q.matches))
	for i := range w.q.matches {
		seen[w.q.matches[i].object.Named()] = true
	}
	for object := range w.q.published(w.store, w.admission) {
		if w.selectedTargetsReached() {
			break
		}
		if seen[object.Named()] {
			continue
		}
		if _, valid := runnableApplication(object); !valid {
			continue
		}
		evidence := &applicationEvidence{object: object, root: object.Named()}
		w.captureOwner(evidence)
		accepted, _ := evidence.ownerAccepts(w.reads)
		if !accepted {
			continue
		}
		if reused, pending := w.matchReceipt(ctx, evidence, nil); reused || pending {
			if pending {
				return nil
			}
			seen[object.Named()] = true
			continue
		}
		matched, err := w.admission.readApplication(ctx, w.store, w.reads, evidence)
		if err != nil {
			return err
		}
		if matched {
			w.q.recordMatch(evidence)
			seen[object.Named()] = true
		}
	}
	return nil
}

func (w *qualificationWalk) captureOwner(evidence *applicationEvidence) {
	productions := w.reads.Productions(evidence.object.Named(), w.q.loader.Index)
	for i := range productions {
		production := &productions[i]
		owner := production.Owner
		if owner == (manifest.NamedResource{}) && production.Declaration != (manifest.NamedResource{}) &&
			production.Declaration != evidence.object.Named() && w.reads.GeneratedDocument(evidence.object.Named()) {
			current, document, emission := w.reads.GetEmission(evidence.object.Named())
			emission.Finish()
			if manifest.Equal(current, evidence.object) {
				evidence.declaration, evidence.document = production.Declaration, document
				evidence.visibleGeneration = true
			}
			continue
		}
		if owner == (manifest.NamedResource{}) || owner == evidence.object.Named() {
			continue
		}
		output, accepted := acceptedApplicationOutput(w.reads, owner)
		if !accepted || !manifest.Equal(w.reads.GetDeclaration(evidence.object.Named()), evidence.object) {
			continue
		}
		index := slices.IndexFunc(output.Children, func(child store.Child) bool {
			return child.Obj != nil && manifest.Equal(child.Obj, evidence.object)
		})
		if index < 0 {
			continue
		}
		child := &output.Children[index]
		evidence.owner, evidence.declaration = owner, production.Declaration
		evidence.document, evidence.child, evidence.visibleGeneration = child.Doc, child, child.VisibleGeneration
		if evidence.declaration != (manifest.NamedResource{}) {
			w.reads.GetDeclaration(evidence.declaration)
		}
		return
	}
}

func (w *qualificationWalk) visit(ctx context.Context, evidence *applicationEvidence) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, fmt.Errorf("context: %w", err)
	}
	object := evidence.object
	if object == nil {
		return false, nil
	}
	if !manifest.KindOf(object).Runnable {
		return w.visitGeneration(ctx, evidence)
	}
	if reused, pending := w.matchReceipt(ctx, evidence, nil); reused || pending {
		return reused, nil
	}
	matched, err := w.admission.readApplication(ctx, w.store, w.reads, evidence)
	if err != nil {
		return false, err
	}
	if matched {
		evidence.root = w.q.root
		w.q.recordMatch(evidence)
		return true, nil
	}
	if !w.applicationContinuation(object) {
		return false, nil
	}
	return w.continueRoute(ctx, evidence)
}

func (w *qualificationWalk) continueRoute(ctx context.Context, evidence *applicationEvidence) (bool, error) {
	object := evidence.object
	route := applicationRoute{
		object: object,
		source: evidence.source.artifact,
		owner:  evidence.owner,
		root:   evidence.builtRoot,
	}
	if w.visits[route] != 0 {
		return false, nil
	}
	w.visits[route] = qualificationVisiting
	defer func() {
		if w.q.pending || !w.reads.Valid() {
			delete(w.visits, route)
		} else {
			w.visits[route] = qualificationSettled
		}
	}()
	execution, err := w.execution(ctx, evidence)
	if err != nil || execution == nil {
		return false, err
	}
	if reused, pending := w.matchReceipt(ctx, evidence, execution); reused || pending {
		return reused, nil
	}
	if w.q.session == nil && w.store.GetDeclaration(execution.Named()) != nil &&
		w.reads.QualificationSettled(execution.Named()) {
		return false, nil
	}
	if current, admitErr := w.captureConstructed(ctx, evidence, execution.Named()); admitErr != nil || !current {
		return false, admitErr
	}
	batch, err := w.children(ctx, evidence, execution)
	if err != nil || w.q.pending {
		return false, err
	}
	if !batch.complete {
		w.await(evidence, execution.Named())
		return false, nil
	}
	return w.follow(ctx, &batch, execution.Named())
}

//nolint:nilnil // A raw application executes through an ordinary literal Kustomization.
func (w *qualificationWalk) execution(
	ctx context.Context,
	evidence *applicationEvidence,
) (manifest.BaseManifest, error) {
	if evidence.object.Suspended() {
		return nil, nil
	}
	application, applies := manifest.ApplicationFor(evidence.object)
	produces := manifest.KindOf(evidence.object).Parent
	_, _, renders := evidence.object.SourcePath()
	if !produces && !applies || manifest.KindOf(evidence.object).Structural && !renders {
		return nil, nil
	}
	if !applies || w.reads.Policy(evidence.object.Named()).Root || evidence.visibleGeneration {
		return evidence.object, nil
	}
	resolved := &evidence.source
	if resolved.artifact == nil {
		return nil, nil
	}
	if !resolved.artifact.WorkingTree() {
		return nil, nil
	}
	possible, err := w.entryFrontier(ctx, resolved.artifact, application.Path)
	ordinary := w.ordinaryApplication(evidence, &application, possible)
	if err != nil || !possible && !ordinary {
		return nil, err
	}
	literal := manifest.NewEntryKustomization(application.Source, application.Path, evidence.object.Named())
	if ordinary {
		w.reads.RequireData(literal.Named())
	}
	if current, ok := w.reads.GetDeclaration(literal.Named()).(*manifest.Kustomization); ok {
		return current, nil
	}
	committed, err := w.commit(
		ctx,
		&selection.PrepareRequest{
			Generation: []store.Child{{Obj: literal, Entry: &application}},
			Reads:      w.reads,
			Invocation: w.q.session != nil,
		},
	)
	if err != nil {
		return nil, err
	}
	if !committed || !w.reads.Valid() {
		w.q.pending = true
		return nil, nil
	}
	return literal, nil
}

func (w *qualificationWalk) commit(ctx context.Context, request *selection.PrepareRequest) (bool, error) {
	if w.q.session != nil {
		*request = w.q.session.prepareRequest(request)
	}
	if w.q.loader.Options.Session == nil {
		for i := range request.Generation {
			child := &request.Generation[i]
			if child.Obj != nil {
				request.Reads.AdmitConstructed(child)
			}
		}
		return request.Reads.Valid(), nil
	}
	request.Missing = w.q.loader.Options.Missing
	if request.Preparation == nil {
		request.Preparation = &w.q.preparation
	}
	stage := selection.Execution
	if w.q.session != nil {
		stage = selection.Construction
	}
	_, committed, err := w.q.loader.Options.Session.Admit(ctx, selection.AdmissionOptions{
		Request: *request, Stage: stage,
	})
	w.q.changed = w.q.changed || request.Reads != nil && !request.Reads.Valid()
	return committed, err
}

func (w *qualificationWalk) admitConstructed(ctx context.Context, evidence *applicationEvidence) (bool, error) {
	if w.store.GetDeclaration(evidence.object.Named()) != nil {
		return true, nil
	}
	if evidence.child == nil {
		return false, nil
	}
	incoming := *evidence.child
	if application, applies := manifest.ApplicationFor(evidence.object); applies {
		incoming.Entry = &application
	}
	committed, err := w.commit(
		ctx,
		&selection.PrepareRequest{Generation: []store.Child{incoming}, Reads: w.reads, Invocation: true},
	)
	if err != nil {
		return false, err
	}
	w.q.changed = w.q.changed || w.store.GetDeclaration(evidence.object.Named()) != nil
	w.q.pending = !committed || !w.reads.Valid()
	return !w.q.pending, nil
}

func (a *Admission) entryPath() string {
	relative, _ := filepath.Rel(a.input.LocalPath, a.path)
	return filepath.ToSlash(relative)
}

type qualificationBatch struct {
	children []store.Child
	root     string
	complete bool
	private  bool
}

func (w *qualificationWalk) children(
	ctx context.Context,
	evidence *applicationEvidence,
	execution manifest.BaseManifest,
) (qualificationBatch, error) {
	if frame := w.q.prepared[execution.Named()]; frame.canSupply(execution, w.reads) {
		root := filepath.Join(frame.source.LocalPath, manifest.NormalizeClaimBase(frame.owner.Path))
		return qualificationBatch{root: root, children: frame.children, complete: true}, nil
	}
	if w.store.GetDeclaration(execution.Named()) == nil {
		if w.q.session == nil {
			return w.runtimeFrontier(ctx, evidence, execution)
		}
		if ks, ok := execution.(*manifest.Kustomization); ok {
			return w.construct(ctx, evidence, ks)
		}
		return qualificationBatch{}, nil
	}
	output, accepted := acceptedApplicationOutput(w.reads, execution.Named())
	if accepted {
		return qualificationBatch{children: output.Children, complete: true}, nil
	}
	if w.q.session == nil {
		return w.runtimeFrontier(ctx, evidence, execution)
	}
	if output != nil {
		return qualificationBatch{}, nil
	}
	ks, ok := execution.(*manifest.Kustomization)
	if !ok {
		return qualificationBatch{}, nil
	}
	return w.construct(ctx, evidence, ks)
}

func (w *qualificationWalk) runtimeFrontier(
	ctx context.Context,
	evidence *applicationEvidence,
	execution manifest.BaseManifest,
) (qualificationBatch, error) {
	source, relative, applies := execution.SourcePath()
	if !applies {
		return qualificationBatch{}, nil
	}
	input := evidence.source.artifact
	if input == nil {
		input, _ = w.reads.GetArtifact(source, store.ArtifactContent).(*store.SourceArtifact)
	}
	if input == nil {
		return qualificationBatch{}, nil
	}
	possible, err := w.entryFrontier(ctx, input, relative)
	if err != nil {
		return qualificationBatch{}, err
	}
	return qualificationBatch{complete: !possible}, nil
}

func (w *qualificationWalk) construct(
	ctx context.Context,
	evidence *applicationEvidence,
	ks *manifest.Kustomization,
) (qualificationBatch, error) {
	input := evidence.source.artifact
	if input == nil {
		source := ks.EffectiveSourceID()
		input, _ = w.reads.GetArtifact(source, store.ArtifactContent).(*store.SourceArtifact)
	}
	if input == nil || w.q.trees == nil || !w.q.session.canPrepare(ks) {
		return qualificationBatch{}, nil
	}
	root := filepath.Join(input.LocalPath, manifest.NormalizeClaimBase(ks.Path))
	children, current := w.q.loader.BuiltChildren(root, ks.Named(), w.reads)
	if current {
		return qualificationBatch{root: root, children: children, complete: true}, nil
	}
	if w.q.closed {
		return qualificationBatch{}, nil
	}
	possible, err := w.entryFrontier(ctx, input, ks.Path)
	if err != nil {
		return qualificationBatch{}, err
	}
	if !possible {
		return qualificationBatch{complete: true}, nil
	}
	if err := w.prepare(ctx, evidence, ks, input); err != nil {
		return qualificationBatch{}, err
	}
	if w.q.pending {
		return qualificationBatch{}, nil
	}
	children, current = w.q.loader.BuiltChildren(root, ks.Named(), w.reads)
	return qualificationBatch{root: root, children: children, complete: current}, nil
}

func (w *qualificationWalk) prepare(
	ctx context.Context,
	evidence *applicationEvidence,
	ks *manifest.Kustomization,
	input *store.SourceArtifact,
) error {
	if evidence.builtRoot != "" {
		current, err := w.admitConstructed(ctx, evidence)
		if err != nil || !current {
			return err
		}
	}
	support, current := w.preparationSupport(evidence, ks.Named(), ks.EffectiveSourceID())
	defer w.store.Retire(support)
	defer support.ReleaseCapture()
	if !current {
		w.q.pending = true
		return nil
	}
	prepared, known := w.q.session.prepared[ks.Named()]
	reused := known && prepared.registered && prepared.matches(ks, input, support)
	if err := w.q.session.prepareEntryPackage(ctx, ks, input, support); err != nil {
		if ctx.Err() != nil {
			return fmt.Errorf("context: %w", ctx.Err())
		}
		return nil
	}
	if frame := w.q.session.prepared[ks.Named()]; frame == nil || !frame.registered {
		return nil
	}
	w.q.changed = w.q.changed || !reused
	w.q.pending = !w.reads.Valid()
	return nil
}

func (w *qualificationWalk) preparationSupport(
	evidence *applicationEvidence,
	origin, source manifest.NamedResource,
) (*store.ReadSet, bool) {
	support := &store.ReadSet{}
	support.Init(w.store, origin, nil)
	support.GetArtifact(source, store.ArtifactContent)
	support.GetDeclaration(source)
	if evidence.declaration != (manifest.NamedResource{}) {
		support.GetDeclaration(evidence.declaration)
	}
	switch {
	case evidence.builtRoot != "":
		batch, current := w.q.loader.BuiltChildren(evidence.builtRoot, evidence.owner, support)
		return support, current &&
			slices.ContainsFunc(
				batch,
				func(child store.Child) bool { return child.Obj != nil && manifest.Equal(child.Obj, evidence.object) },
			)
	case evidence.owner != (manifest.NamedResource{}):
		accepted, _ := evidence.ownerAccepts(support)
		return support, accepted
	case selection.Root(selection.CaptureRoot(w.q.loader.RootEvidence(evidence.object.Named(), nil), w.q.loader.Store, w.q.loader.Index)).InitialEntry:
		current := support.SourceAdmission(evidence.object.Named(), source)
		return support, current
	default:
		support.GetDeclaration(evidence.object.Named())
		current := support.Valid()
		return support, current
	}
}

func (w *qualificationWalk) await(evidence *applicationEvidence, id manifest.NamedResource) {
	if w.q.closed && w.q.replay != id {
		w.q.pending = w.admission.entryBound.Load()
		decision := w.q.loader.Options.Session.Snapshot()
		if w.q.pending || evidence.owner == (manifest.NamedResource{}) ||
			!decision.Reconcile(evidence.owner) || decision.OrdinaryDemand(evidence.owner) {
			return
		}
	}
	if id == manifest.NewEntryKustomization(manifest.BootstrapSourceID, w.admission.entryPath(), manifest.NamedResource{}).
		Named() {
		return
	}
	if evidence.owner != (manifest.NamedResource{}) && evidence.builtRoot == "" {
		accepted, pending := evidence.ownerAccepts(w.reads)
		if !accepted {
			w.q.pending = pending
			return
		}
	}
	w.q.pending = true
	if status, known := w.reads.GetStatus(id); known && status.Outcome() != store.OutcomePending {
		return
	}
	w.q.candidates = []manifest.NamedResource{id}
	if len(w.admission.entryTargets) != 0 && id == evidence.object.Named() {
		w.q.reach = w.captureEntryReach(id)
	}
	w.reads.RequireQualification(id)
}
