package discovery

import (
	"cmp"
	"context"
	"fmt"
	"io/fs"
	"log/slog"
	"maps"
	"path"
	"path/filepath"
	"slices"

	"github.com/home-operations/flate/internal/engine/change/inputprojection"
	"github.com/home-operations/flate/internal/engine/selection"

	"github.com/home-operations/flate/internal/engine/kustomize"
	"github.com/home-operations/flate/internal/engine/loader"
	"github.com/home-operations/flate/internal/engine/source/safepath"
	"github.com/home-operations/flate/internal/engine/store"
	"github.com/home-operations/flate/pkg/manifest"
)

func (a *Admission) captureApplication(
	ctx context.Context,
	owner *applicationEvidence,
	producer manifest.NamedResource,
	reads *store.ReadSet,
) (*kustomize.EntryEvidence, error) {
	q := a.qualification
	st := a.store
	if q == nil || q.trees == nil {
		return nil, nil //nolint:nilnil // A nil result denotes successful absence.
	}
	if owner.receipt != nil && owner.receipt.Producer == producer {
		return owner.receipt, nil
	}
	support := cmp.Or(owner.owner, owner.declaration)
	if support == (manifest.NamedResource{}) || owner.builtRoot != "" || owner.document == nil {
		return nil, nil //nolint:nilnil // Incomplete evidence grants no receipt.
	}
	c := entryCapture{
		admission:     a,
		store:         st,
		reads:         reads,
		owner:         owner.owner,
		qualification: q,
		seen:          map[manifest.NamedResource]bool{},
	}
	c.queue = []manifest.NamedResource{support}
	if owner.owner == (manifest.NamedResource{}) {
		source := owner.application.Source
		if artifact := owner.source.artifact; artifact.WorkingTree() && artifact.Extent == store.ExtentInput {
			// An input alias already reads the captured repository content.
			c.seen[source] = true
		} else {
			c.queue = append(c.queue, source)
		}
	}
	if !c.collect() || !c.mapFiles() {
		return nil, nil //nolint:nilnil // Incomplete evidence grants no receipt.
	}
	slices.Sort(c.files)
	var children []store.Child
	if output, ok := reads.GetArtifact(support, store.ArtifactContent).(*store.Emission); ok {
		children = output.Children
	}
	request := a.entryRequest
	request.Source, request.SubPath, request.EntryOccurrences = a.Input(), a.entryPath(), a.entryOccurrences
	if len(a.entryOccurrences) != 0 {
		for _, occurrence := range a.entryOccurrences {
			c.files = append(c.files, occurrence.File)
		}
		slices.Sort(c.files)
	}
	generatedEntry := owner.visibleGeneration && receiptRenderOwner(support) && owner.source.artifact.WorkingTree() &&
		a.input.MatchesOrigin(a.path, owner.source.artifact.ApplicationOrigin(owner.application.Path))
	evidence, err := q.trees.ApplicationReceipt(ctx, request, kustomize.EntryEvidence{
		GeneratedEntry: generatedEntry,
		Producer:       producer,
		Application:    owner.document,
		Owner:          support,
		Root:           owner.root,
		Inputs:         c.inputs,
		Renders:        c.renders,
		Records:        st.ChildFootprint(support, children, q.loader.Index),
	}, slices.Compact(c.files))
	if err != nil || evidence.Receipt == nil {
		return nil, err
	}
	return &evidence, nil
}

type entryCapture struct {
	admission     *Admission
	store         *store.Store
	reads         *store.ReadSet
	owner         manifest.NamedResource
	qualification *entryQualification
	files         []string
	inputs        []kustomize.EntryInput
	renders       []*manifest.RenderReceipt
	seen          map[manifest.NamedResource]bool
	queue         []manifest.NamedResource
}

func (c *entryCapture) collect() bool {
	for len(c.queue) != 0 {
		id := c.queue[0]
		c.queue = c.queue[1:]
		if c.seen[id] {
			continue
		}
		c.seen[id] = true
		if !c.visit(id) {
			return false
		}
	}
	for file, readers := range c.qualification.loader.Index.Readers {
		if slices.ContainsFunc(readers, func(reader manifest.FileReader) bool {
			return reader.Exact && !reader.Materialize && c.seen[reader.Reader]
		}) {
			c.files = append(c.files, file)
		}
	}
	return true
}

func (c *entryCapture) visit(id manifest.NamedResource) bool {
	st, q := c.reads, c.qualification
	if data := st.ReferenceFacts(id, q.loader.Index, manifest.NamedResource{}).Data; data != nil {
		c.inputs = append(c.inputs, kustomize.EntryInput{Resource: id, Data: data})
	}
	artifact, source := st.GetArtifact(id, store.ArtifactContent).(*store.SourceArtifact)
	if !c.captureSource(id, artifact, source) || !c.captureOutput(id) {
		return false
	}
	record, _ := q.loader.Index.Lookup(id)
	if source && !artifact.WorkingTree() && record.Path == "" {
		slog.Debug(
			"discovery: entry application evidence incomplete",
			"resource",
			id,
			"reason",
			"source declaration cannot be reacquired",
		)
		return false
	}
	if st.GetObject(id) == nil && record.Path == "" && len(st.Productions(id, q.loader.Index)) == 0 &&
		id != manifest.BootstrapSourceID {
		return false
	}
	if record.File != "" {
		c.files = append(c.files, record.File)
	}
	c.files = append(c.files, record.Inputs...)
	c.collectRelations(id)
	return true
}

func (c *entryCapture) captureSource(id manifest.NamedResource, artifact *store.SourceArtifact, source bool) bool {
	if !id.KindInfo().SourceArtifact {
		return true
	}
	if !source || artifact.FS == nil || !artifact.Reads.Valid() {
		return false
	}
	if !artifact.WorkingTree() || artifact.Extent != store.ExtentInput {
		c.inputs = append(c.inputs, kustomize.EntryInput{Resource: id, Artifact: artifact})
	}
	return true
}

func (c *entryCapture) captureOutput(id manifest.NamedResource) bool {
	if output, ok := c.reads.GetArtifact(id, store.ArtifactContent).(*store.Emission); ok {
		if !output.Complete || !output.OutputReads.Valid() || output.Render == nil && !id.ReportHidden() {
			slog.Debug(
				"discovery: entry application evidence incomplete",
				"resource",
				id,
				"reason",
				"producer output lacks a valid receipt",
			)
			return false
		}
		if output.Render != nil {
			c.renders = append(c.renders, output.Render)
		}
		c.queue = append(c.queue, slices.Collect(output.OutputReads.Inputs())...)
	} else if id == c.owner {
		return false
	}
	return true
}

func (c *entryCapture) collectRelations(id manifest.NamedResource) {
	st, q := c.reads, c.qualification
	productions := st.Productions(id, q.loader.Index)
	for i := range productions {
		production := &productions[i]
		if production.Owner != (manifest.NamedResource{}) {
			c.queue = append(c.queue, production.Owner)
		}
		if production.Declaration != (manifest.NamedResource{}) {
			c.queue = append(c.queue, production.Declaration)
		}
	}
	collect := func(ref manifest.Reference) bool {
		c.queue = append(c.queue, ref.NamedResource)
		return true
	}
	if object := st.GetDeclaration(id); object != nil {
		manifest.References(object, manifest.ReferenceRelations)(collect)
		return
	}
	q.loader.References(id)(collect)
}

func (c *entryCapture) mapFiles() bool {
	a := c.admission
	var inputOrigin string
	var mounts map[string]*store.SourceArtifact
	mountsReady := false
	for i, file := range c.files {
		absolute := filepath.Join(a.input.LocalPath, filepath.FromSlash(file))
		if safepath.Contains(a.input.LocalPath, absolute) {
			continue
		}
		if !mountsReady {
			inputOrigin = a.input.OriginPath(a.input.LocalPath)
			var scratch [8]manifest.NamedResource
			admitted := slices.AppendSeq(scratch[:0], maps.Keys(a.resolvedSources.snapshot()))
			mounts = loader.AdmittedWorkingTreeMounts(c.reads.GetArtifact, nil, admitted...)
			mountsReady = true
		}
		origin := loader.ProjectWorkingTreePath(absolute, mounts)
		if !safepath.Contains(inputOrigin, origin) {
			return false
		}
		relative, _ := filepath.Rel(inputOrigin, origin)
		c.files[i] = filepath.ToSlash(relative)
	}
	return true
}

// Admission borrows the canonical entry owner before and after Freeze.
func (d *Session) Admission() *Admission {
	if d == nil {
		return nil
	}
	return d.entryAdmission
}

func (a *Admission) publishEntry(reads *store.ReadSet, producer manifest.NamedResource) *selection.EntryRoute {
	if a.loader == nil || a.loader.Options.Session == nil {
		return nil
	}
	if reads == nil && a.acceptedEntry != nil {
		a.loader.Options.Session.PublishEntry(a.acceptedEntry)
		return a.acceptedEntry
	}
	changed := a.entryChanged || len(a.entryTargets) == 0 && len(a.changedApplications) != 0
	route := &selection.EntryRoute{
		Bound: a.entryBound.Load(), Selected: len(a.entryTargets) != 0, Changed: changed, Projected: a.entryProjected,
		ProjectedPatches: a.entryProjectedPatches,
	}
	if a.trees != nil && a.input != nil && a.requestedEntry.Source != (manifest.NamedResource{}) {
		route.Application = &a.requestedEntry
	}
	q := a.qualification
	if q != nil {
		route.Previous = q.previous.Producer
		route.PreviousGeneratedEntry = q.previous.GeneratedEntry
		route.Reach = q.reach
		route.Replay = q.replay
		if len(q.matches) != 0 {
			route.Root = q.matches[0].root
		}
	} else if a.entryEvidence != nil {
		route.Previous, route.Root = a.entryEvidence.Producer, a.entryEvidence.Root
		route.PreviousGeneratedEntry = a.entryEvidence.GeneratedEntry
	}
	if reads != nil && reads.Valid() {
		route.Reads, route.Producer = reads, producer
	}
	switch {
	case q == nil, !route.Selected:
	case route.Bound, route.Reads != nil:
		winners := q.winners
		route.Winners = &winners
	}
	a.loader.Options.Session.PublishEntry(route)
	switch {
	case route.Reads != nil, route.Winners != nil:
		return route
	default:
		return nil
	}
}

// EntryProducers MUST expose only the pending ordered prefix.
func (a *Admission) EntryProducers() []manifest.NamedResource {
	if a == nil || a.entryBound.Load() || a.qualification == nil {
		return nil
	}
	q := a.qualification
	if q.receiptPending && q.previous.Receipt != nil && len(q.refresh) != 0 {
		return slices.Clone(q.refresh)
	}
	return slices.Clone(q.candidates)
}

func (w *qualificationWalk) qualifyEntry(ctx context.Context) error {
	a, st := w.admission, w.store
	if w.reads == nil {
		w.reads = &store.ReadSet{}
		w.reads.Init(st, manifest.BootstrapSourceID, nil)
		defer st.Retire(w.reads)
		defer w.reads.ReleaseCapture()
	}
	if a.qualification == nil {
		if a.loader == nil {
			a.store = st
			a.loader = loader.New(st)
			a.loader.SourceProvider = a.Provider
			a.loader.SourceRoot = a.input.LocalPath
			a.entryDemand = true
		}
		session := a.session
		a.qualification = &entryQualification{
			loader:  a.loader,
			trees:   a.trees,
			session: session,
			closed:  !a.entryDemand || a.entryBound.Load() && (session == nil || a.assessment.Valid()),
		}
	}
	if a.entryEvidence != nil {
		a.qualification.previous = *a.entryEvidence
	}
	return a.qualification.advance(ctx, w)
}

func (a *Admission) cancelQualification() {
	a.retireQualification()
	a.publishEntry(nil, manifest.NamedResource{})
}

// BindEntry proposals MUST pass loader admission before AcceptEntry.
func (a *Admission) BindEntry(ctx context.Context, st *store.Store) (*manifest.Kustomization, Binding, error) {
	if a == nil {
		return nil, Binding{}, nil
	}
	root, binding, err := a.captureBinding(ctx, st, a.inventoryPath())
	if binding.Reads != nil {
		a.publishEntry(binding.support, binding.Producer)
	}
	return root, binding, err
}

func (a *Admission) captureBinding(
	ctx context.Context, st *store.Store, inventory string,
) (*manifest.Kustomization, Binding, error) {
	if err := ctx.Err(); err != nil {
		a.cancelQualification()
		return nil, Binding{}, fmt.Errorf("context: %w", err)
	}
	a.store = st
	if a.assessment != nil && a.assessment.Valid() && a.session == nil && a.applications.Load() == nil {
		return nil, Binding{}, nil
	}
	reads := &store.ReadSet{}
	reads.Init(st, manifest.BootstrapSourceID, nil)
	reads.GetObject(manifest.BootstrapSourceID)
	reads.GetArtifact(manifest.BootstrapSourceID, store.ArtifactContent)
	root, binding, err := a.proposeEntry(ctx, reads, inventory)
	if err == nil && binding.Descendant != "" {
		binding.documents, err = a.requestedDocuments(ctx, binding.Descendant, reads.WorkScope())
	}
	if err != nil || binding.Reads == nil || !reads.Valid() {
		if ctx.Err() != nil {
			a.retireQualification()
		}
		a.publishEntry(nil, manifest.NamedResource{})
		if err != nil || !a.qualification.retainReceiptWait(reads) {
			reads.ReleaseCapture()
			st.Retire(reads)
		}
		return nil, Binding{}, err
	}
	if root != nil && st.GetDeclaration(root.Named()) != nil {
		if current, ok := reads.GetDeclaration(root.Named()).(*manifest.Kustomization); ok &&
			manifest.Equal(current, root) {
			root = current
		}
	}
	binding.support = a.captureEntrySupport(reads, binding.Entry)
	binding.qualification = a.qualification
	a.qualification.proposal = reads
	return root, binding, nil
}

func (a *Admission) proposeEntry(
	ctx context.Context, reads *store.ReadSet, inventory string,
) (*manifest.Kustomization, Binding, error) {
	walk := qualificationWalk{admission: a, inventory: inventory, store: a.store, reads: reads}
	if err := walk.qualifyEntry(ctx); err != nil {
		return nil, Binding{}, err
	}
	q := a.qualification
	named := len(a.entryTargets) != 0
	if q.awaitingProducer(named) || a.awaitingEntryDiscovery(q) {
		return nil, Binding{}, nil
	}
	relative := a.relativeInventoryPath(inventory)
	coordinate := relative
	if inventory != a.path {
		coordinate = a.requestedEntry.Path
	}
	key := manifest.NewEntryKustomization(manifest.BootstrapSourceID, coordinate, manifest.NamedResource{}).Named()
	binding := Binding{Source: manifest.BootstrapSourceID, Key: key, Path: relative, Reads: reads}
	var root *manifest.Kustomization
	var err error
	if len(q.matches) == 0 {
		root, err = a.literalEntry(inventory, relative)
	} else {
		selected := &q.matches[0]
		root = walk.applicationEntry(ctx, selected, relative)
		binding.SupportOwner = selected.owner
		if selected.descendant {
			binding.Descendant = relative
		}
	}
	if err != nil {
		return nil, Binding{}, err
	}
	if root == nil && !a.entryBound.Load() && !named {
		return nil, Binding{}, nil
	}
	if q.receiptCoversTargets(len(a.entryTargets)) {
		binding.Entry, err = a.captureApplication(ctx, &q.matches[0], root.Named(), reads)
		if err != nil {
			return nil, Binding{}, err
		}
	}
	if root != nil {
		binding.Producer, binding.Source, binding.Path = root.Named(), root.EffectiveSourceID(), manifest.NormalizeClaimBase(
			root.Path,
		)
	}
	a.captureConflictingApplications(reads)
	return root, binding, nil
}

func (q *entryQualification) awaitingProducer(named bool) bool {
	return q.receiptPending || q.pending && (named || len(q.matches) == 0)
}

func (a *Admission) awaitingEntryDiscovery(q *entryQualification) bool {
	return len(q.winners) < len(a.entryTargets) && a.session != nil && !a.session.finalized
}

func (q *entryQualification) receiptCoversTargets(targets int) bool {
	if targets == 0 {
		return len(q.matches) != 0
	}
	// A receipt records the support for only one producer.
	return len(q.matches) == 1 && len(q.winners) == targets
}

func (a *Admission) captureConflictingApplications(reads *store.ReadSet) {
	if len(a.entryTargets) != 0 {
		return
	}
	q := a.qualification
	if len(q.matches) > 1 && slices.ContainsFunc(q.matches, func(evidence applicationEvidence) bool {
		return evidence.owner != (manifest.NamedResource{}) &&
			!slices.ContainsFunc(q.matches, func(terminal applicationEvidence) bool {
				return terminal.object.Named() == evidence.owner
			})
	}) {
		detail := make([]string, len(q.matches))
		for i := range q.matches {
			detail[i] = q.matches[i].object.Named().String()
		}
		reads.AddWarning(
			manifest.Warning{
				Category: manifest.WarnConflictingProducers,
				Message:  "multiple applications apply the entry; selected the first by resource identity",
				Detail:   detail,
			},
		)
	}
}

//nolint:nilnil // A directory without a builder supplies no literal entry.
func (a *Admission) literalEntry(directory, relative string) (*manifest.Kustomization, error) {
	input := a.Input()
	if !a.entryDemand {
		input = inputprojection.Authored(input)
	}
	file, known := a.session.cachedEntryBuilder(input, directory)
	if !known {
		file = nativeEntry(input.FS, relative)
	}
	if file == "" {
		return nil, nil
	}
	if _, err := fs.ReadFile(input.FS, filepath.ToSlash(file)); err != nil {
		return nil, manifest.NewError(manifest.ErrInput, fmt.Errorf("inspect entry package: %w", err), "")
	}
	return manifest.NewEntryKustomization(manifest.BootstrapSourceID, relative, manifest.NamedResource{}), nil
}

func (w *qualificationWalk) applicationEntry(
	ctx context.Context,
	selected *applicationEvidence,
	relative string,
) *manifest.Kustomization {
	root := manifest.NewEntryKustomization(
		manifest.BootstrapSourceID,
		relative,
		selected.object.Named(),
	)
	if candidate, ok := selected.object.(*manifest.Kustomization); ok {
		if !w.reads.Preflight(candidate.Named()).DependencyCycle &&
			(selected.visibleGeneration || w.reuseEntry(ctx, selected, candidate, relative)) {
			root = candidate
		}
	}

	return root
}

func (w *qualificationWalk) reuseEntry(
	ctx context.Context,
	selected *applicationEvidence,
	candidate *manifest.Kustomization,
	relative string,
) bool {
	a, reads := w.admission, w.reads
	_, policy := reads.Lookup(candidate.Named())
	if !policy.Root {
		return false
	}
	if selected.owner == (manifest.NamedResource{}) ||
		selected.owner == manifest.NewEntryKustomization(manifest.BootstrapSourceID, relative, manifest.NamedResource{}).
			Named() {
		record, indexed := a.qualification.loader.Index.Lookup(candidate.Named())
		direct := record.Builder == "" && record.Authored != candidate.Named() ||
			record.Builder != "" && record.File == record.Builder
		if indexed && !reads.Policy(candidate.Named()).Supplied() &&
			!selection.Root(
				selection.CaptureRoot(
					a.qualification.loader.RootEvidence(candidate.Named(), nil),
					a.qualification.loader.Store,
					a.qualification.loader.Index,
				),
			).InitialEntry &&
			direct {
			return false
		}
	}
	if a.session != nil {
		return a.session.entryCandidateReady(ctx, reads, candidate)
	}
	for ref := range manifest.References(candidate, manifest.ReferenceRelations) {
		if ref.Ordering && reads.GetDeclaration(ref.NamedResource) == nil {
			return false
		}
	}
	eligibility := entryPrerequisites{loader: a.loader, reads: reads}
	return eligibility.eligible(candidate, nil)
}

// AcceptEntry installs only a proposal whose original support remains current.
//
//nolint:gocritic // The SDK accepts an immutable binding proposal by value.
func (a *Admission) AcceptEntry(ctx context.Context, binding Binding) (Binding, error) {
	accepted := false
	defer func() {
		if !accepted {
			a.publishEntry(nil, manifest.NamedResource{})
		}
	}()
	if valid, err := a.acceptEntryReads(ctx, &binding); err != nil || !valid {
		return Binding{}, err
	}
	reads := binding.Reads
	q := a.qualification
	usesPreviousReceipt := q != nil && binding.Entry != nil && binding.Entry.Receipt == q.previous.Receipt
	if usesPreviousReceipt && (a.entryEvidence == nil || a.entryEvidence.Receipt != binding.Entry.Receipt) {
		slog.Debug("discovery: entry application reused", "resource", binding.Entry.Owner)
	}
	a.acceptProducerKeys(binding.Producer, binding.Key)
	a.entryEvidence = binding.Entry
	a.assessment = reads
	a.store.Retire(reads)
	binding.Reads = nil
	a.entryBound.Store(true)
	if len(a.entryTargets) != 0 {
		// Without fresh support reads, publishEntry would reuse the old winners.
		a.acceptedEntry = nil
	}
	a.acceptedEntry = a.publishEntry(binding.support, binding.Producer)
	binding.support = nil
	binding.qualification = nil
	accepted = true
	if q := a.qualification; q != nil {
		a.loader, a.trees = q.loader, q.trees
		q.close()
		a.qualification = nil
	}
	return binding, nil
}

func (a *Admission) acceptEntryReads(ctx context.Context, binding *Binding) (bool, error) {
	if err := ctx.Err(); err != nil {
		a.retireQualification()
		return false, err
	}
	if binding.Reads == nil || !binding.Reads.Valid() {
		if binding.Reads != nil {
			binding.Reads.ReleaseCapture()
			binding.Reads.Retire()
		}
		return false, nil
	}
	q := a.qualification
	if binding.qualification != nil && (q != binding.qualification || q.proposal != binding.Reads) {
		return false, nil
	}
	if q != nil && !a.applications.CompareAndSwap(q.arrivals, nil) {
		return false, nil
	}
	reads := binding.Reads
	reads.Accept(store.AssessmentEntryBinding)
	if !reads.Valid() {
		reads.ReleaseCapture()
		reads.Retire()
		return false, nil
	}
	if binding.Entry != nil && q != nil && q.trees != nil {
		if err := q.trees.RecordEntry(ctx, binding.Entry.Receipt, reads); err != nil {
			return false, err
		}
	}
	if !reads.Valid() {
		reads.ReleaseCapture()
		reads.Retire()
		return false, nil
	}
	return true, nil
}

func (w *qualificationWalk) ordinaryApplication(
	evidence *applicationEvidence,
	application *manifest.EntryApplication,
	possible bool,
) bool {
	id := evidence.object.Named()
	policy := w.reads.Policy(id)
	if w.q.session != nil || !policy.Root || policy.Supplied() ||
		!w.q.loader.Options.Session.Snapshot().OrdinaryDemand(id) || !w.reads.ControllerDemand(id) {
		return false
	}
	if possible {
		return true
	}
	info, err := fs.Stat(evidence.source.artifact.FS, path.Clean(application.Path))
	return err == nil && info.IsDir()
}

func (a *Admission) captureEntrySupport(original *store.ReadSet, entry *kustomize.EntryEvidence) *store.ReadSet {
	if a.loader == nil || a.loader.Options.Session == nil || a.qualification == nil ||
		len(a.qualification.matches) == 0 ||
		!original.Valid() {
		return nil
	}
	q := a.qualification
	selected := &q.matches[0]
	application := selected.application
	var support *store.ReadSet
	var current bool
	if selected.receipt != nil && selected.receipt == entry {
		support = &store.ReadSet{}
		support.Init(a.store, manifest.BootstrapSourceID, nil)
		q.captureReceiptInputs(support)
		current = true
		if a.entryBound.Load() || receiptRenderOwner(selected.owner) {
			current, _ = selected.ownerAccepts(support)
		}
		if selected.declaration != (manifest.NamedResource{}) {
			support.GetDeclaration(selected.declaration)
		}
	} else {
		walk := qualificationWalk{q: q, admission: a, store: a.store}
		support, current = walk.preparationSupport(selected, selected.object.Named(), application.Source)
		if object := a.store.GetDeclaration(selected.object.Named()); object != nil {
			current = current && manifest.Equal(support.GetDeclaration(selected.object.Named()), selected.object)
		}
	}
	defer a.store.Retire(support)
	support.GetArtifact(manifest.BootstrapSourceID, store.ArtifactContent)
	a.applicationSource(a.store, support, &application)
	if !current || !support.MatchesContent(original, application.Source) ||
		!support.MatchesContent(original, manifest.BootstrapSourceID) ||
		!original.Valid() ||
		!support.Valid() {
		support.ReleaseCapture()
		return nil
	}
	return support
}

func (a *Admission) acceptProducerKeys(producer, key manifest.NamedResource) {
	if len(a.entryTargets) > 1 {
		// A shared comparison key cannot pair independent roots across snapshots.
		producer = manifest.NamedResource{}
	}
	l := a.loader
	if q := a.qualification; q != nil {
		l = q.loader
	}
	if current, known := store.Bindings(l.Index).Slot(producer); !known || current != key {
		index := *l.Index
		index.ProducerKeys = nil
		if producer != (manifest.NamedResource{}) {
			index.ProducerKeys = map[manifest.NamedResource]manifest.NamedResource{producer: key}
		}
		l.Index = &index
	}
}

// PrivateEntryOperands returns physical roots that must settle before ordinary full execution.
func (a *Admission) PrivateEntryOperands() []manifest.NamedResource {
	if a == nil || a.qualification == nil || a.qualification.operandsSettled || a.entryBound.Load() {
		return nil
	}
	if len(a.entryTargets) != 0 {
		return a.qualification.candidates
	}
	return a.qualification.active
}

func (q *entryQualification) orderMatches() {
	slices.SortFunc(q.matches, func(a, b applicationEvidence) int { return a.object.Named().Compare(b.object.Named()) })
	q.matches = slices.CompactFunc(
		q.matches,
		func(a, b applicationEvidence) bool { return a.object.Named() == b.object.Named() },
	)
}
