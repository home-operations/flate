package discovery

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"path"
	"path/filepath"
	"slices"

	"github.com/home-operations/flate/internal/selection"

	"github.com/home-operations/flate/internal/kustomize"
	"github.com/home-operations/flate/internal/loader"
	"github.com/home-operations/flate/internal/source/safepath"
	"github.com/home-operations/flate/internal/store"
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
	if q.accepted != nil {
		return q.accepted, nil
	}
	if owner.owner == (manifest.NamedResource{}) || owner.builtRoot != "" {
		return nil, nil //nolint:nilnil // Construction support is not accepted render evidence.
	}
	document := owner.document
	if document == nil {
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
	c.queue = []manifest.NamedResource{owner.owner}
	if !c.collect() || !c.mapFiles() {
		return nil, nil //nolint:nilnil // Incomplete evidence grants no receipt.
	}
	slices.Sort(c.files)
	output, ok := reads.GetArtifact(owner.owner, store.ArtifactContent).(*store.Emission)
	if !ok {
		return nil, nil //nolint:nilnil // Incomplete evidence grants no receipt.
	}
	request := a.entryRequest
	request.Source, request.SubPath = a.input, a.entryPath()
	evidence, err := q.trees.ApplicationReceipt(ctx, request, kustomize.EntryEvidence{
		Producer:    producer,
		Application: document,
		Owner:       owner.owner,
		Root:        owner.root,
		Inputs:      c.inputs,
		Renders:     c.renders,
		Records:     st.ChildFootprint(owner.owner, output.Children, q.loader.Index),
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
	if source && !artifact.InputAlias && id != manifest.BootstrapSourceID && record.Path == "" {
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
	a := c.admission
	if id != manifest.BootstrapSourceID && !artifact.InputAlias &&
		artifact.OriginPath(artifact.LocalPath) != a.input.OriginPath(a.input.LocalPath) {
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
	references := q.loader.References(id)
	if object := st.GetDeclaration(id); object != nil {
		references = manifest.References(object, manifest.ReferenceRelations)
	}
	for ref := range references {
		c.queue = append(c.queue, ref.NamedResource)
	}
}

func (c *entryCapture) mapFiles() bool {
	a := c.admission
	for i, file := range c.files {
		absolute := filepath.Join(a.input.LocalPath, filepath.FromSlash(file))
		if safepath.Contains(a.input.LocalPath, absolute) {
			continue
		}
		mapped, ok := c.mapFile(absolute)
		if !ok {
			return false
		}
		c.files[i] = mapped
	}
	return true
}

func (c *entryCapture) mapFile(absolute string) (string, bool) {
	a := c.admission
	for id := range a.sources {
		artifact, _ := c.reads.GetArtifact(id, store.ArtifactContent).(*store.SourceArtifact)
		if artifact == nil || !artifact.InputAlias || !safepath.Contains(artifact.LocalPath, absolute) {
			continue
		}
		origin := artifact.OriginPath(absolute)
		if safepath.Contains(a.input.OriginPath(a.input.LocalPath), origin) {
			relative, _ := filepath.Rel(a.input.OriginPath(a.input.LocalPath), origin)
			return filepath.ToSlash(relative), true
		}
	}
	return "", false
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
	if reads == nil && a.acceptedEntry != nil && a.acceptedEntry.Reads != nil {
		a.loader.Options.Session.PublishEntry(a.acceptedEntry)
		return a.acceptedEntry
	}
	route := &selection.EntryRoute{Bound: a.entryBound.Load()}
	if a.trees != nil && a.input != nil && a.requestedEntry.Source != (manifest.NamedResource{}) {
		route.Application = &a.requestedEntry
	}
	if q := a.qualification; q != nil {
		route.Previous = q.previous.Producer
		if q.accepted != nil {
			route.Root = q.accepted.Root
		} else if len(q.matches) != 0 {
			route.Root = q.matches[0].root
		}
	} else if a.entryEvidence != nil {
		route.Previous, route.Root = a.entryEvidence.Producer, a.entryEvidence.Root
	}
	if reads != nil && reads.Valid() {
		route.Reads, route.Producer = reads, producer
	}
	a.loader.Options.Session.PublishEntry(route)
	return route
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

func (a *Admission) qualifyEntry(
	ctx context.Context,
	st *store.Store,
	reads *store.ReadSet,
) ([]manifest.BaseManifest, error) {
	if reads == nil {
		reads = &store.ReadSet{}
		reads.Init(st, manifest.BootstrapSourceID, nil)
		defer st.Retire(reads)
		defer reads.ReleaseCapture()
	}
	if a.qualification == nil {
		if a.loader == nil {
			a.loader = loader.New(st)
			a.loader.SourceRoot = a.input.LocalPath
			a.entryDemand = true
		}
		session := a.session
		a.qualification = &entryQualification{
			loader:  a.loader,
			trees:   a.trees,
			session: session,
			closed:  a.entryBound.Load() || !a.entryDemand,
		}
	}
	if a.entryEvidence != nil {
		a.qualification.previous = *a.entryEvidence
	}
	if err := a.qualification.advance(ctx, a, st, reads); err != nil {
		return nil, err
	}
	out := make([]manifest.BaseManifest, 0, len(a.qualification.matches))
	for i := range a.qualification.matches {
		out = append(out, a.qualification.matches[i].object)
	}
	return out, nil
}

// BindEntry proposals MUST pass loader admission before AcceptEntry.
func (a *Admission) BindEntry(ctx context.Context, st *store.Store) (*manifest.Kustomization, Binding, error) {
	if a == nil {
		return nil, Binding{}, nil
	}
	if err := ctx.Err(); err != nil {
		a.publishEntry(nil, manifest.NamedResource{})
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
	root, binding, err := a.proposeEntry(ctx, reads)
	if err != nil || binding.Reads == nil || !reads.Valid() {
		a.publishEntry(nil, manifest.NamedResource{})
		reads.ReleaseCapture()
		st.Retire(reads)
		return nil, Binding{}, err
	}
	if root != nil && st.GetDeclaration(root.Named()) != nil {
		if current, ok := reads.GetDeclaration(root.Named()).(*manifest.Kustomization); ok &&
			manifest.Equal(current, root) {
			root = current
		}
	}
	binding.support = a.captureEntrySupport(reads)
	a.publishEntry(binding.support, binding.Producer)
	return root, binding, nil
}

func (a *Admission) proposeEntry(ctx context.Context, reads *store.ReadSet) (*manifest.Kustomization, Binding, error) {
	appliers, err := a.qualifyEntry(ctx, a.store, reads)
	if err != nil {
		return nil, Binding{}, err
	}
	q := a.qualification
	if q.pending && len(appliers) == 0 {
		return nil, Binding{}, nil
	}
	relative, _ := filepath.Rel(a.input.LocalPath, a.path)
	relative = filepath.ToSlash(relative)
	key := manifest.NewEntryKustomization(manifest.BootstrapSourceID, relative, manifest.NamedResource{}).Named()
	var root *manifest.Kustomization
	if len(q.matches) == 0 {
		root, err = a.literalEntry(relative)
	} else {
		root = a.applicationEntry(ctx, reads, &q.matches[0])
	}
	if err != nil {
		return nil, Binding{}, err
	}
	if root == nil && !a.entryBound.Load() {
		return nil, Binding{}, nil
	}
	var receipt *kustomize.EntryEvidence
	if len(q.matches) != 0 {
		receipt, err = a.captureApplication(ctx, &q.matches[0], root.Named(), reads)
		if err != nil {
			return nil, Binding{}, err
		}
	}
	binding := Binding{Source: manifest.BootstrapSourceID, Key: key, Path: relative, Reads: reads, Entry: receipt}
	if root != nil {
		binding.Producer, binding.Source, binding.Path = root.Named(), root.SourceID(), manifest.NormalizeClaimBase(
			root.Path,
		)
		if binding.Source.Kind == "" || binding.Source.Name == "" {
			binding.Source = manifest.BootstrapSourceID
		}
	}
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
	return root, binding, nil
}

//nolint:nilnil // A directory without a builder supplies no literal entry.
func (a *Admission) literalEntry(relative string) (*manifest.Kustomization, error) {
	for _, filename := range manifest.KustomizeBuilderFilenames {
		_, err := fs.ReadFile(a.input.FS, filepath.ToSlash(filepath.Join(relative, filename)))
		if errors.Is(err, fs.ErrNotExist) {
			continue
		}
		if err != nil {
			return nil, manifest.NewError(manifest.ErrInput, fmt.Errorf("inspect entry package: %w", err), "")
		}
		return manifest.NewEntryKustomization(manifest.BootstrapSourceID, relative, manifest.NamedResource{}), nil
	}
	return nil, nil
}

func (a *Admission) applicationEntry(
	ctx context.Context,
	reads *store.ReadSet,
	selected *applicationEvidence,
) *manifest.Kustomization {
	relative, _ := filepath.Rel(a.input.LocalPath, a.path)
	root := manifest.NewEntryKustomization(
		manifest.BootstrapSourceID,
		filepath.ToSlash(relative),
		selected.object.Named(),
	)
	if candidate, ok := selected.object.(*manifest.Kustomization); ok && a.reuseEntry(ctx, reads, selected, candidate) {
		root = candidate
	}

	return root
}

func (a *Admission) reuseEntry(
	ctx context.Context,
	reads *store.ReadSet,
	selected *applicationEvidence,
	candidate *manifest.Kustomization,
) bool {
	_, policy := reads.Lookup(candidate.Named())
	if !policy.Root {
		return false
	}
	if selected.owner == (manifest.NamedResource{}) ||
		selected.owner == manifest.NewEntryKustomization(manifest.BootstrapSourceID, a.entryPath(), manifest.NamedResource{}).
			Named() {
		record, indexed := a.qualification.loader.Index.Lookup(candidate.Named())
		if indexed && reads.Policy(candidate.Named()).Origin != manifest.SuppliedDeclaration &&
			!selection.Root(a.qualification.loader.RootEvidence(candidate.Named(), nil)).InitialEntry &&
			(record.Builder == "" && record.Authored != candidate.Named() || record.Builder != "" && record.File == record.Builder) {
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
	eligibility := entryPrerequisites{loader: a.loader, reads: reads, substitutions: a.substitutions}
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
	if err := ctx.Err(); err != nil {
		//nolint:wrapcheck // Cancellation must preserve the caller-visible sentinel identity.
		return Binding{}, err
	}
	if binding.Reads == nil || !binding.Reads.Valid() {
		if binding.Reads != nil {
			binding.Reads.ReleaseCapture()
			binding.Reads.Retire()
		}
		return Binding{}, nil
	}
	if q := a.qualification; q != nil && !a.applications.CompareAndSwap(q.arrivals, nil) {
		return Binding{}, nil
	}
	reads := binding.Reads
	reads.Accept(store.AssessmentEntryBinding)
	if !reads.Valid() {
		reads.ReleaseCapture()
		reads.Retire()
		return Binding{}, nil
	}
	if binding.Entry != nil && a.qualification != nil && a.qualification.trees != nil {
		if err := a.qualification.trees.RecordEntry(ctx, binding.Entry.Receipt, reads); err != nil {
			return Binding{}, err
		}
	}
	if !reads.Valid() {
		reads.ReleaseCapture()
		reads.Retire()
		return Binding{}, nil
	}
	if q := a.qualification; q != nil && q.accepted != nil {
		slog.Debug("discovery: entry application reused", "resource", q.accepted.Owner)
	}
	a.entryEvidence = binding.Entry
	a.assessment = reads
	a.store.Retire(reads)
	binding.Reads = nil
	a.entryBound.Store(true)
	a.acceptedEntry = a.publishEntry(binding.support, binding.Producer)
	binding.support = nil
	accepted = true
	if q := a.qualification; q != nil {
		a.loader, a.trees = q.loader, q.trees
		q.close()
		a.qualification = nil
	}
	return binding, nil
}

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
		Work: d.cfg.Work.RenderWork(), Source: input, ApplyIgnore: true, SubPath: root.Path,
		StrictSubstitutions: d.cfg.StrictSubstitutions, SkipKinds: d.cfg.SkipKinds,
		DependencySubstitutions: d.substitutions, Preparation: &a.entryPreparation,
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

//nolint:gocritic // Results distinguish accepted reuse from pending refresh.
func (q *entryQualification) reusePrevious(
	ctx context.Context,
	a *Admission,
	st *store.Store,
	reads *store.ReadSet,
) (bool, bool) {
	for _, id := range q.refresh {
		reads.GetArtifact(id, store.ArtifactContent)
		status, _ := reads.GetStatus(id)
		if status.Outcome() == store.OutcomeFailed || status.Outcome() == store.OutcomeSkipped {
			q.previous = kustomize.EntryEvidence{}
			return false, false
		}
		if status.Outcome() != store.OutcomePassed {
			q.pending = true
			q.receiptPending = true
			return false, true
		}
	}
	q.captureReceiptInputs(reads)
	if !q.trees.EntryValid(ctx, q.previous, st) || !reads.Valid() {
		q.previous = kustomize.EntryEvidence{}
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
		history:  true,
	}
	if a.entryBound.Load() {
		if accepted, _ := evidence.ownerAccepts(reads); !accepted {
			q.previous = kustomize.EntryEvidence{}
			return false, false
		}
	}
	matched, err := a.readApplication(ctx, st, reads, &evidence)
	if err != nil || !matched || !reads.Valid() {
		q.previous = kustomize.EntryEvidence{}
		return false, false
	}
	accepted := q.previous
	q.accepted = &accepted
	q.matches = append(q.matches, evidence)
	return true, false
}

func (w *qualificationWalk) ordinaryApplication(
	evidence *applicationEvidence,
	application *manifest.EntryApplication,
	possible bool,
) bool {
	id := evidence.object.Named()
	policy := w.reads.Policy(id)
	if w.q.session != nil || !policy.Root || policy.Origin == manifest.SuppliedDeclaration ||
		!w.q.loader.Options.Session.Snapshot().OrdinaryDemand(id) || !w.reads.OrdinaryDemand(id) {
		return false
	}
	if possible {
		return true
	}
	info, err := fs.Stat(evidence.source.artifact.FS, path.Clean(application.Path))
	return err == nil && info.IsDir()
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

func (a *Admission) captureEntrySupport(original *store.ReadSet) *store.ReadSet {
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
	if selected.history && q.accepted != nil {
		support = &store.ReadSet{}
		support.Init(a.store, manifest.BootstrapSourceID, nil)
		q.captureReceiptInputs(support)
		current = true
		if a.entryBound.Load() {
			current, _ = selected.ownerAccepts(support)
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
