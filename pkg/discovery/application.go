package discovery

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"maps"
	"path/filepath"
	"slices"

	"github.com/home-operations/flate/pkg/kustomize"
	"github.com/home-operations/flate/pkg/loader"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source/safepath"
	"github.com/home-operations/flate/pkg/store"
)

type entryQualification struct {
	loader     *loader.Loader
	trees      *kustomize.TreeCache
	current    manifest.NamedResource
	receipt    *manifest.RenderReceipt
	accepted   *kustomize.EntryEvidence
	previous   kustomize.EntryEvidence
	refresh    []manifest.NamedResource
	candidates []manifest.NamedResource
	visited    map[manifest.NamedResource]bool
}

func (a *Admission) captureApplication(
	ctx context.Context,
	st *store.Store,
	owner manifest.BaseManifest,
) (*manifest.RenderReceipt, error) {
	q := a.qualification
	if q == nil {
		return nil, nil //nolint:nilnil // A nil result denotes successful absence.
	}
	if q.receipt != nil {
		return q.receipt, nil
	}
	document := q.captureDocument(st, owner)
	if document == nil {
		return nil, nil //nolint:nilnil // Incomplete evidence grants no receipt.
	}
	c := entryCapture{admission: a, store: st, qualification: q, seen: map[manifest.NamedResource]bool{}}
	c.queue = []manifest.NamedResource{q.current}
	if !c.collect() || !c.mapFiles() {
		return nil, nil //nolint:nilnil // Incomplete evidence grants no receipt.
	}
	slices.Sort(c.files)
	output, ok := st.GetArtifact(q.current, store.ArtifactContent).(*store.Emission)
	if !ok {
		return nil, nil //nolint:nilnil // Incomplete evidence grants no receipt.
	}
	return q.trees.ApplicationReceipt(ctx, a.input, kustomize.EntryEvidence{
		Application: document, Owner: q.current, Inputs: c.inputs, Renders: c.renders,
		Records: st.ChildFootprint(q.current, output.Children, q.loader.Index),
	}, slices.Compact(c.files))
}

type entryCapture struct {
	admission     *Admission
	store         *store.Store
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
	st, q := c.store, c.qualification
	if supplied := st.ReferenceFacts(id, q.loader.Index).Supplied; supplied != nil {
		c.inputs = append(c.inputs, kustomize.EntryInput{Resource: id, Data: supplied})
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
	if output, ok := c.store.GetArtifact(id, store.ArtifactContent).(*store.Emission); ok {
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
	} else if id == c.qualification.current {
		return false
	}
	return true
}

func (c *entryCapture) collectRelations(id manifest.NamedResource) {
	st, q := c.store, c.qualification
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
		artifact, _ := c.store.GetArtifact(id, store.ArtifactContent).(*store.SourceArtifact)
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

func (d *Session) applications() []manifest.BaseManifest {
	objects := map[manifest.NamedResource]manifest.BaseManifest{}
	add := func(obj manifest.BaseManifest) {
		if _, valid := manifest.ApplicationFor(obj); valid && objects[obj.Named()] == nil {
			objects[obj.Named()] = obj
		}
	}
	{
		childValues := d.loader.PathObjects()
		for childIndex := range childValues {
			child := childValues[childIndex]

			obj := d.cfg.Store.GetObject(child.Identity)
			if obj == nil {
				obj = d.loader.CatalogObject(child.Identity)
			}
			add(obj)
		}
	}
	for id, record := range d.loader.Index.Records { //nolint:gocritic // Map iteration preserves a value snapshot without an extra lookup.
		obj := record.Declaration
		if obj == nil && objects[id] == nil {
			obj = d.loader.CatalogObject(id)
			if obj == nil {
				obj = d.loader.IndexedObject(id, record).Obj
			}
		}
		add(obj)
	}
	var applications []manifest.BaseManifest
	if len(objects) != 0 {
		applications = make([]manifest.BaseManifest, 0, len(objects))
	}
	for _, id := range slices.SortedFunc(maps.Keys(objects), manifest.NamedResource.Compare) {
		applications = append(applications, objects[id])
	}
	return applications
}

type applicationEvidence struct {
	object      manifest.BaseManifest
	application manifest.EntryApplication
	source      entryResolution
	history     bool
	next        *applicationEvidence
}

// AddApplication captures published application evidence without resolving or rendering it.
func (a *Admission) AddApplication(st *store.Store, obj manifest.BaseManifest) {
	a.addApplication(st, obj, false)
}

func (a *Admission) addApplication(st *store.Store, obj manifest.BaseManifest, history bool) {
	if a == nil || a.entryBound.Load() {
		return
	}
	application, valid := manifest.ApplicationFor(obj)
	if !valid {
		return
	}
	evidence := &applicationEvidence{
		object:      obj,
		application: application,
		source:      a.applicationSource(st, &application),
		history:     history,
	}
	for {
		evidence.next = a.applications.Load()
		if a.applications.CompareAndSwap(evidence.next, evidence) {
			return
		}
	}
}

// AddHistory admits application evidence only from validated accepted render history.
//
//nolint:gocritic // Public history admission preserves its record value snapshot.
func (a *Admission) AddHistory(st *store.Store, id manifest.NamedResource, record manifest.DiscoveryRecord) {
	if record.Application == "" {
		return
	}
	var document map[string]any
	if err := json.Unmarshal([]byte(record.Application), &document); err != nil {
		return
	}
	obj, _, _, err := manifest.ParseDoc(document, manifest.ParseDocOptions{})
	if err == nil && obj != nil && obj.Named() == id {
		a.addApplication(st, obj, true)
	}
}

func (a *Admission) applicationSource(st *store.Store, application *manifest.EntryApplication) entryResolution {
	resolved := entryResolution{object: st.GetObject(application.Source)}
	resolved.artifact, _ = st.GetArtifact(application.Source, store.ArtifactContent).(*store.SourceArtifact)
	if resolved.object == nil {
		resolved.object = a.sources[application.Source].object
	}
	if resolved.object == nil {
		resolved.object = application.Declaration
	}
	return resolved
}

func (a *Admission) qualifyEntry(
	ctx context.Context,
	st *store.Store,
	objects []manifest.BaseManifest,
) ([]manifest.BaseManifest, error) {
	var appliers []manifest.BaseManifest
	for _, obj := range objects {
		application, valid := manifest.ApplicationFor(obj)
		if !valid {
			continue
		}
		resolved := a.applicationSource(st, &application)
		qualified, err := a.qualifyApplication(ctx, &application, &resolved)
		if err != nil {
			return nil, err
		}
		if qualified {
			appliers = append(appliers, obj)
		}
	}
	for evidence := a.applications.Load(); evidence != nil; evidence = evidence.next {
		if !evidence.history {
			if current := st.GetObject(
				evidence.object.Named(),
			); current == nil ||
				!manifest.Equal(current, evidence.object) {
				continue
			}
		}
		obj, application, resolved := evidence.object, evidence.application, evidence.source
		qualified, err := a.qualifyApplication(ctx, &application, &resolved)
		if err != nil {
			return nil, err
		}
		if qualified {
			appliers = append(appliers, obj)
		}
	}
	slices.SortStableFunc(appliers, func(a, b manifest.BaseManifest) int { return a.Named().Compare(b.Named()) })
	return slices.CompactFunc(appliers, func(a, b manifest.BaseManifest) bool { return a.Named() == b.Named() }), nil
}

func (a *Admission) qualifyApplication(
	ctx context.Context,
	application *manifest.EntryApplication,
	resolved *entryResolution,
) (bool, error) {
	artifact := resolved.artifact
	if artifact == nil {
		var err error
		*resolved, err = a.resolve(ctx, application.Source, resolved.object)
		if err != nil {
			return false, err
		}
		artifact = resolved.artifact
	}
	if artifact == nil {
		return false, nil
	}
	return loader.AppliesToEntry(resolved, application, a.input, a.path), nil
}

//nolint:ireturn // loader.SourceView requires this manifest interface signature.
func (r *entryResolution) GetObject(manifest.NamedResource) manifest.BaseManifest {
	return r.object
}

//nolint:ireturn // loader.SourceView requires this artifact interface signature.
func (r *entryResolution) GetArtifact(manifest.NamedResource, store.ArtifactRead) store.Artifact {
	if r.artifact == nil {
		return nil
	}
	return r.artifact
}

//nolint:ireturn // Declarations retain their heterogeneous manifest types.
func (a *Admission) entry(
	ctx context.Context,
	st *store.Store,
	objects []manifest.BaseManifest,
) (*manifest.Kustomization, manifest.BaseManifest, error) {
	appliers, err := a.qualifyEntry(ctx, st, objects)
	if err != nil {
		return nil, nil, err
	}
	relative, _ := filepath.Rel(a.input.LocalPath, a.path)
	if len(appliers) == 0 {
		found := false
		for _, filename := range manifest.KustomizeBuilderFilenames {
			_, err := fs.ReadFile(a.input.FS, filepath.ToSlash(filepath.Join(relative, filename)))
			if err != nil && !errors.Is(err, fs.ErrNotExist) {
				return nil, nil, manifest.NewError(manifest.ErrInput, fmt.Errorf("inspect entry package: %w", err), "")
			}
			if err == nil {
				found = true
				break
			}
		}
		if !found {
			return nil, nil, nil
		}
	}
	var owner manifest.BaseManifest
	var applier manifest.NamedResource
	if len(appliers) != 0 {
		owner = appliers[0]
		applier = owner.Named()
	}
	return manifest.NewEntryKustomization(manifest.BootstrapSourceID, filepath.ToSlash(relative), applier), owner, nil
}

// BindEntry qualifies the admitted entry against the current declaration fixed point.
// It returns immutable producer metadata; callers admit the producer before publication.
func (a *Admission) BindEntry(ctx context.Context, st *store.Store) (*manifest.Kustomization, Binding, error) {
	if a == nil {
		return nil, Binding{}, nil
	}
	if err := ctx.Err(); err != nil {
		return nil, Binding{}, fmt.Errorf("context: %w", err)
	}
	if q := a.qualification; q != nil {
		if !q.reusePrevious(ctx, st, a) {
			return nil, Binding{}, nil
		}
		q.advance(st)
	}
	root, owner, err := a.entry(ctx, st, st.ListObjects("", ""))
	if err != nil || root == nil {
		return nil, Binding{}, err
	}
	if owner != nil {
		root.EntryReceipt, err = a.captureApplication(ctx, st, owner)
		if err != nil {
			return nil, Binding{}, err
		}
	}
	root, binding := a.bindApplication(st, root, owner)
	return root, binding, nil
}

func (a *Admission) bindApplication(
	st *store.Store,
	root *manifest.Kustomization,
	owner manifest.BaseManifest,
) (*manifest.Kustomization, Binding) {
	key := root.Named()
	if candidate, ok := owner.(*manifest.Kustomization); ok && len(candidate.DependsOn) == 0 &&
		(candidate.PostBuild == nil || len(candidate.PostBuild.SubstituteFrom) == 0) {
		if current := st.GetObject(candidate.Named()); current == nil || manifest.Equal(current, candidate) {
			root = candidate
		}
	}
	a.entryBound.Store(true)
	a.applications.Store(nil)
	var evidence *kustomize.EntryEvidence
	if a.qualification != nil {
		evidence = a.qualification.accepted
		*a.qualification = entryQualification{}
		a.qualification = nil
	}
	source := root.SourceID()
	if source.Name == "" || source.Kind == "" {
		source = manifest.BootstrapSourceID
	}
	return root, Binding{
		Producer: root.Named(),
		Source:   source,
		Key:      key,
		Path:     manifest.NormalizeClaimBase(root.Path),
		Entry:    evidence,
	}
}

func (q *entryQualification) reusePrevious(ctx context.Context, st *store.Store, a *Admission) bool {
	if q.previous.Receipt == nil {
		return true
	}
	for _, id := range q.refresh {
		status, _ := st.GetStatus(id)
		if status.Outcome() == store.OutcomeFailed || status.Outcome() == store.OutcomeSkipped {
			q.previous = kustomize.EntryEvidence{}
			break
		}
		if status.Outcome() != store.OutcomePassed {
			return false
		}
	}
	if q.previous.Receipt != nil && q.trees.EntryValid(ctx, q.previous, st) {
		object, _, _, err := manifest.ParseDoc(q.previous.Application, manifest.ParseDocOptions{})
		if err == nil && object != nil && ctx.Err() == nil {
			a.addApplication(st, object, true)
			q.receipt = q.previous.Receipt
			accepted := q.previous
			q.accepted = &accepted
			slog.Debug("discovery: entry application reused", "producer", q.previous.Owner)
		}
	}
	q.previous = kustomize.EntryEvidence{}
	return true
}

// EntryInputs returns receipt inputs or one producer needed to qualify a changed literal entry.
func (a *Admission) EntryInputs() []manifest.NamedResource {
	if a == nil || a.qualification == nil || a.entryBound.Load() {
		return nil
	}
	q := a.qualification
	if q.previous.Receipt != nil {
		return slices.Clone(q.refresh)
	}
	if len(q.candidates) != 0 {
		return slices.Clone(q.candidates[:1])
	}
	return nil
}

func (d *Session) entryReceipt(ctx context.Context, a *Admission) {
	q := a.qualification
	relative, _ := filepath.Rel(d.cfg.Input.LocalPath, d.cfg.Path)
	root := manifest.NewEntryKustomization(
		manifest.BootstrapSourceID,
		filepath.ToSlash(relative),
		manifest.NamedResource{},
	)
	q.previous = d.cfg.Trees.PreviousEntry(
		ctx,
		kustomize.RenderRequest{
			Source:                  d.cfg.Input,
			ApplyIgnore:             true,
			SubPath:                 root.Path,
			Spec:                    root,
			WipeSecrets:             d.cfg.WipeSecrets,
			StrictSubstitutions:     d.cfg.StrictSubstitutions,
			SkipKinds:               d.cfg.SkipKinds,
			DependencySubstitutions: d.substitutions,
		},
	)
	for i := range q.previous.Inputs {
		input := &q.previous.Inputs[i]
		if input.Binding.Present {
			q.refresh = append(q.refresh, input.Resource)
		}
	}
	if !d.literalEntryChanged() {
		return
	}
	q.visited = make(map[manifest.NamedResource]bool)
	for id := range d.loader.Index.Files() {
		record, _ := d.loader.Index.Lookup(id)
		if record.Path == "" || !safepath.Contains(d.cfg.Path, record.Path) {
			continue
		}
		object := d.loader.CatalogObject(id)
		if object == nil || record.Parent == (manifest.NamedResource{}) && !d.loader.AdmitsRoot(object, record.Path) {
			continue
		}
		if _, _, applies := object.SourcePath(); applies {
			continue
		}
		if _, producer := object.(interface {
			EmissionPolicy() manifest.EmissionPolicy
		}); producer {
			q.candidates = append(q.candidates, id)
			q.visited[id] = true
		}
	}
	slices.SortFunc(q.candidates, manifest.NamedResource.Compare)
}

func (d *Session) literalEntryChanged() bool {
	changes := d.loader.Options.Changes
	if changes == nil {
		return false
	}
	delta, _, _ := changes.ForSource(manifest.BootstrapSourceID)
	for id := range d.loader.Index.Files() {
		record, _ := d.loader.Index.Lookup(id)
		if id.KindInfo().Runnable {
			continue
		}
		root := record.Parent == (manifest.NamedResource{}) && record.BuilderOwner == (manifest.NamedResource{})
		if !root || filepath.Dir(record.Path) != d.cfg.Path || !d.cfg.Selection.Matches(id) ||
			!delta.Contains(record.File) {
			continue
		}
		object := d.loader.CatalogObject(id)
		if object == nil {
			continue
		}
		if _, _, applies := object.SourcePath(); applies {
			continue
		}
		read := false
		for reader := range d.loader.Index.ReadersOfFile(record.File) {
			if reader.Reader != id && reader.Reader != (manifest.NamedResource{}) {
				read = true
				break
			}
		}
		if !read {
			return true
		}
	}
	return false
}

func (q *entryQualification) advance(st *store.Store) {
	if len(q.candidates) == 0 {
		return
	}
	id := q.candidates[0]
	output, ok := st.GetArtifact(id, store.ArtifactContent).(*store.Emission)
	status, _ := st.GetStatus(id)
	complete := ok && output.Complete && output.OutputReads.Valid()
	settled := complete
	switch status.Outcome() {
	case store.OutcomePending, store.OutcomePassed, store.OutcomeBlocked:
	case store.OutcomeFailed, store.OutcomeSkipped, store.OutcomeSuspended, store.OutcomeUnchanged:
		settled = true
	}
	if !settled {
		return
	}
	q.candidates = q.candidates[1:]
	if !complete {
		return
	}
	for _, child := range output.Children {
		if _, producer := child.Obj.(interface {
			EmissionPolicy() manifest.EmissionPolicy
		}); !producer || q.visited[child.Obj.Named()] {
			continue
		}
		id := child.Obj.Named()
		q.visited[id] = true
		q.candidates = append(q.candidates, id)
	}
	slices.SortFunc(q.candidates, manifest.NamedResource.Compare)
}

func (q *entryQualification) captureDocument(st *store.Store, owner manifest.BaseManifest) map[string]any {
	producer := st.EmissionOwner(owner.Named())
	if producer == (manifest.NamedResource{}) {
		return nil
	}
	_, document, output := st.GetEmission(owner.Named())
	output.Finish()
	if document != nil {
		q.current = producer
	}
	return document
}
