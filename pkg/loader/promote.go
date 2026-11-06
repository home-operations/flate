package loader

import (
	"cmp"
	"context"
	"log/slog"
	"maps"
	"path/filepath"
	"slices"

	"github.com/home-operations/flate/internal/kustomizeinput"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

// PromoteOptions preserves the per-document admission boundary.
type PromoteOptions struct {
	Record    manifest.Production
	Reads     *store.ReadSet
	Documents []store.Child                             // Parsed, sanitized documents replace raw file acquisition.
	Admit     func(manifest.BaseManifest) (bool, error) // Admission applies to the requested object and every sibling.
}

// Promote materializes admitted documents and reports whether id is available.
// Authored proves which raw identity may be projected; absent evidence requires an exact id.
// Existing objects take precedence over raw files.
func Promote(
	ctx context.Context,
	st *store.Store,
	id manifest.NamedResource,
	opts PromoteOptions, //nolint:gocritic // The SDK requires an independent options value for each promotion.
) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err //nolint:wrapcheck // Cancellation preserves the caller context error identity.
	}
	record := opts.Record
	path := record.Path
	if st == nil || path == "" && opts.Documents == nil {
		return false, nil
	}
	if obj, policy := st.Lookup(id); obj != nil && policy.Root {
		return opts.Admit(obj)
	}
	source := record.Source
	if source == (manifest.NamedResource{}) {
		source = manifest.BootstrapSourceID
	}
	objs := opts.Documents
	if objs == nil {
		reader := documentReader{}
		var sources sourceReader = st
		if opts.Reads != nil {
			sources = opts.Reads
		}
		mounted := map[string]*store.SourceArtifact{}
		mountSource(mounted, sources.GetArtifact(source, store.ArtifactContent))
		reader.setSources(mounted)
		var err error
		objs, err = reader.parseFile(
			ctx,
			path,
			manifest.ParseDocOptions{DefaultNamespace: ""},
		)
		if err != nil {
			if ctx.Err() != nil {
				return false, ctx.Err() //nolint:wrapcheck // Cancellation preserves the caller context error identity.
			}
			slog.Debug("loader: promote re-parse failed", "id", id.String(), "path", path, "err", err)
			return false, nil
		}
	}
	authored := record.Authored
	if authored == (manifest.NamedResource{}) {
		authored = id
	}
	opts.Record.Source, opts.Record.Authored = source, authored
	if err := opts.admitDocuments(ctx, st, id, objs); err != nil {
		return false, err
	}
	obj := st.GetObject(id)
	if obj == nil {
		return false, nil
	}
	return opts.Admit(obj)
}

// BuiltInput binds prepared documents and observed source-relative reads to their ordinary producer.
type BuiltInput struct {
	Root          string
	Owner, Source manifest.NamedResource
	Reads         map[string]bool
	// RegisterBuilt owns Support on every call, including rejection.
	Support *store.ReadSet
}

// RegisterBuilt replaces package resource evidence with authoritative producer-bound documents.
// Documents MUST be sanitized immutable build output.
func (l *Loader) RegisterBuilt(
	input BuiltInput, //nolint:gocritic // The SDK requires an independent input value for each registration.
	docs []store.Child,
) {
	retained := false
	defer func() {
		if !retained {
			l.Store.Retire(input.Support)
		}
	}()
	if l.frozen || input.Support == nil || !input.Support.Valid() {
		return
	}
	root := input.Root
	var sourceRoot string
	{
		var admitted bool
		sourceRoot, admitted = l.admitBuiltRoot(root)
		if !admitted {
			return
		}
	}
	rel, _ := filepath.Rel(sourceRoot, root)
	selected := map[string]bool{}
	inputs := l.selectBuiltInputs(sourceRoot, rel, &input, selected)
	slices.Sort(inputs.inputs)
	inputs.inputs = slices.Compact(inputs.inputs)
	slices.Sort(inputs.shared)
	inputs.shared = slices.Compact(inputs.shared)
	builder := l.documents.read(root).file
	builder = l.fileCoordinate(cmp.Or(builder, root))
	records := make([]manifest.DiscoveryRecord, len(docs))
	locations := make([]Occurrence, len(docs))
	authored := map[manifest.NamedResource]store.Child{}
	for i := range docs {
		record, original := l.builtRecord(&input, docs[i].Obj, &inputs, builder)
		records[i] = record
		locations[i], _ = l.catalogOccurrence(docs[i].Obj.Named())
		if original.Obj != nil {
			authored[docs[i].Obj.Named()] = original
		}
	}
	l.removeReplacedRecords(new(input), authored, selected)
	for i := range docs {
		if locations[i].Declaration != (OccurrenceID{}) {
			l.locations[docs[i].Obj.Named()] = locations[i]
		}
	}
	cached := l.documents.cache[root]
	previous := cached.builtInput
	cached.built = append([]store.Child{}, docs...)
	input.Reads = maps.Clone(input.Reads)
	cached.builtInput = &input
	l.documents.cache[root] = cached
	if l.candidates == nil {
		l.candidates = map[manifest.NamedResource]store.Child{}
	}
	l.publishBuiltRecords(&input, docs, records, authored)
	l.scopes.complete = false
	l.admit(sourceRoot)
	retained = input.Support.Valid()
	if previous != nil && previous.Support != input.Support {
		l.Store.Retire(previous.Support)
	}
}

func (l *Loader) builtRecord(
	input *BuiltInput,
	obj manifest.BaseManifest,
	inputs *builtInputs,
	builder string,
) (manifest.DiscoveryRecord, store.Child) {
	id := obj.Named()
	record := manifest.DiscoveryRecord{
		Declaration: obj, File: builder, Builder: builder, BuilderOwner: input.Owner, Inputs: inputs.inputs,
	}
	var authored store.Child
	evidence := admissionClosure{loader: l, objects: l.Store}
	raw := evidence.record(id)
	if raw.File != "" {
		original := l.IndexedObject(id, raw)
		if raw.Authored != (manifest.NamedResource{}) && original.Obj != nil {
			authored = original
			l.retainBuiltDeclaration(&record, &raw, id)
		}
		same := manifest.Equal(obj, original.Obj)
		if l.canCompareBuiltInputs(same, &raw) {
			evidence := admissionClosure{loader: l, objects: l.Store}
			delta, _, _ := evidence.delta(input.Source)
			same = delta != nil && !evidence.fileChanged(input.Source, raw.File) &&
				!slices.ContainsFunc(
					inputs.shared,
					func(file string) bool { return evidence.fileChanged(input.Source, file) },
				)
		}
		if !same && original.Doc != nil {
			same = l.stampedBuiltEqual(obj, &original, input)
		}
		if same {
			record.File, record.Inputs = raw.File, inputs.shared
		}
	}
	if raw.Authored == id {
		record.Authored = id
	}
	l.setBuiltProduction(&record, obj, id, input)
	return record, authored
}

// BuiltChildren requires current construction membership and the original preparation capture.
func (l *Loader) BuiltChildren(root string, owner manifest.NamedResource, reads *store.ReadSet) ([]store.Child, bool) {
	cached := l.documents.cache[root]
	input := cached.builtInput
	if l.frozen || reads == nil || input == nil || input.Root != root || input.Owner != owner ||
		input.Support == nil || !input.Support.Valid() || !reads.Matches(input.Support) || !input.Support.Valid() {
		return nil, false
	}
	builder := l.fileCoordinate(cmp.Or(cached.file, root))
	children := make([]store.Child, 0, len(cached.built))
	for i := range cached.built {
		child := cached.built[i]
		id := child.Obj.Named()
		if id == owner {
			continue
		}
		record, ok := l.Index.Lookup(id)
		if !ok || record.Declaration != child.Obj || record.Builder != builder || record.BuilderOwner != owner ||
			!l.roots[id].independent &&
				!slices.ContainsFunc(record.Productions, func(production manifest.Production) bool {
					return production.Owner == owner && production.Source == input.Source &&
						production.File == record.File && production.Path == "" &&
						production.Declaration == (manifest.NamedResource{})
				}) {
			return nil, false
		}
		if current, policy := reads.Lookup(id); current != nil && policy.Origin == manifest.SuppliedDeclaration &&
			reads.GetDeclaration(id) == current && current != record.Declaration {
			continue
		}
		if !reads.SourceAdmission(id, input.Source) {
			return nil, false
		}
		child.Obj, child.Productions, child.SourceSupport = record.Declaration, record.Productions, input.Source
		children = append(children, child)
	}
	if !input.Support.Valid() || !reads.Valid() {
		return nil, false
	}
	return children, true
}

// PromotionDocuments returns the immutable indexed batch used to promote id.
func (l *Loader) PromotionDocuments(id manifest.NamedResource) []store.Child {
	r, ok := l.Index.Lookup(id)
	if !ok {
		return nil
	}
	if r.Builder == "" {
		if r.Declaration != nil {
			if child := l.IndexedObject(id, r); child.Obj != nil {
				return []store.Child{child}
			}
		}
		return nil
	}
	return l.documents.cache[l.registeredRoot(&r)].built
}

func (l *Loader) registeredRoot(record *manifest.DiscoveryRecord) string {
	//nolint:gocritic // Map iteration supplies value snapshots without an additional lookup.
	for root, doc := range l.documents.cache {
		if doc.builtInput != nil && doc.builtInput.Support != nil && doc.builtInput.Support.Valid() &&
			doc.builtInput.Owner == record.BuilderOwner &&
			(l.fileCoordinate(root) == record.Builder || l.fileCoordinate(doc.file) == record.Builder) &&
			slices.ContainsFunc(doc.built, func(child store.Child) bool { return child.Obj == record.Declaration }) {
			return root
		}
	}
	return ""
}

func (l *Loader) preparedObject(id manifest.NamedResource) store.Child {
	r, _ := l.Index.Lookup(id)
	if r.Builder != "" {
		if child := l.candidates[id]; child.Obj == r.Declaration {
			return child
		}
		children := l.PromotionDocuments(id)
		for i := range children {
			child := &children[i]
			if child.Obj == r.Declaration {
				return *child
			}
		}
	}
	return store.Child{}
}

func (o *PromoteOptions) admitDocuments(
	ctx context.Context,
	st *store.Store,
	id manifest.NamedResource,
	objs []store.Child,
) error {
	source := o.Record.Source
	authored := o.Record.Authored
	record := o.Record
	addWarning := st.AddWarning
	if o.Reads != nil {
		addWarning = o.Reads.AddWarning
	}
	for i := range objs {
		child := objs[i]
		obj := child.Obj
		switch {
		case o.Documents == nil && obj.Named() == id && authored != id:
			continue
		case o.Documents == nil && obj.Named() == authored && authored.Namespace != id.Namespace:
			obj = obj.WithNamespace(id.Namespace)
		}
		admitted, err := o.Admit(obj)
		if err != nil {
			return err
		}
		if !admitted {
			continue
		}
		if existing, policy := st.Lookup(obj.Named()); existing != nil && policy.Root {
			continue
		}
		if err := ctx.Err(); err != nil {
			return err //nolint:wrapcheck // Cancellation preserves the caller context error identity.
		}
		for warningIndex := range child.Warnings {
			warning := child.Warnings[warningIndex]
			warning.Resource = obj.Named()
			addWarning(warning)
		}
		if o.Reads == nil {
			st.AddObject(obj, child.Doc)
			continue
		}
		child.Obj = obj
		o.Reads.Admit(child, source, record.Owner)
	}
	return nil
}

type builtInputs struct{ inputs, shared []string }

func (l *Loader) selectBuiltInputs(
	sourceRoot, rel string,
	inputValue *BuiltInput,
	selected map[string]bool,
) builtInputs {
	input := *inputValue
	var inputs, shared []string
	{
		walkVisits := l.scopeWalk(
			new(graphVisit{root: sourceRoot, dir: filepath.ToSlash(rel), source: input.Source, owner: input.Owner}),
		)
		for walkIndex := range walkVisits {
			visit := walkVisits[walkIndex]

			for _, file := range []string{visit.coordinate, visit.localCoordinate()} {
				if file == "" {
					continue
				}
				inputs = append(inputs, file)
				if visit.role != kustomizeinput.Resource || visit.document != nil {
					shared = append(shared, file)
				}
			}
			if visit.role != kustomizeinput.Resource {
				continue
			}

			selected[visit.file] = true
			cached := l.documents.cache[visit.file]
			cached.selected = true
			l.documents.cache[visit.file] = cached
			if visit.document != nil {
				dir := filepath.Dir(visit.file)
				cached := l.documents.cache[dir]
				cached.selected = true
				l.documents.cache[dir] = cached
			}
		}
	}
	return builtInputs{inputs: inputs, shared: shared}
}

func (l *Loader) removeReplacedRecords(
	inputValue *BuiltInput,
	authored map[manifest.NamedResource]store.Child,
	selected map[string]bool,
) {
	input := *inputValue
	l.removeBuiltBatch(&input, authored)
	//nolint:gocritic // The iterator supplies filtered value snapshots through its callback.
	for id, record := range l.Index.Files() {
		if id == input.Owner {
			continue
		}
		if record.Builder != "" && record.BuilderOwner != input.Owner {
			continue
		}
		if _, retained := authored[id]; retained {
			continue
		}
		if record.Builder == "" && slices.ContainsFunc(record.Productions, func(production manifest.Production) bool {
			return production.Owner != (manifest.NamedResource{}) && production.Owner != input.Owner
		}) && !slices.ContainsFunc(record.Productions, func(production manifest.Production) bool {
			return production.Owner == input.Owner
		}) {
			continue
		}
		if record.Path == "" || !selected[record.Path] {
			continue
		}
		if existing := l.Store.GetObject(
			id,
		); existing != nil && l.Store.GetArtifact(id, store.ArtifactContent) == nil &&
			(existing == l.admitted[id] || existing == l.IndexedObject(id, record).Obj) {
			l.Store.DeleteObject(id)
		}
		delete(l.Index.Records, id)
		delete(l.locations, id)
		delete(l.candidates, id)
		delete(l.admitted, id)
	}
}

func (l *Loader) removeBuiltBatch(input *BuiltInput, authored map[manifest.NamedResource]store.Child) {
	batch := l.documents.cache[input.Root].built
	for i := range batch {
		child := &batch[i]
		id := child.Obj.Named()
		record, ok := l.Index.Lookup(id)
		if !ok || id == input.Owner || record.Declaration != child.Obj || record.BuilderOwner != input.Owner {
			continue
		}
		if _, retained := authored[id]; retained {
			continue
		}
		if existing := l.Store.GetObject(id); existing != nil &&
			l.Store.GetArtifact(id, store.ArtifactContent) == nil &&
			(existing == l.candidates[id].Obj || existing == l.admitted[id]) {
			l.Store.DeleteObject(id)
		}
		delete(l.Index.Records, id)
		delete(l.locations, id)
		delete(l.candidates, id)
		delete(l.admitted, id)
	}
}

func (l *Loader) retainBuiltDeclaration(
	record *manifest.DiscoveryRecord,
	rawValue *manifest.DiscoveryRecord,
	id manifest.NamedResource,
) {
	raw := *rawValue
	if l.documents.compact {
		record.Authored, record.Path = raw.Authored, raw.Path
		if raw.Authored != id {
			record.File = raw.File
		}
		record.Productions = slices.Clone(raw.Productions)
	}
}

func (l *Loader) stampedBuiltEqual(obj manifest.BaseManifest, originalValue *store.Child, inputValue *BuiltInput) bool {
	same := false
	original := *originalValue
	input := *inputValue
	if producer, ok := l.Store.GetObject(input.Owner).(interface {
		EmissionPolicy() manifest.EmissionPolicy
	}); ok {
		doc := manifest.DeepCopyMap(original.Doc)
		producer.EmissionPolicy().Stamp([]manifest.DocOccurrence{{Document: doc}})
		parsed, err := manifest.ParseDoc(
			doc,
			manifest.ParseDocOptions{},
		)
		same = err == nil && manifest.Equal(obj, parsed.Object)
	}
	return same
}

func (l *Loader) setBuiltProduction(
	record *manifest.DiscoveryRecord,
	obj manifest.BaseManifest,
	id manifest.NamedResource,
	inputValue *BuiltInput,
) {
	builder := record.Builder
	input := *inputValue
	record.Refs = discoveryReferences(nil, obj)
	if l.roots[id].independent {
		record.Parent = manifest.NamedResource{}
		record.Productions = slices.DeleteFunc(record.Productions, func(production manifest.Production) bool {
			return production.Owner != (manifest.NamedResource{}) && production.Owner != id
		})
		return
	}
	if id != input.Owner {
		record.Parent = input.Owner
		production := manifest.Production{Owner: input.Owner, Source: input.Source, File: record.File}
		if record.File != builder {
			production.Authored = record.Authored
			if production.Authored == (manifest.NamedResource{}) {
				production.Authored = id
			}
		}
		if !slices.Contains(record.Productions, production) {
			record.Productions = append(record.Productions, production)
		}
	}
}

func (l *Loader) publishBuiltRecords(
	input *BuiltInput,
	docs []store.Child,
	records []manifest.DiscoveryRecord,
	authored map[manifest.NamedResource]store.Child,
) {
	for i := range docs {
		child := docs[i]
		obj := child.Obj
		id := obj.Named()
		if id == input.Owner {
			continue
		}
		independent := l.roots[id].independent
		if existing := l.Store.GetObject(
			id,
		); !independent && existing != nil && l.Store.GetArtifact(id, store.ArtifactContent) == nil &&
			(existing == l.candidates[id].Obj || existing == l.admitted[id]) {
			l.Store.DeleteObject(id)
		}
		if !independent {
			delete(l.admitted, id)
		}
		l.Index.Records[id] = records[i]
		if original, retained := authored[id]; retained {
			child = original
		}
		l.candidates[id] = child
	}
}

func (l *Loader) admitBuiltRoot(root string) (string, bool) {
	if l.documents.compact {
		l.construction++
	}
	l.ensureScopes()
	sourceRoot := cmp.Or(new(l.documents).mountedRoot(root), l.SourceRoot, root)
	l.Admit(sourceRoot)
	packageFile := l.documents.read(root).file
	if packageFile != "" && l.documents.cache[packageFile].eligible == fileInactive {
		return "", false
	}
	return sourceRoot, true
}

func (l *Loader) canCompareBuiltInputs(same bool, raw *manifest.DiscoveryRecord) bool {
	return !same && l.documents.compact && l.Options.Changes != nil && raw.Path != ""
}
