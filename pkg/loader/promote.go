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
}

// RegisterBuilt replaces package resource evidence with authoritative producer-bound documents.
// Documents MUST be sanitized immutable build output.
func (l *Loader) RegisterBuilt(
	input BuiltInput, //nolint:gocritic // The SDK requires an independent input value for each registration.
	docs []store.Child,
) {
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
	var inputs, shared []string
	selected := map[string]bool{}
	built2 := l.selectBuiltInputs(sourceRoot, rel, new(input), selected)
	inputs, shared = built2.inputs, built2.shared

	slices.Sort(inputs)
	inputs = slices.Compact(inputs)
	slices.Sort(shared)
	shared = slices.Compact(shared)
	builder := l.documents.read(root).file
	builder = l.fileCoordinate(cmp.Or(builder, root))
	records := make([]manifest.DiscoveryRecord, len(docs))
	authored := map[manifest.NamedResource]store.Child{}
	for i, child := range docs {
		obj := child.Obj
		id := obj.Named()
		record := manifest.DiscoveryRecord{
			Declaration:  obj,
			File:         builder,
			Builder:      builder,
			BuilderOwner: input.Owner,
			Inputs:       inputs,
		}
		if raw, ok := l.Index.Lookup(id); ok {
			original := l.IndexedObject(id, raw)
			if raw.Authored != (manifest.NamedResource{}) && original.Obj != nil {
				authored[id] = original
				l.retainBuiltDeclaration(&record, new(raw), id)
			}
			same := manifest.Equal(obj, original.Obj)
			if l.canCompareBuiltInputs(same, &raw) {
				evidence := admissionClosure{loader: l, objects: l.Store}
				delta, _, _ := evidence.delta(input.Source)
				same = delta != nil && !evidence.fileChanged(input.Source, raw.File) &&
					!slices.ContainsFunc(
						shared,
						func(file string) bool { return evidence.fileChanged(input.Source, file) },
					)
			}
			if !same && original.Doc != nil {
				same = l.stampedBuiltEqual(obj, new(original), new(input))
			}
			if same {
				record.File, record.Inputs = raw.File, shared
			}
		}
		setBuiltProduction(&record, obj, id, new(input))
		records[i] = record
	}
	l.removeReplacedRecords(new(input), authored, selected)
	cached := l.documents.cache[root]
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
	//nolint:gocritic // Map iteration supplies value snapshots without an additional lookup.
	for root, doc := range l.documents.cache {
		if doc.built != nil && (l.fileCoordinate(root) == r.Builder || l.fileCoordinate(doc.file) == r.Builder) {
			return doc.built
		}
	}
	return nil
}

func (l *Loader) preparedObject(id manifest.NamedResource) store.Child {
	r, _ := l.Index.Lookup(id)
	if r.Builder != "" {
		if child := l.candidates[id]; child.Obj == r.Declaration {
			return child
		}
		for _, child := range l.PromotionDocuments(id) {
			if child.Obj == r.Declaration {
				return child
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
	for _, child := range objs {
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
		o.Reads.Admit(obj, source, record.Owner, child.Doc)
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
	//nolint:gocritic // The iterator supplies filtered value snapshots through its callback.
	for id, record := range l.Index.Files() {
		if id == input.Owner {
			continue
		}
		if _, retained := authored[id]; retained {
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

func setBuiltProduction(
	record *manifest.DiscoveryRecord,
	obj manifest.BaseManifest,
	id manifest.NamedResource,
	inputValue *BuiltInput,
) {
	builder := record.Builder
	input := *inputValue
	record.Refs = discoveryReferences(nil, obj)
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
	for i, child := range docs {
		obj := child.Obj
		id := obj.Named()
		if id == input.Owner {
			continue
		}
		if existing := l.Store.GetObject(
			id,
		); existing != nil && l.Store.GetArtifact(id, store.ArtifactContent) == nil &&
			(existing == l.candidates[id].Obj || existing == l.admitted[id]) {
			l.Store.DeleteObject(id)
		}
		delete(l.admitted, id)
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
