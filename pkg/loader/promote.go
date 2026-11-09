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
	"github.com/home-operations/flate/pkg/selection"
	"github.com/home-operations/flate/pkg/store"
)

// PromoteOptions binds acquisition to an exact source and captured generation.
type PromoteOptions struct {
	Record    manifest.Production
	Reads     *store.ReadSet
	Documents []store.Child
}

// Promote acquires exact children; callers must admit the generation before publication.
func Promote(
	ctx context.Context,
	st *store.Store,
	id manifest.NamedResource,
	opts PromoteOptions, //nolint:gocritic // The public acquisition owns a value snapshot of its captured generation.
) ([]store.Child, error) {
	if err := ctx.Err(); err != nil {
		return nil, err //nolint:wrapcheck // Cancellation preserves the caller context error identity.
	}
	if st == nil || opts.Record.Path == "" && opts.Documents == nil {
		return nil, nil
	}
	if object, policy := st.Lookup(id); object != nil && policy.Root {
		return nil, nil
	}
	record := opts.Record
	if record.Source == (manifest.NamedResource{}) {
		record.Source = manifest.BootstrapSourceID
	}
	authored := record.Authored
	if authored == (manifest.NamedResource{}) {
		authored = id
	}
	children, err := promotionDocuments(ctx, st, &opts)
	if err != nil {
		return nil, err
	}
	var acquired []store.Child
	for i := range children {
		if err := ctx.Err(); err != nil {
			return nil, err //nolint:wrapcheck // Cancellation preserves the caller context error identity.
		}
		child := children[i]
		original := child.Obj.Named()
		if !projectPromotionChild(&child, id, authored, opts.Documents == nil) {
			continue
		}
		if existing, policy := st.Lookup(child.Obj.Named()); existing != nil && policy.Root {
			continue
		}
		child.SourceSupport = record.Source
		if len(child.Productions) == 0 {
			production := record
			production.Authored = original
			child.Productions = []manifest.Production{production}
		}
		acquired = append(acquired, child)
	}
	return acquired, nil
}

func projectPromotionChild(child *store.Child, id, authored manifest.NamedResource, parsed bool) bool {
	if !parsed {
		return true
	}
	original := child.Obj.Named()
	if original == id && authored != id {
		return false
	}
	if original == authored && authored.Namespace != id.Namespace {
		child.Obj = child.Obj.WithNamespace(id.Namespace)
	}
	return true
}

func promotionDocuments(ctx context.Context, st *store.Store, opts *PromoteOptions) ([]store.Child, error) {
	if opts.Documents != nil {
		return opts.Documents, nil
	}
	reader := documentReader{}
	var sources sourceReader = st
	if opts.Reads != nil {
		sources = opts.Reads
	}
	source := opts.Record.Source
	if source == (manifest.NamedResource{}) {
		source = manifest.BootstrapSourceID
	}
	mounted := map[string]*store.SourceArtifact{}
	mountSource(mounted, sources.GetArtifact(source, store.ArtifactContent))
	reader.setSources(mounted)
	children, err := reader.parseFile(ctx, opts.Record.Path, manifest.ParseDocOptions{DefaultNamespace: ""})
	if err == nil {
		return children, nil
	}
	if ctx.Err() != nil {
		return nil, ctx.Err() //nolint:wrapcheck // Cancellation preserves the caller context error identity.
	}
	slog.Debug("loader: promote re-parse failed", "path", opts.Record.Path, "err", err)
	return nil, nil
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
	sourceRoot, admitted := l.admitBuiltRoot(root)
	if !admitted {
		return
	}
	rel, _ := filepath.Rel(sourceRoot, root)
	selected := map[string]bool{}
	inputs := l.selectBuiltInputs(sourceRoot, rel, &input, selected)
	slices.Sort(inputs.inputs)
	slices.Sort(inputs.shared)
	inputs.inputs, inputs.shared = slices.Compact(inputs.inputs), slices.Compact(inputs.shared)
	builder := l.fileCoordinate(cmp.Or(l.documents.read(root).file, root))
	records := make([]manifest.DiscoveryRecord, len(docs))
	locations := make([]authoredOccurrence, len(docs))
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
		if locations[i].Declaration != (occurrenceID{}) {
			l.locations[docs[i].Obj.Named()] = locations[i]
		}
	}
	cached := l.documents.cache[root]
	previous := cached.builtInput
	cached.built = append([]store.Child{}, docs...)
	input.Reads = maps.Clone(input.Reads)
	// Directory membership and node-kind probes MUST NOT claim descendant content.
	maps.DeleteFunc(input.Reads, func(_ string, exact bool) bool { return !exact })
	cached.builtInput = &input
	l.documents.cache[root] = cached
	for i, facts := range l.scopes.rootFacts {
		if filepath.Join(l.SourceRoot, filepath.FromSlash(facts.Path)) == root &&
			(facts.Built != (cached.built != nil) || facts.Selected != cached.selected || facts.Excluded != cached.buildExcluded) {
			l.scopes.rootFacts = slices.Clone(l.scopes.rootFacts)
			l.scopes.rootFacts[i].Built, l.scopes.rootFacts[i].Selected, l.scopes.rootFacts[i].Excluded =
				cached.built != nil, cached.selected, cached.buildExcluded
			break
		}
	}
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
	raw, _ := l.Index.Lookup(id)
	if raw.Path == "" && id.Namespace != "" {
		if occurrence, known := l.catalogOccurrence(id); known && occurrence.Authored.Namespace == "" {
			bare := id
			bare.Namespace = ""
			raw, _ = l.Index.Lookup(bare)
		}
	}
	if raw.File != "" {
		original := l.IndexedObject(id, raw)
		if raw.Authored != (manifest.NamedResource{}) && original.Obj != nil {
			authored = original
			l.retainBuiltDeclaration(&record, &raw, id)
		}
		same := manifest.Equal(obj, original.Obj)
		if l.canCompareBuiltInputs(same, &raw) {
			evidence := l.SourceFacts(input.Source, nil)
			same = evidence.Delta != nil && !l.comparedFileChanged(&evidence, raw.File) &&
				!slices.ContainsFunc(
					inputs.shared,
					func(file string) bool { return l.comparedFileChanged(&evidence, file) },
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
	return l.currentBuilt(root, owner, reads, l.documents.cache[root].built)
}

func (l *Loader) currentBuilt(
	root string,
	owner manifest.NamedResource,
	reads *store.ReadSet,
	built []store.Child,
) ([]store.Child, bool) {
	cached := l.documents.cache[root]
	input := cached.builtInput
	if l.frozen || reads == nil || input == nil || input.Root != root || input.Owner != owner ||
		input.Support == nil || !input.Support.Valid() || !reads.Matches(input.Support) || !input.Support.Valid() {
		return nil, false
	}
	builder := l.fileCoordinate(cmp.Or(cached.file, root))
	children := make([]store.Child, 0, len(built))
	for i := range built {
		child := built[i]
		id := child.Obj.Named()
		if id == owner {
			continue
		}
		record, ok := l.Index.Lookup(id)
		if !ok || record.Declaration != child.Obj || record.Builder != builder || record.BuilderOwner != owner ||
			!selection.Root(l.roots.get(id).view()).Independent &&
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
	return l.documents.cache[l.registeredRoot(id, &r)].built
}

func (l *Loader) registeredRoot(id manifest.NamedResource, record *manifest.DiscoveryRecord) string {
	built := l.builtRoots[id]
	input := built.input
	if input == nil || input.Support == nil || !input.Support.Valid() ||
		input.Owner != record.BuilderOwner {
		return ""
	}
	doc := l.documents.cache[input.Root]
	if doc.builtInput != input || doc.built[built.index].Obj != record.Declaration ||
		l.fileCoordinate(input.Root) != record.Builder && l.fileCoordinate(doc.file) != record.Builder {
		return ""
	}
	return input.Root
}

// PreparedObject captures only the requested indexed document and its build support.
func (l *Loader) PreparedObject(id manifest.NamedResource, reads *store.ReadSet) (store.Child, bool) {
	record, _ := l.Index.Lookup(id)
	if record.Builder == "" {
		return store.Child{}, false
	}
	child := l.candidates[id]
	root := l.registeredRoot(id, &record)
	if child.Obj != record.Declaration {
		if root == "" {
			return store.Child{}, false
		}
		child = l.documents.cache[root].built[l.builtRoots[id].index]
	}
	if root != "" && reads != nil && child.Obj != nil {
		children, current := l.currentBuilt(root, record.BuilderOwner, reads, []store.Child{child})
		if len(children) != 0 {
			return children[0], current
		}
	}
	return child, false
}

type builtRoot struct {
	input *BuiltInput
	index int
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
	cached := l.documents.cache[input.Root]
	batch := cached.built
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
	for i := range batch {
		id := batch[i].Obj.Named()
		if l.builtRoots[id].input == cached.builtInput {
			delete(l.builtRoots, id)
		}
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
	if selection.Root(l.roots.get(id).view()).Independent {
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
	if l.builtRoots == nil {
		l.builtRoots = make(map[manifest.NamedResource]builtRoot)
	}
	for i := range docs {
		child := docs[i]
		obj := child.Obj
		id := obj.Named()
		l.builtRoots[id] = builtRoot{input: input, index: i}
		if id == input.Owner {
			continue
		}
		independent := selection.Root(l.roots.get(id).view()).Independent
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
	if packageFile != "" && l.documents.cache[packageFile].eligible == selection.RootInactive {
		return "", false
	}
	return sourceRoot, true
}

func (l *Loader) canCompareBuiltInputs(same bool, raw *manifest.DiscoveryRecord) bool {
	return !same && l.documents.compact && l.Options.Changes != nil && raw.Path != ""
}
