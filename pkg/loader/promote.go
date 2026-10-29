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

// PromoteOptions preserves the run's secret policy and per-document admission boundary.
type PromoteOptions struct {
	Record      manifest.Production
	Reads       *store.ReadSet
	WipeSecrets bool                             // SOPS ciphertext is replaced with offline placeholders.
	Documents   []store.Child                    // Parsed, sanitized documents replace raw file acquisition.
	Admit       func(manifest.BaseManifest) bool // Admission applies to the requested object and every sibling.
}

// Promote materializes admitted documents and reports whether id is available.
// Authored proves which raw identity may be projected; absent evidence requires an exact id.
// Existing objects take precedence over raw files.
func Promote(ctx context.Context, st *store.Store, id manifest.NamedResource, opts PromoteOptions) bool {
	record := opts.Record
	path := record.Path
	if st == nil || path == "" && opts.Documents == nil || ctx.Err() != nil {
		return false
	}
	if obj := st.GetObject(id); obj != nil {
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
		objs, err = reader.parseFile(path, manifest.ParseDocOptions{WipeSecrets: opts.WipeSecrets, DefaultNamespace: ""})
		if err != nil {
			slog.Debug("loader: promote re-parse failed", "id", id.String(), "path", path, "err", err)
			return false
		}
	}
	authored := record.Authored
	if authored == (manifest.NamedResource{}) {
		authored = id
	}
	for _, child := range objs {
		obj := child.Obj
		if opts.Documents == nil && obj.Named() == authored {
			if authored.Namespace != id.Namespace {
				obj = obj.WithNamespace(id.Namespace)
			}
		} else if opts.Documents == nil && obj.Named() == id {
			continue
		}
		if !opts.Admit(obj) || st.GetObject(obj.Named()) != nil {
			continue
		}
		if ctx.Err() != nil {
			return false
		}
		if opts.Reads != nil {
			opts.Reads.Admit(obj, source, record.Owner, child.Doc)
		} else {
			st.AddObject(obj, child.Doc)
		}
	}
	obj := st.GetObject(id)
	return obj != nil && opts.Admit(obj)
}

// BuiltInput binds prepared documents and observed source-relative reads to their ordinary producer.
type BuiltInput struct {
	Root          string
	Owner, Source manifest.NamedResource
	Reads         map[string]bool
}

// RegisterBuilt replaces package resource evidence with authoritative producer-bound documents.
// Documents MUST be sanitized immutable build output.
func (l *Loader) RegisterBuilt(input BuiltInput, docs []store.Child) {
	root := input.Root
	parent := l.Store.GetObject(input.Owner)
	standalone := false
	if parent != nil {
		_, _, applies := parent.SourcePath()
		standalone = !applies
	}
	if standalone {
		input.Owner = manifest.NamedResource{}
	}
	l.ensureScopes()
	sourceRoot := cmp.Or(l.documents.mountedRoot(root), l.SourceRoot, root)
	l.Admit(sourceRoot)
	_, packageFile := l.documents.read(root)
	if packageFile != "" && l.documents.cache[packageFile].eligible == fileInactive {
		return
	}
	rel, _ := filepath.Rel(sourceRoot, root)
	var inputs, shared []string
	for _, visit := range l.scopeWalk(graphVisit{root: sourceRoot, dir: filepath.ToSlash(rel), source: input.Source, owner: input.Owner}) {
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
	slices.Sort(inputs)
	inputs = slices.Compact(inputs)
	slices.Sort(shared)
	shared = slices.Compact(shared)
	_, builder := l.documents.read(root)
	builder = l.fileCoordinate(cmp.Or(builder, root))
	records := make([]manifest.DiscoveryRecord, len(docs))
	for i, child := range docs {
		obj := child.Obj
		id := obj.Named()
		if standalone && manifest.KindOf(obj).Data {
			continue
		}
		record := manifest.DiscoveryRecord{File: builder, Builder: builder, BuilderOwner: input.Owner, Inputs: inputs}
		if raw, ok := l.Index.Lookup(id); ok {
			original := l.IndexedObject(id, raw)
			same := manifest.Equal(obj, original.Obj)
			if !same && original.Doc != nil {
				if producer, ok := l.Store.GetObject(input.Owner).(interface {
					EmissionPolicy() manifest.EmissionPolicy
				}); ok {
					doc := manifest.DeepCopyMap(original.Doc)
					producer.EmissionPolicy().Stamp([]manifest.DocOccurrence{{Document: doc}})
					projected, _, _, err := manifest.ParseDoc(doc, manifest.ParseDocOptions{WipeSecrets: l.Options.WipeSecrets})
					same = err == nil && manifest.Equal(obj, projected)
					original.Obj = projected
				}
			}
			if same {
				record.File, record.Inputs = raw.File, shared
				record.Declaration = original.Obj
			}
		}
		record.Refs = discoveryReferences(nil, obj)
		if id != input.Owner {
			record.Parent = input.Owner
			record.Productions = []manifest.Production{{Owner: input.Owner, Source: input.Source, File: record.File}}
			if record.File != builder {
				record.Productions[0].Authored = id
			}
		}
		records[i] = record
	}
	for id, record := range l.Index.Files() {
		if id == input.Owner || standalone && id.KindInfo().Data {
			continue
		}
		if record.Path == "" || !l.documents.cache[record.Path].selected {
			continue
		}
		if existing := l.Store.GetObject(id); existing != nil && l.Store.GetArtifact(id, store.ArtifactContent) == nil && (existing == l.admitted[id] || existing == l.IndexedObject(id, record).Obj) {
			l.Store.DeleteObject(id)
		}
		delete(l.Index.Records, id)
		delete(l.candidates, id)
		delete(l.admitted, id)
	}
	cached := l.documents.cache[root]
	cached.built = append([]store.Child{}, docs...)
	input.Reads = maps.Clone(input.Reads)
	cached.builtInput = &input
	l.documents.cache[root] = cached
	if l.candidates == nil {
		l.candidates = map[manifest.NamedResource]store.Child{}
	}
	for i, child := range docs {
		obj := child.Obj
		id := obj.Named()
		if id == input.Owner || standalone && manifest.KindOf(obj).Data {
			continue
		}
		if existing := l.Store.GetObject(id); existing != nil && l.Store.GetArtifact(id, store.ArtifactContent) == nil && (existing == l.candidates[id].Obj || existing == l.admitted[id]) {
			l.Store.DeleteObject(id)
		}
		delete(l.admitted, id)
		l.Index.Records[id] = records[i]
		if records[i].Declaration != nil {
			child.Obj = records[i].Declaration
		}
		l.candidates[id] = child
	}
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
	for root, doc := range l.documents.cache {
		if doc.built != nil && (l.fileCoordinate(root) == r.Builder || l.fileCoordinate(doc.file) == r.Builder) {
			return doc.built
		}
	}
	return nil
}
