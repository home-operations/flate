package loader

import (
	"cmp"
	"context"
	"log/slog"
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
		mountSource(mounted, sources.GetArtifact(source))
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

// RegisterBuilt replaces package resource evidence with authoritative built documents.
// The root MUST be loaded and docs MUST be sanitized immutable build output.
func (l *Loader) RegisterBuilt(root string, docs []store.Child) {
	l.ensureScopes()
	sourceRoot := cmp.Or(l.SourceRoot, l.documents.mountedRoot(root), root)
	l.Admit(sourceRoot)
	_, packageFile := l.documents.read(root)
	if l.documents.cache[packageFile].eligible == fileInactive {
		return
	}
	rel, _ := filepath.Rel(sourceRoot, root)
	var inputs, shared []string
	for _, visit := range l.scopeWalk(graphVisit{root: sourceRoot, dir: filepath.ToSlash(rel), source: manifest.BootstrapSourceID}) {
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
		if len(l.Index.Errors) != 0 {
			delete(l.Index.Errors, l.fileCoordinate(visit.file))
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
	builder = l.fileCoordinate(builder)
	records := make([]manifest.DiscoveryRecord, len(docs))
	for i, child := range docs {
		obj := child.Obj
		id := obj.Named()
		record := manifest.DiscoveryRecord{File: builder, Builder: builder, Inputs: inputs}
		if raw, ok := l.Index.Lookup(id); ok && manifest.KindOf(obj).DiscoverySeed() && manifest.Equal(obj, l.indexedObject(id, raw).Obj) {
			record.File, record.Inputs = raw.File, shared
		}
		for ref := range manifest.References(obj, manifest.ReferenceRelations) {
			if discoveryReference(ref) {
				record.Refs = append(record.Refs, ref.NamedResource)
			}
		}
		records[i] = record
	}
	for id, record := range l.Index.Files() {
		if record.Path == "" || !l.documents.cache[record.Path].selected {
			continue
		}
		if existing := l.Store.GetObject(id); existing != nil && (existing == l.admitted[id] || existing == l.indexedObject(id, record).Obj) {
			l.Store.DeleteObject(id)
		}
		delete(l.Index.Records, id)
		delete(l.candidates, id)
		delete(l.admitted, id)
	}
	cached := l.documents.cache[root]
	cached.built = append([]store.Child{}, docs...)
	l.documents.cache[root] = cached
	if l.candidates == nil {
		l.candidates = map[manifest.NamedResource]store.Child{}
	}
	for i, child := range docs {
		obj := child.Obj
		id := obj.Named()
		if existing := l.Store.GetObject(id); existing != nil && (existing == l.candidates[id].Obj || existing == l.admitted[id]) {
			l.Store.DeleteObject(id)
		}
		delete(l.admitted, id)
		l.Index.Records[id] = records[i]
		l.candidates[id] = child
	}
	l.scopes.complete = false
	l.admit(sourceRoot)
}

// BuiltDocuments returns the authoritative package batch for an indexed built object.
func (l *Loader) BuiltDocuments(id manifest.NamedResource) []store.Child {
	r, ok := l.Index.Lookup(id)
	if !ok || r.Builder == "" {
		return nil
	}
	return l.documents.cache[filepath.Dir(filepath.Join(l.SourceRoot, filepath.FromSlash(r.Builder)))].built
}
