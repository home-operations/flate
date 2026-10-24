package loader

import (
	"cmp"
	"context"
	"log/slog"
	"path/filepath"

	"github.com/home-operations/flate/internal/kustomizeinput"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

// PromoteOptions preserves the run's secret policy and per-document admission boundary.
type PromoteOptions struct {
	Record      manifest.Production
	Reads       *store.ReadSet
	WipeSecrets bool                             // SOPS ciphertext is replaced with offline placeholders.
	Documents   []manifest.BaseManifest          // Parsed, sanitized documents replace raw file acquisition.
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
	for _, obj := range objs {
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
			opts.Reads.Admit(obj, source, record.Owner)
		} else {
			st.AddObject(obj)
		}
	}
	obj := st.GetObject(id)
	return obj != nil && opts.Admit(obj)
}

// RegisterBuilt replaces selected bootstrap resource evidence with authoritative built documents.
// The root MUST be loaded and docs MUST be sanitized immutable build output.
func (l *Loader) RegisterBuilt(root string, docs []manifest.BaseManifest) {
	l.ensureScopes()
	sourceRoot := cmp.Or(l.SourceRoot, l.documents.mountedRoot(root), root)
	rel, _ := filepath.Rel(sourceRoot, root)
	for _, visit := range l.scopeWalk(graphVisit{root: sourceRoot, dir: filepath.ToSlash(rel), source: manifest.BootstrapSourceID}) {
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
	for id, record := range l.Index.Files() {
		if record.Path == "" || !l.documents.cache[record.Path].selected {
			continue
		}
		if existing := l.Store.GetObject(id); existing != nil && (existing == l.admitted[id] || existing == l.indexedObject(id, record)) {
			l.Store.DeleteObject(id)
		}
		delete(l.Index.Records, id)
		delete(l.candidates, id)
		delete(l.admitted, id)
	}
	cached := l.documents.cache[root]
	cached.built = docs
	l.documents.cache[root] = cached
	_, builder := l.documents.read(root)
	if l.candidates == nil {
		l.candidates = map[manifest.NamedResource]manifest.BaseManifest{}
	}
	for _, obj := range docs {
		id := obj.Named()
		record := manifest.DiscoveryRecord{File: l.fileCoordinate(builder)}
		for ref := range manifest.References(obj) {
			if discoveryReference(ref) {
				record.Refs = append(record.Refs, ref.NamedResource)
			}
		}
		l.Index.Records[id] = record
		l.candidates[id] = obj
	}
	l.scopes.complete = false
}
