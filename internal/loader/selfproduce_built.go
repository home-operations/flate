package loader

import (
	"cmp"
	"path/filepath"
	"slices"

	"github.com/home-operations/flate/internal/store"
	"github.com/home-operations/flate/pkg/manifest"
)

func (l *Loader) prepareBuiltProductions(
	b *selfProduceBuilder,
	repoRoot string,
	prepared map[string][]store.Child,
	preparedOwners map[manifest.NamedResource]manifest.NamedResource,
) {
	var builtDirs []string
	//nolint:gocritic // Map iteration supplies value snapshots without an additional lookup.
	for dir, doc := range l.documents.cache {
		if doc.builtInput != nil && doc.builtInput.Support != nil && doc.builtInput.Support.Valid() {
			builtDirs = append(builtDirs, dir)
		}
	}
	slices.Sort(builtDirs)
	for _, dir := range builtDirs {
		doc := l.documents.cache[dir]
		rel, _ := filepath.Rel(repoRoot, dir)
		root := graphVisit{
			root:   repoRoot,
			dir:    filepath.ToSlash(rel),
			owner:  doc.builtInput.Owner,
			source: doc.builtInput.Source,
		}
		l.recordBuiltOwner(&root)
		var candidates []manifest.NamedResource
		for i := range doc.built {
			child := &doc.built[i]
			obj := child.Obj
			for ref := range manifest.References(obj, manifest.ReferenceRelations) {
				candidates = append(candidates, ref.NamedResource)
			}
		}
		slices.SortFunc(candidates, manifest.NamedResource.Compare)
		candidates = slices.Compact(candidates)
		builder := l.documents.read(dir).file
		root.coordinate = l.fileCoordinate(cmp.Or(builder, dir))
		sourceRoot := new(l.documents).mountedRoot(dir)
		for name, exact := range doc.builtInput.Reads {
			file := filepath.Join(sourceRoot, filepath.FromSlash(name))
			coordinate := l.fileCoordinate(file)
			local := l.fileCoordinate(new(l.documents).localPath(file))
			l.recordReader(
				coordinate,
				local,
				new(manifest.FileReader{Reader: root.owner, Source: root.source, Exact: exact}),
			)
		}
		for i := range doc.built {
			child := &doc.built[i]
			obj := child.Obj
			if record, ok := l.Index.Lookup(obj.Named()); ok && record.Builder == root.coordinate {
				prepared[record.File] = append(prepared[record.File], *child)
				preparedOwners[obj.Named()] = doc.builtInput.Owner
				b.recordProducer(
					new(producerDeclaration{obj: obj, identity: obj.Named(), authored: obj.Named(), visit: root}),
					candidates,
				)
			}
		}
	}
}

func (l *Loader) recordBuiltOwner(root *graphVisit) {
	if record, ok := l.Index.Lookup(root.owner); ok {
		self := manifest.Production{
			Owner:    root.owner,
			Authored: record.Authored,
			Path:     record.Path,
			File:     record.File,
			Source:   root.source,
		}
		if !slices.Contains(record.Productions, self) {
			record.Productions = append(record.Productions, self)
		}
		l.Index.Records[root.owner] = record
	}
}
