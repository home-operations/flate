package loader

import (
	"cmp"
	"maps"
	"path/filepath"
	"slices"

	"github.com/home-operations/flate/internal/engine/store"
	"github.com/home-operations/flate/pkg/manifest"
)

func (l *Loader) prepareBuiltProductions(
	b *selfProduceBuilder,
	repoRoot string,
	prepared map[string][]store.Child,
	preparedOwners map[manifest.NamedResource]manifest.NamedResource,
) {
	owners := slices.SortedFunc(maps.Keys(l.builtBatches), manifest.NamedResource.Compare)
	for _, owner := range owners {
		batch := l.builtBatches[owner]
		if batch.input.Support == nil || !batch.input.Support.Valid() {
			continue
		}
		dir := batch.input.Root
		rel, _ := filepath.Rel(repoRoot, dir)
		root := graphVisit{
			root:   repoRoot,
			dir:    filepath.ToSlash(rel),
			owner:  batch.input.Owner,
			source: batch.input.Source,
		}
		l.recordBuiltOwner(&root)
		var candidates []manifest.NamedResource
		for i := range batch.documents {
			child := &batch.documents[i]
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
		for name, exact := range batch.input.Reads {
			file := filepath.Join(sourceRoot, filepath.FromSlash(name))
			coordinate := l.fileCoordinate(file)
			local := l.fileCoordinate(new(l.documents).localPath(file))
			l.recordReader(
				coordinate,
				local,
				new(manifest.FileReader{Reader: root.owner, Source: root.source, Exact: exact}),
			)
		}
		for i := range batch.documents {
			child := &batch.documents[i]
			obj := child.Obj
			if record, ok := l.Index.Lookup(obj.Named()); ok && record.Builder == root.coordinate &&
				record.BuilderOwner == owner && record.Declaration == obj {
				prepared[record.File] = append(prepared[record.File], *child)
				preparedOwners[obj.Named()] = batch.input.Owner
				b.recordProducer(
					&producerDeclaration{
						obj: obj, identity: obj.Named(), authored: obj.Named(), visit: root,
						substituteDisabled: manifest.HasSubstituteDisabled(child.Doc),
					},
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
