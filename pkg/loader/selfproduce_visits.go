package loader

import (
	"cmp"
	"slices"

	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source/safepath"
	"github.com/home-operations/flate/pkg/store"
)

func (l *Loader) recordPreparedProductions(
	b *selfProduceBuilder,
	v *graphVisit,
	children []store.Child,
	preparedOwners map[manifest.NamedResource]manifest.NamedResource,
) {
	for i := range children {
		child := &children[i]
		id := child.Obj.Named()
		r := l.Index.Records[id]
		if v.owner == (manifest.NamedResource{}) || v.owner == id || v.owner == r.Parent {
			continue
		}
		p := manifest.Production{Owner: v.owner, Source: v.source, File: r.File}
		if !slices.Contains(r.Productions, p) {
			r.Productions = append(r.Productions, p)
		}
		if manifest.KindOf(child.Obj).Structural &&
			(r.Parent == preparedOwners[id] || r.Parent == (manifest.NamedResource{})) {
			r.Parent = manifest.NamedResource{}
			b.emittedFiles[r.File] = appendUniqueProducer(b.emittedFiles[r.File], v.owner)
		}
		l.Index.Records[id] = r
	}
}

func (l *Loader) recordGraphGenerators(v *graphVisit, generators *[]generatorRecord) {
	{
		gValues := v.document.ConfigMapGenerator
		for gIndex := range gValues {
			g := gValues[gIndex]
			*generators = append(
				*generators,
				generatorRecord{
					root:        v.root,
					file:        v.file,
					namespace:   cmp.Or(v.namespace, g.Namespace, v.fallback),
					isConfigMap: true,
					args:        g.GeneratorArgs,
					owner:       v.owner,
					source:      v.source,
					reader:      l.documents,
				},
			)
		}
	}
	{
		gValues2 := v.document.SecretGenerator
		for gIndex2 := range gValues2 {
			g := gValues2[gIndex2]
			*generators = append(
				*generators,
				generatorRecord{
					root:      v.root,
					file:      v.file,
					namespace: cmp.Or(v.namespace, g.Namespace, v.fallback),
					args:      g.GeneratorArgs,
					owner:     v.owner,
					source:    v.source,
					reader:    l.documents,
				},
			)
		}
	}
}

func (l *Loader) recordProductionInput(
	v *graphVisit,
	repoRoot string,
	pinned map[manifest.NamedResource]bool,
	incomplete map[manifest.NamedResource][]string,
) bool {
	if file := cmp.Or(v.origin, v.file); len(pinned) != 0 && !pinned[v.owner] && safepath.Contains(repoRoot, file) {
		ownership := l.Index.OwnersOfFile(l.fileCoordinate(file))
		pinnedOwner := func(owner manifest.NamedResource) bool { return pinned[owner] }
		if slices.ContainsFunc(ownership.Owners, pinnedOwner) || slices.ContainsFunc(ownership.Ancestors, pinnedOwner) {
			return false
		}
	}
	if v.owner != (manifest.NamedResource{}) && v.coordinate != "" {
		l.recordReader(v.coordinate, v.localCoordinate(), new(manifest.FileReader{Reader: v.owner, Source: v.source}))
	}
	if v.incomplete != "" {
		if v.owner != (manifest.NamedResource{}) {
			incomplete[v.owner] = append(incomplete[v.owner], v.source.String()+":"+v.coordinate+": "+v.incomplete)
		}
		return false
	}
	return true
}
