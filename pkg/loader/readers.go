package loader

import (
	"io/fs"
	"path"
	"path/filepath"
	"slices"
	"strings"

	"github.com/home-operations/flate/pkg/manifest"
)

func (l *Loader) recordReader(file, local string, id manifest.NamedResource) {
	l.Index.Readers[file] = appendUniqueProducer(l.Index.Readers[file], id)
	if local != "" {
		l.Index.Readers[local] = appendUniqueProducer(l.Index.Readers[local], id)
	}
}

func (l *Loader) finalizeRecords(repoRoot string, emittedFiles map[string][]manifest.NamedResource) {
	objects := map[manifest.NamedResource]manifest.BaseManifest{}
	object := func(id manifest.NamedResource) manifest.BaseManifest {
		if obj := l.Store.GetObject(id); obj != nil {
			return obj
		}
		if obj, seen := objects[id]; seen {
			return obj
		}
		r, _ := l.Index.Lookup(id)
		obj := l.indexedObject(id, r)
		objects[id] = obj
		return obj
	}
	sources := map[manifest.NamedResource]graphVisit{}
	visited := map[manifest.NamedResource]struct{}{}
	var collect func(manifest.NamedResource, manifest.NamedResource)
	collect = func(id, consumer manifest.NamedResource) {
		if _, seen := visited[id]; seen {
			return
		}
		visited[id] = struct{}{}
		obj := object(id)
		for ref := range manifest.References(obj) {
			if ref.Use == "chart" {
				collect(ref.NamedResource, consumer)
				continue
			}
			if len(ref.Files) == 0 || ref.Name == "" || ref.Kind == "" {
				continue
			}
			v, seen := sources[ref.NamedResource]
			if !seen {
				root, pinned, external := SourceView(l.Store, ref.NamedResource, repoRoot)
				if !external {
					v.root = root
					prefix, _ := filepath.Rel(repoRoot, root)
					if prefix != "." {
						v.prefix = filepath.ToSlash(prefix)
						if !pinned {
							v.localBase = l.documents.localBase(root, repoRoot)
						}
					}
				}
				sources[ref.NamedResource] = v
			}
			if v.root == "" {
				continue
			}
			for _, name := range ref.Files {
				clean := path.Clean(name)
				if name == "" || !fs.ValidPath(clean) || clean == "." || strings.IndexByte(clean, 0) >= 0 {
					continue
				}
				v.coordinate = path.Join(v.prefix, clean)
				l.recordReader(v.coordinate, v.localCoordinate(), consumer)
			}
		}
	}
	for id, r := range l.Index.Records {
		if r.File != "" {
			if len(r.Refs) != 0 && repoRoot != "" {
				clear(visited)
				collect(id, id)
			}
			if parents := emittedFiles[r.File]; id.KindInfo().Structural && r.Parent == (manifest.NamedResource{}) && len(parents) == 1 && parents[0] != id {
				r.Parent = parents[0]
			}
			if len(r.Productions) == 0 && r.Authored != (manifest.NamedResource{}) {
				r.Productions = []manifest.Production{{Authored: r.Authored, Path: r.Path, File: r.File, Source: manifest.BootstrapSourceID}}
			}
		}
		slices.SortFunc(r.Productions, func(a, b manifest.Production) int { return a.Owner.Compare(b.Owner) })
		l.Index.Records[id] = r
	}
}
