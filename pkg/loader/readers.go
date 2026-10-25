package loader

import (
	"context"
	"io/fs"
	"path"
	"path/filepath"
	"slices"
	"strings"

	"github.com/home-operations/flate/internal/chartinput"
	"github.com/home-operations/flate/pkg/manifest"
)

func (l *Loader) recordReader(file, local string, id manifest.NamedResource) {
	l.Index.Readers[file] = appendUniqueProducer(l.Index.Readers[file], id)
	if local != "" {
		l.Index.Readers[local] = appendUniqueProducer(l.Index.Readers[local], id)
	}
}

func (l *Loader) finalizeRecords(ctx context.Context, repoRoot string, emittedFiles map[string][]manifest.NamedResource) error {
	objects := map[manifest.NamedResource]manifest.BaseManifest{}
	object := func(id manifest.NamedResource) manifest.BaseManifest {
		if obj := l.Store.GetObject(id); obj != nil {
			return obj
		}
		if obj, seen := objects[id]; seen {
			return obj
		}
		r, _ := l.Index.Lookup(id)
		obj := l.indexedObject(id, r).Obj
		objects[id] = obj
		return obj
	}
	sources := map[manifest.NamedResource]graphVisit{}
	charts := map[string][]string{}
	inputs := map[manifest.NamedResource][]string{}
	visited := map[manifest.NamedResource]struct{}{}
	var collect func(manifest.NamedResource, []string) ([]string, error)
	collect = func(id manifest.NamedResource, files []string) ([]string, error) {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if _, seen := visited[id]; seen {
			return files, nil
		}
		visited[id] = struct{}{}
		for ref := range manifest.References(object(id), manifest.ReferenceRelations) {
			if ref.Content {
				record, _ := l.Index.Lookup(ref.NamedResource)
				files = append(files, record.Inputs...)
			}
			if ref.Use == "chart" {
				var err error
				files, err = collect(ref.NamedResource, files)
				if err != nil {
					return nil, err
				}
				continue
			}
			if len(ref.Files) == 0 && ref.ChartPath == "" || ref.Name == "" || ref.Kind == "" {
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
			add := func(name string) {
				v.coordinate = path.Join(v.prefix, name)
				files = append(files, v.coordinate)
				if local := v.localCoordinate(); local != "" {
					files = append(files, local)
				}
			}
			for _, name := range ref.Files {
				clean := path.Clean(name)
				if name != "" && fs.ValidPath(clean) && clean != "." && strings.IndexByte(clean, 0) < 0 {
					add(clean)
				}
			}
			if ref.ChartPath == "" {
				continue
			}
			ref.ChartPath = path.Clean(ref.ChartPath)
			location := filepath.Join(v.root, filepath.FromSlash(ref.ChartPath))
			names, seen := charts[location]
			if !seen {
				input, name := l.documents.input(location)
				info, err := fs.Stat(input, name)
				if err == nil {
					if info.IsDir() {
						names = append(names, path.Join(ref.ChartPath, ".helmignore"))
						if sub, err := fs.Sub(input, name); err == nil {
							// Reconciliation owns chart-local loading errors, including policy failures.
							_ = chartinput.Walk(ctx, sub, func(name string, _ fs.FileInfo, _ []byte) error {
								names = append(names, path.Join(ref.ChartPath, name))
								return nil
							})
						}
					} else if info.Mode().IsRegular() {
						names = append(names, ref.ChartPath)
					}
				}
				if err := ctx.Err(); err != nil {
					return nil, err
				}
				charts[location] = names
			}
			for _, name := range names {
				add(name)
			}
		}
		return files, nil
	}
	for id, r := range l.Index.Records {
		if len(r.Refs) == 0 || repoRoot == "" {
			continue
		}
		clear(visited)
		files := slices.Clone(r.Inputs)
		files, err := collect(id, files)
		if err != nil {
			return err
		}
		if len(files) != 0 {
			slices.Sort(files)
			inputs[id] = slices.Compact(files)
		}
	}
	for id, r := range l.Index.Records {
		if r.Path != "" {
			if id.KindInfo().Discovery {
				if _, known := l.roots[id]; !known {
					if l.roots == nil {
						l.roots = map[manifest.NamedResource]bool{}
					}
					l.roots[id] = l.rootAdmission(r.Path, r.File, id.KindInfo().Structural).admits(l, id, r)
				}
			} else if doc := l.documents.cache[r.Path]; r.Authored != (manifest.NamedResource{}) && !doc.rootKnown {
				doc.rootAdmitted = l.rootAdmission(r.Path, r.File, false).admits(l, id, r)
				doc.rootKnown = true
				l.documents.cache[r.Path] = doc
			}
		}
		if files, ok := inputs[id]; ok {
			r.Inputs = files
		}
		if r.File != "" {
			if parents := emittedFiles[r.File]; id.KindInfo().Structural && r.Parent == (manifest.NamedResource{}) && len(parents) == 1 && parents[0] != id {
				r.Parent = parents[0]
			}
			if len(r.Productions) == 0 && r.Authored != (manifest.NamedResource{}) {
				r.Productions = []manifest.Production{{Authored: r.Authored, Path: r.Path, File: r.File, Source: manifest.BootstrapSourceID}}
			}
		}
		// Owner evidence MUST survive compaction while provisional origins are removed.
		r.Productions = slices.DeleteFunc(r.Productions, func(p manifest.Production) bool {
			if p.Owner != (manifest.NamedResource{}) || p.Path == "" {
				return false
			}
			return slices.ContainsFunc(r.Productions, func(owner manifest.Production) bool {
				return owner.Owner != (manifest.NamedResource{}) && owner.Source == p.Source && owner.Authored == p.Authored && owner.Path == p.Path && owner.File == p.File
			})
		})
		slices.SortFunc(r.Productions, func(a, b manifest.Production) int { return a.Owner.Compare(b.Owner) })
		l.Index.Records[id] = r
	}
	return nil
}
