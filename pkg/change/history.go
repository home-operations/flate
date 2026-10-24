package change

import (
	"cmp"
	"io/fs"
	"iter"
	"maps"
	"path"
	"path/filepath"
	"slices"
	"strings"

	securejoin "github.com/cyphar/filepath-securejoin"

	"github.com/home-operations/flate/pkg/loader"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

// RefineHistory freezes accepted contributor evidence into the pre-Run selection.
// Callers MUST finish refinement before concurrent selection begins.
func (f *Filter) RefineHistory(history map[manifest.NamedResource]iter.Seq2[manifest.NamedResource, manifest.DiscoveryRecord], narrow map[manifest.NamedResource]bool) {
	if !f.Enabled() || len(history) == 0 {
		return
	}
	accepted := map[manifest.NamedResource]map[manifest.NamedResource]manifest.DiscoveryRecord{}
	replace := map[manifest.NamedResource]bool{}
	for owner, records := range history {
		if records == nil {
			continue
		}
		accepted[owner] = map[manifest.NamedResource]manifest.DiscoveryRecord{}
		replace[owner] = narrow[owner]
		for id, record := range records {
			record.ContentRoots = maps.Clone(record.ContentRoots)
			for source, roots := range record.ContentRoots {
				resolved := make([]string, 0, len(roots))
				for _, root := range roots {
					base, ok := f.resolveRoot(source, root)
					if !ok {
						replace[owner] = false
						base = root
					}
					resolved = append(resolved, base)
				}
				record.ContentRoots[source] = resolved
			}
			accepted[owner][id] = record
		}
	}
	if len(accepted) == 0 {
		return
	}
	original := f.index
	for {
		index := &manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{}, Claims: map[string][]manifest.NamedResource{}, Readers: map[string][]manifest.FileReader{}}
		if original != nil {
			for id, record := range original.Records {
				productions := slices.DeleteFunc(slices.Clone(record.Productions), func(p manifest.Production) bool { return replace[p.Owner] })
				if len(record.Productions) != 0 && len(productions) == 0 {
					record.Refs = []manifest.NamedResource{}
					record.Parent = manifest.NamedResource{}
					record.ContentRoots = nil
				}
				record.Productions = productions
				index.Records[id] = record
			}
			for file, ids := range original.Claims {
				for _, id := range ids {
					record := index.Records[id]
					if record.Refs != nil && len(record.Productions) == 0 && record.File == "" {
						continue
					}
					index.Claims[file] = append(index.Claims[file], id)
				}
			}
			for file, readers := range original.Readers {
				for _, reader := range readers {
					record := index.Records[reader.Reader]
					if record.Refs != nil && len(record.Productions) == 0 && record.File == "" {
						continue
					}
					index.Readers[file] = append(index.Readers[file], reader)
				}
			}
		}
		for _, records := range accepted {
			for id, record := range records {
				current := index.Records[id]
				if current.Refs == nil {
					current.Refs = []manifest.NamedResource{}
				}
				current.Refs = append(slices.Clone(current.Refs), record.Refs...)
				slices.SortFunc(current.Refs, manifest.NamedResource.Compare)
				current.Refs = slices.Compact(current.Refs)
				current.Productions = append(slices.Clone(current.Productions), record.Productions...)
				if current.ContentRoots == nil {
					current.ContentRoots = map[manifest.NamedResource][]string{}
				} else {
					current.ContentRoots = maps.Clone(current.ContentRoots)
				}
				for source, roots := range record.ContentRoots {
					current.ContentRoots[source] = append(slices.Clone(current.ContentRoots[source]), roots...)
				}
				index.Records[id] = current
			}
		}
		for id, record := range index.Records {
			slices.SortFunc(record.Productions, func(a, b manifest.Production) int {
				return cmp.Or(a.Owner.Compare(b.Owner), a.Declaration.Compare(b.Declaration), a.Authored.Compare(b.Authored), a.Source.Compare(b.Source), cmp.Compare(a.Path, b.Path), cmp.Compare(a.File, b.File))
			})
			record.Productions = slices.Compact(record.Productions)
			index.Records[id] = record
			for source, roots := range record.ContentRoots {
				for _, root := range roots {
					base, ok := f.resolveRoot(source, root)
					if !ok {
						continue
					}
					art, _ := f.objs.GetArtifact(source).(*store.SourceArtifact)
					physical := loader.OriginPath(art, filepath.Join(art.LocalPath, filepath.FromSlash(base)))
					rel, err := filepath.Rel(f.root, physical)
					if err != nil {
						continue
					}
					prefix := filepath.ToSlash(rel)
					if prefix == "." {
						prefix = ""
					}
					claim := prefix
					if claim != "" {
						claim += "/"
					}
					index.Claims[claim] = append(index.Claims[claim], id)
					if prefix != "" {
						index.Readers[prefix] = append(index.Readers[prefix], manifest.FileReader{Reader: id, Source: source})
					}
				}
			}
		}
		f.index = index
		f.keep = map[manifest.NamedResource]role{}
		f.selectWork(manifest.NamedResource{}, store.Child{}, f.objs)
		restored := false
		for owner, eligible := range replace {
			if eligible && f.keep[owner] == primary {
				replace[owner] = false
				restored = true
			}
		}
		if !restored {
			return
		}
	}
}

type rootView struct {
	fs.FS
	valid bool
}

func (v *rootView) Lstat(name string) (fs.FileInfo, error) {
	info, err := fs.Lstat(v.FS, strings.TrimLeft(filepath.ToSlash(name), "/"))
	if err != nil {
		v.valid = false
	}
	return info, err
}

func (v *rootView) Readlink(name string) (string, error) {
	target, err := fs.ReadLink(v.FS, strings.TrimLeft(filepath.ToSlash(name), "/"))
	resolved := path.Join(path.Dir(strings.TrimLeft(name, "/")), target)
	if err != nil || path.IsAbs(target) || resolved == ".." || strings.HasPrefix(resolved, "../") {
		v.valid = false
	}
	return target, err
}

func (f *Filter) resolveRoot(source manifest.NamedResource, root string) (string, bool) {
	if f.objs == nil || root == "" || path.IsAbs(root) || strings.Contains(root, "://") || strings.HasPrefix(path.Clean(root), "../") || path.Clean(root) == ".." {
		return "", false
	}
	art, _ := f.objs.GetArtifact(source).(*store.SourceArtifact)
	if art == nil || art.FS == nil {
		return "", false
	}
	view := &rootView{FS: art.FS, valid: true}
	resolved, err := securejoin.SecureJoinVFS("/", root, view)
	if err != nil || !view.valid {
		return "", false
	}
	resolved = strings.TrimPrefix(filepath.ToSlash(resolved), "/")
	if resolved == "" {
		resolved = "."
	}
	info, err := fs.Stat(art.FS, resolved)
	return resolved, err == nil && info.IsDir()
}
