package loader

import (
	"io/fs"
	"iter"
	"maps"
	"path"
	"path/filepath"
	"slices"
	"strings"

	securejoin "github.com/cyphar/filepath-securejoin"

	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

func (c *admissionClosure) readHistory() {
	c.accepted = map[manifest.NamedResource]map[manifest.NamedResource]manifest.DiscoveryRecord{}
	c.replace = map[manifest.NamedResource]bool{}
	for owner, history := range c.history {
		if history.Records == nil {
			continue
		}
		c.replace[owner] = history.Complete
		records := map[manifest.NamedResource]manifest.DiscoveryRecord{}
		for id, record := range history.Records {
			record.ContentRoots = maps.Clone(record.ContentRoots)
			for source, roots := range record.ContentRoots {
				resolved := make([]string, 0, len(roots))
				for _, root := range roots {
					base, ok := resolveRoot(c.objects, source, root)
					if !ok {
						c.replace[owner] = false
						base = root
						if strings.Contains(root, "/../") || strings.HasSuffix(root, "/..") {
							base = "."
						}
					}
					resolved = append(resolved, base)
				}
				record.ContentRoots[source] = resolved
			}
			records[id] = record
		}
		c.accepted[owner] = records
	}
}

func (c *admissionClosure) record(id manifest.NamedResource) manifest.DiscoveryRecord {
	record, indexed := c.loader.Index.Lookup(id)
	if !indexed && id.Namespace != "" {
		if occurrence, ok := c.loader.occurrence(id); ok && occurrence.Authored.Namespace == "" {
			bare := id
			bare.Namespace = ""
			record, _ = c.loader.Index.Lookup(bare)
		}
	}
	if occurrence, ok := c.loader.occurrence(id); ok && len(occurrence.facts.roots) != 0 {
		source, _, _ := occurrence.SourcePath()
		if source.Name == "" || source.Kind == "" {
			source = manifest.BootstrapSourceID
		}
		record.ContentRoots = map[manifest.NamedResource][]string{source: occurrence.facts.roots}
	}
	if len(c.accepted) == 0 {
		return record
	}
	if record.File == "" && len(record.Productions) != 0 && len(c.productions(id)) == 0 {
		record.Refs, record.ContentRoots, record.Parent = nil, nil, manifest.NamedResource{}
	}
	for owner, records := range c.accepted {
		if historical, ok := records[id]; ok {
			record.Refs = append(slices.Clone(record.Refs), historical.Refs...)
			record.ContentRoots = maps.Clone(record.ContentRoots)
			if record.ContentRoots == nil {
				record.ContentRoots = map[manifest.NamedResource][]string{}
			}
			for source, roots := range historical.ContentRoots {
				if owner != id && c.replace[id] {
					continue
				}
				record.ContentRoots[source] = append(slices.Clone(record.ContentRoots[source]), roots...)
			}
		}
	}
	return record
}

func (c *admissionClosure) historicalReferences(id manifest.NamedResource) iter.Seq[manifest.Reference] {
	return func(yield func(manifest.Reference) bool) {
		for _, records := range c.accepted {
			for _, ref := range records[id].Refs {
				reference := manifest.Reference{NamedResource: ref, Content: true}
				if object := c.object(id); object != nil {
					for declared := range manifest.References(object, manifest.ReferenceRelations) {
						if declared.NamedResource == ref {
							reference = declared
							if !declared.Ordering {
								break
							}
						}
					}
				}
				if !yield(reference) {
					return
				}
			}
		}
	}
}

type admissionRootView struct {
	fs.FS
	valid bool
}

func (v *admissionRootView) Lstat(name string) (fs.FileInfo, error) {
	info, err := fs.Lstat(v.FS, strings.TrimLeft(filepath.ToSlash(name), "/"))
	if err != nil {
		v.valid = false
	}
	return info, err
}

func (v *admissionRootView) Readlink(name string) (string, error) {
	target, err := fs.ReadLink(v.FS, strings.TrimLeft(filepath.ToSlash(name), "/"))
	resolved := path.Join(path.Dir(strings.TrimLeft(name, "/")), target)
	if err != nil || path.IsAbs(target) || resolved == ".." || strings.HasPrefix(resolved, "../") {
		v.valid = false
	}
	return target, err
}

func resolveRoot(objects sourceStore, source manifest.NamedResource, root string) (string, bool) {
	if root == "" || path.IsAbs(root) || strings.Contains(root, "://") || strings.HasPrefix(path.Clean(root), "../") || path.Clean(root) == ".." {
		return "", false
	}
	art, _ := objects.GetArtifact(source, store.ArtifactContent).(*store.SourceArtifact)
	if art == nil || art.FS == nil {
		return "", false
	}
	view := &admissionRootView{FS: art.FS, valid: true}
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
