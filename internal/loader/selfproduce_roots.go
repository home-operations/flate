package loader

import (
	"maps"
	"path/filepath"
	"slices"
	"strings"

	"github.com/home-operations/flate/internal/selection"
	"github.com/home-operations/flate/internal/source/safepath"
	"github.com/home-operations/flate/internal/store"
	"github.com/home-operations/flate/pkg/manifest"
)

func (l *Loader) unreachedGeneratorRoots(repoRoot string, remaining map[string]struct{}) []string {
	if len(remaining) == 0 {
		return nil
	}
	var roots []string
	for _, dir := range slices.Sorted(maps.Keys(remaining)) {
		if !l.documents.cache[dir].traversed ||
			l.documents.cache[l.documents.cache[dir].file].eligible < selection.RootEligible {
			continue
		}
		rel, err := filepath.Rel(repoRoot, l.documents.cache[dir].file)
		if err != nil {
			continue
		}
		roots = append(roots, filepath.ToSlash(filepath.Dir(rel)))
	}
	return roots
}

func (l *Loader) packageRoots(repoRoot string, kss []store.Child) []string {
	if len(l.documents.cache) == 0 {
		return nil
	}
	l.ensureScopes()
	l.documents.replayEdges()
	sourceRoot := new(l.documents).mountedRoot(repoRoot)
	roots := map[string]bool{}
	l.collectPackageRoots(repoRoot, sourceRoot, roots)
	for i := range kss {
		child := &kss[i]
		//nolint:errcheck // Kustomization-only inputs must panic on an invalid object type.
		ks := child.Obj.(*manifest.Kustomization)
		view := l.scopes.sources[l.scopes.inputs[i].source]
		l.filterClaimedPackageRoots(repoRoot, roots, ks, &view)
	}
	if len(roots) == 0 {
		return nil
	}
	var dirs []string
	for dir := range roots {
		if l.documents.cache[dir].incoming && !roots[dir] {
			continue
		}
		if rel, err := filepath.Rel(
			repoRoot,
			dir,
		); err == nil && !safepath.Escaped(rel) &&
			new(l.documents).mountedRoot(dir) == sourceRoot {
			dirs = append(dirs, filepath.ToSlash(rel))
		}
	}
	slices.Sort(dirs)
	return dirs
}

func (l *Loader) collectPackageRoots(repoRoot, sourceRoot string, roots map[string]bool) {
	//nolint:gocritic // Map iteration supplies value snapshots without an additional lookup.
	for dir, doc := range l.documents.cache {
		if (doc.traversed || dir == l.entryPath) && doc.file != "" {
			roots[dir] = false
			if new(l.documents).mountedRoot(dir) == sourceRoot {
				rel, err := filepath.Rel(repoRoot, dir)
				roots[dir] = err == nil && !safepath.Escaped(rel) && l.anchorRoot(repoRoot, filepath.ToSlash(rel))
			}
			if doc.buildExcluded {
				doc.buildExcluded = false
				l.documents.cache[dir] = doc
			}
		}
	}
}

func (l *Loader) filterClaimedPackageRoots(
	repoRoot string,
	roots map[string]bool,
	ks *manifest.Kustomization,
	view *sourceScope,
) {
	root, pinned, external := view.root, view.pinned, view.external
	if ks.IsFragment() {
		return
	}
	base := new(l.documents).claimBase(root, ks.Path)
	local := filepath.Join(root, base)
	if pinned {
		local = filepath.Join(repoRoot, base)
	} else if !external {
		physical := new(l.documents).localPath(local)
		if doc, exists := l.documents.cache[physical]; exists && physical != local {
			doc.buildExcluded = true
			l.documents.cache[physical] = doc
		}
	}
	prefix := ""
	if external || pinned {
		prefix = strings.TrimSuffix(local, string(filepath.Separator)) + string(filepath.Separator)
	}
	if external {
		l.excludeExternalPackageRoots(roots, local, prefix)
		return
	}
	record, _ := l.Index.Lookup(ks.Named())
	if filepath.Dir(record.Path) != local {
		delete(roots, local)
	}
	if pinned {
		for dir := range roots {
			if strings.HasPrefix(dir, prefix) {
				delete(roots, dir)
			}
		}
	}
}

func (l *Loader) excludeExternalPackageRoots(roots map[string]bool, local, prefix string) {
	for dir, anchored := range roots {
		if anchored || dir == l.entryPath || dir != local && !strings.HasPrefix(dir, prefix) {
			continue
		}
		doc := l.documents.cache[dir]
		doc.buildExcluded = true
		l.documents.cache[dir] = doc
		delete(roots, dir)
	}
}
