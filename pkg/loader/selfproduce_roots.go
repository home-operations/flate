package loader

import (
	"maps"
	"path/filepath"
	"slices"
	"strings"

	"github.com/home-operations/flate/internal/kustomizeinput"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source/safepath"
	"github.com/home-operations/flate/pkg/store"
)

func (l *Loader) unreachedGeneratorRoots(repoRoot string, remaining map[string]struct{}) []string {
	if len(remaining) == 0 {
		return nil
	}
	var roots []string
	for _, dir := range slices.Sorted(maps.Keys(remaining)) {
		if !l.documents.cache[dir].traversed || l.documents.cache[l.documents.cache[dir].file].eligible < fileEligible {
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
	for i, child := range kss {
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

func (l *Loader) collectPackageRoots(repoRoot string, sourceRoot string, roots map[string]bool) {
	{
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

// StandaloneRoots returns sorted, unbuilt bootstrap packages outside Flux ownership.
// Coordinates are borrowed until the next loader admission.
func (l *Loader) StandaloneRoots(repoRoot string) []string {
	l.Admit(repoRoot)
	l.scopes.roots = slices.DeleteFunc(l.scopes.roots, func(dir string) bool {
		return l.excludeStandaloneRoot(repoRoot, dir)
	})
	return l.scopes.roots
}

func (l *Loader) excludeStandaloneRoot(repoRoot string, dir string) bool {
	root := filepath.Join(repoRoot, filepath.FromSlash(dir))
	doc := l.documents.cache[root]
	if doc.buildExcluded || doc.selected || doc.built != nil ||
		isKustomizeComponent(doc.body) && root != l.entryPath {
		return true
	}
	if root == l.entryPath {
		for _, claim := range l.scopes.claims {
			if strings.HasPrefix(dir+"/", claim.Prefix) {
				return true
			}
		}
		return false
	}
	visitValues := l.scopeWalk(new(graphVisit{root: repoRoot, dir: dir}))
	for visitIndex := range visitValues {
		visit := visitValues[visitIndex]
		if visit.role == kustomizeinput.Resource {
			found := false
			l.fileOwners(visit.file, visit.coordinate, func(manifest.NamedResource) bool {
				found = true
				return false
			})
			if found {
				return true
			}
		}
	}
	return false
}

func (l *Loader) excludeExternalPackageRoots(roots map[string]bool, local string, prefix string) {
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
