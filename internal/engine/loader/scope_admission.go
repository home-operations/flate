package loader

import (
	"path/filepath"
	"slices"

	"github.com/home-operations/flate/internal/engine/kustomize/kustomizeinput"
	"github.com/home-operations/flate/internal/engine/store"

	"github.com/home-operations/flate/pkg/manifest"
)

func (l *Loader) visitKustomizationScope(repoRoot string, ks *manifest.Kustomization, i int) {
	input := l.scopes.inputs[i]
	scope := l.scopes.sources[input.source]
	root, pinned, external := scope.root, scope.pinned, scope.external
	if input.scanRequired && !external && !scope.ready {
		return
	}
	if root == "" || ks.IsFragment() {
		return
	}
	v := graphVisit{
		root:      root,
		dir:       new(l.documents).claimBase(root, ks.Path),
		namespace: ks.TargetNamespace,
		fallback:  ks.TargetNamespace,
		owner:     ks.Named(),
		source:    ks.EffectiveSourceID(),
		input:     i + 1,
	}
	if !new(l.documents).scopeCoordinates(&v, repoRoot, pinned, external) {
		return
	}
	l.visitScope(new(v), false)
}

func (r *documentReader) scopeCoordinates(visit *graphVisit, repoRoot string, pinned, external bool) bool {
	if visit.root == repoRoot {
		return true
	}
	prefix, err := filepath.Rel(repoRoot, visit.root)
	if err != nil {
		return false
	}
	visit.prefix = filepath.ToSlash(prefix)
	if !pinned && !external {
		visit.localBase = r.localBase(visit.root, repoRoot)
	}
	return true
}

func (l *Loader) visitRootScopes(repoRoot string, kss []store.Child) {
	l.scopes.roots = l.packageRoots(repoRoot, kss)
	if l.entry == entryKustomize {
		entry := l.fileCoordinate(l.entryPath)
		projected := filepath.Join(repoRoot, filepath.FromSlash(entry))
		if new(l.documents).localPath(l.entryPath) == new(l.documents).localPath(projected) &&
			!slices.Contains(l.scopes.roots, entry) {
			v := graphVisit{
				root: repoRoot, dir: entry, source: manifest.BootstrapSourceID, role: kustomizeinput.Resource,
			}
			l.visitScope(new(v), true)
		}
	}
	for _, dir := range l.scopes.roots {
		v := graphVisit{root: repoRoot, dir: dir, source: manifest.BootstrapSourceID, role: kustomizeinput.Resource}
		claimed := false
		for range l.Index.ClaimOwners(dir+"/", manifest.BootstrapSourceID, manifest.NamedResource{}, nil) {
			claimed = true
			break
		}
		l.visitScope(new(v), l.anchorRoot(repoRoot, dir) && claimed)
	}
}
