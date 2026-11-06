package loader

import (
	"path/filepath"
	"slices"
	"strings"

	"github.com/home-operations/flate/internal/kustomizeinput"
	"github.com/home-operations/flate/pkg/store"

	"github.com/home-operations/flate/pkg/manifest"
)

func (l *Loader) visitKustomizationScope(repoRoot string, ks *manifest.Kustomization, i int) {
	scope := l.scopes.sources[l.scopes.inputs[i].source]
	ref := scope.id
	root, pinned, external := scope.root, scope.pinned, scope.external
	if ref.Name != "" && !external && !scope.ready {
		return
	}
	if ks.IsFragment() {
		return
	}
	if external {
		v := graphVisit{
			root:   repoRoot,
			dir:    new(l.documents).claimBase(repoRoot, ks.Path),
			owner:  ks.Named(),
			source: ref,
			input:  i + 1,
		}
		l.visitScope(new(v), true)
		return
	}
	v := graphVisit{
		root:      root,
		dir:       new(l.documents).claimBase(root, ks.Path),
		namespace: ks.TargetNamespace,
		fallback:  ks.TargetNamespace,
		owner:     ks.Named(),
		source:    ks.SourceID(),
		input:     i + 1,
	}
	if v.source.Name == "" || v.source.Kind == "" {
		v.source = manifest.BootstrapSourceID
	}
	if !new(l.documents).scopeCoordinates(&v, repoRoot, pinned) {
		return
	}
	l.visitScope(new(v), false)
}

func (r *documentReader) scopeCoordinates(visit *graphVisit, repoRoot string, pinned bool) bool {
	if visit.root == repoRoot {
		return true
	}
	prefix, err := filepath.Rel(repoRoot, visit.root)
	if err != nil {
		return false
	}
	visit.prefix = filepath.ToSlash(prefix)
	if !pinned {
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
		if l.anchorRoot(repoRoot, dir) &&
			slices.ContainsFunc(
				l.scopes.claims,
				func(claim manifest.KSClaim) bool { return strings.HasPrefix(dir+"/", claim.Prefix) },
			) {
			l.visitScope(new(v), true)
		} else {
			l.visitScope(new(v), false)
		}
	}
}
