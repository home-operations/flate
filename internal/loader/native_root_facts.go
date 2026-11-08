package loader

import (
	"path/filepath"
	"slices"

	"github.com/home-operations/flate/internal/kustomizeinput"
	"github.com/home-operations/flate/internal/selection"
	"github.com/home-operations/flate/pkg/manifest"
)

func (l *Loader) captureNativeRoots(repoRoot string) error {
	roots := make([]selection.NativeRootFacts, 0, len(l.scopes.roots))
	claims := slices.Clone(l.scopes.claims)
	for _, relative := range l.scopes.roots {
		root := filepath.Join(repoRoot, filepath.FromSlash(relative))
		doc := l.documents.cache[root]
		facts := selection.NativeRootFacts{
			Path: relative, Claims: claims, Entry: root == l.entryPath,
			Built: doc.built != nil, Selected: doc.selected, Excluded: doc.buildExcluded,
			Component: isKustomizeComponent(doc.body),
		}
		walk := l.scopes.walks[graphContext{root: repoRoot, dir: relative, role: kustomizeinput.Resource}]
		for i := range walk.visits {
			visit := &walk.visits[i]
			if visit.role != kustomizeinput.Resource {
				continue
			}
			l.fileOwners(visit.file, visit.coordinate, func(owner manifest.NamedResource) bool {
				facts.Owners = appendUniqueProducer(facts.Owners, owner)
				return true
			})
			if visit.document != nil || !visit.readable || visit.deferDecode {
				continue
			}
			if err := l.visitOccurrences(visit, func(o authoredOccurrence) {
				facts.Identities = append(facts.Identities, o.Identity)
			}); err != nil {
				return err
			}
		}
		roots = append(roots, facts)
	}
	l.scopes.rootFacts = roots
	return nil
}
