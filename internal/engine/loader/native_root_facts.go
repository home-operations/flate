package loader

import (
	"path/filepath"
	"slices"

	"github.com/home-operations/flate/internal/engine/kustomize/kustomizeinput"
	"github.com/home-operations/flate/internal/engine/selection"
	"github.com/home-operations/flate/pkg/manifest"
)

func (l *Loader) captureNativeRoots(repoRoot string) error {
	roots := make([]selection.NativeRootFacts, 0, len(l.scopes.roots))
	index := *l.Index
	built := l.builtDirectories()
	for _, relative := range l.scopes.roots {
		root := filepath.Join(repoRoot, filepath.FromSlash(relative))
		doc := l.documents.cache[root]
		facts := selection.NativeRootFacts{
			Path: relative, Index: &index, Entry: root == l.entryPath,
			Built: built[root], Selected: doc.selected, Excluded: doc.buildExcluded,
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

func (l *Loader) builtDirectories() map[string]bool {
	if len(l.builtBatches) == 0 {
		return nil
	}
	built := make(map[string]bool, len(l.builtBatches))
	for _, batch := range l.builtBatches {
		if !built[batch.input.Root] && batch.input.Support.Valid() {
			built[batch.input.Root] = true
		}
	}
	return built
}

func (l *Loader) refreshNativeBuildFacts(registered string, rebuild bool) {
	if l.frozen {
		return
	}
	roots := &l.scopeRoots
	if l.scopes != nil {
		roots = &l.scopes.rootFacts
	}
	if len(*roots) == 0 {
		return
	}
	var built map[string]bool
	if rebuild {
		built = l.builtDirectories()
	}
	var updated []selection.NativeRootFacts
	for i := range *roots {
		facts := &(*roots)[i]
		root := filepath.Join(l.SourceRoot, filepath.FromSlash(facts.Path))
		complete, selected, excluded := facts.Built, facts.Selected, facts.Excluded
		if root == registered {
			doc := l.documents.cache[root]
			complete, selected, excluded = true, doc.selected, doc.buildExcluded
		}
		if rebuild {
			complete = built[root]
		}
		if complete == facts.Built && selected == facts.Selected && excluded == facts.Excluded {
			continue
		}
		if updated == nil {
			updated = slices.Clone(*roots)
		}
		updated[i].Built, updated[i].Selected, updated[i].Excluded = complete, selected, excluded
	}
	if updated != nil {
		*roots = updated
	}
}
