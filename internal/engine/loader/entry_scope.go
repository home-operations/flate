package loader

import (
	"maps"

	"github.com/home-operations/flate/internal/engine/selection"
	"github.com/home-operations/flate/internal/engine/store"
	"github.com/home-operations/flate/pkg/manifest"
)

// EntryScope outlives registration and replacement of the owner's built batch.
func (l *Loader) EntryScope(owner manifest.NamedResource) *selection.EntryScope {
	return l.entryScopes.Load().Scope(owner)
}

// SetEntryScope runs during serial construction. Readers borrow its immutable snapshot.
func (l *Loader) SetEntryScope(owner manifest.NamedResource, scope *selection.EntryScope) bool {
	current := l.entryScopes.Load()
	previous := current.Scope(owner)
	if previous == scope || previous.Same(scope) {
		return false
	}
	if scope != nil && scope.Preparation != nil {
		reads := &store.ReadSet{}
		reads.Init(l.Store, owner, nil)
		if scope.Preparation.Valid() && reads.Matches(scope.Preparation) {
			if scope.Source != nil {
				reads.GetArtifact(scope.SourceID, store.ArtifactContent|store.ArtifactRevision|store.ArtifactOrigin)
			}
			reads.GetDeclaration(owner)
			scope.Support = reads
		} else {
			reads.Retire()
		}
	}
	if previous != nil {
		previous.Support.Retire()
	}
	next := make(selection.EntryScopes)
	if current != nil {
		maps.Copy(next, *current)
	}
	if scope == nil {
		delete(next, owner)
	} else {
		next[owner] = scope
	}
	if len(next) == 0 {
		l.entryScopes.Store(nil)
		return true
	}
	l.entryScopes.Store(&next)
	return true
}

func retireEntryScopes(scopes *selection.EntryScopes) {
	if scopes != nil {
		for _, scope := range *scopes {
			scope.Support.Retire()
		}
	}
}
