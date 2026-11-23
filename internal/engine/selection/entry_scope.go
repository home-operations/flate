package selection

import (
	"fmt"
	"maps"
	"slices"
	"sync/atomic"

	"github.com/home-operations/flate/internal/engine/store"
	"github.com/home-operations/flate/pkg/manifest"
)

// EntryScope MUST hold for the current source and owning declaration before it admits a child.
type EntryScope struct {
	Children    map[manifest.NamedResource]bool
	Support     *store.ReadSet
	Source      *store.SourceArtifact
	SourceID    manifest.NamedResource
	Preparation *store.ReadSet
}

// Current requires the owner and material read by the proof to remain current.
func (s *EntryScope) Current() bool {
	return s != nil && s.Support.MaterialCurrent() && (s.Source == nil || s.Source.Reads.Valid())
}

// Same permits reusing a proof only while its support remains current.
func (s *EntryScope) Same(next *EntryScope) bool {
	if s == nil || next == nil || s.Preparation != next.Preparation || s.Source != next.Source {
		return false
	}
	return (s.Preparation == nil || s.Current()) && maps.Equal(s.Children, next.Children)
}

// Allows treats absent scope as unrestricted and pending scope as empty.
func (s *EntryScope) Allows(child manifest.NamedResource) bool {
	return s == nil || s.Children[child] && s.Current()
}

// EntryScopes MUST remain immutable after publication.
type EntryScopes map[manifest.NamedResource]*EntryScope

// Scope preserves the distinction between absent and empty owner membership.
func (s *EntryScopes) Scope(owner manifest.NamedResource) *EntryScope {
	if s == nil {
		return nil
	}
	return (*s)[owner]
}

func (s *EntryScopes) allowsProduction(production *manifest.Production, child manifest.NamedResource) bool {
	scope := s.Scope(production.Owner)
	return scope.Allows(child) &&
		(production.Declaration == (manifest.NamedResource{}) || scope.Allows(production.Declaration))
}

func (s *EntryScopes) lookupAllowsCandidate(
	id, owner manifest.NamedResource, independent func() bool, productions func() []manifest.Production,
) bool {
	if owner != (manifest.NamedResource{}) && s.Scope(owner).Allows(id) || independent() {
		return true
	}
	return slices.ContainsFunc(productions(), func(production manifest.Production) bool {
		return production.Owner != (manifest.NamedResource{}) && production.Owner != id &&
			s.allowsProduction(&production, id)
	})
}

// EntryMembershipFailure attributes unresolved membership to its owning package.
func EntryMembershipFailure(path string, owner, child manifest.NamedResource) error {
	return manifest.NewError(manifest.ErrInput, nil,
		fmt.Sprintf("cannot establish membership for %s at --path %q in owning %s", child, path, owner))
}

type entryScopeCapture struct {
	source *atomic.Pointer[EntryScopes]
	scopes *EntryScopes
	proofs []*EntryScope
}

func (c *entryScopeCapture) capture(source *atomic.Pointer[EntryScopes]) {
	c.source = source
	if source == nil {
		return
	}
	c.scopes = source.Load()
	if c.scopes != nil {
		for _, scope := range *c.scopes {
			if scope.Current() {
				c.proofs = append(c.proofs, scope)
			}
		}
	}
}

func (c *entryScopeCapture) current() bool {
	if c.source != nil && c.source.Load() != c.scopes {
		return false
	}
	for _, scope := range c.proofs {
		if !scope.Current() {
			return false
		}
	}
	return true
}

func (c *admissionClosure) captureEntryScopes(scopes *entryScopeCapture) {
	if !c.scopes.current() {
		//nolint:gocritic // Map values MUST remain independent snapshots until publication.
		for id, state := range c.states {
			state.directKnown = false
			c.states[id] = state
		}
	}
	c.scopes = *scopes
}
