package selection

import (
	"maps"

	"github.com/home-operations/flate/internal/engine/store"
	"github.com/home-operations/flate/pkg/manifest"
)

// EntryRoute borrows discovery's immutable matched support and its captured observations.
type EntryRoute struct {
	Application            *manifest.EntryApplication
	Selected               bool
	Winners                *map[OccurrenceID]manifest.NamedResource
	PreviousGeneratedEntry bool
	Previous               manifest.NamedResource
	Producer               manifest.NamedResource
	Root                   manifest.NamedResource
	Replay                 manifest.NamedResource
	Reads                  *store.ReadSet
	Reach                  *EntryReach
	Bound                  bool
	Changed                bool
	Projected              bool
	ProjectedPatches       bool
}

// EntryReach binds immutable reach evidence to the current qualification producer.
type EntryReach struct {
	Producer manifest.NamedResource
	Possible func([]manifest.BaseManifest) bool
}

func (r *EntryRoute) equal(other *EntryRoute) bool {
	if r == other {
		return true
	}
	if r == nil || other == nil {
		return false
	}
	left, right := *r, *other
	left.Winners, right.Winners = nil, nil
	if left != right {
		return false
	}
	if r.Winners == other.Winners {
		return true
	}
	return r.Winners != nil && other.Winners != nil && maps.Equal(*r.Winners, *other.Winners)
}

func (r *EntryRoute) generatedPath(reads *store.ReadSet) bool {
	return r != nil && !r.Selected && r.Producer != (manifest.NamedResource{}) &&
		reads.GeneratedDocument(r.Producer)
}

// PublishEntry MUST receive a completed immutable route from the entry qualification owner.
func (s *Session) PublishEntry(route *EntryRoute) {
	if s == nil {
		return
	}
	previous := s.entry.Load()
	if previous.equal(route) {
		return
	}
	s.entry.Store(route)
}

// EntryReplay reports a producer needed to restore a validated entry route.
func (s *Session) EntryReplay(id manifest.NamedResource) bool {
	if s == nil {
		return false
	}
	route := s.entry.Load()
	return route != nil && route.Replay == id
}

// QualifiedEntryReach borrows the evidence for the current qualification candidate.
func (s *Session) QualifiedEntryReach(id manifest.NamedResource) func([]manifest.BaseManifest) bool {
	if s != nil {
		route := s.entry.Load()
		if route != nil && route.Reach != nil && route.Reach.Producer == id {
			reach := route.Reach
			return func(objects []manifest.BaseManifest) bool {
				possible := reach.Possible(objects)
				current := s.entry.Load()
				return possible || current == nil || current.Reach != reach
			}
		}
	}
	return nil
}

func (a *Admission) entrySupported() bool {
	route := a.entry
	if route == nil || route.Application == nil ||
		route.Producer == (manifest.NamedResource{}) || route.Reads == nil || !a.reads.Matches(route.Reads) {
		return false
	}
	for id := range route.Reads.Inputs() {
		if !route.Reads.MatchesContent(a.reads, id) {
			return false
		}
	}
	a.entryReads = a.reads
	return true
}

func (c *admissionClosure) pendingEntrySources() bool {
	route := c.proposal.entry
	if route == nil || route.Reads == nil {
		return false
	}
	// An unavailable acquisition MUST settle before its accepted route can be replaced.
	pending := false
	for id := range route.Reads.Inputs() {
		if !id.KindInfo().SourceArtifact || c.objects.GetDeclaration(id) == nil ||
			c.objects.GetArtifact(id, store.ArtifactContent) != nil {
			continue
		}
		status, _ := c.objects.GetStatus(id)
		if status.Outcome() == store.OutcomeFailed || status.Outcome() == store.OutcomeSkipped {
			continue
		}
		c.add(id, admissionRole{needed: true, ordinary: true})
		pending = true
	}
	return pending
}
