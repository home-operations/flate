package selection

import (
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

// EntryRoute borrows discovery's immutable matched support and its captured observations.
type EntryRoute struct {
	Application *manifest.EntryApplication
	Previous    manifest.NamedResource
	Producer    manifest.NamedResource
	Root        manifest.NamedResource
	Reads       *store.ReadSet
	Bound       bool
}

// PublishEntry MUST receive a completed immutable route from the entry qualification owner.
func (s *Session) PublishEntry(route *EntryRoute) {
	if s == nil {
		return
	}
	previous := s.entry.Load()
	if previous == route || previous != nil && route != nil && *previous == *route {
		return
	}
	s.entry.Store(route)
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
