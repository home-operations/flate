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
