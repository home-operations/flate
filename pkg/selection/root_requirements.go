package selection

import (
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

// PendingRoots returns a captured acquisition frontier; the caller MUST retire it after admission.
func (s *Session) PendingRoots() *store.ReadSet {
	if s == nil {
		return nil
	}
	var reads *store.ReadSet
	required := false
	decision := s.Snapshot()
	for id, record := range s.facts.SelectionIndex().Files() { //nolint:gocritic // The index yields immutable records by value.
		if !decision.Output(id) || record.Path == "" || record.Parent != (manifest.NamedResource{}) ||
			record.BuilderOwner != (manifest.NamedResource{}) || s.store.GetObject(id) != nil {
			continue
		}
		if reads == nil {
			reads = &store.ReadSet{}
			reads.Init(s.store, manifest.NamedResource{}, nil)
		}
		if Root(s.facts.RootEvidence(id, reads)).Admitted && reads.GetObject(id) == nil {
			reads.RequireData(id)
			required = true
		}
	}
	if !required {
		s.store.Retire(reads)
		return nil
	}
	return reads
}
