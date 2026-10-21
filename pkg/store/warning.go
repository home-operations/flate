package store

import (
	"fmt"
	"slices"

	"github.com/home-operations/flate/pkg/manifest"
)

// AddWarning records one advisory per origin and complete warning payload.
// Count reports distinct origins sharing a resource, category, message and
// ordered detail; incoming Count is ignored. A zero origin identifies a
// render-global advisory independently of the displayed Resource.
func (s *Store) AddWarning(origin manifest.NamedResource, w manifest.Warning) {
	detail := ""
	if len(w.Detail) > 0 {
		detail = fmt.Sprintf("%q", w.Detail)
		w.Detail = slices.Clone(w.Detail)
	} else {
		w.Detail = nil
	}
	key := warningKey{w.Resource, w.Category, w.Message, detail}

	s.warnMu.Lock()
	defer s.warnMu.Unlock()
	if i, ok := s.warnIndex[key]; ok {
		record := &s.warnOrder[i]
		if _, seen := record.origins[origin]; !seen {
			record.origins[origin] = struct{}{}
			record.warning.Count = len(record.origins)
		}
		return
	}
	if s.warnIndex == nil {
		s.warnIndex = make(map[warningKey]int)
	}
	w.Count = 1
	s.warnIndex[key] = len(s.warnOrder)
	s.warnOrder = append(s.warnOrder, warningRecord{
		warning: w,
		origins: map[manifest.NamedResource]struct{}{origin: {}},
	})
}

// Warnings returns a copy of the collected advisories ordered by
// manifest.CompareWarning, or nil when none were recorded.
func (s *Store) Warnings() []manifest.Warning {
	s.warnMu.Lock()
	var out []manifest.Warning
	if len(s.warnOrder) > 0 {
		out = make([]manifest.Warning, len(s.warnOrder))
	}
	for i, record := range s.warnOrder {
		w := record.warning
		w.Detail = slices.Clone(w.Detail)
		out[i] = w
	}
	s.warnMu.Unlock()
	type contribution struct {
		owner manifest.NamedResource
		child Child
	}
	for _, sh := range s.shards {
		byID := map[manifest.NamedResource][]contribution{}
		sh.mu.RLock()
		for id, members := range sh.members {
			if sh.hidden[id] {
				continue
			}
			for _, m := range members {
				if !m.declaration && m.slot.visible.Load() {
					child := m.slot.current.Load().entries[m.position].child
					byID[id] = append(byID[id], contribution{m.slot.id, child})
				}
			}
		}
		sh.mu.RUnlock()
		for id, contributions := range byID {
			if len(contributions) < 2 || !slices.ContainsFunc(contributions[1:], func(c contribution) bool {
				return !semanticEqual(contributions[0].child, c.child)
			}) {
				continue
			}
			slices.SortFunc(contributions, func(a, b contribution) int { return a.owner.Compare(b.owner) })
			owners := make([]string, len(contributions))
			for i, c := range contributions {
				owners[i] = c.owner.String()
			}
			winner := "static object"
			if owner := s.EmissionOwner(id); owner != (manifest.NamedResource{}) {
				winner = owner.String()
			}
			out = append(out, manifest.Warning{
				Resource: id, Category: manifest.WarnConflictingProducers, Count: 1,
				Message: "conflicting producers; using " + winner,
				Detail:  owners,
			})
		}
	}
	slices.SortFunc(out, manifest.CompareWarning)
	return out
}

type warningRecord struct {
	warning manifest.Warning
	origins map[manifest.NamedResource]struct{}
}

type warningKey struct {
	resource                  manifest.NamedResource
	category, message, detail string
}
