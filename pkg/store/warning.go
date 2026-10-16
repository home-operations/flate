package store

import (
	"slices"
	"strings"

	"github.com/home-operations/flate/pkg/manifest"
)

// AddWarning records a render advisory for Result.Warnings. Identical warnings
// (same resource, category, message, and detail) collapse into one entry whose
// Count is incremented — so a producer that fires the same advisory N times
// (e.g. across retries) surfaces once with "(×N)". Producers leave Count unset
// (the zero value normalizes to 1); it exists for the collector's tally.
// Concurrency-safe via a dedicated mutex, off the shard locks' hot path.
func (s *Store) AddWarning(w manifest.Warning) {
	if w.Count <= 0 {
		w.Count = 1
	}
	w.Detail = slices.Clone(w.Detail) // defensive: caller can't mutate stored state
	key := warningKey{w.Resource, w.Category, w.Message, strings.Join(w.Detail, "\x00")}

	s.warnMu.Lock()
	defer s.warnMu.Unlock()
	if i, ok := s.warnIndex[key]; ok {
		s.warnOrder[i].Count += w.Count
		return
	}
	if s.warnIndex == nil {
		s.warnIndex = make(map[warningKey]int)
	}
	s.warnIndex[key] = len(s.warnOrder)
	s.warnOrder = append(s.warnOrder, w)
}

// Warnings returns a deterministically-sorted copy of the collected advisories
// (by category, then resource, then message — see manifest.CompareWarning), or
// nil when none were recorded. The orchestrator copies this into
// Result.Warnings at finalize.
func (s *Store) Warnings() []manifest.Warning {
	s.warnMu.Lock()
	out := slices.Clone(s.warnOrder)
	for i, w := range s.warnOrder {
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
			out = append(out, manifest.Warning{
				Resource: id, Category: manifest.WarnConflictingProducers, Count: 1,
				Message: "conflicting producers; using " + owners[len(owners)-1],
				Detail:  owners,
			})
		}
	}
	slices.SortFunc(out, manifest.CompareWarning)
	return out
}

type warningKey struct {
	resource                  manifest.NamedResource
	category, message, detail string
}
