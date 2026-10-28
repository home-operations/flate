package store

import (
	"fmt"
	"slices"

	"github.com/home-operations/flate/pkg/manifest"
)

// AddWarning retains each complete direct warning payload once, ignoring incoming Count.
func (s *Store) AddWarning(w manifest.Warning) {
	detail := ""
	if len(w.Detail) > 0 {
		detail = fmt.Sprintf("%q", w.Detail)
	} else {
		w.Detail = nil
	}
	key := warningKey{w.Resource, w.Category, w.Message, detail}
	w.Detail = slices.Clone(w.Detail)

	s.warnMu.Lock()
	defer s.warnMu.Unlock()
	if _, ok := s.warnIndex[key]; ok {
		return
	}
	if s.warnIndex == nil {
		s.warnIndex = make(map[warningKey]struct{})
	}
	w.Count = 1
	s.warnIndex[key] = struct{}{}
	s.warnOrder = append(s.warnOrder, w)
}

// Warnings returns a copy of the collected advisories ordered by
// manifest.CompareWarning, or nil when none were recorded.
func (s *Store) Warnings() []manifest.Warning {
	s.warnMu.Lock()
	out := slices.Clone(s.warnOrder)
	s.warnMu.Unlock()
	for _, reads := range s.assessments() {
		start := len(out)
		for _, warning := range reads.warnings {
			if !slices.ContainsFunc(out[start:], func(w manifest.Warning) bool { return manifest.CompareWarning(w, warning) == 0 }) {
				out = append(out, warning)
			}
		}
	}
	type contribution struct {
		owner manifest.NamedResource
		child Child
	}
	for _, sh := range s.shards {
		byID := map[manifest.NamedResource][]contribution{}
		sh.mu.RLock()
		for id, members := range sh.members {
			if sh.derived && sh.cells[id].hidden {
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
			payload := contributions[0].child.payload
			if len(contributions) < 2 || !slices.ContainsFunc(contributions[1:], func(c contribution) bool {
				if payload == nil {
					payload = c.child.payload
				}
				return !semanticEqual(contributions[0].child, c.child) || payload != nil && c.child.payload != nil && !artifactValueEqual(payload, c.child.payload)
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
	n := 0
	for i := 0; i < len(out); {
		warning := out[i]
		warning.Count = 1
		warning.Detail = slices.Clone(warning.Detail)
		if len(warning.Detail) == 0 {
			warning.Detail = nil
		}
		j := i + 1
		for j < len(out) && manifest.CompareWarning(out[i], out[j]) == 0 {
			warning.Count++
			j++
		}
		out[n] = warning
		n++
		i = j
	}
	return out[:n]
}

func (s *Store) assessments() []*ReadSet {
	var bundles []*ReadSet
	for _, sh := range s.shards {
		sh.mu.RLock()
		for _, cell := range sh.cells {
			if cell.links != nil {
				if reads := cell.links.assessment.Load(); reads != nil {
					bundles = append(bundles, reads)
				}
			}
		}
		sh.mu.RUnlock()
	}
	return slices.DeleteFunc(bundles, func(reads *ReadSet) bool { return !reads.Valid() })
}

type warningKey struct {
	resource                  manifest.NamedResource
	category, message, detail string
}
