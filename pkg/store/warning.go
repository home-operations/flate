package store

import (
	"encoding/json"
	"fmt"
	"slices"

	"github.com/home-operations/flate/pkg/manifest"
)

// AddWarning retains each complete direct warning payload once, ignoring incoming Count.
//
//nolint:gocritic // The SDK preserves this value input and its snapshot boundary.
func (s *Store) AddWarning(w manifest.Warning) {
	detail := ""
	if len(w.Detail) > 0 {
		detail = fmt.Sprintf("%q", w.Detail)
	} else {
		w.Detail = nil
	}
	related := ""
	if len(w.Related) > 0 {
		//nolint:errchkjson // Related contains only string resource identities with no custom JSON methods.
		encoded, _ := json.Marshal(w.Related)
		related = string(encoded)
	} else {
		w.Related = nil
	}
	key := warningKey{w.Resource, w.Category, w.Message, detail, related}

	s.warnMu.Lock()
	defer s.warnMu.Unlock()
	if _, ok := s.warnIndex[key]; ok {
		return
	}
	if s.warnIndex == nil {
		s.warnIndex = make(map[warningKey]struct{})
	}
	w.Detail = slices.Clone(w.Detail)
	w.Related = slices.Clone(w.Related)
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
	out = s.appendAssessmentWarnings(out)
	for _, sh := range s.shards {
		byID := sh.warningContributions()
		for id, contributions := range byID {
			payload := contributions[0].child.payload
			if len(contributions) < 2 || !slices.ContainsFunc(contributions[1:], func(c warningContribution) bool {
				if payload == nil {
					payload = c.child.payload
				}
				return !semanticEqual(contributions[0].child, c.child) ||
					payload != nil && c.child.payload != nil && !artifactValueEqual(payload, c.child.payload)
			}) {
				continue
			}
			slices.SortFunc(contributions, func(a, b warningContribution) int { return a.owner.Compare(b.owner) })
			owners := make([]string, len(contributions))
			related := make([]manifest.NamedResource, 0, len(contributions)+1)
			{
				cValues := contributions
				for i := range cValues {
					c := cValues[i]
					owners[i] = manifest.DiagnosticFor(c.owner, s.GetObject(c.owner)).String()
					related = append(related, c.owner)
				}
			}
			winner := "static object"
			if owner := s.EmissionOwner(id); owner != (manifest.NamedResource{}) {
				winner = manifest.DiagnosticFor(owner, s.GetObject(owner)).String()
				related = append(related, owner)
			}
			out = append(out, manifest.Warning{
				Resource: id, Category: manifest.WarnConflictingProducers, Count: 1,
				Message: "conflicting producers; using " + winner,
				Detail:  owners, Related: related,
			})
		}
	}
	slices.SortFunc(out, manifest.CompareWarning)
	return s.compactWarnings(out)
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
	resource                           manifest.NamedResource
	category, message, detail, related string
}

type warningContribution struct {
	owner manifest.NamedResource
	child Child
}

func (s *Store) compactWarnings(out []manifest.Warning) []manifest.Warning {
	n := 0
	for i := 0; i < len(out); {
		warning := out[i]
		warning.Count = 1
		warning.Detail = slices.Clone(warning.Detail)
		warning.Related = slices.Clone(warning.Related)
		if len(warning.Related) == 0 {
			warning.Related = nil
		}
		if warning.Subject.ID == (manifest.NamedResource{}) && warning.Resource != (manifest.NamedResource{}) {
			warning.Subject = manifest.DiagnosticFor(warning.Resource, s.GetObject(warning.Resource))
		}
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

func (s *Store) appendAssessmentWarnings(out []manifest.Warning) []manifest.Warning {
	for _, reads := range s.assessments() {
		start := len(out)
		{
			warningValues := reads.warnings
			for warningIndex := range warningValues {
				warning := warningValues[warningIndex]
				if !slices.ContainsFunc(
					out[start:],
					func(w manifest.Warning) bool { return manifest.CompareWarning(w, warning) == 0 },
				) {
					out = append(out, warning)
				}
			}
		}
	}
	return out
}

func (sh *shard) warningContributions() map[manifest.NamedResource][]warningContribution {
	byID := map[manifest.NamedResource][]warningContribution{}
	sh.mu.RLock()
	for id, members := range sh.members {
		if sh.derived && sh.cells[id].hidden {
			continue
		}
		for _, m := range members {
			if !m.declaration && m.slot.visible.Load() {
				child := m.slot.current.Load().entries[m.position].child
				byID[id] = append(byID[id], warningContribution{m.slot.id, child})
			}
		}
	}
	sh.mu.RUnlock()
	return byID
}
