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
	key := warningKey(w)

	s.warnMu.Lock()
	defer s.warnMu.Unlock()
	if i, ok := s.warnIndex[key]; ok {
		s.warnOrder[i].Count += w.Count
		return
	}
	if s.warnIndex == nil {
		s.warnIndex = make(map[string]int)
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
	defer s.warnMu.Unlock()
	if len(s.warnOrder) == 0 {
		return nil
	}
	out := make([]manifest.Warning, len(s.warnOrder))
	for i, w := range s.warnOrder {
		w.Detail = slices.Clone(w.Detail)
		out[i] = w
	}
	slices.SortFunc(out, manifest.CompareWarning)
	return out
}

// warningKey is the identity used to collapse repeat warnings. Fields (and
// Detail elements) are NUL-joined — a byte that can't appear in a resource id,
// category, message, or dotted value path — so distinct warnings never collide
// (e.g. Detail ["a,b"] and ["a","b"] stay separate).
func warningKey(w manifest.Warning) string {
	return w.Resource.String() + "\x00" + w.Category + "\x00" + w.Message + "\x00" + strings.Join(w.Detail, "\x00")
}
