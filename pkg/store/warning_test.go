package store

import (
	"slices"
	"sync"
	"testing"

	"github.com/home-operations/flate/pkg/manifest"
)

func hrID(name string) manifest.NamedResource {
	return manifest.NamedResource{Kind: "HelmRelease", Namespace: "default", Name: name}
}

// TestWarnings_Empty: a fresh store reports nil (not empty slice) — matching
// the nil-when-empty convention Result.Warnings consumers rely on.
func TestWarnings_Empty(t *testing.T) {
	if got := New().Warnings(); got != nil {
		t.Fatalf("Warnings() on empty store = %v, want nil", got)
	}
}

// TestAddWarning_DedupAndCount: identical advisories collapse and accumulate a
// Count; a differing field (detail here) is a distinct entry.
func TestAddWarning_DedupAndCount(t *testing.T) {
	s := New()
	w := manifest.Warning{Resource: hrID("app"), Category: manifest.WarnStaleValues,
		Message: "values not defined by the chart schema", Detail: []string{"a", "b"}}
	s.AddWarning(w)
	s.AddWarning(w) // identical → Count bump
	s.AddWarning(manifest.Warning{Resource: hrID("app"), Category: manifest.WarnStaleValues,
		Message: "values not defined by the chart schema", Detail: []string{"c"}}) // distinct detail

	got := s.Warnings()
	if len(got) != 2 {
		t.Fatalf("want 2 distinct warnings, got %d: %+v", len(got), got)
	}
	var first manifest.Warning
	for _, w := range got {
		if slices.Equal(w.Detail, []string{"a", "b"}) {
			first = w
		}
	}
	if first.Count != 2 {
		t.Errorf("identical warning Count = %d, want 2", first.Count)
	}
}

// TestAddWarning_DetailCollision: two warnings whose Detail elements would
// collide under a naive comma-join ([a,b] vs [a","b]) stay distinct entries.
func TestAddWarning_DetailCollision(t *testing.T) {
	s := New()
	base := manifest.Warning{Resource: hrID("app"), Category: manifest.WarnStaleValues, Message: "m"}
	w1 := base
	w1.Detail = []string{"a,b"}
	w2 := base
	w2.Detail = []string{"a", "b"}
	s.AddWarning(w1)
	s.AddWarning(w2)
	if got := s.Warnings(); len(got) != 2 {
		t.Fatalf("distinct Detail must not collide; want 2 warnings, got %d: %+v", len(got), got)
	}
}

// TestWarnings_Sorted: output is ordered by (category, resource, message),
// independent of insertion order — deterministic for the footer + Result.
func TestWarnings_Sorted(t *testing.T) {
	s := New()
	s.AddWarning(manifest.Warning{Category: manifest.WarnStaleValues, Resource: hrID("z"), Message: "m"})
	s.AddWarning(manifest.Warning{Category: manifest.WarnEmptyScan, Message: "scan"})
	s.AddWarning(manifest.Warning{Category: manifest.WarnStaleValues, Resource: hrID("a"), Message: "m"})

	got := s.Warnings()
	want := []string{manifest.WarnEmptyScan, manifest.WarnStaleValues, manifest.WarnStaleValues}
	for i, w := range got {
		if w.Category != want[i] {
			t.Fatalf("order[%d] category = %q, want %q (%+v)", i, w.Category, want[i], got)
		}
	}
	// Within WarnStaleValues, resource "a" precedes "z".
	if got[1].Resource.Name != "a" || got[2].Resource.Name != "z" {
		t.Errorf("resource tiebreak wrong: %q then %q", got[1].Resource.Name, got[2].Resource.Name)
	}
}

// TestAddWarning_DefensiveCopy: mutating the caller's Detail slice after Add
// does not change stored state, and the returned slice is a copy.
func TestAddWarning_DefensiveCopy(t *testing.T) {
	s := New()
	detail := []string{"x"}
	s.AddWarning(manifest.Warning{Category: manifest.WarnStaleValues, Resource: hrID("app"), Message: "m", Detail: detail})
	detail[0] = "MUTATED"

	got := s.Warnings()
	if len(got) != 1 || got[0].Detail[0] != "x" {
		t.Fatalf("stored detail aliased caller slice: %+v", got)
	}
	got[0].Detail[0] = "ALSO_MUTATED" // mutating the returned copy must not affect the store
	if again := s.Warnings(); again[0].Detail[0] != "x" {
		t.Errorf("Warnings() returned aliased internal slice: %q", again[0].Detail[0])
	}
}

// TestAddWarning_Concurrent: concurrent producers don't race (run with -race)
// and every emission is accounted for via Count.
func TestAddWarning_Concurrent(t *testing.T) {
	s := New()
	const goroutines, each = 8, 50
	var wg sync.WaitGroup
	for range goroutines {
		wg.Go(func() {
			for range each {
				s.AddWarning(manifest.Warning{Category: manifest.WarnStaleValues,
					Resource: hrID("shared"), Message: "m", Detail: []string{"k"}})
			}
		})
	}
	wg.Wait()
	got := s.Warnings()
	if len(got) != 1 {
		t.Fatalf("want 1 deduped warning, got %d", len(got))
	}
	if got[0].Count != goroutines*each {
		t.Errorf("Count = %d, want %d", got[0].Count, goroutines*each)
	}
}
