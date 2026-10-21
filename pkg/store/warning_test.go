package store

import (
	"slices"
	"sync"
	"testing"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
)

func hrID(name string) manifest.NamedResource {
	return testutil.HelmReleaseID("default", name)
}

func TestWarnings_Empty(t *testing.T) {
	t.Parallel()
	if got := New().Warnings(); got != nil {
		t.Fatalf("Warnings() on empty store = %v, want nil", got)
	}
}

func TestAddWarning_DedupAndCount(t *testing.T) {
	t.Parallel()
	s := New()
	origin := hrID("app")
	w := manifest.Warning{Resource: origin, Category: manifest.WarnStaleValues, Count: 42,
		Message: "values not defined by the chart schema", Detail: []string{"a", "b"}}
	s.AddWarning(origin, w)
	s.AddWarning(origin, w)
	w.Count = -1
	s.AddWarning(hrID("other"), w)
	w.Count = 0
	s.AddWarning(origin, w)
	detail := w
	detail.Detail = []string{"c"}
	s.AddWarning(origin, detail)
	message := w
	message.Message = "another advisory"
	s.AddWarning(origin, message)
	foreign := w
	foreign.Resource.Group = "example.test"
	s.AddWarning(origin, foreign)
	core := w
	core.Resource.Kind += "." + core.Resource.Group
	core.Resource.Group = ""
	assert.Equal(t, core.Resource.String(), w.Resource.String())
	s.AddWarning(origin, core)

	got := s.Warnings()
	if len(got) != 5 {
		t.Fatalf("want 5 distinct warnings, got %d: %+v", len(got), got)
	}
	for _, warning := range got {
		want := 1
		if warning.Resource == w.Resource && warning.Message == w.Message && slices.Equal(warning.Detail, w.Detail) {
			want = 2
		}
		assert.Equal(t, warning.Count, want)
	}
}

func TestAddWarning_OriginIdentity(t *testing.T) {
	t.Parallel()
	origin := hrID("app")
	core := origin
	core.Kind += "." + core.Group
	core.Group = ""
	assert.Equal(t, core.String(), origin.String())
	for _, tc := range []struct {
		name     string
		resource manifest.NamedResource
		origins  []manifest.NamedResource
		want     int
	}{
		{name: "structured origins in global group", origins: []manifest.NamedResource{origin, core, origin, core}, want: 2},
		{name: "global advisory replay", origins: []manifest.NamedResource{{}, {}}, want: 1},
		{name: "zero origin independent of display", resource: origin, origins: []manifest.NamedResource{{}, {}, origin, origin}, want: 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			s := New()
			w := manifest.Warning{Category: manifest.WarnUnresolvedSubstitution, Resource: tc.resource, Message: "m"}
			for _, id := range tc.origins {
				s.AddWarning(id, w)
			}
			w.Count = tc.want
			assert.Diff(t, s.Warnings(), []manifest.Warning{w})
		})
	}
}

func TestAddWarning_DetailCollision(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name string
		a, b []string
		want int
	}{
		{name: "comma", a: []string{"a,b"}, b: []string{"a", "b"}, want: 2},
		{name: "empty element", b: []string{""}, want: 2},
		{name: "embedded NUL", a: []string{"a\x00b"}, b: []string{"a", "b"}, want: 2},
		{name: "element order", a: []string{"a", "b"}, b: []string{"b", "a"}, want: 2},
		{name: "invalid UTF-8", a: []string{"\xff"}, b: []string{"\xfe"}, want: 2},
		{name: "nil and empty", b: []string{}, want: 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			s := New()
			w := manifest.Warning{Resource: hrID("app"), Category: manifest.WarnStaleValues, Message: "m", Detail: tc.a}
			s.AddWarning(w.Resource, w)
			w.Detail = tc.b
			s.AddWarning(w.Resource, w)
			got := s.Warnings()
			assert.Equal(t, len(got), tc.want)
			for _, warning := range got {
				assert.Equal(t, warning.Count, 1)
			}
		})
	}
}

func TestWarnings_Sorted(t *testing.T) {
	t.Parallel()
	want := []manifest.Warning{
		{Category: manifest.WarnEmptyScan, Message: "scan", Count: 1},
		{Category: manifest.WarnStaleValues, Resource: hrID("a"), Message: "m", Count: 1},
		{Category: manifest.WarnStaleValues, Resource: hrID("a"), Message: "m", Detail: []string{""}, Count: 1},
		{Category: manifest.WarnStaleValues, Resource: hrID("a"), Message: "m", Detail: []string{"a"}, Count: 1},
		{Category: manifest.WarnStaleValues, Resource: hrID("a"), Message: "m", Detail: []string{"a", "b"}, Count: 1},
		{Category: manifest.WarnStaleValues, Resource: hrID("a"), Message: "z", Count: 1},
		{Category: manifest.WarnStaleValues, Resource: hrID("z"), Message: "m", Count: 1},
	}
	for _, reverse := range []bool{false, true} {
		name := "forward"
		if reverse {
			name = "reverse"
		}
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			s := New()
			input := slices.Clone(want)
			if reverse {
				slices.Reverse(input)
			}
			for _, w := range input {
				s.AddWarning(w.Resource, w)
			}
			assert.Diff(t, s.Warnings(), want)
		})
	}
}

func TestAddWarning_DefensiveCopy(t *testing.T) {
	t.Parallel()
	s := New()
	detail := []string{"x"}
	s.AddWarning(hrID("app"), manifest.Warning{Category: manifest.WarnStaleValues, Resource: hrID("app"), Message: "m", Detail: detail})
	detail[0] = "MUTATED"

	got := s.Warnings()
	if len(got) != 1 || got[0].Detail[0] != "x" {
		t.Fatalf("stored detail aliased caller slice: %+v", got)
	}
	got[0].Detail[0] = "ALSO_MUTATED"
	got[0].Count = 100
	if again := s.Warnings(); again[0].Detail[0] != "x" || again[0].Count != 1 {
		t.Errorf("Warnings() returned aliased internal state: %+v", again)
	}
}

func TestAddWarning_Concurrent(t *testing.T) {
	t.Parallel()
	for _, first := range []int{0, 1} {
		name := "app first"
		if first == 1 {
			name = "other first"
		}
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			s := New()
			start := [2]chan struct{}{make(chan struct{}), make(chan struct{})}
			added := [2]chan struct{}{make(chan struct{}), make(chan struct{})}
			w := manifest.Warning{Category: manifest.WarnUnresolvedSubstitution, Message: "m", Detail: []string{"k"}}
			var wg sync.WaitGroup
			for i, id := range []manifest.NamedResource{hrID("app"), hrID("other")} {
				wg.Go(func() {
					<-start[i]
					s.AddWarning(id, w)
					close(added[i])
					<-added[1-i]
					s.AddWarning(id, w)
				})
			}
			close(start[first])
			<-added[first]
			close(start[1-first])
			wg.Wait()
			w.Count = 2
			assert.Diff(t, s.Warnings(), []manifest.Warning{w})
		})
	}
}
