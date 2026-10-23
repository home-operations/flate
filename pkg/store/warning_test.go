package store

import (
	"slices"
	"sync"
	"testing"
	"testing/synctest"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
)

type warningEvent struct {
	Origin  manifest.NamedResource
	Warning manifest.Warning
}

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
	var events []warningEvent
	s.AddListener(EventWarningAdded, func(id manifest.NamedResource, payload any) {
		events = append(events, warningEvent{Origin: id, Warning: payload.(manifest.Warning)})
	}, false)
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

	var wantEvents []warningEvent
	for _, contribution := range []struct {
		origin  manifest.NamedResource
		warning manifest.Warning
	}{{origin, w}, {hrID("other"), w}, {origin, detail}, {origin, message}, {origin, foreign}, {origin, core}} {
		contribution.warning.Count = 1
		wantEvents = append(wantEvents, warningEvent{Origin: contribution.origin, Warning: contribution.warning})
	}
	assert.Diff(t, events, wantEvents)

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
	var live, replay manifest.Warning
	s.AddListener(EventWarningAdded, func(_ manifest.NamedResource, payload any) {
		if live.Count == 0 {
			live = payload.(manifest.Warning)
		}
	}, false)
	detail := []string{"x"}
	s.AddWarning(hrID("app"), manifest.Warning{Category: manifest.WarnStaleValues, Resource: hrID("app"), Message: "m", Detail: detail})
	s.AddListener(EventWarningAdded, func(_ manifest.NamedResource, payload any) {
		replay = payload.(manifest.Warning)
	}, true)()
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
	copied := slices.Clone(live.Detail)
	copied[0] = "listener-owned"
	s.AddWarning(hrID("other"), manifest.Warning{Category: manifest.WarnStaleValues, Resource: hrID("app"), Message: "m", Detail: []string{"x"}})
	want := manifest.Warning{Category: manifest.WarnStaleValues, Resource: hrID("app"), Message: "m", Detail: []string{"x"}, Count: 1}
	assert.Diff(t, live, want)
	assert.Diff(t, replay, want)
	assert.Equal(t, s.Warnings()[0].Count, 2)

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

func TestAddWarning_ListenerReplay(t *testing.T) {
	t.Parallel()
	for _, flush := range []bool{false, true} {
		name := "live only"
		if flush {
			name = "replay and live"
		}
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			s := New()
			w := manifest.Warning{Resource: hrID("display"), Category: manifest.WarnStaleValues, Message: "m", Count: 99}
			s.AddWarning(manifest.NamedResource{}, w)
			s.AddWarning(hrID("app"), w)
			seen := map[manifest.NamedResource][]manifest.Warning{}
			unsub := s.AddListener(EventWarningAdded, func(origin manifest.NamedResource, payload any) {
				seen[origin] = append(seen[origin], payload.(manifest.Warning))
			}, flush)
			w.Count = 1
			want := map[manifest.NamedResource][]manifest.Warning{}
			if flush {
				want[manifest.NamedResource{}] = []manifest.Warning{w}
				want[hrID("app")] = []manifest.Warning{w}
			}
			assert.Diff(t, seen, want)
			s.AddWarning(hrID("other"), w)
			s.AddWarning(hrID("other"), w)
			want[hrID("other")] = []manifest.Warning{w}
			assert.Diff(t, seen, want)
			unsub()
			unsub()
			s.AddWarning(hrID("after unsubscribe"), w)
			assert.Diff(t, seen, want)
			assert.Equal(t, s.Warnings()[0].Count, 4)
		})
	}
}

func TestAddWarning_ListenerOverlap(t *testing.T) {
	t.Parallel()
	for _, flush := range []bool{false, true} {
		name := "without replay"
		if flush {
			name = "with replay"
		}
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			s := New()
			origin := hrID("app")
			w := manifest.Warning{Category: manifest.WarnStaleValues, Message: "m", Count: 1}
			paused, resume, done := make(chan struct{}), make(chan struct{}), make(chan struct{})
			release := sync.OnceFunc(func() { close(resume) })
			unsub := s.AddListener(EventWarningAdded, func(_ manifest.NamedResource, _ any) {
				close(paused)
				<-resume
			}, false)
			go func() { defer close(done); s.AddWarning(origin, w) }()
			<-paused
			t.Cleanup(func() { release(); <-done })
			var seen []warningEvent
			s.AddListener(EventWarningAdded, func(id manifest.NamedResource, payload any) {
				seen = append(seen, warningEvent{Origin: id, Warning: payload.(manifest.Warning)})
			}, flush)
			unsub()
			release()
			<-done
			var want []warningEvent
			if flush {
				want = append(want, warningEvent{Origin: origin, Warning: w})
			}
			assert.Diff(t, seen, want)
			s.AddWarning(hrID("other"), w)
			want = append(want, warningEvent{Origin: hrID("other"), Warning: w})
			assert.Diff(t, seen, want)
		})
	}
}

func TestAddWarning_ListenerUnlocked(t *testing.T) {
	t.Parallel()
	for _, flush := range []bool{false, true} {
		name := "live"
		if flush {
			name = "replay"
		}
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			s := New()
			w := manifest.Warning{Category: manifest.WarnStaleValues, Message: "m"}
			if flush {
				s.AddWarning(hrID("app"), w)
			}
			paused, resume, done := make(chan struct{}), make(chan struct{}), make(chan struct{})
			release := sync.OnceFunc(func() { close(resume) })
			go func() {
				defer close(done)
				s.AddListener(EventWarningAdded, func(_ manifest.NamedResource, _ any) {
					assert.Equal(t, len(s.Warnings()), 1)
					close(paused)
					<-resume
				}, flush)
				if !flush {
					s.AddWarning(hrID("app"), w)
				}
			}()
			<-paused
			t.Cleanup(func() { release(); <-done })
			s.AddObject(newCM("independent", "ns"))
			assert.Equal(t, len(s.ListObjects("", "")), 1)
			release()
			<-done
		})
	}
}

func TestStore_ListenerStateLock(t *testing.T) {
	t.Parallel()
	for _, flush := range []bool{false, true} {
		name := "without replay"
		if flush {
			name = "with replay"
		}
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(_ *testing.T) {
				s := New()
				s.lockAll()
				s.AddListener(EventWarningAdded, func(_ manifest.NamedResource, _ any) {}, flush)()
				s.unlockAll()
				s.warnMu.Lock()
				for _, event := range []EventKind{EventObjectAdded, EventStatusUpdated, EventArtifactUpdated} {
					s.AddListener(event, func(_ manifest.NamedResource, _ any) {}, flush)()
				}
				s.warnMu.Unlock()
			})
		})
	}
}

func TestWarnings_DerivedConflictDoesNotDispatch(t *testing.T) {
	t.Parallel()
	s := New()
	var events []warningEvent
	s.AddListener(EventWarningAdded, func(id manifest.NamedResource, payload any) {
		events = append(events, warningEvent{Origin: id, Warning: payload.(manifest.Warning)})
	}, false)
	for _, name := range []string{"a", "b"} {
		owner := &manifest.Kustomization{Name: name, Namespace: "apps"}
		s.AddObject(owner)
		child := emissionChild("shared", name)
		s.RecordEmitted(owner.Named(), &Emission{Children: []Child{child}, Manifests: []map[string]any{child.Doc}, Complete: true})
	}
	warnings := s.Warnings()
	if len(warnings) != 1 || warnings[0].Category != manifest.WarnConflictingProducers {
		t.Fatalf("derived conflict warning = %+v", warnings)
	}
	s.AddListener(EventWarningAdded, func(id manifest.NamedResource, payload any) {
		events = append(events, warningEvent{Origin: id, Warning: payload.(manifest.Warning)})
	}, true)()
	assert.Equal(t, len(events), 0)
}
