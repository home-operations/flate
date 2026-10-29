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
	s.AddWarning(w)
	s.AddWarning(w)
	w.Count = -1
	s.AddWarning(w)
	w.Count = 0
	s.AddWarning(w)
	detail := w
	detail.Detail = []string{"c"}
	s.AddWarning(detail)
	message := w
	message.Message = "another advisory"
	s.AddWarning(message)
	foreign := w
	foreign.Resource.Group = "example.test"
	s.AddWarning(foreign)
	core := w
	core.Resource.Kind += "." + core.Resource.Group
	core.Resource.Group = ""
	assert.Equal(t, core.Resource.String(), w.Resource.String())
	s.AddWarning(core)

	got := s.Warnings()
	w.Count, detail.Count, message.Count, foreign.Count, core.Count = 1, 1, 1, 1, 1
	want := []manifest.Warning{w, detail, message, foreign, core}
	slices.SortFunc(want, manifest.CompareWarning)
	assert.Diff(t, got, warningSubjects(want))
}

func TestAddWarning_ResourceIdentity(t *testing.T) {
	t.Parallel()
	resource := hrID("app")
	core := resource
	core.Kind += "." + core.Group
	core.Group = ""
	assert.Equal(t, core.String(), resource.String())
	s := New()
	want := []manifest.Warning{
		{Category: manifest.WarnUnresolvedSubstitution, Message: "global", Count: 1},
		{Category: manifest.WarnUnresolvedSubstitution, Resource: resource, Message: "grouped", Count: 1},
		{Category: manifest.WarnUnresolvedSubstitution, Resource: core, Message: "core", Count: 1},
	}
	for _, w := range want {
		s.AddWarning(w)
		s.AddWarning(w)
	}
	slices.SortFunc(want, manifest.CompareWarning)
	assert.Diff(t, s.Warnings(), warningSubjects(want))
}

func TestAddWarning_DetailCollision(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name string
		a, b []string
	}{
		{name: "comma", a: []string{"a,b"}, b: []string{"a", "b"}},
		{name: "empty element", b: []string{""}},
		{name: "embedded NUL", a: []string{"a\x00b"}, b: []string{"a", "b"}},
		{name: "element order", a: []string{"a", "b"}, b: []string{"b", "a"}},
		{name: "invalid UTF-8", a: []string{"\xff"}, b: []string{"\xfe"}},
		{name: "nil and empty", b: []string{}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			s := New()
			w := manifest.Warning{Resource: hrID("app"), Category: manifest.WarnStaleValues, Message: "m", Detail: tc.a}
			s.AddWarning(w)
			w.Detail = tc.b
			s.AddWarning(w)
			got := s.Warnings()
			w.Count = 1
			w.Detail = tc.a
			other := w
			other.Detail = tc.b
			want := []manifest.Warning{w, other}
			if len(tc.a) == 0 && len(tc.b) == 0 {
				want = []manifest.Warning{w}
			}
			slices.SortFunc(want, manifest.CompareWarning)
			assert.Diff(t, got, warningSubjects(want))
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
	input := slices.Clone(want)
	for _, reverse := range []bool{false, true} {
		name := "forward"
		if reverse {
			name = "reverse"
		}
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			s := New()
			input := slices.Clone(input)
			if reverse {
				slices.Reverse(input)
			}
			for _, w := range input {
				s.AddWarning(w)
			}
			assert.Diff(t, s.Warnings(), warningSubjects(want))
		})
	}
}

func TestAddWarning_DefensiveCopy(t *testing.T) {
	t.Parallel()
	s := New()
	detail := []string{"x"}
	s.AddWarning(manifest.Warning{Category: manifest.WarnStaleValues, Resource: hrID("app"), Message: "m", Detail: detail})
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
			for i := range 2 {
				wg.Go(func() {
					warning := w
					warning.Message = []string{"second", "first"}[i]
					warning.Detail = []string{warning.Message, "k"}
					<-start[i]
					s.AddWarning(warning)
					close(added[i])
					<-added[1-i]
					s.AddWarning(warning)
				})
			}
			close(start[first])
			<-added[first]
			close(start[1-first])
			wg.Wait()
			w.Count = 1
			w.Message, w.Detail = "first", []string{"first", "k"}
			other := w
			other.Message, other.Detail = "second", []string{"second", "k"}
			assert.Diff(t, s.Warnings(), []manifest.Warning{w, other})
		})
	}
}

func TestWarnings_AcceptedOrigins(t *testing.T) {
	t.Parallel()
	s := New()
	first := &manifest.Kustomization{Name: "first", Namespace: "apps"}
	second := &manifest.Kustomization{Name: "second", Namespace: "apps"}
	s.AddObject(first)
	s.AddObject(second)
	resource := manifest.NamedResource{Kind: manifest.KindSecret, Namespace: "apps", Name: "credentials"}
	s.AddObject(&manifest.Secret{Name: resource.Name, Namespace: resource.Namespace, StringData: map[string]any{"VALUE": "before"}})
	var a, b ReadSet
	a.Init(s, first.Named(), nil)
	b.Init(s, second.Named(), nil)
	defer a.Close()
	defer b.Close()
	want := []manifest.Warning{
		{Resource: resource, Category: manifest.WarnSkippedSubmodule, Message: "same", Count: 2},
		{Resource: resource, Category: manifest.WarnSuppliedPrecedence, Message: "same", Count: 2},
		{Resource: resource, Category: manifest.WarnUnresolvedSubstitution, Message: "same", Count: 2},
	}
	for _, reads := range []*ReadSet{&a, &b} {
		reads.GetObject(reads.ID())
		reads.GetObject(resource)
		for _, warning := range want {
			warning.Count = 99
			reads.AddWarning(warning)
			warning.Detail = []string{}
			reads.AddWarning(warning)
		}
		reads.Accept()
	}
	assert.Diff(t, s.Warnings(), warningSubjects(want))
	assert.Diff(t, s.Warnings(), warningSubjects(want))
	var replacement ReadSet
	replacement.Init(s, first.Named(), nil)
	defer replacement.Close()
	replacement.GetObject(first.Named())
	replacement.Accept()
	for i := range want {
		want[i].Count = 1
	}
	assert.Diff(t, s.Warnings(), warningSubjects(want))
	s.AddObject(&manifest.Secret{Name: resource.Name, Namespace: resource.Namespace, StringData: map[string]any{"VALUE": "after"}})
	assert.Equal(t, b.Valid(), false)
	assert.Diff(t, s.Warnings(), []manifest.Warning(nil))
}

func TestAdvisoryEvidence_AcceptedLifetime(t *testing.T) {
	t.Parallel()
	s := New()
	first := &manifest.Kustomization{Name: "first", Namespace: "apps"}
	second := &manifest.Kustomization{Name: "second", Namespace: "apps"}
	s.AddObject(first)
	s.AddObject(second)
	resource := manifest.NamedResource{Kind: manifest.KindSecret, Namespace: "apps", Name: "credentials"}
	s.AddObject(&manifest.Secret{Name: resource.Name, Namespace: resource.Namespace, StringData: map[string]any{"VALUE": "before"}})
	var a, b, unaccepted ReadSet
	a.Init(s, first.Named(), nil)
	b.Init(s, second.Named(), nil)
	unaccepted.Init(s, first.Named(), nil)
	defer a.Close()
	defer b.Close()
	defer unaccepted.Close()
	for _, reads := range []*ReadSet{&a, &b, &unaccepted} {
		reads.GetObject(reads.ID())
		reads.GetObject(resource)
		keys := []string{reads.ID().Name, "z"}
		reads.RecordSuppliedOverride(SuppliedOverride{Resource: resource, SupplyPath: "last.yaml", Keys: keys})
		keys[0] = "mutated"
	}
	a.Accept()
	b.Accept()
	want := AdvisoryEvidence{Overrides: []SuppliedOverride{
		{Resource: resource, SupplyPath: "last.yaml", Keys: []string{"first", "z"}},
		{Resource: resource, SupplyPath: "last.yaml", Keys: []string{"second", "z"}},
	}}
	evidence := s.AdvisoryEvidence()
	slices.SortFunc(evidence.Overrides, func(a, b SuppliedOverride) int { return slices.Compare(a.Keys, b.Keys) })
	assert.Diff(t, evidence, want)
	evidence.Overrides[0].Keys[0] = "mutated"
	a.Close()
	b.Close()
	for range s.Sweep() {
	}
	evidence = s.AdvisoryEvidence()
	slices.SortFunc(evidence.Overrides, func(a, b SuppliedOverride) int { return slices.Compare(a.Keys, b.Keys) })
	assert.Diff(t, evidence, want)
	var replacement ReadSet
	replacement.Init(s, first.Named(), nil)
	defer replacement.Close()
	replacement.GetObject(first.Named())
	replacement.Accept()
	assert.Diff(t, s.AdvisoryEvidence(), AdvisoryEvidence{Overrides: want.Overrides[1:]})
	s.AddObject(&manifest.Secret{Name: resource.Name, Namespace: resource.Namespace, StringData: map[string]any{"VALUE": "after"}})
	assert.Equal(t, b.Valid(), false)
	assert.Diff(t, s.AdvisoryEvidence(), AdvisoryEvidence{})
	var carried ReadSet
	carried.Init(s, first.Named(), nil)
	carried.CarryAdvisories(nil)
	carried.CarryAdvisories(&a)
	assert.Diff(t, carried.advisories, AdvisoryEvidence{Overrides: want.Overrides[:1]})
	carried.advisories.Overrides[0].Keys[0] = "mutated"
	assert.Equal(t, a.advisories.Overrides[0].Keys[0], "first")
	carried.Retire()
	for range s.Sweep() {
	}
	carried.Init(s, first.Named(), nil)
	defer carried.Close()
	assert.Diff(t, carried.advisories, AdvisoryEvidence{})
}

func warningSubjects(warnings []manifest.Warning) []manifest.Warning {
	out := slices.Clone(warnings)
	for i := range out {
		if out[i].Resource != (manifest.NamedResource{}) {
			out[i].Subject = manifest.DiagnosticFor(out[i].Resource, nil)
		}
	}
	return out
}
func TestWarnings_RelatedIdentity(t *testing.T) {
	t.Parallel()
	s := New()
	first, second := &manifest.Kustomization{Name: "first", Namespace: "apps"}, &manifest.Kustomization{Name: "second", Namespace: "apps"}
	s.AddObject(first)
	s.AddObject(second)
	var a, b ReadSet
	a.Init(s, first.Named(), nil)
	b.Init(s, second.Named(), nil)
	defer a.Close()
	defer b.Close()
	raw := []manifest.NamedResource{first.Named(), second.Named()}
	for i, id := range raw {
		warning := manifest.Warning{Resource: first.Named(), Subject: manifest.DiagnosticFor(first.Named(), first), Related: []manifest.NamedResource{id}, Category: manifest.WarnUnrelatedFailure, Message: "same displayed owner"}
		s.AddWarning(warning)
		warning.Subject.Resource = second.Named()
		s.AddWarning(warning)
		warning.Subject = manifest.DiagnosticFor(first.Named(), first)
		for _, reads := range []*ReadSet{&a, &b} {
			reads.GetObject(reads.ID())
			reads.AddWarning(warning)
			reads.AddWarning(warning)
		}
		warning.Related[0] = manifest.NamedResource{}
		raw[i] = id
	}
	a.Accept()
	b.Accept()
	warnings := s.Warnings()
	if len(warnings) != 2 || warnings[0].Count != 3 || warnings[1].Count != 3 || warnings[0].Related[0] != first.Named() || warnings[1].Related[0] != second.Named() {
		t.Fatalf("origins collapsed: %+v", warnings)
	}
	warnings[0].Related[0] = manifest.NamedResource{}
	if again := s.Warnings(); again[0].Related[0] != first.Named() {
		t.Fatal("Related aliases retained warning")
	}
}
