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
	{
		got := New().Warnings()
		assert.FatalIf(t, got != nil, "Warnings() on empty store = %v, want nil", got)
	}
}

func TestAddWarning_DedupAndCount(t *testing.T) {
	t.Parallel()
	s := New()
	origin := hrID("app")
	w := manifest.Warning{
		Resource: origin, Category: manifest.WarnStaleValues, Count: 42,
		Message: "values not defined by the chart schema", Detail: []string{"a", "b"},
	}
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
	cloneInput := slices.Clone(want)
	for _, reverse := range []bool{false, true} {
		name := "forward"
		if reverse {
			name = "reverse"
		}
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			s := New()
			input := slices.Clone(cloneInput)
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
	s.AddWarning(
		manifest.Warning{Category: manifest.WarnStaleValues, Resource: hrID("app"), Message: "m", Detail: detail},
	)
	detail[0] = "MUTATED"

	got := s.Warnings()
	assert.FatalIf(t, len(got) != 1 || got[0].Detail[0] != "x", "stored detail aliased caller slice: %+v", got)
	got[0].Detail[0] = "ALSO_MUTATED"
	got[0].Count = 100
	{
		again := s.Warnings()
		assert.ErrorIf(
			t,
			again[0].Detail[0] != "x" || again[0].Count != 1,
			"Warnings() returned aliased internal state: %+v",
			again,
		)
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
	s.AddObject(
		&manifest.Secret{
			Name:       resource.Name,
			Namespace:  resource.Namespace,
			StringData: map[string]any{"VALUE": "before"},
		},
	)
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
		reads.Accept(AssessmentReconciled)
	}
	assert.Diff(t, s.Warnings(), warningSubjects(want))
	assert.Diff(t, s.Warnings(), warningSubjects(want))
	var replacement ReadSet
	replacement.Init(s, first.Named(), nil)
	defer replacement.Close()
	replacement.GetObject(first.Named())
	replacement.Accept(AssessmentReconciled)
	for i := range want {
		want[i].Count = 1
	}
	assert.Diff(t, s.Warnings(), warningSubjects(want))
	s.AddObject(
		&manifest.Secret{
			Name:       resource.Name,
			Namespace:  resource.Namespace,
			StringData: map[string]any{"VALUE": "after"},
		},
	)
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
	s.AddObject(
		&manifest.Secret{
			Name:       resource.Name,
			Namespace:  resource.Namespace,
			StringData: map[string]any{"VALUE": "before"},
		},
	)
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
	a.Accept(AssessmentReconciled)
	b.Accept(AssessmentReconciled)
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
	replacement.Accept(AssessmentReconciled)
	assert.Diff(t, s.AdvisoryEvidence(), AdvisoryEvidence{Overrides: want.Overrides[1:]})
	s.AddObject(
		&manifest.Secret{
			Name:       resource.Name,
			Namespace:  resource.Namespace,
			StringData: map[string]any{"VALUE": "after"},
		},
	)
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
	first, second := &manifest.Kustomization{
		Name:      "first",
		Namespace: "apps",
	}, &manifest.Kustomization{
		Name:      "second",
		Namespace: "apps",
	}
	s.AddObject(first)
	s.AddObject(second)
	var a, b ReadSet
	a.Init(s, first.Named(), nil)
	b.Init(s, second.Named(), nil)
	defer a.Close()
	defer b.Close()
	raw := []manifest.NamedResource{first.Named(), second.Named()}
	for i, id := range raw {
		warning := manifest.Warning{
			Resource: first.Named(),
			Subject:  manifest.DiagnosticFor(first.Named(), first),
			Related:  []manifest.NamedResource{id},
			Category: manifest.WarnUnrelatedFailure,
			Message:  "same displayed owner",
		}
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
	a.Accept(AssessmentReconciled)
	b.Accept(AssessmentReconciled)
	warnings := s.Warnings()
	assert.FatalIf(t, len(warnings) != 2 || warnings[0].Count != 3 || warnings[1].Count != 3 ||
		warnings[0].Related[0] != first.Named() ||
		warnings[1].Related[0] != second.Named(), "origins collapsed: %+v", warnings)
	warnings[0].Related[0] = manifest.NamedResource{}
	{
		again := s.Warnings()
		assert.FatalIf(t, again[0].Related[0] != first.Named(), "%s", "Related aliases retained warning")
	}
}

func TestWarnings_RetiredCredentialOrigin(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name    string
		present bool
	}{
		{name: "initially absent"},
		{name: "initially present", present: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			s := New()
			source := &manifest.GitRepository{Name: "source", Namespace: "apps", URL: "https://example.test/repo"}
			credential := &manifest.Secret{
				Name:       "credentials",
				Namespace:  "apps",
				StringData: map[string]any{"token": "before"},
			}
			s.AddObject(source)
			if tc.present {
				s.AddObject(credential)
			}
			var origin ReadSet
			origin.Init(s, source.Named(), make([]Input, 1))
			origin.GetObject(source.Named())
			origin.GetObject(credential.Named())
			assert.Equal(t, origin.input(credential.Named()).cell(conditionFacet), (*registration)(nil))
			assert.Equal(t, origin.input(source.Named()).cell(conditionFacet).head, (*registrationHead)(nil))
			warning := manifest.Warning{
				Resource: source.Named(), Subject: manifest.DiagnosticFor(source.Named(), source),
				Category: manifest.WarnSourceCredentialsUnavailable, Count: 1,
				Message: "source credentials unavailable", Detail: []string{"credentials", "token"},
				Related: []manifest.NamedResource{credential.Named()},
			}
			advisory := AdvisoryEvidence{Overrides: []SuppliedOverride{
				{
					Resource:   credential.Named(),
					SupplyPath: "supplied/credentials.yaml",
					Keys:       []string{"token", "username"},
				},
			}}
			origin.AddWarning(warning)
			origin.RecordSuppliedOverride(advisory.Overrides[0])
			origin.Accept(AssessmentReconciled)
			assert.Diff(t, s.Warnings(), []manifest.Warning{warning})
			assert.Diff(t, s.AdvisoryEvidence(), advisory)

			cleaned, unrelated, published := make(chan struct{}), make(chan struct{}), make(chan struct{})
			publishUnrelated, publishCredential := make(chan struct{}), make(chan struct{})
			var workers sync.WaitGroup
			workers.Go(func() {
				origin.Close()
				for range s.Sweep() {
				}
				close(cleaned)
				<-publishUnrelated
				s.AddObject(
					&manifest.Secret{
						Name:       "unrelated",
						Namespace:  "apps",
						StringData: map[string]any{"token": "other"},
					},
				)
				close(unrelated)
				<-publishCredential
				changed := *credential
				changed.StringData = map[string]any{"token": "after"}
				s.AddObject(&changed)
				close(published)
			})
			workers.Go(func() {
				<-cleaned
				assert.Equal(t, origin.retired.Load(), true)
				assert.Equal(t, origin.input(source.Named()).object.head.reconciliation.Load(), &origin)
				assertRetiredDiagnostics(t, s, []manifest.Warning{warning}, advisory)
				var unchanged ReadSet
				unchanged.Init(s, source.Named(), nil)
				assert.Equal(t, unchanged.Matches(&origin), true)
				unchanged.Retire()
				for range s.Sweep() {
				}
				assertNoWarningSubscriptions(t, s)
				close(publishUnrelated)
				<-unrelated
				assertRetiredDiagnostics(t, s, []manifest.Warning{warning}, advisory)
				close(publishCredential)
				<-published
				assertRetiredDiagnostics(t, s, nil, AdvisoryEvidence{})
				var changed ReadSet
				changed.Init(s, source.Named(), nil)
				assert.Equal(t, changed.Matches(&origin), false)
				changed.Retire()
				for range s.Sweep() {
				}
				origin.ReleaseCapture()
				var successor ReadSet
				successor.Init(s, source.Named(), nil)
				successor.GetObject(credential.Named())
				warning.Message = "successor credential diagnostic"
				successor.AddWarning(warning)
				successor.Accept(AssessmentReconciled)
				successor.Close()
				for range s.Sweep() {
				}
				assertRetiredDiagnostics(t, s, []manifest.Warning{warning}, AdvisoryEvidence{})
				successor.ReleaseCapture()
			})
			workers.Wait()
		})
	}
}

func TestWarnings_RetiredAuthorityFacets(t *testing.T) {
	t.Parallel()
	for _, name := range []string{"conditions", "content", "revision", "origin", "producer", "collection membership"} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			s := New()
			owner := &manifest.Kustomization{Name: "reader", Namespace: "apps"}
			input := &manifest.Kustomization{
				Name:      "input",
				Namespace: "apps",
				Labels:    map[string]string{"selected": "before"},
			}
			s.AddObjects([]manifest.BaseManifest{owner, input})
			var reads ReadSet
			reads.Init(s, owner.Named(), nil)
			var mutate func()
			switch name {
			case "conditions":
				s.UpdateStatus(input.Named(), StatusInfo{Status: StatusReady, Message: "before"})
				reads.GetStatus(input.Named())
				mutate = func() { s.UpdateStatus(input.Named(), StatusInfo{Status: StatusReady, Message: "after"}) }
			case "content", "revision", "origin":
				artifact := &SourceArtifact{
					Content: "content", Revision: "revision",
					Metadata: map[string]string{"org.opencontainers.image.revision": "origin"},
				}
				s.SetArtifact(input.Named(), artifact)
				read := map[string]ArtifactRead{"content": ArtifactContent, "revision": ArtifactRevision, "origin": ArtifactOrigin}[name]
				reads.GetArtifact(input.Named(), read)
				mutate = func() {
					changed := *artifact
					switch name {
					case "content":
						changed.Content = "after"
					case "revision":
						changed.Revision = "after"
					case "origin":
						changed.Metadata = map[string]string{"org.opencontainers.image.revision": "after"}
					}
					s.SetArtifact(input.Named(), &changed)
				}
			case "producer":
				child := emissionChild("declared", "value")
				s.SetDiscoveryIndex(
					&manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{
						child.Obj.Named(): {Productions: []manifest.Production{{Owner: input.Named()}}},
					}},
				)
				reads.Productions(child.Obj.Named(), nil)
				assert.Equal(t, s.shardFor(input.Named()).slots[input.Named()].current.Load(), (*Emission)(nil))
				assert.ErrorIf(
					t,
					reads.input(input.Named()).cell(producerFacet).head == nil,
					"%s",
					"producer fixture captured no authority",
				)
				mutate = func() { s.RecordEmitted(input.Named(), &Emission{Complete: true}, nil) }
			case "collection membership":
				reads.ListObjects(input.Named().Group, input.Named().Kind)
				mutate = func() {
					s.AddObject(&manifest.Kustomization{Name: "additional", Namespace: "apps"})
				}
			}
			warning := manifest.Warning{
				Resource: owner.Named(), Subject: manifest.DiagnosticFor(owner.Named(), owner),
				Category: manifest.WarnUnresolvedSubstitution, Count: 1, Message: "retained diagnostic",
				Detail: []string{"first", "second"}, Related: []manifest.NamedResource{input.Named()},
			}
			advisory := AdvisoryEvidence{Overrides: []SuppliedOverride{{
				Resource: input.Named(), SupplyPath: "supplied/values.yaml", Keys: []string{"first", "second"},
			}}}
			reads.AddWarning(warning)
			reads.RecordSuppliedOverride(advisory.Overrides[0])
			reads.Accept(AssessmentReconciled)
			reads.ReleaseCapture()
			reads.Retire()
			for range s.Sweep() {
			}
			assertRetiredDiagnostics(t, s, []manifest.Warning{warning}, advisory)
			if name == "content" {
				s.SetArtifact(input.Named(), &SourceArtifact{
					Content: "content", Revision: "uncaptured revision",
					Metadata: map[string]string{"org.opencontainers.image.revision": "origin"},
				})
				assertRetiredDiagnostics(t, s, []manifest.Warning{warning}, advisory)
			}
			mutate()
			assertRetiredDiagnostics(t, s, nil, AdvisoryEvidence{})
		})
	}
}

func assertRetiredDiagnostics(t *testing.T, s *Store, warnings []manifest.Warning, advisory AdvisoryEvidence) {
	t.Helper()
	assertNoWarningSubscriptions(t, s)
	for range 2 {
		assert.Diff(t, s.Warnings(), warnings)
		assert.Diff(t, s.AdvisoryEvidence(), advisory)
		assertNoWarningSubscriptions(t, s)
	}
}

func assertNoWarningSubscriptions(t *testing.T, s *Store) {
	t.Helper()
	for _, sh := range s.shards {
		sh.mu.RLock()
		assert.Equal(t, sh.pending.Load(), (*registrationHead)(nil))
		heads := make(
			[]*registrationHead, 1,
			1+len(sh.cells)+len(sh.conditions)+3*len(sh.artifacts)+len(sh.slots)+len(sh.byName),
		)
		heads[0] = &sh.absence
		for _, cell := range sh.cells {
			heads = append(heads, cell.links)
		}
		for _, record := range sh.conditions {
			heads = append(heads, record.stamp().links)
		}
		for _, record := range sh.artifacts {
			heads = append(heads, record.links, record.revisionHead.links, record.originHead.links)
		}
		for _, slot := range sh.slots {
			heads = append(heads, slot.links)
		}
		for _, bucket := range sh.byName {
			heads = append(heads, bucket.links)
		}
		for _, head := range heads {
			if head == nil {
				continue
			}
			assert.ErrorIf(
				t,
				head.first.Load() != nil || head.watch != nil || head.detached.Load() || head.pending.Load(),
				"%s",
				"diagnostics retain live, detached, or pending subscriptions",
			)
		}
		sh.mu.RUnlock()
	}
}
