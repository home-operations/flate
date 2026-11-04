package store

import (
	"sync"
	"testing"

	kustomizev1 "github.com/fluxcd/kustomize-controller/api/v1"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/pkg/manifest"
)

func TestRecordEmitted_ArtifactPredictions(t *testing.T) {
	t.Parallel()
	generator := func(template string) Child {
		return Child{
			Obj: &manifest.RawObject{
				APIVersion: "source.extensions.fluxcd.io/v1beta1",
				Kind:       "ArtifactGenerator",
				Name:       "generator",
				Namespace:  "apps",
				Spec: map[string]any{
					"pathPattern": "@repo/{app}",
					"artifacts":   []any{map[string]any{"name": template}},
				},
			},
		}
	}
	consumer := func(name string) Child {
		return Child{
			Obj: &manifest.Kustomization{
				Name:      "consumer",
				Namespace: "apps",
				SourceRef: kustomizev1.CrossNamespaceSourceReference{Kind: manifest.KindExternalArtifact, Name: name},
			},
		}
	}
	target := manifest.NamedResource{
		Group:     manifest.SourceDomain,
		Kind:      manifest.KindExternalArtifact,
		Namespace: "apps",
		Name:      "app-api",
	}
	for _, mode := range []string{"proposal", "frozen", "other-owner", "other-source", "other-pin"} {
		t.Run(mode, func(t *testing.T) {
			t.Parallel()
			testRecordEmittedArtifactPredictionsCase(
				t,
				&testRecordEmittedArtifactPredictionsCaseInputs{
					generator: &generator,
					consumer:  &consumer,
					target:    &target,
					mode:      &mode,
				},
			)
		},

		// The frozen source identity and pin are captured before publication.

		)
	}

	t.Run("replacement_and_partial_retention", func(t *testing.T) {
		t.Parallel()
		testRecordEmittedArtifactPredictionsReplacementAndPartialRetention(
			t,
			&testRecordEmittedArtifactPredictionsReplacementAndPartialRetentionInputs{
				generator: &generator,
				consumer:  &consumer,
				target:    &target,
			},
		)
	},
	)

	t.Run("retained_consumer", func(t *testing.T) {
		t.Parallel()
		testRecordEmittedArtifactPredictionsRetainedConsumer(
			t,
			&testRecordEmittedArtifactPredictionsRetainedConsumerInputs{
				generator: &generator,
				consumer:  &consumer,
				target:    &target,
			},
		)
	},
	)

	t.Run("retry_rebuilds_admissions", func(t *testing.T) {
		t.Parallel()
		testRecordEmittedArtifactPredictionsRetryRebuildsAdmissions(
			t,
			&testRecordEmittedArtifactPredictionsRetryRebuildsAdmissionsInputs{
				generator: &generator,
				consumer:  &consumer,
				target:    &target,
			},
		)
	},
	)

	t.Run("concurrent_proposals", func(t *testing.T) {
		t.Parallel()
		testRecordEmittedArtifactPredictionsConcurrentProposals(
			t,
			&testRecordEmittedArtifactPredictionsConcurrentProposalsInputs{generator: &generator, consumer: &consumer},
		)
	},
	)
}

//nolint:paralleltest // The test calls a helper that changes process-global state.
//nolint:paralleltest // This test changes process-global state.
func TestRecordEmitted_IdenticalArtifactEmission(t *testing.T) {
	s := New()
	owner := &manifest.Kustomization{Name: "owner", Namespace: "apps"}
	s.AddObject(owner)
	generator := &manifest.RawObject{
		APIVersion: "source.extensions.fluxcd.io/v1beta1",
		Kind:       "ArtifactGenerator",
		Name:       "generator",
		Namespace:  "apps",
		Spec: map[string]any{
			"pathPattern": "@repo/{app}",
			"artifacts":   []any{map[string]any{"name": "app-{app}"}},
		},
	}
	consumer := &manifest.Kustomization{
		Name:      "consumer",
		Namespace: "apps",
		SourceRef: kustomizev1.CrossNamespaceSourceReference{Kind: manifest.KindExternalArtifact, Name: "app-api"},
	}
	next := &Emission{
		Children: []Child{{Obj: generator}, {Obj: consumer}},
		Complete: true,
		Render:   manifest.NewRenderReceipt("receipt", func(a, b any) bool { return a == b }),
	}
	s.RecordEmitted(owner.Named(), next, nil)
	accepted := s.GetArtifact(owner.Named(), ArtifactContent)
	{
		got := testing.AllocsPerRun(10, func() { s.RecordEmitted(owner.Named(), next, nil) })
		assert.FatalIf(t, got != 0, "identical dynamic emission allocated %v times, want zero", got)
	}
	assert.FatalIf(
		t,
		s.GetArtifact(owner.Named(), ArtifactContent) != accepted,
		"%v",
		"identical emission replaced the accepted proposal",
	)
	target := manifest.NamedResource{
		Group:     manifest.SourceDomain,
		Kind:      manifest.KindExternalArtifact,
		Namespace: "apps",
		Name:      "app-api",
	}
	facts := s.ReferenceFacts(target, nil, false, manifest.NamedResource{})
	assert.Diff(t, facts.Productions, s.Productions(target, nil))
	assert.Diff(t, facts.Productions, []manifest.Production{{Owner: owner.Named(), Declaration: generator.Named()}})
	assert.FatalIf(t, facts.Data != nil || facts.Accepted, "prediction supplied authority: %+v", facts)
}

type testRecordEmittedArtifactPredictionsCaseInputs struct {
	generator *func(template string) Child
	consumer  *func(name string) Child
	target    *manifest.NamedResource
	mode      *string
}

func testRecordEmittedArtifactPredictionsCase(
	t *testing.T,
	testCaseInput *testRecordEmittedArtifactPredictionsCaseInputs,
) {
	t.Helper()

	s := New()
	owner := &manifest.Kustomization{Name: "owner", Namespace: "apps"}
	s.AddObject(owner)
	binding := sourceBinding{
		source:   manifest.BootstrapSourceID,
		artifact: &SourceArtifact{Key: "pin", LocalPath: "/view"},
	}
	g, c := (*testCaseInput.generator)("app-{app}"), (*testCaseInput.consumer)(testCaseInput.target.Name)
	s.SetArtifact(
		binding.source,
		&SourceArtifact{Key: binding.artifact.Key, LocalPath: binding.artifact.LocalPath},
	)
	if (*testCaseInput.mode) != "proposal" {
		provenance := manifest.Production{
			Owner:  owner.Named(),
			Source: binding.source,
			File:   "consumer.yaml",
			Path:   "/view/consumer.yaml",
		}
		if (*testCaseInput.mode) == "other-owner" {
			provenance.Owner.Name = "other"
		}
		if (*testCaseInput.mode) == "other-source" {
			provenance.Source.Name = "other"
		}
		decl := provenance
		decl.Declaration, decl.File, decl.Path = g.Obj.Named(), "generator.yaml", ""
		s.SetDiscoveryIndex(
			&manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{
				c.Obj.Named(): {
					File:        provenance.File,
					Path:        provenance.Path,
					Refs:        []manifest.NamedResource{(*testCaseInput.target)},
					Productions: []manifest.Production{provenance},
				}, (*testCaseInput.target): {Productions: []manifest.Production{decl}},
			}},
		)

		if contribution := s.coverage[owner.Named()]; contribution != nil {
			contribution.binding = binding
		}
	}
	proposal := &Emission{Children: []Child{g}, Complete: true, binding: binding}
	if (*testCaseInput.mode) == "proposal" {
		proposal.Children = append(proposal.Children, c)
	}
	if contribution := s.coverage[owner.Named()]; contribution != nil {
		proposal.coverage = contribution.records
	}
	testRecordEmittedArtifactPredictionsCaseCompletion(
		t,
		&testRecordEmittedArtifactPredictionsCaseCompletionState{
			testCaseInput: testCaseInput,
			s:             s,
			owner:         owner,
			g:             &g,
			proposal:      proposal,
		},
	)
}

type testRecordEmittedArtifactPredictionsReplacementAndPartialRetentionInputs struct {
	generator *func(template string) Child
	consumer  *func(name string) Child
	target    *manifest.NamedResource
}

func testRecordEmittedArtifactPredictionsReplacementAndPartialRetention(
	t *testing.T,
	testCaseInput *testRecordEmittedArtifactPredictionsReplacementAndPartialRetentionInputs,
) {
	t.Helper()

	s := New()
	owner := &manifest.Kustomization{Name: "owner", Namespace: "apps"}
	s.AddObject(owner)
	g := (*testCaseInput.generator)("app-{app}")
	s.RecordEmitted(
		owner.Named(),
		&Emission{Children: []Child{g, (*testCaseInput.consumer)(testCaseInput.target.Name)}, Complete: true},
		nil,
	)
	replacement := (*testCaseInput.target)
	replacement.Name = "app-web"
	s.RecordEmitted(
		owner.Named(),
		&Emission{Children: []Child{g, (*testCaseInput.consumer)(replacement.Name)}, Complete: true},
		nil,
	)
	assert.FatalIf(t, len(s.Productions(replacement, nil)) != 1, "%s", "reference replacement lost new prediction")
	assert.FatalIf(
		t,
		len(s.Productions((*testCaseInput.target), nil)) != 0,
		"%s",
		"reference replacement retained old prediction",
	)
	_, _, current := s.GetEmission(owner.Named())
	assert.FatalIf(t, current.Children[0].Obj != g.Obj, "%s", "unchanged declaration replaced")
	current.Finish()
	s.RecordEmitted(owner.Named(), &Emission{}, nil)
	assert.FatalIf(t, len(s.Productions(replacement, nil)) != 1, "%s", "incomplete empty withdrew prediction")
	s.RecordEmitted(owner.Named(), &Emission{Complete: true}, nil)
	assert.FatalIf(t, len(s.Productions(replacement, nil)) != 0, "%s", "successful empty retained prediction")
}

type testRecordEmittedArtifactPredictionsRetainedConsumerInputs struct {
	generator *func(template string) Child
	consumer  *func(name string) Child
	target    *manifest.NamedResource
}

func testRecordEmittedArtifactPredictionsRetainedConsumer(
	t *testing.T,
	testCaseInput *testRecordEmittedArtifactPredictionsRetainedConsumerInputs,
) {
	t.Helper()

	s := New()
	owner := &manifest.Kustomization{Name: "owner", Namespace: "apps"}
	s.AddObject(owner)
	g, retained := (*testCaseInput.generator)("app-{app}"), (*testCaseInput.consumer)(testCaseInput.target.Name)
	retained.Obj = &manifest.Kustomization{
		Name:      "retained",
		Namespace: "apps",
		SourceRef: retained.Obj.(*manifest.Kustomization).SourceRef,
	}
	s.RecordEmitted(
		owner.Named(),
		&Emission{
			Children: []Child{g, (*testCaseInput.consumer)(testCaseInput.target.Name), retained},
			Complete: true,
		},
		nil,
	)
	s.RecordEmitted(
		owner.Named(),
		&Emission{Children: []Child{g, (*testCaseInput.consumer)("app-web")}, Complete: true},
		nil,
	)
	assert.FatalIf(
		t,
		len(s.Productions((*testCaseInput.target), nil)) != 1,
		"%s",
		"retained consumer lost its declaration's prediction",
	)
}

type testRecordEmittedArtifactPredictionsRetryRebuildsAdmissionsInputs struct {
	generator *func(template string) Child
	consumer  *func(name string) Child
	target    *manifest.NamedResource
}

type testRecordEmittedArtifactPredictionsCaseCompletionState struct {
	testCaseInput *testRecordEmittedArtifactPredictionsCaseInputs
	s             *Store
	owner         *manifest.Kustomization
	g             *Child
	proposal      *Emission
}

func testRecordEmittedArtifactPredictionsCaseCompletion(
	t *testing.T,
	scope *testRecordEmittedArtifactPredictionsCaseCompletionState,
) {
	t.Helper()
	if (*scope.testCaseInput.mode) == "other-pin" {
		cloned := *scope.proposal.binding.artifact
		cloned.Key = "other-pin"
		scope.proposal.binding.artifact = &cloned
	}
	scope.s.RecordEmitted(scope.owner.Named(), scope.proposal, nil)
	want := (*scope.testCaseInput.mode) == "proposal" || (*scope.testCaseInput.mode) == "frozen"
	check := func(want bool) {
		t.Helper()
		_, _, current := scope.s.GetEmission(scope.owner.Named())
		defer current.Finish()
		var predicted bool
		{
			entryValues := scope.s.GetArtifact(scope.owner.Named(), ArtifactContent).(*Emission).entries
			for entryIndex := range entryValues {
				a := entryValues[entryIndex]

				for _, p := range a.targets {
					predicted = predicted || p.NamedResource == (*scope.testCaseInput.target)
				}
			}
		}

		assert.FatalIf(t, predicted != want, "predicted = %t, want %t", predicted, want)

		facts := scope.s.ReferenceFacts((*scope.testCaseInput.target), nil, false, manifest.NamedResource{})
		assert.Diff(t, facts.Productions, scope.s.Productions((*scope.testCaseInput.target), nil))
		assert.FatalIf(
			t,
			facts.Data != nil || facts.Accepted || scope.s.GetObject((*scope.testCaseInput.target)) != nil ||
				scope.s.GetArtifact(
					(*scope.testCaseInput.target),
					ArtifactContent,
				) != nil,
			"%s",
			"prediction supplied content",
		)
	}
	check(want)
	_, _, next := scope.s.GetEmission(scope.owner.Named())
	next.Children = []Child{(*scope.g)}
	next.Complete = true
	next.binding = scope.proposal.binding
	scope.s.RecordEmitted(scope.owner.Named(), &next, nil)
	check(want)
	next.Finish()
	if (*scope.testCaseInput.mode) == "frozen" {
		_, _, replacement := scope.s.GetEmission(scope.owner.Named())
		replacement.Children, replacement.Complete, replacement.binding = []Child{
			(*scope.g), (*scope.testCaseInput.consumer)("app-web"),
		}, true, scope.proposal.binding
		scope.s.RecordEmitted(scope.owner.Named(), &replacement, nil)
		check(false)
		replacement.Finish()
		for range 2 {
			_, _, withoutConsumer := scope.s.GetEmission(scope.owner.Named())
			withoutConsumer.Children, withoutConsumer.Complete, withoutConsumer.binding = []Child{
				(*scope.g),
			}, true, scope.proposal.binding
			scope.s.RecordEmitted(scope.owner.Named(), &withoutConsumer, nil)
			check(false)
			withoutConsumer.Finish()
		}
	}
	_, _, next = scope.s.GetEmission(scope.owner.Named())
	next.Children, next.Complete, next.binding = []Child{
		(*scope.testCaseInput.generator)("different-{app}"),
	}, true, scope.proposal.binding
	scope.s.RecordEmitted(scope.owner.Named(), &next, nil)
	check(false)
	next.Finish()
}

func testRecordEmittedArtifactPredictionsRetryRebuildsAdmissions(
	t *testing.T,
	testCaseInput *testRecordEmittedArtifactPredictionsRetryRebuildsAdmissionsInputs,
) {
	t.Helper()

	s := New()
	owner := &manifest.Kustomization{Name: "owner", Namespace: "apps"}
	s.AddObject(owner)
	g := (*testCaseInput.generator)("app-{app}")
	binding := sourceBinding{
		source:   manifest.BootstrapSourceID,
		artifact: &SourceArtifact{Key: "pin", LocalPath: "/view"},
	}
	s.RecordEmitted(
		owner.Named(),
		&Emission{
			Children: []Child{g, (*testCaseInput.consumer)(testCaseInput.target.Name)},
			Complete: true,
			binding:  binding,
		},
		nil,
	)
	release := make(chan struct{})
	barrier := &admissionBarrier{
		ConfigMap: &manifest.ConfigMap{Name: "barrier", Namespace: "apps"},
		prepared:  make(chan struct{}),
		release:   release,
	}
	barrierID := barrier.ConfigMap.Named()
	resume := sync.OnceFunc(func() { close(release) })
	var writers sync.WaitGroup
	t.Cleanup(func() {
		resume()
		writers.Wait()
	})
	writers.Go(func() {
		s.RecordEmitted(
			owner.Named(),
			&Emission{
				Children: []Child{g, {Obj: barrier}, {Obj: &manifest.ConfigMap{Name: "inert", Namespace: "apps"}}},
				Complete: true,
				binding:  binding,
			},
			nil,
		)
	})
	<-barrier.prepared
	replacement := (*testCaseInput.target)
	replacement.Name = "app-web"
	published := make(chan struct{})
	writers.Go(func() {
		defer close(published)
		s.RecordEmitted(
			owner.Named(),
			&Emission{
				Children: []Child{g, (*testCaseInput.consumer)(replacement.Name)},
				Complete: true,
				binding:  binding,
			},
			nil,
		)
	})
	<-published
	check := func() {
		t.Helper()
		accepted := s.GetArtifact(owner.Named(), ArtifactContent).(*Emission)
		var targets []manifest.ProducerTarget
		{
			entryValues := accepted.entries
			for entryIndex := range entryValues {
				a := entryValues[entryIndex]

				if a.child.Obj.Named() == g.Obj.Named() {
					targets = a.targets
				}
			}
		}
		assert.Diff(t, targets, []manifest.ProducerTarget{{NamedResource: replacement}})
		assert.Diff(
			t,
			s.Productions(replacement, nil),
			[]manifest.Production{{Owner: owner.Named(), Declaration: g.Obj.Named(), Source: binding.source}},
		)
		assert.ErrorIf(
			t,
			len(s.Productions((*testCaseInput.target), nil)) != 0,
			"%s",
			"retry retained superseded prediction",
		)
		for _, id := range []manifest.NamedResource{(*testCaseInput.target), replacement} {
			facts := s.ReferenceFacts(id, nil, false, manifest.NamedResource{})
			assert.Diff(t, facts.Productions, s.Productions(id, nil))
			assert.ErrorIf(t, facts.Data != nil || facts.Accepted || s.GetObject(id) != nil ||
				s.GetArtifact(id, ArtifactContent) != nil, "%s", "prediction supplied content")
		}
	}
	check()
	resume()
	writers.Wait()
	assert.FatalIf(t, s.GetObject(barrierID) != barrier, "%s", "retry did not accept the paused proposal")
	check()
}

type testRecordEmittedArtifactPredictionsConcurrentProposalsInputs struct {
	generator *func(template string) Child
	consumer  *func(name string) Child
}

func testRecordEmittedArtifactPredictionsConcurrentProposals(
	t *testing.T,
	testCaseInput *testRecordEmittedArtifactPredictionsConcurrentProposalsInputs,
) {
	t.Helper()

	s := New()
	owner := &manifest.Kustomization{Name: "owner", Namespace: "apps"}
	s.AddObject(owner)
	start := make(chan struct{})
	var wg sync.WaitGroup
	for _, name := range []string{"app-api", "app-web"} {
		wg.Go(func() {
			<-start
			s.RecordEmitted(
				owner.Named(),
				&Emission{
					Children: []Child{(*testCaseInput.generator)("app-{app}"), (*testCaseInput.consumer)(name)},
					Complete: true,
				},
				nil,
			)
		})
	}
	close(start)
	wg.Wait()
	accepted := s.GetArtifact(owner.Named(), ArtifactContent).(*Emission)
	var candidates []manifest.NamedResource
	{
		entryValues := accepted.entries
		for entryIndex := range entryValues {
			a := entryValues[entryIndex]

			for ref := range manifest.References(a.child.Obj, manifest.ReferenceRelations) {
				candidates = append(candidates, ref.NamedResource)
			}
		}
	}
	want := manifest.ProducerTargets((*testCaseInput.generator)("app-{app}").Obj, candidates...)
	{
		entryValues := accepted.entries
		for entryIndex := range entryValues {
			a := entryValues[entryIndex]

			if a.child.Obj.Named() == (*testCaseInput.generator)("").Obj.Named() {
				assert.Diff(t, a.targets, want)
			}
		}
	}
}
