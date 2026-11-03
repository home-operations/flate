package store

import (
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"testing/fstest"

	kustomizev1 "github.com/fluxcd/kustomize-controller/api/v1"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
)

func TestRecordEmitted_SeedDocumentArrival(t *testing.T) {
	t.Parallel()

	s := New()
	p := ownerID("publisher")
	s.AddObject(&manifest.Kustomization{Name: p.Name, Namespace: p.Namespace})
	doc := map[string]any{
		"apiVersion": manifest.SourceDomain + "/v1", "kind": manifest.KindHelmRepository,
		"metadata": map[string]any{"name": "shared", "namespace": "apps"},
		"spec":     map[string]any{"url": "https://charts.example"},
	}
	obj, _, _, err := manifest.ParseDoc(doc, manifest.ParseDocOptions{})
	{
		assert.NoError(t, err)
	}
	s.AddObject(obj)
	foreignDoc := manifest.DeepCopyMap(doc)
	foreignDoc["apiVersion"] = "example.test/v1"
	foreign, _, _, err := manifest.ParseDoc(foreignDoc, manifest.ParseDocOptions{})
	{
		assert.NoError(t, err)
	}
	s.AddObject(foreign)
	var arrivals int
	unsub := s.AddListener(EventObjectAdded, func(id manifest.NamedResource, _ any) {
		if id == obj.Named() {
			arrivals++
		}
	}, false)
	defer unsub()
	s.RecordEmitted(
		p,
		&Emission{
			Children:  []Child{{Obj: obj, Doc: doc}, {Obj: foreign, Doc: foreignDoc}},
			Manifests: testutil.DocumentOccurrences([]map[string]any{foreignDoc, doc}),
			Complete:  true,
		},
		nil,
	)
	{
		assert.FatalIf(t, arrivals != 0, "equal seed acquired document with %d arrivals", arrivals)
	}
	_, _, accepted := s.GetEmission(p)
	{
		assert.FatalIf(
			t,
			len(accepted.Manifests) != 2 || !manifest.ValueEqual(accepted.Manifests[0].Document, foreignDoc) ||
				!manifest.ValueEqual(
					accepted.Manifests[1].Document,
					doc,
				),
			"%s",
			"suppressed arrival lost authoritative document",
		)
	}
	{
		got := s.Productions(obj.Named(), nil)
		assert.FatalIf(t, len(got) != 1 || got[0].Owner != p, "accepted support = %v", got)
	}
	next := manifest.DeepCopyMap(doc)
	next["apiVersion"] = manifest.SourceDomain + "/v1beta2"
	s.RecordEmitted(
		p,
		&Emission{
			Children:  []Child{{Obj: obj, Doc: next}, emissionChild("other", "value")},
			Manifests: testutil.DocumentOccurrences([]map[string]any{next}),
		},
		nil,
	)
	{
		assert.FatalIf(t, arrivals != 1, "changed authoritative document arrivals = %d, want 1", arrivals)
	}
	_, _, accepted = s.GetEmission(p)
	{
		assert.FatalIf(t, len(accepted.Manifests) != 2 || !manifest.ValueEqual(accepted.Manifests[0].Document, next) ||
			!manifest.ValueEqual(accepted.Manifests[1].Document, foreignDoc), "partial version update lost cross-group documents: %v", accepted.Manifests)
	}
	{
		got := s.Productions(foreign.Named(), nil)
		assert.FatalIf(t, len(got) != 1 || got[0].Owner != p, "cross-group support = %v", got)
	}

	next = manifest.DeepCopyMap(next)
	next["metadata"].(map[string]any)["annotations"] = map[string]any{"semantic": "changed"}
	next["metadata"].(map[string]any)["labels"] = map[string]any{"kustomize.toolkit.fluxcd.io/name": p.Name}
	s.RecordEmitted(p, &Emission{Children: []Child{{Obj: obj, Doc: next}}, Complete: true}, nil)
	{
		assert.FatalIf(t, arrivals != 1, "unchanged source document metadata caused %d arrivals, want 1", arrivals)
	}
	_, retained, attempt := s.GetEmission(obj.Named())
	defer attempt.Finish()
	{
		assert.FatalIf(
			t,
			!manifest.ValueEqual(retained, next),
			"%s",
			"suppressed arrival lost updated document metadata",
		)
	}
}

type admissionBarrier struct {
	*manifest.ConfigMap

	calls    atomic.Int64
	prepared chan struct{}
	release  <-chan struct{}
}

func (b *admissionBarrier) Named() manifest.NamedResource {
	// Two collection lookups and one proven-target lookup precede prepareEmission indexing.
	if b.calls.Add(1) == 4 {
		close(b.prepared)
		<-b.release
	}
	return b.ConfigMap.Named()
}

func TestRecordEmitted_ReadsInvalidatedBeforeCommit(t *testing.T) {
	t.Parallel()
	for _, mode := range []string{"stable", "replacement", "payload"} {
		t.Run(mode, func(t *testing.T) {
			t.Parallel()
			testRecordEmittedReadsInvalidatedBeforeCommitCase(
				t,
				&mode,
			)
		},
		)
	}
}

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
	{
		assert.FatalIf(t, s.GetArtifact(owner.Named(), ArtifactContent) != accepted, "%v", "identical emission replaced the accepted proposal")
	}
	target := manifest.NamedResource{
		Group:     manifest.SourceDomain,
		Kind:      manifest.KindExternalArtifact,
		Namespace: "apps",
		Name:      "app-api",
	}
	facts := s.ReferenceFacts(target, nil)
	assert.Diff(t, facts.Productions, s.Productions(target, nil))
	assert.Diff(t, facts.Productions, []manifest.Production{{Owner: owner.Named(), Declaration: generator.Named()}})
	{
		assert.FatalIf(t, facts.Data != nil || facts.Accepted, "prediction supplied authority: %+v", facts)
	}
}

func TestRecordEmitted_PartialSubsetPreservesCompletedOutput(t *testing.T) {
	t.Parallel()
	for _, mode := range []string{"ordinary_subset", "generated_subset", "empty_generated_set"} {
		t.Run(mode, func(t *testing.T) {
			t.Parallel()
			testRecordEmittedPartialSubsetPreservesCompletedOutputCase(
				t,
				&mode,
			)
		},
		)
	}
}

func testRecordEmittedReadsInvalidatedBeforeCommitCase(
	t *testing.T,
	testCaseInput *string,
) {
	t.Helper()

	s := New()
	owner := ownerID("owner")
	s.AddObject(&manifest.Kustomization{Name: owner.Name, Namespace: owner.Namespace})
	input := emissionChild("input", "old")
	s.AddObject(input.Obj)
	acceptedInput := emissionChild("accepted-input", "old")
	s.AddObject(acceptedInput.Obj)
	var accepted ReadSet
	accepted.Init(s, owner, nil)
	defer accepted.Retire()
	accepted.GetObject(acceptedInput.Obj.Named())
	acceptedHead := accepted.input(acceptedInput.Obj.Named()).object.head
	child := &manifest.ConfigMap{Name: "output", Namespace: "apps"}
	output := &Emission{Children: []Child{{Obj: child}}, Fingerprint: "accepted", Complete: true}
	accepted.Bind(output)
	s.RecordEmitted(owner, output, nil)
	previous := s.GetArtifact(owner, ArtifactContent).(*Emission)
	var reads ReadSet
	reads.Init(s, owner, nil)
	defer reads.Retire()
	reads.GetObject(input.Obj.Named())
	rejectedHead := reads.input(input.Obj.Named()).object.head
	{
		assert.FatalIf(
			t,
			acceptedHead == rejectedHead,
			"%s",
			"accepted and successor-only inputs share a registration head",
		)
	}
	name := child.Name
	if (*testCaseInput) == "replacement" {
		name = "replacement"
	}
	release := make(chan struct{})
	barrier := &admissionBarrier{
		ConfigMap: &manifest.ConfigMap{Name: name, Namespace: "apps"},
		prepared:  make(chan struct{}),
		release:   release,
	}
	next := &Emission{Children: []Child{{Obj: barrier}}, Fingerprint: "stale", Complete: true}
	reads.Bind(next)
	resume := sync.OnceFunc(func() { close(release) })
	var payloads map[manifest.NamedResource]*SourceArtifact
	if (*testCaseInput) == "payload" {
		payloads = map[manifest.NamedResource]*SourceArtifact{
			child.Named(): {Key: "stale", Content: "stale", FS: fstest.MapFS{}},
		}
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		s.RecordEmitted(owner, next, payloads)
	}()
	t.Cleanup(func() { resume(); <-done })
	select {
	case <-barrier.prepared:
	case <-done:
		t.Fatal("publication did not reach the admission barrier")
	}
	{
		assert.FatalIf(t, !reads.Valid(), "%s", "reads invalid before concurrent input replacement")
	}
	s.AddObject(emissionChild("input", "new").Obj)
	{
		assert.FatalIf(t, reads.Valid(), "%s", "input replacement did not invalidate the pending publication")
	}
	resume()
	<-done
	{
		assert.FatalIf(t, s.GetArtifact(owner, ArtifactContent) != previous || s.GetObject(child.Named()) != child ||
			s.GetArtifact(child.Named(), ArtifactContent) != nil, "%s", "stale proposal replaced authoritative output")
	}
	{
		assert.FatalIf(
			t,
			(*testCaseInput) == "replacement" && s.GetObject(barrier.ConfigMap.Named()) != nil,
			"%s",
			"stale proposal installed a replacement child",
		)
	}
	{
		assert.ErrorIf(
			t,
			previous.OutputReads != &accepted || accepted.retired.Load(),
			"%s",
			"rejected proposal retired accepted output provenance",
		)
	}
	reads.Retire()
	assert.Equal(t, len(slices.Collect(s.Sweep())), 0)
	{
		assert.ErrorIf(
			t,
			!reads.retired.Load() || rejectedHead.first.Load() != nil,
			"%s",
			"sweep retained rejected attempt registrations",
		)
	}
	{
		assert.ErrorIf(
			t,
			acceptedHead.first.Load() == nil || s.GetArtifact(owner, ArtifactContent) != previous,
			"%s",
			"sweep removed accepted output or its input registration",
		)
	}
	s.AddObject(emissionChild("accepted-input", "new").Obj)
	{
		assert.FatalIf(
			t,
			!slices.Contains(slices.Collect(s.Sweep()), owner),
			"%s",
			"accepted input change did not notify its owner after successor rejection",
		)
	}
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
	testRecordEmittedArtifactPredictionsCaseCompletion(t, &testRecordEmittedArtifactPredictionsCaseCompletionState{testCaseInput: testCaseInput, s: s, owner: owner, g: &g, proposal: proposal})
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
	{
		assert.FatalIf(t, len(s.Productions(replacement, nil)) != 1, "%s", "reference replacement lost new prediction")
	}
	{
		assert.FatalIf(
			t,
			len(s.Productions((*testCaseInput.target), nil)) != 0,
			"%s",
			"reference replacement retained old prediction",
		)
	}
	_, _, current := s.GetEmission(owner.Named())
	{
		assert.FatalIf(t, current.Children[0].Obj != g.Obj, "%s", "unchanged declaration replaced")
	}
	current.Finish()
	s.RecordEmitted(owner.Named(), &Emission{}, nil)
	{
		assert.FatalIf(t, len(s.Productions(replacement, nil)) != 1, "%s", "incomplete empty withdrew prediction")
	}
	s.RecordEmitted(owner.Named(), &Emission{Complete: true}, nil)
	{
		assert.FatalIf(t, len(s.Productions(replacement, nil)) != 0, "%s", "successful empty retained prediction")
	}
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
	{
		assert.FatalIf(
			t,
			len(s.Productions((*testCaseInput.target), nil)) != 1,
			"%s",
			"retained consumer lost its declaration's prediction",
		)
	}
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

func testRecordEmittedArtifactPredictionsCaseCompletion(t *testing.T, scope *testRecordEmittedArtifactPredictionsCaseCompletionState) {
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
		for _, a := range scope.s.GetArtifact(scope.owner.Named(), ArtifactContent).(*Emission).entries {
			for _, p := range a.targets {
				predicted = predicted || p.NamedResource == (*scope.testCaseInput.target)
			}
		}
		{
			assert.FatalIf(t, predicted != want, "predicted = %t, want %t", predicted, want)
		}
		facts := scope.s.ReferenceFacts((*scope.testCaseInput.target), nil)
		assert.Diff(t, facts.Productions, scope.s.Productions((*scope.testCaseInput.target), nil))
		{
			assert.FatalIf(t, facts.Data != nil || facts.Accepted || scope.s.GetObject((*scope.testCaseInput.target)) != nil ||
				scope.s.GetArtifact((*scope.testCaseInput.target), ArtifactContent) != nil, "%s", "prediction supplied content")
		}
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
