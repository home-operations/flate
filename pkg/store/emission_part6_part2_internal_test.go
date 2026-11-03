package store

import (
	"slices"
	"sync"
	"testing"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
)

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
		for _, a := range accepted.entries {
			if a.child.Obj.Named() == g.Obj.Named() {
				targets = a.targets
			}
		}
		assert.Diff(t, targets, []manifest.ProducerTarget{{NamedResource: replacement}})
		assert.Diff(
			t,
			s.Productions(replacement, nil),
			[]manifest.Production{{Owner: owner.Named(), Declaration: g.Obj.Named(), Source: binding.source}},
		)
		{
			assert.ErrorIf(
				t,
				len(s.Productions((*testCaseInput.target), nil)) != 0,
				"%s",
				"retry retained superseded prediction",
			)
		}
		for _, id := range []manifest.NamedResource{(*testCaseInput.target), replacement} {
			facts := s.ReferenceFacts(id, nil)
			assert.Diff(t, facts.Productions, s.Productions(id, nil))
			{
				assert.ErrorIf(t, facts.Data != nil || facts.Accepted || s.GetObject(id) != nil ||
					s.GetArtifact(id, ArtifactContent) != nil, "%s", "prediction supplied content")
			}
		}
	}
	check()
	resume()
	writers.Wait()
	{
		assert.FatalIf(t, s.GetObject(barrierID) != barrier, "%s", "retry did not accept the paused proposal")
	}
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
	for _, a := range accepted.entries {
		for ref := range manifest.References(a.child.Obj, manifest.ReferenceRelations) {
			candidates = append(candidates, ref.NamedResource)
		}
	}
	want := manifest.ProducerTargets((*testCaseInput.generator)("app-{app}").Obj, candidates...)
	for _, a := range accepted.entries {
		if a.child.Obj.Named() == (*testCaseInput.generator)("").Obj.Named() {
			assert.Diff(t, a.targets, want)
		}
	}
}

func testRecordEmittedPartialSubsetPreservesCompletedOutputCase(
	t *testing.T,
	testCaseInput *string,
) {
	t.Helper()

	s := New()
	owner := &manifest.Kustomization{Name: "owner", Namespace: "apps"}
	s.AddObject(owner)
	var output, observer, writer ReadSet
	output.Init(s, owner.Named(), nil)
	observer.Init(s, ownerID("observer"), nil)
	writer.Init(s, owner.Named(), nil)
	defer output.Retire()
	defer observer.Retire()
	defer writer.Retire()
	first, last := emissionChild("first", "a"), emissionChild("last", "b")
	removed, retained := emissionChild("removed", "c"), emissionChild("retained", "d")
	removed.Generated, retained.Generated = true, true
	_, _, complete := output.GetEmission(owner.Named())
	complete.Children = []Child{first, last}
	if (*testCaseInput) != "ordinary_subset" {
		complete.Children = append(complete.Children, removed, retained)
	}
	complete.Manifests = testutil.DocumentOccurrences([]map[string]any{first.Doc, last.Doc})
	complete.Fingerprint = manifest.Fingerprint(complete.Manifests)
	complete.Complete = true
	output.Bind(&complete)
	s.RecordEmitted(owner.Named(), &complete, nil)
	complete.Finish()
	before := observer.GetArtifact(owner.Named(), ArtifactContent).(*Emission)
	borrowed := slices.Clone(before.Children)
	var objects, artifacts int
	stopObjects := s.AddListener(EventObjectAdded, func(manifest.NamedResource, any) { objects++ }, false)
	defer stopObjects()
	stopArtifacts := s.AddListener(
		EventArtifactUpdated,
		func(manifest.NamedResource, any) { artifacts++ },
		false,
	)
	defer stopArtifacts()
	_, _, partial := writer.GetEmission(owner.Named())
	partial.Children = []Child{last}
	if (*testCaseInput) == "generated_subset" {
		partial.Children = append(partial.Children, retained)
	}
	incoming := slices.Clone(partial.Children)
	{
		assert.FatalIf(
			t,
			!before.Complete || !before.OutputReads.Valid(),
			"%s",
			"subset did not start from valid completed output",
		)
	}
	s.RecordEmitted(owner.Named(), &partial, nil)
	{
		assert.FatalIf(t, !slices.EqualFunc(partial.Children, incoming, childEqual) ||
			!slices.EqualFunc(
				before.Children,
				borrowed,
				childEqual,
			), "%s", "partial publication mutated borrowed children")
	}
	partial.Finish()
	after := s.GetArtifact(owner.Named(), ArtifactContent).(*Emission)
	{
		assert.FatalIf(
			t,
			!output.Valid() || !after.OutputReads.Valid(),
			"%s",
			"partial publication invalidated captured output reads",
		)
	}
	if (*testCaseInput) == "ordinary_subset" {
		if !observer.Valid() || objects != 0 || artifacts != 0 {
			t.Fatalf(
				"equal partial subset invalidated output: valid=%t objects=%d artifacts=%d",
				observer.Valid(),
				objects,
				artifacts,
			)
		}
		{
			assert.FatalIf(
				t,
				!after.Complete || after.Fingerprint != before.Fingerprint,
				"%s",
				"equal partial subset lost completed render identity",
			)
		}
		{
			assert.FatalIf(
				t,
				!slices.EqualFunc(after.Children, before.Children, childEqual),
				"%s",
				"equal partial subset changed its children",
			)
		}
	} else {
		{
			assert.FatalIf(t, s.GetObject(removed.Obj.Named()) != nil || s.GetObject(first.Obj.Named()) == nil ||
				s.GetObject(
					last.Obj.Named(),
				) == nil, "%s", "partial subset failed to withdraw generated output while retaining ordinary children")
		}
		{
			got := s.GetObject(retained.Obj.Named())
			assert.FatalIf(
				t,
				(got != nil) != ((*testCaseInput) == "generated_subset"),
				"%s",
				"partial subset retained the wrong generated membership",
			)
		}
		{
			assert.FatalIf(
				t,
				after.Complete || len(after.Failures) != 0,
				"%s",
				"generated withdrawal forced completion or failure",
			)
		}
	}
	assert.Diff(t, after.Manifests, before.Manifests)
}
