package store

import (
	"slices"
	"testing"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
)

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
	assert.FatalIf(
		t,
		!before.Complete || !before.OutputReads.Valid(),
		"%s",
		"subset did not start from valid completed output",
	)
	s.RecordEmitted(owner.Named(), &partial, nil)

	assert.FatalIf(t, !slices.EqualFunc(partial.Children, incoming, Child.equal) ||
		!slices.EqualFunc(
			before.Children,
			borrowed,
			Child.equal,
		), "%s", "partial publication mutated borrowed children")

	partial.Finish()
	after := s.GetArtifact(owner.Named(), ArtifactContent).(*Emission)
	assert.FatalIf(
		t,
		!output.Valid() || !after.OutputReads.Valid(),
		"%s",
		"partial publication invalidated captured output reads",
	)
	if (*testCaseInput) == "ordinary_subset" {
		if !observer.Valid() || objects != 0 || artifacts != 0 {
			t.Fatalf(
				"equal partial subset invalidated output: valid=%t objects=%d artifacts=%d",
				observer.Valid(),
				objects,
				artifacts,
			)
		}

		assert.FatalIf(
			t,
			!after.Complete || after.Fingerprint != before.Fingerprint,
			"%s",
			"equal partial subset lost completed render identity",
		)

		assert.FatalIf(
			t,
			!slices.EqualFunc(after.Children, before.Children, Child.equal),
			"%s",
			"equal partial subset changed its children",
		)
	} else {
		assert.FatalIf(t, s.GetObject(removed.Obj.Named()) != nil || s.GetObject(first.Obj.Named()) == nil ||
			s.GetObject(
				last.Obj.Named(),
			) == nil, "%s", "partial subset failed to withdraw generated output while retaining ordinary children")
		{
			got := s.GetObject(retained.Obj.Named())
			assert.FatalIf(
				t,
				(got != nil) != ((*testCaseInput) == "generated_subset"),
				"%s",
				"partial subset retained the wrong generated membership",
			)
		}
		assert.FatalIf(
			t,
			after.Complete || len(after.Failures) != 0,
			"%s",
			"generated withdrawal forced completion or failure",
		)
	}
	assert.Diff(t, after.Manifests, before.Manifests)
}
