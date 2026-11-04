package store

import (
	"slices"
	"testing"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/pkg/manifest"
)

func testGetDeclarationIndependenceLifecycleCaseCompletion(
	t *testing.T,
	scope *testGetDeclarationIndependenceLifecycleCaseCompletionState,
) {
	t.Helper()
	var want manifest.BaseManifest
	if (*scope.independent) || (*(*scope.testCaseInput).route) == "anchor" ||
		(*(*scope.testCaseInput).route) == "batch_anchor" {
		want = (*scope.raw)
	}
	assert.Equal(t, (*scope.st).GetObject((*scope.raw).Named()), manifest.BaseManifest((*scope.raw)))
	assert.Equal(t, (*scope.st).GetDeclaration((*scope.raw).Named()), want)
	got, _, emission := (*scope.st).GetEmission((*scope.raw).Named())
	defer emission.Finish()
	assert.Equal(t, got, want)
	var reads ReadSet
	reads.Init((*scope.st), ownerID("reader"), nil)
	defer reads.Retire()
	assert.Equal(t, reads.GetDeclaration((*scope.raw).Named()), want)
	assert.Equal(t, reads.input((*scope.raw).Named()).view.independent, (*scope.independent))
	if (*(*scope.testCaseInput).route) == "included" {
		assert.Equal(t, reads.input((*scope.raw).Named()).view.excluded, true)
	}
	if (*(*scope.testCaseInput).route) == "preflight" {
		assert.Equal(t, reads.input((*scope.raw).Named()).view.preflight, "preflight")
	}
	if (*(*scope.testCaseInput).route) == "document" {
		added := 0
		defer (*scope.st).AddListener(EventObjectAdded, func(id manifest.NamedResource, _ any) {
			if id == (*scope.raw).Named() {
				added++
			}
		}, false)()
		doc := map[string]any{"apiVersion": manifest.SourceDomain + "/v1beta2"}
		(*scope.st).AddObject((*scope.raw), doc)
		assert.FatalIf(t, reads.Valid(), "%s", "document replacement retained the captured generation")
		assert.Equal(t, reads.GetDeclaration((*scope.raw).Named()), want)
		assert.Equal(t, reads.input((*scope.raw).Named()).view.independent, (*scope.independent))
		assert.Diff(
			t,
			reads.input((*scope.raw).Named()).view.child.Doc,
			map[string]any{"apiVersion": manifest.SourceDomain + "/v1"},
		)
		var current ReadSet
		current.Init((*scope.st), ownerID("current"), nil)
		defer current.Retire()
		got, document, replacement := current.GetEmission((*scope.raw).Named())
		defer replacement.Finish()
		assert.Equal(t, got, want)
		assert.Equal(t, (*scope.st).GetDeclaration((*scope.raw).Named()), want)
		assert.Diff(t, document, doc)
		assert.Equal(t, current.input((*scope.raw).Named()).view.independent, (*scope.independent))
		assert.FatalIf(
			t,
			replacement.inputHead.generation <= reads.input((*scope.raw).Named()).view.generation,
			"%s",
			"document replacement did not advance the generation",
		)
		(*scope.st).AddObject((*scope.raw), manifest.DeepCopyMap(doc))
		if !current.Valid() || added != 1 {
			t.Fatalf(
				"equal document admission: valid=%t events=%d, want true and 1",
				current.Valid(),
				added,
			)
		}
		assert.Equal(t, (*scope.st).GetDeclaration((*scope.raw).Named()), want)
		assert.Equal(t, current.GetDeclaration((*scope.raw).Named()), want)
		assert.Equal(t, current.input((*scope.raw).Named()).view.independent, (*scope.independent))
	}
}

type testRecordEmittedIndependencePublicationCaseCompletionState struct {
	testCaseInput **testRecordEmittedIndependencePublicationCaseInputs
	st            **Store
	owner         **manifest.Kustomization
	child         *Child
	id            *manifest.NamedResource
	sh            **shard
	absent        *ReadSet
	output        *Emission
	nested        **manifest.Kustomization
}

func testRecordEmittedIndependencePublicationCaseCompletion(
	t *testing.T,
	scope *testRecordEmittedIndependencePublicationCaseCompletionState,
) {
	t.Helper()
	(*scope.st).RecordEmitted((*scope.owner).Named(), scope.output, nil)
	if (*(*scope.testCaseInput).route) == "expanded_inventory" {
		(*scope.st).RecordEmitted(
			(*scope.nested).Named(),
			&Emission{Children: []Child{(*scope.child)}, Complete: true},
			nil,
		)
		(*scope.st).RecordEmitted((*scope.owner).Named(), &Emission{Complete: true}, nil)
	}
	assert.FatalIf(
		t,
		!slices.Contains(slices.Collect((*scope.st).Sweep()), scope.absent.ID()) || scope.absent.Valid(),
		"%s",
		"first publication missed the registered absence",
	)
	if (*(*scope.testCaseInput).route) == "stable" || (*(*scope.testCaseInput).route) == "replacement" {
		(*scope.child) = emissionChild("output", "second")
		children := []Child{(*scope.child)}
		if (*(*scope.testCaseInput).route) == "replacement" {
			children = append(children, emissionChild("extra", "value"))
		}
		(*scope.st).RecordEmitted((*scope.owner).Named(), &Emission{Children: children, Complete: true}, nil)
	}
	(*scope.sh).mu.RLock()
	cell := (*scope.sh).cells[(*scope.id)]
	fact := cell.independent
	(*scope.sh).mu.RUnlock()
	if (*(*scope.testCaseInput).route) == "metadata" {
		assert.Equal(t, cell.excluded, true)
		assert.Equal(t, cell.preflight, "preflight")
	}
	assert.Equal(t, fact, (*(*scope.testCaseInput).independent))
	if (*(*scope.testCaseInput).route) == "reachability" || (*(*scope.testCaseInput).route) == "target" ||
		(*(*scope.testCaseInput).route) == "expanded_coverage" ||
		(*(*scope.testCaseInput).route) ==
			"expanded_inventory" {
		assert.Equal(t, (*scope.st).GetDeclaration((*scope.id)), nil)
		assert.Equal(t, (*scope.st).GetObject((*scope.id)), nil)
	} else if (*(*scope.testCaseInput).route) != "hidden_producer" {
		assert.Equal(t, (*scope.st).GetDeclaration((*scope.id)), scope.child.Obj)
		var reads ReadSet
		reads.Init((*scope.st), (*scope.owner).Named(), nil)
		defer reads.Retire()
		_, _, next := reads.GetEmission((*scope.owner).Named())
		defer next.Finish()
		reads.Bind(&next)
		var want manifest.BaseManifest
		if *(*scope.testCaseInput).independent {
			want = scope.child.Obj
		}
		assert.Equal(t, reads.GetDeclaration((*scope.id)), want)
	}
}
