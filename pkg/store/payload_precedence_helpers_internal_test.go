package store

import (
	"context"
	"io"
	"io/fs"
	"testing"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/task"
)

func testRecordEmittedPayloadPrecedenceCaseCompletion(
	t *testing.T,
	scope *testRecordEmittedPayloadPrecedenceCaseCompletionState,
) {
	t.Helper()
	var opened fs.File
	start, done := make(chan struct{}), make(chan struct{})
	tasks := task.NewBounded(2)
	for i := range *scope.owners {
		tasks.Go(t.Context(), (*scope.owners)[i].Name, func(context.Context) {
			if i == 1 {
				tasks.YieldSlot(func() { <-start })
			}
			(*scope.s).RecordEmitted(
				(*scope.owners)[i],
				&Emission{
					Children:  []Child{(*scope.children)[i]},
					Manifests: testutil.DocumentOccurrences([]map[string]any{(*scope.children)[i].Doc}),
					Complete:  true,
				},
				map[manifest.NamedResource]*SourceArtifact{(*scope.target): (*scope.payloads)[i]},
			)
			if i == 0 {
				close(start)
				tasks.YieldSlot(func() { <-done })
			} else {
				close(done)
			}
		})
	}
	tasks.BlockTillDone()
	assert.FatalIf(t, tasks.Failures() != 0, "%s", "publication panicked")
	(*scope.s).RecordEmitted(
		(*scope.zz),
		&Emission{
			Children:  []Child{(*scope.declaration)},
			Manifests: testutil.DocumentOccurrences([]map[string]any{scope.declaration.Doc}),
			Complete:  true,
		},
		nil,
	)
	assert.Diff(
		t,
		(*scope.s).Warnings(),
		[]manifest.Warning{
			{
				Resource: (*scope.target),
				Subject:  manifest.DiagnosticFor((*scope.target), nil),
				Related:  []manifest.NamedResource{(*scope.a), (*scope.z), (*scope.zz), (*scope.zz)},
				Category: manifest.WarnConflictingProducers,
				Count:    1,
				Message:  "conflicting producers; using " + scope.zz.String(),
				Detail:   []string{scope.a.String(), scope.z.String(), scope.zz.String()},
			},
		},
	)
	seen := 0
	unsubscribe := (*scope.s).AddListener(EventArtifactUpdated, func(id manifest.NamedResource, value any) {
		if id == (*scope.target) {
			seen++
			assert.ErrorIf(t, value != (*scope.last), "%s", "replay selected a different payload")
		}
	}, true)
	unsubscribe()
	assert.FatalIf(t, seen != 1, "generated payload replay count = %d", seen)
	assert.FatalIf(
		t,
		(*scope.s).EmissionOwner((*scope.target)) != (*scope.zz) ||
			(*scope.s).GetArtifact((*scope.target), ArtifactContent) != (*scope.last),
		"%s",
		"declaration winner suppressed greatest byte contributor",
	)
	checkDeclaration := func() {
		t.Helper()
		obj, doc, attempt := (*scope.s).GetEmission((*scope.target))
		defer attempt.Finish()
		assert.Equal(t, obj, scope.declaration.Obj)
		assert.Diff(t, doc, scope.declaration.Doc)
		assert.Equal(t, (*scope.s).EmissionOwner((*scope.target)), (*scope.zz))
	}
	checkDeclaration()
	(*scope.s).AddObject(scope.declaration.Obj, scope.declaration.Doc)
	assert.Equal(t, (*scope.s).DeleteObject((*scope.target)), true)
	assert.FatalIf(
		t,
		(*scope.s).GetArtifact((*scope.target), ArtifactContent) != (*scope.last),
		"%s",
		"static deletion discarded the surviving emitted payload",
	)
	checkDeclaration()
	{
		data, readFileErr := fs.ReadFile((*scope.first).FS, "file")
		assert.FatalIf(t, readFileErr != nil || string(data) != "a", "losing payload = %q, %v", data, readFileErr)
	}
	opened, (*scope.err) = (*scope.last).FS.Open("file")
	assert.NoError(t, (*scope.err))
	defer func() { _ = opened.Close() }()
	(*scope.s).RecordEmitted((*scope.z), &Emission{Complete: true}, nil)
	assert.FatalIf(
		t,
		(*scope.s).GetArtifact((*scope.target), ArtifactContent) != (*scope.first),
		"%s",
		"withdrawal did not select the whole surviving descriptor",
	)
	checkDeclaration()
	{
		data, err := fs.ReadFile(
			(*scope.s).GetArtifact((*scope.target), ArtifactContent).(*SourceArtifact).FS,
			"file",
		)
		assert.FatalIf(t, err != nil ||
			string(data) != "a", "surviving payload = %q, %v", data, err)
	}
	(*scope.s).RecordEmitted((*scope.a), &Emission{Complete: true}, nil)
	assert.FatalIf(
		t,
		(*scope.s).GetObject((*scope.target)) == nil ||
			(*scope.s).GetArtifact((*scope.target), ArtifactContent) != nil,
		"%s",
		"declaration-only survivor retained byte authority",
	)
	checkDeclaration()
	{
		data, err := io.ReadAll(opened)
		assert.FatalIf(
			t,
			err != nil || string(data) != "z",
			"already-open withdrawn backing = %q, %v",
			data,
			err,
		)
	}
	assert.FatalIf(
		t,
		(*scope.peer).GetArtifact((*scope.target), ArtifactContent) != (*scope.first),
		"%s",
		"withdrawal discarded another store's shared cache backing",
	)
	(*scope.peer).RecordEmitted((*scope.a), &Emission{Complete: true}, nil)
	for _, payload := range []*SourceArtifact{(*scope.first), (*scope.last)} {
		{
			data, err := fs.ReadFile(
				payload.FS,
				"file",
			)
			assert.FatalIf(t, err != nil ||
				string(data) != payload.Metadata["origin"], "unowned cache backing = %q, %v", data, err)
		}
	}
}
