package cli

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/output"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/change"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/orchestrator"
	"github.com/home-operations/flate/pkg/store"
)

func TestRenderTrees_InputWarningOnly(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	warning := manifest.Warning{Category: manifest.WarnBaselineStatusUnavailable, Message: "before failure", Count: 1}
	runs, err := orchestrator.RenderTrees(
		t.Context(),
		[]orchestrator.Tree{
			{
				ID:    manifest.TreeID{Cluster: ".", Side: manifest.TreeHead},
				Path:  root,
				Input: &store.SourceArtifact{LocalPath: root, Warnings: []manifest.Warning{warning}},
			},
		},
		orchestrator.Config{Concurrency: 2},
	)
	assert.FatalIf(t, len(runs) != 1, "missing diagnostic run: %v", err)
	run := runs[0]
	assert.FatalIf(t, run.Orchestrator != nil || run.Result == nil || !errors.Is(run.Err, manifest.ErrInput) ||
		len(run.Result.Manifests)+len(run.Result.Failed) != 0, "failed bootstrap exposed content: %+v", run)
	assert.Diff(t, run.Result.Warnings, []manifest.Warning{warning})
	for _, unavailable := range []bool{false, true} {
		t.Run(fmt.Sprintf("unavailable_%t", unavailable), func(t *testing.T) {
			t.Parallel()
			renderTreesInputWarningOnlyCase(t, (*struct {
				Resource manifest.NamedResource
				Subject  manifest.DiagnosticIdentity
				Related  []manifest.NamedResource
				Category string
				Message  string
				Detail   []string
				Count    int
			})(&warning), &root, &unavailable)
		})
	}
}

func renderTreesInputWarningOnlyCase(t *testing.T, warning *struct {
	Resource manifest.NamedResource
	Subject  manifest.DiagnosticIdentity
	Related  []manifest.NamedResource
	Category string
	Message  string
	Detail   []string
	Count    int
}, root *string, unavailable *bool,
) {
	t.Helper()

	baseWarning, headWarning := (*warning), (*warning)
	baseWarning.Message, headWarning.Message = "base before failure", "head before failure"
	base := &store.SourceArtifact{
		LocalPath: (*root),
		FS:        os.DirFS(filepath.Join((*root), "missing")),
		Warnings:  []manifest.Warning{baseWarning},
	}
	head := &store.SourceArtifact{
		LocalPath: (*root),
		FS:        os.DirFS((*root)),
		Warnings:  []manifest.Warning{headWarning},
	}
	wantErr := os.ErrNotExist
	if *unavailable {
		base.FS = nil
		wantErr = manifest.ErrInput
	}
	runs, err := orchestrator.RenderTrees(t.Context(), []orchestrator.Tree{
		{
			ID:              manifest.TreeID{Cluster: ".", Side: manifest.TreeBase},
			Path:            (*root),
			Input:           base,
			BaseInput:       head,
			ExternalChanges: change.NewSet(nil),
		},
		{
			ID:              manifest.TreeID{Cluster: ".", Side: manifest.TreeHead},
			Path:            (*root),
			Input:           head,
			BaseInput:       base,
			ExternalChanges: change.NewSet(nil),
		},
	}, orchestrator.Config{Concurrency: 2, HeadOnly: true})
	assert.FatalIf(t, !errors.Is(err, wantErr), "baseline discovery error lost: %v", err)
	var warnings []manifest.Warning
	for _, run := range runs {
		assert.FatalIf(t, run.Orchestrator != nil || run.Result == nil ||
			len(
				run.Result.Manifests,
			)+len(
				run.Result.Failed,
			) != 0, "failed preparation exposed content: %+v", run)
		for wIndex := range run.Result.Warnings {
			w := run.Result.Warnings[wIndex]
			if w.Category == manifest.WarnBaselineStatusUnavailable {
				warnings = append(warnings, w)
			}
		}
	}
	assert.Diff(t, warnings, []manifest.Warning{baseWarning, headWarning})
	roots := []manifest.NamedResource{testutil.KustomizationID("", "")}
	sides, selectedErr, remainder := orchestrator.Select(runs, err, roots, nil)
	assert.FatalIf(
		t,
		!errors.Is(selectedErr, wantErr) || !errors.Is(remainder, wantErr) || len(sides) != len(runs),
		"preparation projection = %+v %v %v",
		sides,
		selectedErr,
		remainder,
	)
	var stderr bytes.Buffer
	finished := finishSelected(
		&selectedRequest{roots: roots, stderr: &stderr},
		&selectedInvocation{
			groups: []selectedGroup{{cluster: ".", sides: sides, err: selectedErr, remainder: remainder}},
		},
		&selectedPayload{},
	)
	if _, reported := errors.AsType[reportedError](finished); !reported {
		assert.NoError(t, (output.Encoder{Writer: &stderr}).Errors(finished))
	}
	for wIndex := range []manifest.Warning{baseWarning, headWarning} {
		w := []manifest.Warning{baseWarning, headWarning}[wIndex]
		assert.Equal(t, strings.Count(stderr.String(), w.Message), 1)
	}
	if !(*unavailable) {
		cause, ok := errors.AsType[*os.PathError](err)
		assert.FatalIf(
			t,
			!ok || strings.Count(stderr.String(), cause.Error()) != 1,
			"baseline discovery cause was duplicated: %v\n%s",
			err,
			stderr.String(),
		)
	}
}
