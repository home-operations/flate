package orchestrator_test

import (
	"errors"
	"io/fs"
	"testing"
	"testing/fstest"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/orchestrator"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

type failedPreparationFS struct{ fs.FS }

func (f failedPreparationFS) ReadDir(name string) ([]fs.DirEntry, error) {
	return nil, &fs.PathError{Op: "readdir", Path: name, Err: fs.ErrPermission}
}

func TestRenderTrees_BootstrapWarnings(t *testing.T) {
	t.Parallel()
	roots := [2]string{t.TempDir(), t.TempDir()}
	warning := manifest.Warning{
		Category: manifest.WarnBaselineStatusUnavailable,
		Message:  "before preparation failure",
		Count:    1,
	}
	inputs := [2]*store.SourceArtifact{}
	for side, root := range roots {
		inputs[side] = &store.SourceArtifact{
			LocalPath: root,
			LocalRoot: root,
			FS:        failedPreparationFS{fstest.MapFS{}},
			Warnings:  []manifest.Warning{warning},
		}
	}
	invocation, err := orchestrator.RenderTrees(
		t.Context(),
		[]orchestrator.Tree{
			{ID: manifest.TreeID{Cluster: ".", Side: manifest.TreeBase}, RepoRoot: roots[0], Input: inputs[0]},
			{ID: manifest.TreeID{Cluster: ".", Side: manifest.TreeHead}, RepoRoot: roots[1], Input: inputs[1]},
		},
		orchestrator.Config{Concurrency: 2},
	)
	assert.FatalIf(t, invocation == nil, "missing invocation: %v", err)
	t.Cleanup(func() { assert.NoError(t, invocation.Close()) })
	runs := invocation.Results
	base, head := runs[0], runs[1]
	assert.FatalIf(t, !errors.Is(err, fs.ErrPermission), "preparation error: %v", err)
	for _, side := range []orchestrator.Rendered{base, head} {
		assert.FatalIf(
			t,
			side.Orchestrator != nil || side.Result == nil || len(side.Result.Manifests) != 0,
			"fatal preparation exposed content: %+v",
			side,
		)
		assert.Diff(t, side.Result.Warnings, []manifest.Warning{warning})
	}
}
