package orchestrator_test

import (
	"errors"
	"io/fs"
	"testing"
	"testing/fstest"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/orchestrator"
	"github.com/home-operations/flate/pkg/store"
)

type failedPreparationFS struct{ fs.FS }

func (f failedPreparationFS) ReadDir(name string) ([]fs.DirEntry, error) {
	return nil, &fs.PathError{Op: "readdir", Path: name, Err: fs.ErrPermission}
}

func TestRenderTrees_BootstrapWarnings(t *testing.T) {
	t.Parallel()
	roots := [2]string{t.TempDir(), t.TempDir()}
	warning := manifest.Warning{Category: manifest.WarnBaselineStatusUnavailable, Message: "before preparation failure", Count: 1}
	inputs := [2]*store.SourceArtifact{}
	for side, root := range roots {
		inputs[side] = &store.SourceArtifact{LocalPath: root, LocalRoot: root, FS: failedPreparationFS{fstest.MapFS{}}, Warnings: []manifest.Warning{warning}}
	}
	base, head, err := orchestrator.RenderTrees(t.Context(), orchestrator.Tree{RepoRoot: roots[0]}, orchestrator.Tree{RepoRoot: roots[1]}, orchestrator.Config{BaseInput: inputs[0], Input: inputs[1], Concurrency: 2})
	if !errors.Is(err, fs.ErrPermission) {
		t.Fatalf("preparation error: %v", err)
	}
	for _, side := range []orchestrator.Rendered{base, head} {
		if side.Orchestrator != nil || side.Result == nil || len(side.Result.Manifests) != 0 {
			t.Fatalf("fatal preparation exposed content: %+v", side)
		}
		assert.Diff(t, side.Result.Warnings, []manifest.Warning{warning})
	}
}
