package orchestrator

import (
	"context"
	"errors"
	"io/fs"
	"path/filepath"
	"testing"
	"testing/fstest"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/source"
	"github.com/home-operations/flate/pkg/store"
	"github.com/home-operations/flate/pkg/task"
)

type planningCancellationFS struct {
	fs.FS

	armed  bool
	cancel context.CancelFunc
}

func (f *planningCancellationFS) Open(name string) (fs.File, error) {
	if name == "flux/owner.yaml" {
		f.armed = true
	}
	if name == ".sourceignore" && f.armed && f.cancel != nil {
		f.cancel()
		f.cancel = nil
	}
	return f.FS.Open(name)
}

func TestPlanTree_CancellationDuringRound(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	files := &planningCancellationFS{cancel: cancel, FS: fstest.MapFS{
		".sourceignore": {Data: []byte("# source policy\n")},
		"flux/owner.yaml": {Data: []byte(testutil.KustomizationYAML("owner", "apps",
			"  path: ./work\n  sourceRef: {kind: OCIRepository, name: missing}\n"))},
	}}
	r := renderInvocation{cfg: &Config{Concurrency: 2}, budget: task.NewBounded(2)}
	inputs := source.NewHTTPInputs(nil, nil, r.budget)
	defer inputs.Close()
	entries, err := r.planTree(ctx, &Tree{
		Path: filepath.Join(root, "flux"), RepoRoot: root,
		Input: &store.SourceArtifact{LocalPath: root, FS: files},
	}, &inputs)
	assert.Equal(t, files.cancel == nil, true)
	assert.Equal(t, entries == nil, true)
	assert.Equal(t, errors.Is(err, context.Canceled), true)
	assert.Equal(t, err, ctx.Err())
}
