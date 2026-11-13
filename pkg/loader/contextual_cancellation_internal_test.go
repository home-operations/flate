package loader

import (
	"context"
	"errors"
	"io/fs"
	"path/filepath"
	"testing"
	"testing/fstest"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

type contextualCancellationFS struct {
	fs.FS

	cancel context.CancelFunc
	target string
	armed  func() bool
}

func (f *contextualCancellationFS) Open(name string) (fs.File, error) {
	if name == f.target && f.cancel != nil && (f.armed == nil || f.armed()) {
		f.cancel()
		f.cancel = nil
	}
	return f.FS.Open(name)
}

func TestResolveAuthored_CancellationDuringRound(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	input := &contextualCancellationFS{
		FS: fstest.MapFS{
			"kustomization.yaml":       {Data: []byte("resources: []\n")},
			"round/kustomization.yaml": {Data: []byte("resources: []\n")},
		}, cancel: cancel, target: "round/kustomization.yaml",
	}
	st := store.New()
	st.SetArtifact(manifest.BootstrapSourceID, &store.SourceArtifact{LocalPath: root, FS: input})
	l := New(st)
	l.SourceRoot, l.entryPath, l.entry = root, root, entryKustomize
	l.mountSources()
	l.ensureScopes()
	l.documents.compact = true
	st.AddObject(&manifest.Kustomization{Name: "round", Namespace: "apps", Path: "./round"})
	input.armed = func() bool { return l.construction > 0 }
	stale := compactOccurrence(&manifest.Kustomization{Name: "stale", Namespace: "apps"}, manifest.TemplateProjection{})
	l.scopes.contextual = map[occurrenceContext]authoredOccurrence{{file: filepath.Join(root, "stale.yaml")}: stale}
	err := l.resolveAuthored(ctx, root)
	assert.Equal(t, input.cancel == nil, true)
	assert.Equal(t, errors.Is(err, context.Canceled), true)
	assert.Equal(t, l.construction, uint64(1))
}
