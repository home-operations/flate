package loader

import (
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"testing/fstest"

	"github.com/home-operations/flate/internal/engine/change/inputprojection"
	"github.com/home-operations/flate/internal/engine/selection"
	"github.com/home-operations/flate/internal/engine/source/cache"
	"github.com/home-operations/flate/internal/engine/store"
	"github.com/home-operations/flate/internal/engine/tree"
	"github.com/home-operations/flate/internal/testutil/assert"
	"github.com/home-operations/flate/pkg/manifest"
)

func TestParsedInputs_OverlayUsesEffectiveMaterial(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	const file = "resource.yaml"
	const url = "https://fixture.invalid/${AUTHORED}"
	const body = "apiVersion: source.toolkit.fluxcd.io/v1\nkind: GitRepository\n" +
		"metadata: {name: payload, namespace: apps}\nspec: {url: '" + url + "'}\n"
	var opens atomic.Int64
	authored := &store.SourceArtifact{
		Provider: store.ProviderInput, LocalPath: root, Content: "authored",
		FS: parsedReadFS{
			FS:    fstest.MapFS{file: {Data: []byte(body)}},
			reads: map[string]*atomic.Int64{file: &opens},
		},
	}
	builder := tree.NewBuilder(nil)
	assert.NoError(t, builder.AddFile(file, []byte(strings.ReplaceAll(body, "AUTHORED", "PROJECTED")), 0o644))
	projected, err := builder.Build()
	assert.NoError(t, err)
	overlay := authored.WithSourceView(&inputprojection.Overlay{Tree: projected, Authored: authored})
	overlay.Content = "projected"

	l := New(store.New())
	t.Cleanup(l.Close)
	l.documents.setSources(map[string]*store.SourceArtifact{root: authored})
	objects, err := l.documents.fileObjects(t.Context(), filepath.Join(root, file), manifest.ParseDocOptions{})
	assert.NoError(t, err)
	assert.FatalIf(t, len(objects) != 1 || objects[0].Obj == nil, "expected one authored object, got %v", objects)
	id := objects[0].Obj.Named()
	occurrence := selection.OccurrenceFacts{
		Declaration: selection.OccurrenceID{Source: root, File: file, Document: objects[0].outcome.Ordinal},
		Identity:    id,
		Authored:    id,
	}
	prior, err := l.projectionFile(t.Context(), authored, file, true)
	assert.NoError(t, err)
	assert.Diff(t, prior.inputs.Names, map[string]struct{}{"AUTHORED": {}})

	opens.Store(0)
	child, err := l.OccurrenceObject(t.Context(), &occurrence, authored)
	assert.NoError(t, err)
	assert.FatalIf(t, child.Obj == nil, "expected cached authored object")
	assert.Equal(t, child.Obj.Named(), id)
	assert.Equal(t, opens.Load(), int64(0))
	child, err = l.OccurrenceObject(t.Context(), &occurrence, overlay)
	assert.NoError(t, err)
	assert.FatalIf(t, child.Obj == nil, "expected reread authored object")
	assert.Equal(t, child.Obj.(*manifest.GitRepository).URL, url)
	assert.Equal(t, opens.Load(), int64(1))

	projection, err := l.projectionFile(t.Context(), overlay, file, true)
	assert.NoError(t, err)
	assert.Diff(t, projection.inputs.Names, map[string]struct{}{"PROJECTED": {}})
	again, err := l.projectionFile(t.Context(), authored, file, true)
	assert.NoError(t, err)
	assert.Equal(t, again.inputs, prior.inputs)
}

func TestInputCache_RestoreUsesAuthoredMaterial(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	const file = "object.yaml"
	owner, err := cache.New(t.Context(), t.TempDir())
	assert.NoError(t, err)
	t.Cleanup(func() { assert.NoError(t, owner.Close()) })
	persistent := owner.CanPersistRenders()
	t.Logf("render persistence enabled=%t", persistent)
	files := fstest.MapFS{
		file: {Data: []byte("apiVersion: v1\nkind: ConfigMap\nmetadata: {name: object}\n")},
	}
	planning := cachedFixtureLoader(t, root, files, owner)
	t.Cleanup(planning.Close)
	assert.NoError(t, planning.PersistInputs(t.Context()))
	prepared := planning.ParsedInputs()
	authored := planning.documents.artifacts[root]
	revised := *authored
	revised.Revision = "different-revision"
	overlay := authored.WithSourceView(&inputprojection.Overlay{Authored: &revised})
	assert.Equal(t, prepared.Matches(authored), true)
	assert.Equal(t, prepared.Matches(overlay), false)

	l := New(store.New())
	t.Cleanup(l.Close)
	l.Options.Cache, l.Options.ParsedInputs = owner, prepared
	l.documents.compact, l.documents.parsed = true, prepared
	l.documents.setSources(map[string]*store.SourceArtifact{root: overlay})
	absolute := filepath.Join(root, file)
	l.documents.restore(absolute)
	assert.Equal(t, l.documents.cache[absolute].parsed == nil, true)

	before := planning.inputCaches[root]
	if persistent {
		assert.FatalIf(t, before == nil, "expected cached inputs before revision changed")
		l.inputCaches = map[string]*inputCache{root: before}
	}
	assert.NoError(t, l.restoreCachedInputs(t.Context()))
	after := l.inputCaches[root]
	if persistent {
		assert.FatalIf(t, after == nil, "expected cached inputs after revision changed")
		assert.Equal(t, after == before, false)
		assert.Equal(t, after.input, &revised)
		assert.Equal(t, l.documents.cache[absolute].deferred != nil, true)
	} else {
		assert.Equal(t, after == nil, true)
	}
}
