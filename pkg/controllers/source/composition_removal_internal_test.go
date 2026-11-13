package source

import (
	"errors"
	"io/fs"
	"testing"
	"testing/fstest"

	sourcev1 "github.com/fluxcd/source-controller/api/v1"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil/cachetest"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/schedule"
	"github.com/home-operations/flate/pkg/source/cache"
	"github.com/home-operations/flate/pkg/store"
	"github.com/home-operations/flate/pkg/task"
)

func TestSource_CompositionRemoval(t *testing.T) {
	t.Parallel()
	st := store.New()
	c := New(st, task.NewBounded(2), cachetest.New(t, "", cache.New))
	input := &manifest.GitRepository{Name: "input", Namespace: "ns"}
	source := &manifest.GitRepository{
		Name:      "source",
		Namespace: "ns",
		Include: []sourcev1.GitRepositoryInclude{
			{GitRepositoryRef: manifest.LocalObjectReference{Name: "input"}, FromPath: "file", ToPath: "included"},
		},
	}
	st.AddObject(source)
	st.AddObject(input)
	raw := &store.SourceArtifact{
		Kind:    manifest.KindGitRepository,
		Content: "raw",
		FS:      fstest.MapFS{"base": {Data: []byte("base")}},
	}
	c.Fetchers[source.Named().Kind] = compositionFetcher{raw}
	st.SetArtifact(
		input.Named(),
		&store.SourceArtifact{Content: "input", FS: fstest.MapFS{"file": {Data: []byte("included")}}},
	)
	run := sourcePass(t, c, source.Named(), schedule.Result{})
	artifact, _ := st.GetArtifact(source.Named(), store.ArtifactContent).(*store.SourceArtifact)
	assert.FatalIf(t, artifact == nil, "no composed source")
	data, err := fs.ReadFile(artifact.FS, "included")
	assert.NoError(t, err)
	assert.Equal(t, string(data), "included")
	next := *source
	next.Include = nil
	st.AddObject(&next)
	current := sourcePass(t, c, next.Named(), schedule.Result{Assessment: run})
	run.Retire()
	defer current.Close()
	artifact, _ = st.GetArtifact(source.Named(), store.ArtifactContent).(*store.SourceArtifact)
	assert.FatalIf(t, artifact == nil, "no raw base after removing includes")
	data, err = fs.ReadFile(artifact.FS, "base")
	assert.NoError(t, err)
	assert.Equal(t, string(data), "base")
	_, err = fs.ReadFile(artifact.FS, "included")
	assert.Equal(t, errors.Is(err, fs.ErrNotExist), true)
}
