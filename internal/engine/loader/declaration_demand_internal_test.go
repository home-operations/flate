package loader

import (
	"errors"
	"path/filepath"
	"strconv"
	"testing"
	"testing/fstest"

	"github.com/home-operations/flate/internal/engine/change"
	"github.com/home-operations/flate/internal/engine/selection"
	"github.com/home-operations/flate/internal/engine/store"
	"github.com/home-operations/flate/internal/testutil/assert"
	"github.com/home-operations/flate/pkg/manifest"
)

func TestCompleteDeclarations_AfterRender(t *testing.T) {
	t.Parallel()
	input := &declarationReadFS{FS: fstest.MapFS{
		"input.yaml": {Data: []byte(
			"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: input}\ndata: {key: authored}\n",
		)},
	}}
	l, changes := completionFixture(t, input)
	t.Cleanup(l.Close)
	l.Options.Changes = changes
	l.configureDocuments()
	l.mountSources()
	input.opens.Store(0)
	file := filepath.Join(l.SourceRoot, "input.yaml")
	objects, err := l.documents.fileObjects(t.Context(), file, manifest.ParseDocOptions{})
	assert.NoError(t, err)
	assert.Equal(t, l.documents.cache[file].declarations == nil, true)
	assert.Equal(t, input.opens.Load(), int64(1))
	expected := manifest.Fingerprint(objects[0].Doc)
	l.documents.parsed = l.ParsedInputs()
	index, err := l.CompleteDeclarations(t.Context(), selection.DemandDeclarations(manifest.TreeHead, changes, l.Index))
	assert.Equal(t, l.documents.skipDeclarations, true)
	retained := l.documents.cache[file]
	assert.Equal(t, retained.unprepared, false)
	assert.Equal(t, &retained.objects[0] == &objects[0], true)
	assert.NoError(t, err)
	assert.Equal(
		t,
		index.Declarations[manifest.BootstrapSourceID]["input.yaml"].Objects[objects[0].Obj.Named()],
		expected,
	)
	assert.Equal(t, input.opens.Load(), int64(2))
	_, err = l.CompleteDeclarations(t.Context(), selection.DemandDeclarations(manifest.TreeHead, changes, l.Index))
	assert.NoError(t, err)
	assert.Equal(t, input.opens.Load(), int64(2))
}

func TestCompleteDeclarations_Demand(t *testing.T) {
	t.Parallel()
	for _, indexed := range []bool{false, true} {
		t.Run(strconv.FormatBool(indexed), func(t *testing.T) {
			t.Parallel()
			root := t.TempDir()
			before := &store.SourceArtifact{LocalPath: root, FS: fstest.MapFS{
				"input.yaml": {
					Data: []byte(
						"apiVersion: apps/v1\nkind: Deployment\nmetadata: {name: work}\nspec: {replicas: 1}\n",
					),
				},
			}}
			input := &declarationReadFS{FS: fstest.MapFS{
				"input.yaml": {
					Data: []byte(
						"apiVersion: apps/v1\nkind: Deployment\nmetadata: {name: work}\nspec: {replicas: 2}\n",
					),
				},
			}}
			after := &store.SourceArtifact{LocalPath: root, FS: input}
			changes, err := change.Detect(t.Context(), before, after, change.DetectConfig{})
			assert.NoError(t, err)
			st := store.New()
			st.SetArtifact(manifest.BootstrapSourceID, after)
			l := New(st)
			t.Cleanup(l.Close)
			l.SourceRoot = root
			id := manifest.NamedResource{Group: "apps", Kind: "Deployment", Name: "work"}
			if indexed {
				l.Index.Records[id] = manifest.DiscoveryRecord{File: "input.yaml", Authored: id}
			}
			input.opens.Store(0)
			demand := selection.DemandDeclarations(manifest.TreeHead, changes, l.Index)
			index, err := l.CompleteDeclarations(t.Context(), demand)
			assert.NoError(t, err)
			want := int64(0)
			if indexed {
				want = 1
			}
			assert.Equal(t, input.opens.Load(), want)
			observation := index.Declarations[manifest.BootstrapSourceID]["input.yaml"]
			assert.Equal(t, observation.Complete, indexed)
			assert.Equal(t, observation.Objects[id] != "", indexed)
			_, err = l.CompleteDeclarations(t.Context(), demand)
			assert.NoError(t, err)
			assert.Equal(t, input.opens.Load(), want)
		})
	}
}

func TestCompleteDeclarations_RequiredScalar(t *testing.T) {
	t.Parallel()
	input := &declarationReadFS{FS: fstest.MapFS{"input.yaml": {Data: []byte("ordinary repository text\n")}}}
	l, changes := completionFixture(t, input)
	input.opens.Store(0)
	index, err := l.CompleteDeclarations(t.Context(), selection.DemandDeclarations(manifest.TreeHead, changes, l.Index))
	assert.NoError(t, err)
	observation := index.Declarations[manifest.BootstrapSourceID]["input.yaml"]
	assert.Equal(t, observation.Complete, true)
	assert.Equal(t, len(observation.Objects), 0)
	objects, err := l.documents.fileObjects(
		t.Context(),
		filepath.Join(l.SourceRoot, "input.yaml"),
		manifest.ParseDocOptions{RequiredIdentity: true},
	)
	for i := range objects {
		err = errors.Join(err, objects[i].outcome.Err)
	}
	assert.Equal(t, errors.Is(err, manifest.ErrInput), true)
	assert.Equal(t, input.opens.Load(), int64(2))
}

func TestCompleteDeclarations_QualifiedWithoutBootstrap(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	var stores [2]*store.Store
	var inputs [2]*declarationReadFS
	source := &manifest.GitRepository{Name: "source", Namespace: "apps"}
	for side := range stores {
		inputs[side] = &declarationReadFS{FS: fstest.MapFS{
			"input.yaml": {
				Data: []byte(
					"apiVersion: apps/v1\nkind: Deployment\nmetadata: {name: work}\nspec: {replicas: " + strconv.Itoa(
						side+1,
					) + "}\n",
				),
			},
		}}
		stores[side] = store.New()
		stores[side].AddObject(source)
		stores[side].SetArtifact(source.Named(), &store.SourceArtifact{LocalPath: root, FS: inputs[side]})
	}
	changes, err := change.Qualify(
		t.Context(),
		detectChanges(t, nil, nil),
		change.SourcePair{Stores: stores},
		manifest.WorkScope{},
	)
	assert.NoError(t, err)
	l := New(stores[1])
	t.Cleanup(l.Close)
	l.SourceRoot = root
	id := manifest.NamedResource{Group: "apps", Kind: "Deployment", Name: "work"}
	l.Index.Records[id] = manifest.DiscoveryRecord{File: "input.yaml", Authored: id}
	inputs[1].opens.Store(0)
	index, err := l.CompleteDeclarations(t.Context(), selection.DemandDeclarations(manifest.TreeHead, changes, l.Index))
	assert.NoError(t, err)
	assert.Equal(t, inputs[1].opens.Load(), int64(1))
	observation := index.Declarations[source.Named()]["input.yaml"]
	assert.Equal(t, observation.Complete, true)
	assert.Equal(t, observation.Objects[id] != "", true)
}
