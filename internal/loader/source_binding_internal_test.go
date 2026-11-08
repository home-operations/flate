package loader

import (
	"path/filepath"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"testing/fstest"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/change"
	"github.com/home-operations/flate/internal/selection"
	"github.com/home-operations/flate/internal/store"
	"github.com/home-operations/flate/pkg/manifest"
)

func TestSourceFacts_FrozenComparisonSide(t *testing.T) {
	t.Parallel()
	for side := range 2 {
		t.Run(strconv.Itoa(side), func(t *testing.T) {
			t.Parallel()
			root := t.TempDir()
			st, peer := store.New(), store.New()
			input := &store.SourceArtifact{LocalPath: root, Content: "initial", FS: fstest.MapFS{}}
			st.SetArtifact(manifest.BootstrapSourceID, input)
			peer.SetArtifact(
				manifest.BootstrapSourceID,
				&store.SourceArtifact{LocalPath: root, Content: "peer", FS: fstest.MapFS{}},
			)
			l := New(st)
			l.SourceRoot = root
			_, err := l.Load(t.Context(), root, manifest.NamedResource{}, nil)
			assert.NoError(t, err)
			assert.NoError(t, l.FinalizeIndex(t.Context(), root, true))
			assert.Equal(t, l.SourceFacts(manifest.BootstrapSourceID, nil).Context.SideKnown, false)
			stores := [2]*store.Store{st, peer}
			if side == 1 {
				stores[0], stores[1] = stores[1], stores[0]
			}
			l.Options.Changes, err = change.Qualify(
				t.Context(),
				change.NewSet([]string{"changed.yaml"}),
				stores,
				manifest.WorkScope{},
			)
			assert.NoError(t, err)
			var reads store.ReadSet
			reads.Init(st, manifest.NamedResource{}, nil)
			defer reads.Retire()
			facts := l.SourceFacts(manifest.BootstrapSourceID, &reads)
			assert.Equal(t, facts.Context.SideKnown, true)
			assert.Equal(t, facts.Context.Side, manifest.TreeSide(side))
			assert.Equal(t, facts.Paired, true)
			assert.Equal(t, facts.InitialKnown, true)
			assert.Equal(t, facts.Initial, input)
			assert.Equal(t, facts.ConfiguredInput, (*store.SourceArtifact)(nil))
			l.Options.ConfiguredInput = input
			configured := l.SourceFacts(manifest.BootstrapSourceID, nil)
			assert.Equal(t, configured.ConfiguredInput, input)
			assert.Equal(t, configured.Initial, input)
			assert.Equal(t, configured.Derived, false)
			l.Options.ConfiguredInput = nil
			unconfigured := l.SourceFacts(manifest.BootstrapSourceID, nil)
			assert.Equal(t, unconfigured.ConfiguredInput, (*store.SourceArtifact)(nil))
			assert.Equal(t, unconfigured.Initial, input)
			assert.Equal(t, unconfigured.Derived, false)
			st.SetArtifact(
				manifest.BootstrapSourceID,
				&store.SourceArtifact{LocalPath: root, Content: "replacement", FS: fstest.MapFS{}},
			)
			assert.Equal(t, reads.Valid(), false)
			replacement := l.SourceFacts(manifest.BootstrapSourceID, nil)
			assert.Equal(t, replacement.Paired, false)
			assert.Equal(t, replacement.Initial, input)
		})
	}
}

func TestSourceFacts_PerSourcePublication(t *testing.T) {
	t.Parallel()
	l := New(store.New())
	l.SourceRoot = t.TempDir()
	ids := []manifest.NamedResource{
		{Kind: manifest.KindGitRepository, Name: "first", Namespace: "apps"},
		{Kind: manifest.KindGitRepository, Name: "second", Namespace: "apps"},
	}
	for _, id := range ids {
		l.SourceFacts(id, nil)
	}
	var bindings [64]*map[manifest.NamedResource]*atomic.Pointer[admissionSource]
	for i := range l.bindings {
		bindings[i] = l.bindings[i].Load()
	}
	previous := make([]*admissionSource, len(ids))
	for i, id := range ids {
		previous[i] = l.sourceBinding(id)
	}
	l.frozen = true
	var workers sync.WaitGroup
	for _, id := range ids {
		workers.Go(func() {
			input := &store.SourceArtifact{LocalPath: l.SourceRoot, FS: fstest.MapFS{}}
			l.Store.SetArtifact(id, input)
			facts := l.SourceFacts(id, nil)
			assert.Equal(t, facts.Input, input)
			assert.Equal(t, l.SourceFacts(id, nil).Derived, false)
		})
	}
	workers.Wait()
	for i := range l.bindings {
		assert.Equal(t, l.bindings[i].Load() == bindings[i], true)
	}
	for i, id := range ids {
		assert.Equal(t, previous[i].input, (*store.SourceArtifact)(nil))
		assert.Equal(t, l.sourceBinding(id) == previous[i], false)
	}
	newSource := manifest.NamedResource{Kind: manifest.KindGitRepository, Name: "runtime", Namespace: "apps"}
	input := &store.SourceArtifact{LocalPath: l.SourceRoot, FS: fstest.MapFS{}}
	l.Store.SetArtifact(newSource, input)
	assert.Equal(t, l.SourceFacts(newSource, nil).Input, input)
	for i := range l.bindings {
		assert.Equal(t, l.bindings[i].Load() == bindings[i], true)
	}
	count := 0
	for _, bucket := range bindings {
		if bucket != nil {
			count += len(*bucket)
		}
	}
	assert.Equal(t, count, len(ids))
}

func TestSourceFacts_ChangedFileCoordinates(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name, origin, file, indexed string
		paired                      bool
	}{
		{name: "same_root", origin: "/workspace/selected", file: "input.txt", indexed: "input.txt", paired: true},
		{name: "ancestor_root", origin: "/workspace", file: "selected/input.txt", indexed: "input.txt", paired: true},
		{name: "other_root", origin: "/other", file: "input.txt", indexed: "../../other/input.txt", paired: true},
		{name: "unpaired", origin: "/workspace", file: "selected/input.txt", indexed: "selected/input.txt"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			l := New(store.New())
			l.SourceRoot = "/workspace/selected"
			toIndex, err := filepath.Rel(l.SourceRoot, tc.origin)
			assert.NoError(t, err)
			evidence := &admissionSource{
				source: manifest.BootstrapSourceID, delta: change.NewSet([]string{tc.file}),
				input:  &store.SourceArtifact{LocalPath: "/mounted", LocalRoot: tc.origin, InputAlias: true},
				origin: tc.origin, toIndex: toIndex, paired: tc.paired,
			}
			facts := l.sourceViewFacts(evidence, true)
			assert.Diff(t, facts.Files, []selection.SourceFile{{Path: tc.file, Indexed: tc.indexed}})
			again := l.sourceViewFacts(evidence, false)
			assert.Equal(t, &facts.Files[0], &again.Files[0])
			assert.Diff(t, evidence.delta.Paths(), []string{tc.file})
		})
	}
}

func TestSourceFacts_PartitionsConstruction(t *testing.T) {
	t.Parallel()
	l := New(store.New())
	l.SourceRoot = t.TempDir()
	first := manifest.NamedResource{Kind: manifest.KindGitRepository, Name: "first", Namespace: "apps"}
	l.SourceFacts(first, nil)
	bucket := l.sourceBucket(first)
	original := bucket.Load()
	var workers sync.WaitGroup
	for i := range 128 {
		id := first
		id.Name = strconv.Itoa(i)
		if l.sourceBucket(id) == bucket {
			continue
		}
		workers.Go(func() {
			facts := l.SourceFacts(id, nil)
			assert.Equal(t, facts.Source, id)
			assert.Equal(t, l.SourceFacts(id, nil).Derived, false)
		})
	}
	workers.Wait()
	assert.Equal(t, bucket.Load(), original)
	assert.Equal(t, len(*original), 1)
	for i := range 128 {
		id := first
		id.Name = strconv.Itoa(i)
		if l.sourceBucket(id) != bucket {
			assert.Equal(t, l.sourceBinding(id).source, id)
		}
	}
}
