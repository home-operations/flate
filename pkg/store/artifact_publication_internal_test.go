package store

import (
	"context"
	"slices"
	"testing"
	"testing/fstest"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/task"
)

func TestStore_SetArtifact(t *testing.T) {
	t.Parallel()
	s := New()
	id := manifest.NamedResource{Group: manifest.SourceDomain, Kind: "GitRepository", Name: "r"}
	art := &SourceArtifact{Kind: "GitRepository", URL: "https://example", LocalPath: "/tmp/x"}

	count := 0
	s.AddListener(EventArtifactUpdated, func(_ manifest.NamedResource, _ any) {
		count++
	}, false)
	s.SetArtifact(id, art)
	s.SetArtifact(id, art)
	assert.Equal(t, count, 1)
	got := s.GetArtifact(id, ArtifactContent)
	assert.ErrorIf(t, got == nil, "GetArtifact: nil")
	t.Run("equal acquisition replaces stale reads", func(t *testing.T) {
		t.Parallel()
		st := New()
		source := &manifest.GitRepository{Name: "source"}
		credential := &manifest.ConfigMap{Name: "credential"}
		st.AddObjects([]manifest.BaseManifest{source, credential})
		var stale, valid, pending ReadSet
		stale.Init(st, source.Named(), nil)
		stale.GetObject(credential.Named())
		st.AddObject(&manifest.ConfigMap{Name: credential.Name, Data: map[string]any{"value": "next"}})
		assert.Equal(t, stale.Valid(), false)
		valid.Init(st, source.Named(), nil)
		valid.GetObject(credential.Named())
		defer valid.Retire()
		first := &SourceArtifact{Content: "generation", FS: fstest.MapFS{}, Reads: &stale}
		next := first.WithReads(&valid)
		st.SetArtifact(source.Named(), first)
		sh := st.shardFor(source.Named())
		content := sh.artifacts[source.Named()].generation
		acquisition := sh.artifacts[source.Named()].acquisitionHead.generation
		events := 0
		st.AddListener(EventArtifactUpdated, func(id manifest.NamedResource, value any) {
			assert.Equal(t, id, source.Named())
			assert.Equal(t, value, any(next))
			assert.Equal(t, st.GetArtifact(id, ArtifactContent), Artifact(next))
			events++
		}, false)
		pending.Init(st, ownerID("pending"), nil)
		defer pending.Retire()
		captured, published := make(chan struct{}), make(chan struct{})
		tasks := task.NewBounded(2)
		tasks.Go(t.Context(), "capture", func(context.Context) {
			assert.Equal(t, pending.GetArtifact(source.Named(), ArtifactContent), Artifact(nil))
			assert.Equal(t, pending.Valid(), true)
			close(captured)
			tasks.YieldSlot(func() { <-published })
		})
		tasks.Go(t.Context(), "replace", func(context.Context) {
			tasks.YieldSlot(func() { <-captured })
			st.SetArtifact(source.Named(), next)
			close(published)
		})
		tasks.BlockTillDone()
		assert.Equal(t, tasks.Failures(), 0)
		assert.Equal(t, events, 1)
		assert.Equal(t, pending.Valid(), false)
		assert.Diff(t, slices.Collect(st.Sweep()), []manifest.NamedResource{pending.ID()})
		assert.Equal(t, len(slices.Collect(st.Sweep())), 0)
		assert.Equal(t, sh.artifacts[source.Named()].generation, content)
		assert.Equal(t, sh.artifacts[source.Named()].acquisitionHead.generation > acquisition, true)
		assert.Equal(t, first.SameMaterial(next, ArtifactContent), true)
		var fresh ReadSet
		fresh.Init(st, ownerID("fresh"), nil)
		defer fresh.Retire()
		assert.Equal(t, fresh.GetArtifact(source.Named(), ArtifactContent), Artifact(next))
		assert.Equal(t, fresh.Valid(), true)
		assert.Equal(t, pending.GetArtifact(source.Named(), ArtifactContent), Artifact(nil))
	})
	t.Run("equal live content retains authority", func(t *testing.T) {
		t.Parallel()
		st := New()
		source := &manifest.GitRepository{Name: "source"}
		st.AddObject(source)
		first := &SourceArtifact{Content: "generation", FS: fstest.MapFS{}}
		st.SetArtifact(source.Named(), first)
		var live ReadSet
		live.Init(st, ownerID("live"), nil)
		defer live.Retire()
		live.GetArtifact(source.Named(), ArtifactContent)
		sh := st.shardFor(source.Named())
		content := sh.artifacts[source.Named()].generation
		events := 0
		st.AddListener(EventArtifactUpdated, func(manifest.NamedResource, any) { events++ }, false)
		next := *first
		st.SetArtifact(source.Named(), &next)
		assert.Equal(t, sh.artifacts[source.Named()].generation, content)
		assert.Equal(t, live.Valid(), true)
		assert.Equal(t, events, 0)
		assert.Equal(t, len(slices.Collect(st.Sweep())), 0)
	})
}

func TestStore_RecordEmitted_DistinctPointerDedup(t *testing.T) {
	t.Parallel()
	s := New()
	id := testutil.KustomizationID("flux-system", "apps")
	s.AddObject(&manifest.Kustomization{Name: id.Name, Namespace: id.Namespace})
	makeArt := func() *Emission {
		return &Emission{
			Complete: true,

			Manifests: testutil.DocumentOccurrences([]map[string]any{
				{
					"apiVersion": "v1",
					"kind":       "ConfigMap",
					"metadata":   map[string]any{"name": "cm-1", "namespace": "default"},
					"data":       map[string]any{"k": "v"},
				},
			}),
		}
	}

	events := 0
	s.AddListener(EventArtifactUpdated, func(_ manifest.NamedResource, _ any) { events++ }, false)

	s.RecordEmitted(id, makeArt(), nil)
	s.RecordEmitted(id, makeArt(), nil)
	s.RecordEmitted(id, makeArt(), nil)

	assert.Equal(t, events, 1)
}

// TestStore_SetArtifact_PointerIdentityShortCircuit pins Item 7's
// pointer-identity fast path: re-setting the SAME pointer skips
// reflection entirely. Source-controller refresh loops cache their
// own SourceArtifact and re-publish it on every tick; the
// short-circuit cuts that hot path from ~58 ns (reflect.DeepEqual on
// aliased pointers) to ~17 ns (the map lookup alone).
func TestStore_SetArtifact_PointerIdentityShortCircuit(t *testing.T) {
	t.Parallel()
	s := New()
	id := manifest.NamedResource{Group: manifest.SourceDomain, Kind: manifest.KindGitRepository, Name: "r"}
	art := &SourceArtifact{Kind: "GitRepository", URL: "https://example", LocalPath: "/tmp/x"}

	events := 0
	s.AddListener(EventArtifactUpdated, func(_ manifest.NamedResource, _ any) { events++ }, false)

	s.SetArtifact(id, art)
	s.SetArtifact(id, art) // same pointer
	s.SetArtifact(id, art) // same pointer

	assert.Equal(t, events, 1) // same-pointer re-set short-circuits
}

func TestStore_RecordEmitted_DifferentContent(t *testing.T) {
	t.Parallel()
	s := New()
	id := testutil.KustomizationID("flux-system", "apps")
	s.AddObject(&manifest.Kustomization{Name: id.Name, Namespace: id.Namespace})

	events := 0
	s.AddListener(EventArtifactUpdated, func(_ manifest.NamedResource, _ any) { events++ }, false)

	s.RecordEmitted(id, &Emission{Complete: true, Manifests: testutil.DocumentOccurrences([]map[string]any{
		{"apiVersion": "v1", "kind": "ConfigMap", "metadata": map[string]any{"name": "cm-1"}},
	})}, nil)
	s.RecordEmitted(id, &Emission{Complete: true, Manifests: testutil.DocumentOccurrences([]map[string]any{
		{"apiVersion": "v1", "kind": "ConfigMap", "metadata": map[string]any{"name": "cm-2"}},
	})}, nil)

	assert.Equal(t, events, 2)
}
