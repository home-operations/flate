package cache

import (
	"slices"
	"testing"
	"testing/fstest"

	"github.com/home-operations/flate/internal/engine/store"
	"github.com/home-operations/flate/internal/testutil/assert"
	"github.com/home-operations/flate/pkg/manifest"
)

func equalBindingPolicy(a, b *[]string) bool { return slices.Equal(*a, *b) }

func TestRenderBinding_Refresh(t *testing.T) {
	t.Parallel()
	artifact := &store.SourceArtifact{URL: "first", Key: "family", Content: "generation", FS: fstest.MapFS{}}
	owner := manifest.NamedResource{Kind: manifest.KindGitRepository, Name: "source"}
	binding := RenderBinding[[]string]{Policy: []string{"Secret"}}
	binding.Refresh("render", owner, artifact)
	prior := binding
	changed := *artifact
	changed.URL, changed.Revision = "second", "revision"
	binding.Refresh("render", owner, &changed)
	assert.Equal(t, binding.Key, prior.Key)
	assert.Equal(t, binding.Artifact.Key, "family")
	assert.Equal(t, binding.Source, owner)
	assert.Equal(t, binding.Artifact, changed.HistoryInput())
	assert.Equal(t, prior.Artifact, artifact.HistoryInput())
	assert.Equal(t, &binding.Policy[0], &prior.Policy[0])
	assert.Equal(t, binding.Fingerprint() == prior.Fingerprint(), false)
	assert.Equal(t, artifact.SameMaterial(&changed, store.ArtifactContent), true)
}

func TestRenderBinding_Equal(t *testing.T) {
	t.Parallel()
	original := RenderBinding[[]string]{
		Key: "render", Source: manifest.NamedResource{Name: "source"},
		Artifact: store.ArtifactHistory{URL: "url"}, Policy: []string{"Secret"},
	}
	for _, tc := range []struct {
		name   string
		mutate func(*RenderBinding[[]string])
		equal  bool
	}{
		{name: "equal", mutate: func(b *RenderBinding[[]string]) { b.Policy = slices.Clone(b.Policy) }, equal: true},
		{name: "render key", mutate: func(b *RenderBinding[[]string]) { b.Key = "other" }},
		{name: "source", mutate: func(b *RenderBinding[[]string]) { b.Source.Name = "other" }},
		{name: "history", mutate: func(b *RenderBinding[[]string]) { b.Artifact.URL = "other" }},
		{name: "policy", mutate: func(b *RenderBinding[[]string]) { b.Policy = []string{"ConfigMap"} }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			other := original
			tc.mutate(&other)
			assert.Equal(t, original.Equal(&other, equalBindingPolicy), tc.equal)
			assert.Equal(t, other.Equal(&original, equalBindingPolicy), tc.equal)
		})
	}
}

func BenchmarkRenderBinding_Equal(b *testing.B) {
	left := RenderBinding[[]string]{Key: "render", Policy: []string{"Secret"}}
	right := left
	b.ReportAllocs()
	for b.Loop() {
		if !left.Equal(&right, equalBindingPolicy) {
			b.Fatal("equal binding differs")
		}
	}
}

func BenchmarkRenderBinding_Refresh(b *testing.B) {
	artifact := &store.SourceArtifact{URL: "url", Key: "family", Content: "generation"}
	owner := manifest.NamedResource{Name: "source"}
	var binding RenderBinding[[]string]
	b.ReportAllocs()
	for b.Loop() {
		binding.Refresh("render", owner, artifact)
	}
}
