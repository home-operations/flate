package store

import (
	"testing"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
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
	s.SetArtifact(id, art) // idempotent
	assert.Equal(t, count, 1)
	got := s.GetArtifact(id, ArtifactContent)
	{
		assert.ErrorIf(t, got == nil, "GetArtifact: nil")
	}
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
