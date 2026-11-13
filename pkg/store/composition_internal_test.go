package store

import (
	"io/fs"
	"testing"
	"testing/fstest"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/pkg/manifest"
)

type composedStoreSource struct{ manifest.ExternalArtifact }

func (s *composedStoreSource) Composition() manifest.ArtifactComposition {
	return manifest.ArtifactComposition{Includes: []manifest.ArtifactInclude{{ToPath: "included"}}}
}

func TestReadSet_StaleCompositionCapturesAbsence(t *testing.T) {
	t.Parallel()
	st := New()
	source := &composedStoreSource{manifest.ExternalArtifact{Name: "source", Namespace: "ns"}}
	input := &manifest.ConfigMap{Name: "input", Namespace: "ns", Data: map[string]any{"value": "before"}}
	st.AddObject(source)
	st.AddObject(input)
	var support ReadSet
	support.Init(st, source.Named(), nil)
	defer support.Close()
	support.GetDeclaration(input.Named())
	st.SetArtifact(source.Named(), (&SourceArtifact{Content: "before", FS: fstest.MapFS{}}).WithReads(&support))
	st.AddObject(&manifest.ConfigMap{Name: "input", Namespace: "ns", Data: map[string]any{"value": "after"}})
	assert.Equal(t, support.Valid(), false)
	var external ReadSet
	external.Init(st, manifest.NamedResource{Kind: "Consumer", Name: "reader"}, nil)
	defer external.Close()
	assert.Equal(t, external.GetArtifact(source.Named(), ArtifactContent), Artifact(nil))
	assert.Equal(t, external.Valid(), true)
	st.SetArtifact(source.Named(), &SourceArtifact{Content: "after", FS: fstest.MapFS{}})
	assert.Equal(t, external.Valid(), false)
}

func TestStore_ComposedPayloadAuthority(t *testing.T) {
	t.Parallel()
	for _, seeded := range []bool{false, true} {
		name := "emitted"
		if seeded {
			name = "seeded"
		}
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			st := New()
			producer := &manifest.Kustomization{Name: "producer", Namespace: "ns"}
			source := &composedStoreSource{manifest.ExternalArtifact{Name: "source", Namespace: "ns"}}
			st.AddObject(producer)
			if seeded {
				st.AddObject(source)
			}
			payload := func(content string) *SourceArtifact {
				return &SourceArtifact{Content: content, FS: fstest.MapFS{"base": {Data: []byte(content)}}}
			}
			emit := func(base *SourceArtifact) {
				st.RecordEmitted(
					producer.Named(),
					&Emission{Complete: true, Children: []Child{{Obj: source}}},
					map[manifest.NamedResource]*SourceArtifact{source.Named(): base},
				)
			}
			raw := payload("first")
			emit(raw)
			assert.Equal(t, st.GetArtifact(source.Named(), ArtifactContent), Artifact(nil))
			reads := new(ReadSet)
			reads.Init(st, source.Named(), nil)
			assert.Equal(t, reads.GetArtifact(source.Named(), ArtifactContent), Artifact(raw))
			final := payload("completed").WithReads(reads)
			st.SetArtifact(source.Named(), final)
			assert.Equal(t, reads.Valid(), true)
			assert.Equal(t, reads.GetArtifact(source.Named(), ArtifactContent), Artifact(raw))
			assert.Equal(t, st.GetArtifact(source.Named(), ArtifactContent), Artifact(final))
			external := new(ReadSet)
			external.Init(st, manifest.NamedResource{Kind: "Consumer", Name: "other"}, nil)
			assert.Equal(t, external.GetArtifact(source.Named(), ArtifactContent), Artifact(final))
			replacement := payload("second")
			emit(replacement)
			assert.Equal(t, reads.Valid(), false)
			assert.Equal(t, external.Valid(), false)
			assert.Equal(t, st.GetArtifact(source.Named(), ArtifactContent), Artifact(nil))
			next := new(ReadSet)
			next.Init(st, source.Named(), nil)
			assert.Equal(t, next.GetArtifact(source.Named(), ArtifactContent), Artifact(replacement))
			st.SetArtifact(source.Named(), payload("new completed").WithReads(next))
			assert.Equal(t, next.Valid(), true)
			emit(nil)
			assert.Equal(t, next.Valid(), false)
			assert.Equal(t, st.GetArtifact(source.Named(), ArtifactContent), Artifact(nil))
			plain := &manifest.ExternalArtifact{Name: "source", Namespace: "ns"}
			st.RecordEmitted(
				producer.Named(),
				&Emission{Complete: true, Children: []Child{{Obj: plain}}},
				map[manifest.NamedResource]*SourceArtifact{plain.Named(): replacement},
			)
			if seeded {
				st.AddObject(plain)
			}
			restored := st.GetArtifact(source.Named(), ArtifactContent).(*SourceArtifact)
			data, err := fs.ReadFile(restored.FS, "base")
			assert.NoError(t, err)
			assert.Equal(t, string(data), "second")
		})
	}
}

func TestReadSet_CompositionGate(t *testing.T) {
	t.Parallel()
	st := New()
	id := manifest.NamedResource{Kind: "Input", Name: "input"}
	ordinary := manifest.DependencyRef{NamedResource: id, Artifact: true, ArtifactOnly: true}
	required := ordinary
	required.RequiresDeclaration = true
	var reads ReadSet
	reads.Init(st, manifest.NamedResource{Kind: "Source", Name: "source"}, nil)
	reads.Require(ordinary)
	reads.Require(required)
	reads.Require(required)
	count := 0
	for gate := range reads.Gates() {
		if count == 0 {
			assert.Equal(t, gate.DependencyRef, ordinary)
		} else {
			assert.Equal(t, gate.DependencyRef, required)
		}
		count++
	}
	assert.Equal(t, count, 2)
	var firstRequired ReadSet
	firstRequired.Init(st, manifest.NamedResource{Kind: "Source", Name: "required"}, nil)
	firstRequired.Require(required)
	for gate := range firstRequired.Gates() {
		assert.Equal(t, gate.DependencyRef, required)
	}
}
