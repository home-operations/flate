package store

import (
	"slices"
	"testing"
	"testing/fstest"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/pkg/manifest"
)

func testCapturePayloadAuthorityCase(t *testing.T, testCaseInput *bool,
) {
	t.Helper()

	s := New()
	a, z := ownerID("a"), ownerID("z")
	s.AddObjects(
		[]manifest.BaseManifest{
			&manifest.Kustomization{Name: a.Name, Namespace: a.Namespace},
			&manifest.Kustomization{Name: z.Name, Namespace: z.Namespace},
		},
	)
	child := emissionChild("input", "a")
	declaration := emissionChild("input", "z")
	target := child.Obj.Named()
	payload := &SourceArtifact{Key: "first", Digest: "first", FS: fstest.MapFS{"file": {Data: []byte("first")}}}
	s.RecordEmitted(
		a,
		&Emission{Children: []Child{child}, Complete: false},
		map[manifest.NamedResource]*SourceArtifact{target: payload},
	)
	s.RecordEmitted(z, &Emission{Children: []Child{declaration}, Complete: true}, nil)
	if *testCaseInput {
		s.AddObject(declaration.Obj)
	}
	var first, second ReadSet
	first.Init(s, ownerID("first-reader"), nil)
	defer first.Retire()
	assert.Equal(t, first.GetObject(target), declaration.Obj)
	assert.Equal(t, first.GetArtifact(target, ArtifactContent), Artifact(payload))
	{
		assert.FatalIf(
			t,
			!first.Valid(),
			"%s",
			"capturing independent declaration and payload invalidated a current reader",
		)
	}
	next := &SourceArtifact{Key: "next", Digest: "next", FS: fstest.MapFS{"file": {Data: []byte("next")}}}
	s.RecordEmitted(
		a,
		&Emission{Children: []Child{child}, Complete: true},
		map[manifest.NamedResource]*SourceArtifact{target: next},
	)
	assert.Diff(t, slices.Collect(s.Sweep()), []manifest.NamedResource{first.ID()})
	{
		assert.FatalIf(t, first.Valid(), "%s", "payload-only replacement did not invalidate the registered reader")
	}
	assert.Equal(t, first.GetArtifact(target, ArtifactContent), Artifact(payload))
	second.Init(s, ownerID("second-reader"), nil)
	defer second.Retire()
	assert.Equal(t, second.GetArtifact(target, ArtifactContent), Artifact(next))
	{
		assert.FatalIf(t, !second.Valid(), "%s", "new payload authority could not be captured")
	}
	s.RecordEmitted(a, &Emission{Complete: true}, nil)
	{
		assert.FatalIf(t, second.Valid(), "%s", "last payload withdrawal retained registered artifact authority")
	}
	assert.Equal(t, s.GetObject(target), declaration.Obj)
	assert.Equal(t, s.GetArtifact(target, ArtifactContent), Artifact(nil))
}
